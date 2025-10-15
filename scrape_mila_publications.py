#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper MILA publications -> Excel (sans lxml)
- Cible les listes EN + FR: /en/research/publications et /fr/recherche/publications
- Pagination auto (détectée) avec limite MAX_PAGES_HARD_CAP
- Parser principal (cartes) + fallback en découpant par dates si besoin
- Score de pertinence + 2 onglets Excel: filtered / all
- NEW: colonne 'matched_keywords' listant les mots-clés qui ont matché
- Toujours écrire ignored_by_robots.csv (même vide)
- Fail-soft: si erreur, log dans data/scrape_error.log, sortie code 0
"""

import os, re, sys, time, random, urllib.parse, traceback
from typing import List, Dict, Optional, Tuple
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

# -----------------------------
# Configuration
# -----------------------------
BASE_URL = os.environ.get("MILA_BASE_URL", "https://mila.quebec")
LIST_PATHS = os.environ.get(
    "MILA_PUBLICATION_PATHS",
    "/en/research/publications,/fr/recherche/publications"
).split(",")

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "data")
OUTPUT_XLSX = os.environ.get("OUTPUT_XLSX", os.path.join(OUTPUT_DIR, "mila_publications.xlsx"))
OUTPUT_IGNORED = os.path.join(OUTPUT_DIR, "ignored_by_robots.csv")
OUTPUT_ERRLOG = os.path.join(OUTPUT_DIR, "scrape_error.log")

DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MILA-Publication-Research/2.3; +https://github.com/your-org/your-repo)"}
TIMEOUT = 30
MAX_RETRIES = 3
MAX_PAGES_HARD_CAP = int(os.environ.get("MAX_PAGES_HARD_CAP", "60"))
RELEVANCE_MIN_HITS = int(os.environ.get("RELEVANCE_MIN_HITS", "1"))

# -----------------------------
# Thématique & scoring
# -----------------------------
KEYWORDS = [kw.lower() for kw in [
    # Rare diseases & bio/clinique
    "rare", "rare disease", "orphan", "maladies rares", "maladie rare",
    "genetic", "genetics", "genomic", "genome", "proteomic", "transcriptomic",
    "biobank", "biomedical", "clinical", "clinique", "health", "healthcare",
    "medical", "radiology", "radiomics", "ehrs", "precision medicine",
    "disease", "pathogenicity", "patient", "trial", "therapeutic", "drug",
    "drug discovery", "repurposing", "molecular", "protein", "variant",

    # HPC / supercomputing
    "hpc", "supercomputer", "supercomputing", "superordinateur", "gpu",
    "cuda", "cluster", "accelerated computing", "distributed training",

    # Quantum
    "quantum", "quantique", "qubit", "qaoa", "vqe", "annealing",

    # IA méthodes utiles
    "graph neural network", "gnn", "transformer", "large language model",
    "llm", "foundation model", "multimodal", "self-supervised", "few-shot",
    "federated learning", "differential privacy", "interpretability"
]]

COLUMNS = [
    "title","authors","year","date","venue","type","tags","abstract","doi","pdf_url","code_url",
    "language","url","slug","page_h1","page_meta_title","page_meta_desc","raw_text_length",
    "relevance_score","matched_keywords"  # NEW
]

# -----------------------------
# HTTP
# -----------------------------
SESSION = requests.Session()
SESSION.headers.update(DEFAULT_HEADERS)

def polite_sleep(a=0.35, b=0.9):
    time.sleep(random.uniform(a, b))

def fetch(url: str):
    for i in range(MAX_RETRIES):
        try:
            resp = SESSION.get(url, timeout=TIMEOUT)
            if resp.status_code in (429, 503):
                polite_sleep(1.5, 3.0); continue
            return resp
        except requests.RequestException:
            polite_sleep(0.8, 1.8+i)
    return None

# -----------------------------
# robots.txt (basique)
# -----------------------------
IGNORED_BY_ROBOTS: List[Dict[str, str]] = []
ROBOTS_RULES = {"disallow": []}

def load_robots():
    try:
        robots_url = urllib.parse.urljoin(BASE_URL, "/robots.txt")
        r = fetch(robots_url)
        if not r or not getattr(r, "ok", False): return
        disallows, active = [], False
        for raw in (r.text or "").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"): continue
            if line.lower().startswith("user-agent:"):
                ua = line.split(":",1)[1].strip()
                active = (ua == "*")
            elif active and line.lower().startswith("disallow:"):
                disallows.append(line.split(":",1)[1].strip())
        ROBOTS_RULES["disallow"] = disallows
    except Exception:
        pass

def robots_blocking_rule(path: str) -> Optional[str]:
    for d in ROBOTS_RULES.get("disallow", []):
        if d and path.startswith(d):
            return d
    return None

def can_fetch_robots(url: str) -> Tuple[bool, Optional[str]]:
    try:
        parsed = urllib.parse.urlparse(url)
        rule = robots_blocking_rule(parsed.path or "/")
        return (rule is None, rule)
    except Exception:
        return (True, None)

# -----------------------------
# Utils parsing & relevance
# -----------------------------
DATE_RX = re.compile(r"\b(20\d{2}|19\d{2})-(0?[1-9]|1[0-2])-(0?[1-9]|[12]\d|3[01])\b")

def guess_language_from_path(path: str) -> str:
    path = path.lower()
    if path.startswith("/fr/"): return "fr"
    if path.startswith("/en/") or path.startswith("/en-"): return "en"
    return ""

def clean_text(s: Optional[str]) -> str:
    if not s: return ""
    return re.sub(r"\s+", " ", s).strip()

def find_keyword_hits(text: str) -> list[str]:
    """Return a de-duplicated, sorted list of keywords that appear in text."""
    t = text.lower()
    return sorted({kw for kw in KEYWORDS if kw in t})

# -----------------------------
# Pagination & parsing
# -----------------------------
def detect_last_page(soup: BeautifulSoup) -> Optional[int]:
    # Try 'Last page NNN' or take max from '?page=' links
    text = soup.get_text(" ")
    m = re.search(r"Last page\s+(\d{1,4})", text, flags=re.I)
    if m:
        try: return int(m.group(1))
        except: return None
    pages = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "publications?page=" in href:
            try:
                page = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(href).query)).get("page")
                if page is not None: pages.append(int(page))
            except: pass
    return max(pages) if pages else None

def parse_cards(list_url: str, soup: BeautifulSoup) -> List[Dict[str,str]]:
    rows, seen = [], set()
    containers = []
    containers.extend(soup.find_all("article"))
    containers.extend(soup.select("div.views-row, div.node--type-publication, div.view-content > div"))
    containers = list(dict.fromkeys(containers))

    for box in containers:
        txt = clean_text(box.get_text(" "))
        if len(txt) < 40: continue

        h = box.find(["h2","h3","h1"])
        title = clean_text(h.get_text()) if h else ""
        if not title:
            a_t = box.find("a")
            if a_t: title = clean_text(a_t.get_text())
        if not title: continue

        authors = []
        for a in box.find_all("a", href=True):
            t = clean_text(a.get_text())
            if t and 1 <= t.count(" ") <= 3 and len(t) <= 40:
                authors.append(t)
        authors = ", ".join(dict.fromkeys([a for a in authors if a]))

        doi = ""; pdf_url = ""; code_url = ""
        for a in box.find_all("a", href=True):
            href = a["href"].strip()
            if "doi.org" in href: doi = href
            if "arxiv.org" in href and not pdf_url: pdf_url = href
            if href.lower().endswith(".pdf") and not pdf_url: pdf_url = urllib.parse.urljoin(list_url, href)
            if any(x in href.lower() for x in ["github.com","gitlab.com","code","source"]): code_url = href

        m_date = DATE_RX.search(txt)
        date = m_date.group(0) if m_date else ""
        year = m_date.group(1) if m_date else (re.search(r"\b(20\d{2}|19\d{2})\b", txt).group(1) if re.search(r"\b(20\d{2}|19\d{2})\b", txt) else "")

        venue, ptype = "", ""
        low = txt.lower()
        if "preprint" in low or "arxiv" in low: ptype = "preprint"
        if "published" in low: ptype = "published"
        if date:
            tail = txt.split(date, 1)[1]
            m_venue = re.search(r"([A-Za-z0-9][A-Za-z0-9 \-–&,:()]+)", tail)
            if m_venue: venue = clean_text(m_venue.group(1))

        abstract = ""
        for p in box.find_all("p"):
            pt = clean_text(p.get_text(" "))
            if 140 <= len(pt) <= 900:
                abstract = pt; break
        if not abstract:
            abstract = (txt[:600] + "…") if len(txt) > 600 else txt

        lang = guess_language_from_path(urllib.parse.urlparse(list_url).path)
        slug = urllib.parse.urlparse(list_url).path.strip("/") + (("?"+urllib.parse.urlparse(list_url).query) if urllib.parse.urlparse(list_url).query else "")
        url = list_url

        key = (title, date, venue, doi)
        if key in seen: continue
        seen.add(key)

        hay = " ".join([title, abstract, venue])
        hits = find_keyword_hits(hay)
        score = len(hits)

        rows.append({
            "title": title, "authors": authors, "year": year, "date": date, "venue": venue, "type": ptype,
            "tags": "", "abstract": abstract, "doi": doi, "pdf_url": pdf_url, "code_url": code_url,
            "language": lang, "url": url, "slug": slug, "page_h1": "Publications",
            "page_meta_title": "", "page_meta_desc": "", "raw_text_length": str(len(txt)),
            "relevance_score": score, "matched_keywords": ", ".join(hits)
        })
    return rows

def parse_blocks(list_url: str, soup: BeautifulSoup) -> List[Dict[str,str]]:
    rows = []
    text = clean_text(soup.get_text(" "))
    parts = re.split(r"(?=(?:^|\s)(?:19|20)\d{2}-\d{1,2}-\d{1,2})", text)  # split on dates
    lang = guess_language_from_path(urllib.parse.urlparse(list_url).path)
    slug = urllib.parse.urlparse(list_url).path.strip("/") + (("?"+urllib.parse.urlparse(list_url).query) if urllib.parse.urlparse(list_url).query else "")
    url = list_url

    for chunk in parts:
        chunk = clean_text(chunk)
        if len(chunk) < 60: continue
        m_date = DATE_RX.search(chunk)
        date = m_date.group(0) if m_date else ""
        year = m_date.group(1) if m_date else (re.search(r"\b(20\d{2}|19\d{2})\b", chunk).group(1) if re.search(r"\b(20\d{2}|19\d{2})\b", chunk) else "")

        # Title = 1st decent phrase before date
        title = ""
        if m_date:
            head = chunk[:m_date.start()]
            pieces = re.split(r"[•\-\—–]| {2,}", head)
            candidates = [p.strip() for p in pieces if len(p.strip()) >= 8]
            title = candidates[-1] if candidates else head.strip()
        else:
            title = chunk[:120]

        tail = chunk[m_date.end():] if m_date else chunk
        venue = ""
        m_venue = re.search(r"([A-Za-z0-9][A-Za-z0-9 \-–&,:()]+)", tail)
        if m_venue: venue = clean_text(m_venue.group(1))

        low = chunk.lower()
        ptype = "preprint" if ("preprint" in low or "arxiv" in low) else ("published" if "published" in low else "")

        # Simplified link scan (global)
        doi = ""; pdf_url = ""; code_url = ""
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "doi.org" in href and not doi: doi = href
            if "arxiv.org" in href and not pdf_url: pdf_url = href
            if href.lower().endswith(".pdf") and not pdf_url: pdf_url = urllib.parse.urljoin(list_url, href)
            if any(x in href.lower() for x in ["github.com","gitlab.com","code","source"]): code_url = href

        abstract = chunk if len(chunk) <= 600 else (chunk[:600] + "…")

        hay = " ".join([title, abstract, venue])
        hits = find_keyword_hits(hay)
        score = len(hits)

        rows.append({
            "title": title, "authors": "", "year": year, "date": date, "venue": venue, "type": ptype,
            "tags": "", "abstract": abstract, "doi": doi, "pdf_url": pdf_url, "code_url": code_url,
            "language": lang, "url": url, "slug": slug, "page_h1": "Publications",
            "page_meta_title": "", "page_meta_desc": "", "raw_text_length": str(len(chunk)),
            "relevance_score": score, "matched_keywords": ", ".join(hits)
        })
    return rows

def scrape_publications_list(start_url: str) -> List[Dict[str,str]]:
    out = []
    r = fetch(start_url)
    if not r or not getattr(r, "ok", False):
        return out
    soup = BeautifulSoup(r.text or "", "html.parser")

    last_page = detect_last_page(soup) or 1
    last_page = min(last_page, MAX_PAGES_HARD_CAP)

    # Page 1
    rows = parse_cards(start_url, soup)
    if len(rows) < 3:
        rows = parse_blocks(start_url, soup)
    out.extend(rows)

    # Next pages
    for page in tqdm(range(2, last_page + 1), desc=f"Paginate {start_url}"):
        url = f"{start_url}?page={page}"
        allowed, rule = can_fetch_robots(url)
        if not allowed:
            IGNORED_BY_ROBOTS.append({
                "url": url, "reason": "robots.txt disallow",
                "matched_rule": rule or "",
                "checked_at_utc": datetime.utcnow().isoformat(timespec="seconds")
            })
            continue
        rr = fetch(url)
        if not rr or not getattr(rr, "ok", False):
            continue
        s2 = BeautifulSoup(rr.text or "", "html.parser")
        rows = parse_cards(url, s2)
        if len(rows) < 3:
            rows = parse_blocks(url, s2)
        out.extend(rows)
        polite_sleep()
    return out

# -----------------------------
# Exports
# -----------------------------
def _write_empty_outputs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pd.DataFrame(columns=["url","reason","matched_rule","checked_at_utc"]).to_csv(OUTPUT_IGNORED, index=False)
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        pd.DataFrame(columns=COLUMNS).to_excel(xw, index=False, sheet_name="filtered")
        pd.DataFrame(columns=COLUMNS).to_excel(xw, index=False, sheet_name="all")

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    load_robots()

    all_rows: List[Dict[str,str]] = []
    for path in LIST_PATHS:
        path = path.strip()
        if not path: continue
        list_url = urllib.parse.urljoin(BASE_URL, path)
        allowed, rule = can_fetch_robots(list_url)
        if not allowed:
            IGNORED_BY_ROBOTS.append({
                "url": list_url, "reason": "robots.txt disallow",
                "matched_rule": rule or "",
                "checked_at_utc": datetime.utcnow().isoformat(timespec="seconds")
            })
            continue
        all_rows.extend(scrape_publications_list(list_url))

    # Export robots (always)
    if IGNORED_BY_ROBOTS:
        pd.DataFrame(IGNORED_BY_ROBOTS).to_csv(OUTPUT_IGNORED, index=False)
    else:
        pd.DataFrame(columns=["url","reason","matched_rule","checked_at_utc"]).to_csv(OUTPUT_IGNORED, index=False)

    # Excel (2 sheets)
    if not all_rows:
        filtered = pd.DataFrame(columns=COLUMNS)
        full = pd.DataFrame(columns=COLUMNS)
    else:
        full = pd.DataFrame(all_rows)
        # ensure columns & order
        for c in COLUMNS:
            if c not in full.columns: full[c] = ""
        full = full[COLUMNS]
        full.drop_duplicates(subset=["title","date","venue","doi"], inplace=True, ignore_index=True)
        # filter by relevance
        filtered = full[full["relevance_score"] >= RELEVANCE_MIN_HITS].copy()

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        filtered.to_excel(xw, index=False, sheet_name="filtered")
        full.to_excel(xw, index=False, sheet_name="all")

    print(f"[✓] Export: {OUTPUT_XLSX}")
    print(f"[i] Rows (all): {len(full)} | Rows (filtered): {len(filtered)} | Robots ignored: {len(IGNORED_BY_ROBOTS)}")

if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(OUTPUT_ERRLOG, "w", encoding="utf-8") as f:
            f.write(f"[{datetime.utcnow().isoformat(timespec='seconds')}] {type(e).__name__}: {e}\n")
            f.write("\nTraceback:\n")
            f.write(traceback.format_exc())
        _write_empty_outputs()
        print(f"[!] Error captured: {e}. See {OUTPUT_ERRLOG}")
        sys.exit(0)
