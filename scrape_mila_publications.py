#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper MILA publications -> Excel
- CIBLE: la LISTE paginée /en/research/publications?page=N (server-rendered)
- Extrait: titre, auteurs, date, venue, type (preprint/published), doi, arxiv/pdf, abstract court, langue, URL (liste)
- Filtrage par mots-clés (rare diseases, HPC, quantum, AI santé)
- Respect basique de robots.txt (journal 'ignored_by_robots.csv')
- Mode fail-soft: génère toujours les artifacts; loggue les erreurs dans data/scrape_error.log

Réf: La page et son pager "Last page ..." existent (voir site). 
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
LIST_PATHS = os.environ.get("MILA_PUBLICATION_PATHS", "/en/research/publications").split(",")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "data")
OUTPUT_XLSX = os.environ.get("OUTPUT_XLSX", os.path.join(OUTPUT_DIR, "mila_publications.xlsx"))
OUTPUT_IGNORED = os.path.join(OUTPUT_DIR, "ignored_by_robots.csv")
OUTPUT_ERRLOG = os.path.join(OUTPUT_DIR, "scrape_error.log")

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MILA-Publication-Research/2.0; +https://github.com/your-org/your-repo)"
}
TIMEOUT = 30
MAX_RETRIES = 3
MAX_PAGES_HARD_CAP = int(os.environ.get("MAX_PAGES_HARD_CAP", "80"))  # sécurité CI: évite 600 pages d’un coup

# -----------------------------
# Thématique
# -----------------------------
KEYWORDS = [kw.lower() for kw in [
    # Rare diseases
    "rare disease","orphan disease","orphan diseases","maladies rares","maladie rare",
    "genetic disorder","inherited disorder","rare genetic","orphan drug","orphan designation",

    # HPC / supercomputers
    "supercomputer","super-computer","superordinateur","hpc","high performance computing","high-performance computing",
    "gpu cluster","compute cluster","accelerated computing","cuda","multi-gpu","distributed training",

    # Quantum computing
    "quantum","quantum computing","quantum algorithm","qubit","quantique","informatique quantique","annealing",

    # AI for health / bio
    "bioinformatics","genomics","proteomics","transcriptomics","metagenomics",
    "drug discovery","drug repurposing","molecular","protein","variant","pathogenicity",
    "clinical","medical imaging","radiology","radiomics","ehrs","healthcare","precision medicine","biomedical",
    "disease model","patient stratification","therapeutic","trial","biobank",

    # Méthodes utiles
    "graph neural network","gnn","transformer","large language model","llm","foundation model",
    "multimodal","self-supervised","few-shot","federated learning","differential privacy","interpretability",
]]

COLUMNS = [
    "title","authors","year","date","venue","type","tags","abstract","doi","pdf_url","code_url",
    "language","url","slug","page_h1","page_meta_title","page_meta_desc","raw_text_length"
]

# -----------------------------
# HTTP Session
# -----------------------------
SESSION = requests.Session()
SESSION.headers.update(DEFAULT_HEADERS)

# -----------------------------
# Robots.txt — cache & journal
# -----------------------------
IGNORED_BY_ROBOTS: List[Dict[str, str]] = []
ROBOTS_RULES = {"disallow": []}

def polite_sleep(a=0.5, b=1.1):
    time.sleep(random.uniform(a, b))

def fetch(url: str) -> Optional[requests.Response]:
    for i in range(MAX_RETRIES):
        try:
            resp = SESSION.get(url, timeout=TIMEOUT)
            if resp.status_code in (429, 503):
                polite_sleep(2, 4)
                continue
            if resp.ok:
                return resp
            if resp.status_code in (401,403,404):
                return resp
        except requests.RequestException:
            polite_sleep(1, 2 + i)
    return None

def load_robots():
    try:
        robots_url = urllib.parse.urljoin(BASE_URL, "/robots.txt")
        r = fetch(robots_url)
        if not r or not r.ok:
            return
        disallows, active = [], False
        for raw in r.text.splitlines():
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
# Helpers parsing
# -----------------------------
def guess_language_from_path(path: str) -> Optional[str]:
    path = path.lower()
    if path.startswith("/fr/"): return "fr"
    if path.startswith("/en/") or path.startswith("/en-"): return "en"
    return None

def clean_text(s: Optional[str]) -> str:
    if not s: return ""
    return re.sub(r"\s+", " ", s).strip()

def extract_meta(soup: BeautifulSoup) -> Tuple[str,str]:
    mt = soup.find("meta", attrs={"property":"og:title"}) or soup.find("meta", attrs={"name":"title"})
    md = soup.find("meta", attrs={"name":"description"}) or soup.find("meta", attrs={"property":"og:description"})
    return (clean_text(mt["content"]) if mt and mt.has_attr("content") else "",
            clean_text(md["content"]) if md and md.has_attr("content") else "")

def is_relevant(text: str) -> bool:
    t = text.lower()
    hits = sum(1 for kw in KEYWORDS if kw in t)
    return hits >= 1

# -----------------------------
# Parsing de la LISTE (carte par carte)
# -----------------------------
def detect_last_page(soup: BeautifulSoup) -> Optional[int]:
    # Cherche un libellé "Last page NNN" visible dans le pager
    pager_text = soup.get_text(" ")
    m = re.search(r"Last page\s+(\d{1,4})", pager_text, flags=re.I)
    if m:
        try:
            return int(m.group(1))
        except:
            return None
    # Fallback: chercher ?page=XYZ dans les liens
    pages = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "publications?page=" in href:
            try:
                q = urllib.parse.urlparse(href).query
                page = dict(urllib.parse.parse_qsl(q)).get("page")
                if page is not None:
                    pages.append(int(page))
            except:
                pass
    return max(pages) if pages else None

def parse_list_items(list_url: str, soup: BeautifulSoup) -> List[Dict[str,str]]:
    rows = []
    # Heuristiques robustes pour trouver les cartes:
    containers = []
    # 1) balises article
    containers.extend(soup.find_all(["article"]))
    # 2) blocs div avec classes usuelles
    containers.extend(soup.select("div.views-row, div.node--type-publication, div.view-content > div"))
    # dédupliquer
    containers = list(dict.fromkeys(containers))

    for box in containers:
        text = clean_text(box.get_text(" "))
        if len(text) < 40:  # évite les éléments de menu, brèves, etc.
            continue

        # Titre: premier h2/h3/h1 raisonnable
        h = box.find(["h2","h3","h1"])
        title = clean_text(h.get_text()) if h else ""
        if not title:
            # parfois le titre est un <a> fort
            a_title = box.find("a")
            if a_title:
                title = clean_text(a_title.get_text())

        # Auteurs: heuristique — lignes de noms propres proches du titre
        authors = []
        # liens d'auteurs cliquables ou lignes juste après le titre
        for a in box.find_all("a", href=True):
            # exclure liens génériques (pager, social, menu)
            if any(k in a["href"] for k in ["/news","/about","/research/publications", "/en/", "/fr/"]):
                continue
            # si le texte ressemble à un nom court, on le garde
            t = clean_text(a.get_text())
            if 1 <= t.count(" ") <= 3 and len(t) <= 40:
                authors.append(t)
        authors = ", ".join(dict.fromkeys([a for a in authors if a]))

        # DOI / arXiv / PDF
        doi = ""
        pdf_url = ""
        code_url = ""
        for a in box.find_all("a", href=True):
            href = a["href"].strip()
            if "doi.org" in href:
                doi = href
            if "arxiv.org" in href:
                pdf_url = href  # on place arXiv comme PDF/landing
            if href.lower().endswith(".pdf") and not pdf_url:
                pdf_url = urllib.parse.urljoin(list_url, href)
            if any(x in href.lower() for x in ["github.com","gitlab.com","code","source"]):
                code_url = href

        # Date & venue (souvent une ligne séparée)
        # ex: "2025-10-02" puis "ArXiv (preprint)" ou "IEEE ... (published)"
        m_date = re.search(r"\b(20\d{2}|19\d{2})-(0?[1-9]|1[0-2])-(0?[1-9]|[12]\d|3[01])\b", text)
        date = m_date.group(0) if m_date else ""
        year = m_date.group(1) if m_date else (re.search(r"\b(20\d{2}|19\d{2})\b", text).group(1) if re.search(r"\b(20\d{2}|19\d{2})\b", text) else "")

        # Venue: après la date, essayer de capter la ligne suivante entre la date et le lien DOI
        venue = ""
        if date:
            # coupe le texte autour de la date
            parts = text.split(date, 1)
            tail = parts[1] if len(parts) > 1 else ""
            # premier segment "propre"
            m_venue = re.search(r"([A-Za-z0-9][A-Za-z0-9 \-–&,:()]+)", tail)
            if m_venue:
                venue = clean_text(m_venue.group(1))
                # nettoyer parenthèses décoratives
                venue = re.sub(r"\s{2,}", " ", venue)

        # Type
        _type = ""
        if "preprint" in text.lower() or "arxiv" in text.lower():
            _type = "preprint"
        if "published" in text.lower():
            _type = "published"

        # Abstract court: chercher un blurb de 200–800 chars
        abstract = ""
        for p in box.find_all("p"):
            pt = clean_text(p.get_text(" "))
            if 160 <= len(pt) <= 800:
                abstract = pt
                break
        if not abstract:
            # fallback: tronquer le texte total
            abstract = (text[:600] + "…") if len(text) > 600 else text

        # Slug/URL (la liste n’a pas forcément de page-détail par article)
        parsed = urllib.parse.urlparse(list_url)
        lang = guess_language_from_path(parsed.path) or ""
        slug = (parsed.path.strip("/") + ("?" + parsed.query if parsed.query else ""))
        url = list_url  # on laisse l’URL liste; les liens externes (doi/arxiv) sont fournis

        row = {
            "title": title,
            "authors": authors,
            "year": year,
            "date": date,
            "venue": venue,
            "type": _type,
            "tags": "",
            "abstract": abstract,
            "doi": doi,
            "pdf_url": pdf_url,
            "code_url": code_url,
            "language": lang,
            "url": url,
            "slug": slug,
            "page_h1": "Publications",
            "page_meta_title": "",
            "page_meta_desc": "",
            "raw_text_length": str(len(text)),
        }

        # pertinence (titre + abstract + venue)
        hay = " ".join([row["title"], row["abstract"], row["venue"]])
        if is_relevant(hay):
            rows.append(row)

    return rows

def scrape_publications_list(start_url: str) -> List[Dict[str,str]]:
    out = []
    resp = fetch(start_url)
    if not resp or not resp.ok:
        return out
    soup = BeautifulSoup(resp.text, "lxml")

    # Meta (non critique)
    mt, md = extract_meta(soup)

    # Pagination
    last_page = detect_last_page(soup) or 1
    # Sécurité: éviter d'exploser le runner
    last_page = min(last_page, MAX_PAGES_HARD_CAP)

    # Page 1
    out.extend(parse_list_items(start_url, soup))

    # Pages suivantes
    for page in tqdm(range(2, last_page + 1), desc="Pagination"):
        url = f"{start_url}?page={page}"
        allowed, rule = can_fetch_robots(url)
        if not allowed:
            IGNORED_BY_ROBOTS.append({
                "url": url,
                "reason": "robots.txt disallow",
                "matched_rule": rule or "",
                "checked_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
            })
            continue
        r = fetch(url)
        if not r or not r.ok:
            continue
        soup = BeautifulSoup(r.text, "lxml")
        out.extend(parse_list_items(url, soup))
        polite_sleep()
    return out

# -----------------------------
# Main
# -----------------------------
def _write_empty_outputs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pd.DataFrame(columns=["url", "reason", "matched_rule", "checked_at_utc"]).to_csv(OUTPUT_IGNORED, index=False)
    pd.DataFrame(columns=COLUMNS).to_excel(OUTPUT_XLSX, index=False)

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    load_robots()

    all_rows: List[Dict[str,str]] = []
    for path in LIST_PATHS:
        path = path.strip()
        if not path:
            continue
        list_url = urllib.parse.urljoin(BASE_URL, path)
        allowed, rule = can_fetch_robots(list_url)
        if not allowed:
            IGNORED_BY_ROBOTS.append({
                "url": list_url,
                "reason": "robots.txt disallow",
                "matched_rule": rule or "",
                "checked_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
            })
            continue
        rows = scrape_publications_list(list_url)
        all_rows.extend(rows)

    # Export robots (toujours)
    if IGNORED_BY_ROBOTS:
        pd.DataFrame(IGNORED_BY_ROBOTS).to_csv(OUTPUT_IGNORED, index=False)
    else:
        pd.DataFrame(columns=["url", "reason", "matched_rule", "checked_at_utc"]).to_csv(OUTPUT_IGNORED, index=False)

    # Export Excel
    if not all_rows:
        df = pd.DataFrame(columns=COLUMNS)
    else:
        df = pd.DataFrame(all_rows)
        # garder colonnes d’intérêt
        for c in COLUMNS:
            if c not in df.columns:
                df[c] = ""
        df = df[COLUMNS]
        df.drop_duplicates(subset=["title","date","venue","doi"], inplace=True, ignore_index=True)

    df.to_excel(OUTPUT_XLSX, index=False)
    print(f"[✓] Export: {OUTPUT_XLSX}")
    print(f"[i] Rows: {len(df)} | Robots ignored: {len(IGNORED_BY_ROBOTS)}")

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
