#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper MILA publications -> Excel

Nouveautés:
- Couvre EN + FR: /en/research/publications et /fr/recherche/publications
- Pagination auto (pager "Last page N"); hard-cap configurable
- Parser principal (cartes) + Fallback "par blocs" (découpage à partir des dates)
- Mots-clés élargis + relevance_score
- Excel avec 2 onglets: "filtered" et "all"
- Toujours écrire ignored_by_robots.csv (même vide) + fail-soft error log
"""

import os, re, sys, time, random, urllib.parse, traceback
from typing import List, Dict, Optional, Tuple
from datetime import datetime

import requests
from bs4 import BeautifulSoup, NavigableString
import pandas as pd
from tqdm import tqdm

# -----------------------------
# Configuration
# -----------------------------
BASE_URL = os.environ.get("MILA_BASE_URL", "https://mila.quebec")
# EN + FR list pages
LIST_PATHS = os.environ.get(
    "MILA_PUBLICATION_PATHS",
    "/en/research/publications,/fr/recherche/publications"
).split(",")

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "data")
OUTPUT_XLSX = os.environ.get("OUTPUT_XLSX", os.path.join(OUTPUT_DIR, "mila_publications.xlsx"))
OUTPUT_IGNORED = os.path.join(OUTPUT_DIR, "ignored_by_robots.csv")
OUTPUT_ERRLOG = os.path.join(OUTPUT_DIR, "scrape_error.log")

DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MILA-Publication-Research/2.1; +https://github.com/your-org/your-repo)"}
TIMEOUT = 30
MAX_RETRIES = 3
# Sécurité runner (augmente si besoin)
MAX_PAGES_HARD_CAP = int(os.environ.get("MAX_PAGES_HARD_CAP", "60"))

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
    "asthma", "sickle cell", "cystic fibrosis", "muscular dystrophy",

    # HPC / supercomputing
    "hpc", "supercomputer", "supercomputing", "superordinateur", "gpu",
    "cuda", "nvidia", "cluster", "accelerated computing", "distributed training",

    # Quantum
    "quantum", "quantique", "qubit", "qaoa", "vqe", "annealing",

    # IA méthodes utiles
    "graph neural network", "gnn", "transformer", "large language model",
    "llm", "foundation model", "multimodal", "self-supervised", "few-shot",
    "federated learning", "differential privacy", "interpretability"
]]

RELEVANCE_MIN_HITS = int(os.environ.get("RELEVANCE_MIN_HITS", "1"))

COLUMNS = [
    "title","authors","year","date","venue","type","tags","abstract","doi","pdf_url","code_url",
    "language","url","slug","page_h1","page_meta_title","page_meta_desc","raw_text_length","relevance_score"
]

# -----------------------------
# HTTP Session
# -----------------------------
SESSION = requests.Session()
SESSION.headers.update(DEFAULT_HEADERS)

def polite_sleep(a=0.35, b=0.9):
    time.sleep(random.uniform(a, b))

def fetch(url: str) -> Optional[requests.Response]:
    for i in range(MAX_RETRIES):
        try:
            resp = SESSION.get(url, timeout=TIMEOUT)
            if resp.status_code in (429, 503):
                polite_sleep(1.5, 3.0); continue
            if resp.ok: return resp
            if resp.status_code in (401,403,404): return resp
        except requests.RequestException:
            polite_sleep(0.8, 1.8+i)
    return None

# -----------------------------
# Robots.txt — cache & journal
# -----------------------------
IGNORED_BY_ROBOTS: List[Dict[str, str]] = []
ROBOTS_RULES = {"disallow": []}

def load_robots():
    try:
        robots_url = urllib.parse.urljoin(BASE_URL, "/robots.txt")
        r = fetch(robots_url)
        if not r or not r.ok: return
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
# Helpers parsing & relevance
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

def extract_meta(soup: BeautifulSoup) -> Tuple[str,str]:
    mt = soup.find("meta", attrs={"property":"og:title"}) or soup.find("meta", attrs={"name":"title"})
    md = soup.find("meta", attrs={"name":"description"}) or soup.find("meta", attrs={"property":"og:description"})
    return (clean_text(mt["content"]) if mt and mt.has_attr("content") else "",
            clean_text(md["content"]) if md and md.has_attr("content") else "")

def relevance_score(text: str) -> int:
    t = text.lower()
    return sum(1 for kw in KEYWORDS if kw in t)

# -----------------------------
# Parsing LISTE: cartes
# -----------------------------
def detect_last_page(soup: BeautifulSoup) -> Optional[int]:
    pager_text = soup.get_text(" ")
    m = re.search(r"Last page\s+(\d{1,4})", pager_text, flags=re.I)
    if m:
        try: return int(m.group(1))
        except: return None
    # fallback: prendre le max de ?page= dans les liens
    pages = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "publications?page=" in href:
            try:
                q = urllib.parse.urlparse(href).query
                page = dict(urllib.parse.parse_qsl(q)).get("page")
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

        # Titre (h2/h3/h1 ou premier <a> significatif)
        h = box.find(["h2","h3","h1"])
        title = clean_text(h.get_text()) if h else ""
        if not title:
            a_t = box.find("a")
            if a_t: title = clean_text(a_t.get_text())
        if not title: continue

        # Auteurs: liens proches du titre ou petits <a>
        authors = []
        for a in box.find_all("a", href=True):
            t = clean_text(a.get_text())
            if not t: continue
            if 1 <= t.count(" ") <= 3 and len(t) <= 40:
                # heuristique "nom propre court"
                authors.append(t)
        authors = ", ".join(dict.fromkeys([a for a in authors if a]))

        # DOI / arXiv / PDF / code
        doi, pdf_url, code_url = "", "", ""
        for a in box.find_all("a", href=True):
            href = a["href"].strip()
            if "doi.org" in href: doi = href
            if "arxiv.org" in href and not pdf_url: pdf_url = href
            if href.lower().endswith(".pdf") and not pdf_url: pdf_url = urllib.parse.urljoin(list_url, href)
            if any(x in href.lower() for x in ["github.com","gitlab.com","code","source"]): code_url = href

        # Date & year
        m_date = DATE_RX.search(txt)
        date = m_date.group(0) if m_date else ""
        year = m_date.group(1) if m_date else (re.search(r"\b(20\d{2}|19\d{2})\b", txt).group(1) if re.search(r"\b(20\d{2}|19\d{2})\b", txt) else "")

        # Venue/type
        venue, ptype = "", ""
        lower = txt.lower()
        if "preprint" in lower or "arxiv" in lower: ptype = "preprint"
        if "published" in lower: ptype = "published"
        # petit heuristique de venue: texte après la date
        if date:
            after = txt.split(date, 1)[1]
            m_venue = re.search(r"([A-Za-z0-9][A-Za-z0-9 \-–&,:()]+)", after)
            if m_venue: venue = clean_text(m_venue.group(1))

        # Abstract court
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

        score = relevance_score(" ".join([title, abstract, venue]))
        rows.append({
            "title": title, "authors": authors, "year": year, "date": date, "venue": venue, "type": ptype,
            "tags": "", "abstract": abstract, "doi": doi, "pdf_url": pdf_url, "code_url": code_url,
            "language": lang, "url": url, "slug": slug, "page_h1": "Publications",
            "page_meta_title": "", "page_meta_desc": "", "raw_text_length": str(len(txt)),
            "relevance_score": score
        })
    return rows

# -----------------------------
# Fallback: parser "par blocs"
# -----------------------------
def parse_blocks(list_url: str, soup: BeautifulSoup) -> List[Dict[str,str]]:
    rows = []
    # Prend tout le flux textuel utile (évite header/footer)
    main = soup
    txts = [s for s in main.stripped_strings if isinstance(s, (str,))]

    # Découpe en blocs quand on voit une date YYYY-MM-DD
    blocks, cur = [], []
    for s in txts:
        if DATE_RX.search(s):
            if cur: blocks.append(cur); cur = []
        cur.append(s)
    if cur: blocks.append(cur)

    lang = guess_language_from_path(urllib.parse.urlparse(list_url).path)
    slug = urllib.parse.urlparse(list_url).path.strip("/") + (("?"+urllib.parse.urlparse(list_url).query) if urllib.parse.urlparse(list_url).query else "")
    url = list_url

    for b in blocks:
        btxt = clean_text(" ".join(b))
        if len(btxt) < 60: continue

        # titre = 1ère phrase assez longue avant la date
        m_date = DATE_RX.search(btxt)
        date = m_date.group(0) if m_date else ""
        year = m_date.group(1) if m_date else (re.search(r"\b(20\d{2}|19\d{2})\b", btxt).group(1) if re.search(r"\b(20\d{2}|19\d{2})\b", btxt) else "")

        title = ""
        if m_date:
            head = btxt[:m_date.start()]
            # coupe au précédent point/retour
            parts = re.split(r"[•\-\—–]| {2,}", head)
            candidates = [p.strip() for p in parts if len(p.strip()) >= 8]
            title = candidates[-1] if candidates else head.strip()
        else:
            title = btxt[:120]

        # venue/type
        tail = btxt[m_date.end():] if m_date else btxt
        venue = ""
        m_venue = re.search(r"([A-Za-z0-9][A-Za-z0-9 \-–&,:()]+)", tail)
        if m_venue: venue = clean_text(m_venue.group(1))

        lower = btxt.lower()
        ptype = "preprint" if ("preprint" in lower or "arxiv" in lower) else ("published" if "published" in lower else "")

        # DOI / arXiv / PDF dans les liens
        doi, pdf_url, code_url = "", "", ""
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "doi.org" in href and href not in doi: doi = href
            if "arxiv.org" in href and not pdf_url: pdf_url = href
            if href.lower().endswith(".pdf") and not pdf_url: pdf_url = urllib.parse.urljoin(list_url, href)
            if any(x in href.lower() for x in ["github]()
