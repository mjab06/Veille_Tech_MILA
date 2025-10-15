#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper MILA publications -> Excel (focus: rare diseases, HPC/supercomputers, quantum computing, AI in health)

Sorties toujours présentes (même si erreurs):
  - data/mila_publications.xlsx
  - data/ignored_by_robots.csv
  - data/scrape_error.log (uniquement s'il y a des erreurs)

Comportement "fail-soft": en cas d'exception, on journalise et on retourne code 0
pour que le workflow n'échoue pas (les artifacts servent au diagnostic).
"""

import os, re, time, random, urllib.parse, sys, traceback
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
PUBLICATION_PATHS = os.environ.get("MILA_PUBLICATION_PATHS", "/en/publications/,/fr/publications/").split(",")

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "data")
OUTPUT_XLSX = os.environ.get("OUTPUT_XLSX", os.path.join(OUTPUT_DIR, "mila_publications.xlsx"))
OUTPUT_IGNORED = os.path.join(OUTPUT_DIR, "ignored_by_robots.csv")
OUTPUT_ERRLOG = os.path.join(OUTPUT_DIR, "scrape_error.log")

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MILA-Publication-Research/1.1; +https://github.com/your-org/your-repo)"
}
TIMEOUT = 30
MAX_RETRIES = 3

# -----------------------------
# Filtrage thématique
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
# Session HTTP
# -----------------------------
SESSION = requests.Session()
SESSION.headers.update(DEFAULT_HEADERS)

# -----------------------------
# Robots.txt — cache & journal
# -----------------------------
IGNORED_BY_ROBOTS: List[Dict[str, str]] = []   # lignes pour CSV
ROBOTS_RULES = {"disallow": []}                # règles Disallow pour UA "*"

def polite_sleep(a=1.0, b=2.0):
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
            if resp.status_code in (401, 403, 404):
                return resp
        except requests.RequestException:
            polite_sleep(1, 2 + i)
    return None

def load_robots():
    """Charge une fois robots.txt et extrait les Disallow du bloc UA *."""
    try:
        robots_url = urllib.parse.urljoin(BASE_URL, "/robots.txt")
        r = fetch(robots_url)
        if not r or not r.ok:
            return
        disallows = []
        active = False
        for raw in r.text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("user-agent:"):
                ua = line.split(":", 1)[1].strip()
                active = (ua == "*")
            elif active and line.lower().startswith("disallow:"):
                path = line.split(":", 1)[1].strip()
                disallows.append(path)
        ROBOTS_RULES["disallow"] = disallows
    except Exception:
        pass

def robots_blocking_rule(path: str) -> Optional[str]:
    """Retourne la règle Disallow qui bloque ce path, sinon None."""
    for d in ROBOTS_RULES.get("disallow", []):
        if d and path.startswith(d):
            return d
    return None

def can_fetch_robots(url: str) -> Tuple[bool, Optional[str]]:
    """Renvoie (autorisé, règle_bloquante_ou_None)."""
    try:
        parsed = urllib.parse.urlparse(url)
        rule = robots_blocking_rule(parsed.path or "/")
        return (rule is None, rule)
    except Exception:
        return (True, None)

# -----------------------------
# Utilitaires parsing
# -----------------------------
def guess_language_from_path(path: str) -> Optional[str]:
    path = path.lower()
    if path.startswith("/fr/"):
        return "fr"
    if path.startswith("/en/") or path.startswith("/en-"):
        return "en"
    return None

def clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()

def extract_meta(soup: BeautifulSoup) -> Tuple[str, str]:
    mt = soup.find("meta", attrs={"property": "og:title"}) or soup.find("meta", attrs={"name": "title"})
    md = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
    return (clean_text(mt["content"]) if mt and mt.has_attr("content") else "",
            clean_text(md["content"]) if md and md.has_attr("content") else "")

def is_relevant(text: str) -> bool:
    t = text.lower()
    hits = sum(1 for kw in KEYWORDS if kw in t)
    return hits >= 1  # ajuste à 2 pour plus de précision

# -----------------------------
# Découverte d’URLs
# -----------------------------
def get_sitemap_urls() -> List[str]:
    urls = []
    for sm in ["/sitemap.xml", "/sitemap_index.xml"]:
        sitemap_url = urllib.parse.urljoin(BASE_URL, sm)
        r = fetch(sitemap_url)
        if not r or not r.ok or "xml" not in r.headers.get("Content-Type", ""):
            continue
        soup = BeautifulSoup(r.text, "lxml-xml")
        locs = [loc.text.strip() for loc in soup.find_all("loc")]

        # Si index de sitemaps, ouvrir chaque sous-sitemap
        if any(x.endswith(".xml") for x in locs):
            for sub in locs:
                rr = fetch(sub)
                if not rr or not rr.ok:
                    continue
                s2 = BeautifulSoup(rr.text, "lxml-xml")
                for loc in s2.find_all("loc"):
                    u = loc.text.strip()
                    urls.append(u)
        else:
            urls.extend(locs)

    # Filtrer pour ne garder que les pages liées aux publications/research
    candidates = []
    for u in urls:
        if any(p.strip("/") in u for p in ["publications", "publication", "research", "papers", "paper"]):
            candidates.append(u)
    return sorted(set(candidates))

def gather_candidate_urls() -> List[str]:
    candidates = set()

    # 1) via sitemaps
    for u in get_sitemap_urls():
        candidates.add(u)

    # 2) via pages d’index si autorisées
    for path in PUBLICATION_PATHS:
        path = path.strip()
        if not path:
            continue
        index_url = urllib.parse.urljoin(BASE_URL, path)
        allowed, rule = can_fetch_robots(index_url)
        if not allowed:
            IGNORED_BY_ROBOTS.append({
                "url": index_url,
                "reason": "robots.txt disallow",
                "matched_rule": rule or "",
                "checked_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
            })
            continue

        r = fetch(index_url)
        if not r or not r.ok:
            continue
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("#"):
                continue
            u = urllib.parse.urljoin(index_url, href)
            if any(k in u for k in ["/publications/", "/publication/", "/research/", "/papers/", "/paper/"]):
                candidates.add(u)

        # Pagination simple rel=next
        next_link = soup.find("a", attrs={"rel": "next"}) or soup.find("link", attrs={"rel": "next"})
        page_guard = 0
        while next_link and page_guard < 50:
            next_url = next_link.get("href")
            if not next_url:
                break
            next_url = urllib.parse.urljoin(index_url, next_url)
            allowed, rule = can_fetch_robots(next_url)
            if not allowed:
                IGNORED_BY_ROBOTS.append({
                    "url": next_url,
                    "reason": "robots.txt disallow",
                    "matched_rule": rule or "",
                    "checked_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
                })
                break
            rr = fetch(next_url)
            if not rr or not rr.ok:
                break
            soup = BeautifulSoup(rr.text, "lxml")
            for a in soup.find_all("a", href=True):
                u = urllib.parse.urljoin(next_url, a["href"])
                if any(k in u for k in ["/publications/", "/publication/", "/research/", "/papers/", "/paper/"]):
                    candidates.add(u)
            next_link = soup.find("a", attrs={"rel": "next"}) or soup.find("link", attrs={"rel": "next"})
            page_guard += 1
            polite_sleep()

    return sorted(candidates)

# -----------------------------
# Parsing de page publication
# -----------------------------
def parse_publication_page(url: str) -> Dict[str, str]:
    r = fetch(url)
    data = {c: "" for c in COLUMNS}
    data["url"] = url

    if not r or not r.ok:
        return data

    soup = BeautifulSoup(r.text, "lxml")

    # Titre / meta
    h1 = soup.find(["h1", "h2"])
    title = clean_text(h1.get_text()) if h1 else ""
    meta_title, meta_desc = extract_meta(soup)

    # Contenu brut
    raw_text = clean_text(soup.get_text(" "))
    data["raw_text_length"] = str(len(raw_text))
    data["page_h1"] = title
    data["page_meta_title"] = meta_title
    data["page_meta_desc"] = meta_desc

    # DOI
    m_doi = re.search(r"\b10\.\d{4,9}/[-._;()/:A-Za-z0-9]+\b", raw_text)
    data["doi"] = m_doi.group(0) if m_doi else ""

    # PDF / Code links
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text_a = a.get_text(" ").strip().lower()
        if href.lower().endswith(".pdf") or "pdf" in text_a:
            data["pdf_url"] = urllib.parse.urljoin(url, href)
        if any(x in href.lower() for x in ["github.com", "gitlab.com", "code", "source"]):
            data["code_url"] = urllib.parse.urljoin(url, href)

    # Date/Année
    m_date = re.search(r"\b(20\d{2}|19\d{2})[-/\.](0?[1-9]|1[0-2])[-/\.](0?[1-9]|[12]\d|3[01])\b", raw_text)
    if m_date:
        data["date"] = m_date.group(0)
        data["year"] = m_date.group(1)
    else:
        m_year = re.search(r"\b(20\d{2}|19\d{2})\b", raw_text)
        data["year"] = m_year.group(1) if m_year else ""

    # Auteurs (heuristique simple)
    authors = ""
    author_labels = soup.find_all(string=re.compile(r"^(Authors?|Auteurs?)\b", re.I))
    for lbl in author_labels:
        parent = lbl.parent
        if parent:
            txt = clean_text(parent.get_text(" "))
            m = re.search(r"(Authors?|Auteurs?):\s*(.+)", txt, re.I)
            if m:
                authors = m.group(2)
                break
    data["authors"] = authors

    # Tags (badges usuels)
    tags = []
    for tag in soup.select(".tag, .badge, .label, .chip"):
        tt = clean_text(tag.get_text())
        if tt:
            tags.append(tt)
    data["tags"] = ", ".join(sorted(set(tags))) if tags else ""

    # Type / langue / slug / venue
    parsed = urllib.parse.urlparse(url)
    slug = parsed.path.strip("/")
    data["slug"] = slug
    data["language"] = guess_language_from_path(parsed.path) or ""

    m_venue = re.search(r"(Published in|In:)\s*([A-Za-z0-9 \-–:]+)", raw_text, re.I)
    data["venue"] = m_venue.group(2).strip() if m_venue else ""

    # Abstract
    abstract = ""
    abs_hdr = soup.find(string=re.compile(r"^(Abstract|Résumé)\b", re.I))
    if abs_hdr and abs_hdr.parent:
        nxt = abs_hdr.parent.find_next("p")
        if nxt:
            abstract = clean_text(nxt.get_text(" "))
    if not abstract:
        for p in soup.find_all("p"):
            txt = clean_text(p.get_text(" "))
            if len(txt) > 160:
                abstract = txt
                break
    data["abstract"] = abstract

    data["title"] = title or meta_title

    # Type ultra-heuristique
    if any(x in slug for x in ["blog", "news"]):
        data["type"] = "post"
    elif any(x in slug for x in ["publication", "paper"]):
        data["type"] = "paper"
    else:
        data["type"] = ""

    return data

# -----------------------------
# Main
# -----------------------------
def _write_empty_outputs():
    # CSV robots
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
    if IGNORED_BY_ROBOTS:
        pd.DataFrame(IGNORED_BY_ROBOTS).to_csv(OUTPUT_IGNORED, index=False)
    else:
        pd.DataFrame(columns=["url", "reason", "matched_rule", "checked_at_utc"]).to_csv(OUTPUT_IGNORED, index=False)
    # Excel
    pd.DataFrame(columns=COLUMNS).to_excel(OUTPUT_XLSX, index=False)

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Charger robots.txt une fois
    load_robots()

    print(f"[i] Base URL: {BASE_URL}")
    print(f"[i] Publication paths: {PUBLICATION_PATHS}")
    print("[i] Collecte des URLs candidates…")

    urls = gather_candidate_urls()
    urls = [u for u in urls if u.startswith(BASE_URL)]
    urls = sorted(set(urls))

    print(f"[i] {len(urls)} URLs candidates trouvées.")

    rows = []
    for url in tqdm(urls, desc="Analyse des pages"):
        allowed, rule = can_fetch_robots(url)
        if not allowed:
            IGNORED_BY_ROBOTS.append({
                "url": url,
                "reason": "robots.txt disallow",
                "matched_rule": rule or "",
                "checked_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
            })
            continue

        data = parse_publication_page(url)

        hay = " ".join([
            data.get("title", ""),
            data.get("page_meta_title", ""),
            data.get("page_meta_desc", ""),
            data.get("abstract", "")
        ])

        if is_relevant(hay):
            rows.append(data)

        polite_sleep(0.5, 1.2)

    print(f"[i] {len(rows)} pages pertinentes conservées.")

    # Export journal robots (toujours créer le CSV)
    if IGNORED_BY_ROBOTS:
        pd.DataFrame(IGNORED_BY_ROBOTS).to_csv(OUTPUT_IGNORED, index=False)
    else:
        pd.DataFrame(columns=["url", "reason", "matched_rule", "checked_at_utc"]).to_csv(OUTPUT_IGNORED, index=False)
    print(f"[✓] Journal robots: {OUTPUT_IGNORED}")

    # Export Excel
    if not rows:
        df = pd.DataFrame(columns=COLUMNS)
    else:
        df = pd.DataFrame(rows)[COLUMNS]
        df.drop_duplicates(subset=["url"], inplace=True, ignore_index=True)
        if "title" in df.columns:
            df["title"] = df["title"].fillna("").str.strip()

    df.to_excel(OUTPUT_XLSX, index=False)
    print(f"[✓] Export terminé: {OUTPUT_XLSX}")

if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        # journalise l'erreur et produit des fichiers vides pour ne pas casser le pipeline
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(OUTPUT_ERRLOG, "w", encoding="utf-8") as f:
            f.write(f"[{datetime.utcnow().isoformat(timespec='seconds')}] {type(e).__name__}: {e}\n")
            f.write("\nTraceback:\n")
            f.write(traceback.format_exc())
        _write_empty_outputs()
        print(f"[!] Erreur capturée: {e}. Voir {OUTPUT_ERRLOG}")
        sys.exit(0)  # <-- IMPORTANT: ne fait pas échouer le job
