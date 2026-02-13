from __future__ import annotations

import csv
import os
import re
import time
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
import pandas as pd
from bs4 import BeautifulSoup


PROMO_PATHS = [
    "/sale", "/sales",
    "/offers", "/offer",
    "/promotions", "/promotion",
    "/clearance",
    "/deals", "/deal",
    "/weekly-ad", "/weeklyad",
    "/catalogue", "/catalog", "/brochure",
    "/leaflet",
    "/special-offers",
    "/outlet",
]

PROMO_KEYWORDS = [
    "sale", "offer", "offers", "promotion", "promotions", "clearance",
    "deals", "discount", "save", "special", "outlet", "black-friday",
    "leaflet", "catalogue", "catalog", "weekly",
]

SITEMAP_CANDIDATES = [
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap-index.xml",
]

UA = "Mozilla/5.0 (compatible; DublinDealsBot/1.0; +https://github.com/)"

def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def sleep_polite():
    base = env_float("SLEEP_BASE_SECS", 0.6)
    jitter = env_float("SLEEP_JITTER_SECS", 0.9)
    time.sleep(max(0.0, base + random.random() * jitter))

def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return "https://" + url

def domain_of(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""

def same_domain(a: str, b: str) -> bool:
    return domain_of(a) == domain_of(b)

def score_url(url: str, discovered_via: str) -> int:
    u = url.lower()
    s = 0
    for kw in PROMO_KEYWORDS:
        if kw in u:
            s += 8
    if u.endswith(".pdf"):
        s += 10  # leaflets are often PDF
    if "sitemap" in discovered_via:
        s += 3
    if "common_path" in discovered_via:
        s += 5
    if "homepage_scan" in discovered_via:
        s += 4
    return s

@dataclass
class PromoUrlRow:
    store_name: str
    category: str
    website: str
    website_domain: str
    promo_url: str
    priority_score: int
    discovered_via: str
    captured_at: str


def fetch(url: str, timeout: int) -> Optional[str]:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout, allow_redirects=True)
        if r.status_code >= 200 and r.status_code < 300:
            return r.text
        return None
    except Exception:
        return None


def extract_links_from_html(base_url: str, html: str) -> Set[str]:
    out: Set[str] = set()
    soup = BeautifulSoup(html, "lxml")
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        absu = urljoin(base_url, href)
        if absu.startswith("http://") or absu.startswith("https://"):
            out.add(absu.split("#")[0])
    return out


def sitemap_urls(site_base: str, timeout: int, max_urls: int = 4000) -> Set[str]:
    found: Set[str] = set()
    for path in SITEMAP_CANDIDATES:
        sm_url = urljoin(site_base, path)
        xml = fetch(sm_url, timeout=timeout)
        sleep_polite()
        if not xml:
            continue

        # Very lightweight parse: regex loc tags
        locs = re.findall(r"<loc>\s*(.*?)\s*</loc>", xml, flags=re.IGNORECASE)
        for loc in locs[:max_urls]:
            loc = loc.strip()
            if loc.startswith("http://") or loc.startswith("https://"):
                found.add(loc.split("#")[0])
        # If this is an index sitemap, we still just return URLs (good enough for MVP)
        if found:
            return found
    return found


def main() -> int:
    stores_path = "stores_with_websites.csv"
    if not os.path.exists(stores_path):
        print(f"[fatal] missing {stores_path}. (Run refresh_stores first.)")
        return 2

    timeout = env_int("REQUEST_TIMEOUT_SECS", 25)
    max_per_store = env_int("MAX_PROMO_URLS_PER_STORE", 25)
    max_pages = env_int("MAX_PAGES_PER_RUN", 350)

    rows: List[PromoUrlRow] = []
    seen_global: Set[str] = set()

    stores = []
    with open(stores_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            stores.append(r)

    print(f"[info] stores_with_websites={len(stores)} max_pages_per_run={max_pages}")

    processed = 0
    for i, s in enumerate(stores, start=1):
        store_name = (s.get("name") or "").strip()
        category = (s.get("category") or "").strip()
        website = normalize_url(s.get("website") or "")
        dom = domain_of(website)
        if not website or not dom:
            continue

        store_candidates: Dict[str, Tuple[int, str]] = {}

        # 1) Common promo paths
        for p in PROMO_PATHS:
            u = urljoin(website if website.endswith("/") else website + "/", p.lstrip("/"))
            store_candidates[u] = (score_url(u, "common_path"), "common_path")
        processed += 1

        # 2) Sitemap discovery
        sm = sitemap_urls(website, timeout=timeout)
        for u in sm:
            ul = u.lower()
            if any(kw in ul for kw in PROMO_KEYWORDS):
                store_candidates[u] = (score_url(u, "sitemap"), "sitemap")

        # 3) Homepage link scan
        home_html = fetch(website, timeout=timeout)
        sleep_polite()
        if home_html:
            links = extract_links_from_html(website, home_html)
            for u in links:
                ul = u.lower()
                if any(kw in ul for kw in PROMO_KEYWORDS):
                    if same_domain(u, website):
                        store_candidates[u] = (score_url(u, "homepage_scan"), "homepage_scan")

        # Rank + cap
        ranked = sorted(store_candidates.items(), key=lambda kv: kv[1][0], reverse=True)
        kept = 0
        for promo_url, (sc, via) in ranked:
            if kept >= max_per_store:
                break
            if promo_url in seen_global:
                continue
            seen_global.add(promo_url)

            rows.append(
                PromoUrlRow(
                    store_name=store_name or "(unnamed)",
                    category=category or "unknown",
                    website=website,
                    website_domain=dom,
                    promo_url=promo_url,
                    priority_score=int(sc),
                    discovered_via=via,
                    captured_at=now_iso(),
                )
            )
            kept += 1

        if i % 25 == 0:
            print(f"[progress] stores scanned={i}/{len(stores)} promo_urls_total={len(rows)}")

        if len(rows) >= max_pages:
            print(f"[cap] reached MAX_PAGES_PER_RUN={max_pages}; stopping promo discovery.")
            break

        # be polite between stores too
        sleep_polite()

    # Write promo_urls.csv
    out_csv = "promo_urls.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()) if rows else [
            "store_name","category","website","website_domain","promo_url","priority_score","discovered_via","captured_at"
        ])
        writer.writeheader()
        for r in sorted(rows, key=lambda x: x.priority_score, reverse=True):
            writer.writerow(asdict(r))
    print(f"[write] {out_csv} rows={len(rows)}")

    # Write promo_urls.xlsx (clickable links)
    df = pd.DataFrame([asdict(r) for r in rows])
    if not df.empty:
        df = df.sort_values("priority_score", ascending=False)
    out_xlsx = "promo_urls.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="promo_urls")
    print(f"[write] {out_xlsx} rows={len(df)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
