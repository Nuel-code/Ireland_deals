from __future__ import annotations

import csv
import os
import re
import time
import random
import hashlib
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
import pandas as pd
from bs4 import BeautifulSoup


UA = "Mozilla/5.0 (compatible; DublinDealsBot/1.0; +https://github.com/)"

PRICE_RE = re.compile(
    r"(?P<cur>€|eur|£|gbp)\s*(?P<amt>\d{1,5}(?:[.,]\d{2})?)",
    flags=re.IGNORECASE
)
PERCENT_RE = re.compile(r"(\d{1,3})\s*%", flags=re.IGNORECASE)

INSTORE_HIGH = [
    "in-store", "in store", "participating stores", "while stocks last",
    "selected stores", "in selected stores", "available in store",
]
INSTORE_LOW = [
    "delivery", "shipping", "checkout", "add to cart", "add to basket",
    "buy now", "order online", "cart",
]

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

def domain_of(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""

def to_float(s: str) -> Optional[float]:
    try:
        s = s.replace(",", ".")
        return float(s)
    except Exception:
        return None

def confidence_from_text(t: str) -> str:
    tl = t.lower()
    if any(k in tl for k in INSTORE_HIGH):
        return "HIGH"
    if any(k in tl for k in INSTORE_LOW):
        return "LOW"
    return "MEDIUM"

def best_title(soup: BeautifulSoup, fallback: str = "") -> str:
    for sel in ["h1", "h2"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            return el.get_text(" ", strip=True)[:160]
    if soup.title and soup.title.get_text(strip=True):
        return soup.title.get_text(" ", strip=True)[:160]
    return (fallback or "").strip()[:160] or "(untitled)"

def extract_prices(text: str) -> Tuple[Optional[float], Optional[float], Optional[int]]:
    # naive: first two prices become old/new depending on ordering;
    # if only one price, set new_price only.
    matches = list(PRICE_RE.finditer(text))
    prices: List[float] = []
    for m in matches[:6]:
        amt = m.group("amt")
        v = to_float(amt)
        if v is not None:
            prices.append(v)

    discount = None
    pm = PERCENT_RE.search(text)
    if pm:
        try:
            discount = int(pm.group(1))
        except Exception:
            discount = None

    if not prices:
        return None, None, discount
    if len(prices) == 1:
        return prices[0], None, discount

    # Heuristic: larger price is old, smaller is new
    p_sorted = sorted(prices[:2], reverse=True)
    old_p = p_sorted[0]
    new_p = p_sorted[1]
    return new_p, old_p, discount

@dataclass
class DealRow:
    store_name: str
    category: str
    website_domain: str
    source_url: str
    title: str
    new_price: Optional[float]
    old_price: Optional[float]
    discount_percent: Optional[int]
    in_store_confidence: str
    needs_review: bool
    addr: str
    lat: Optional[float]
    lon: Optional[float]
    captured_at: str
    publish: Optional[bool]  # optional column (can be empty)


def fetch(url: str, timeout: int) -> Optional[str]:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout, allow_redirects=True)
        if 200 <= r.status_code < 300 and r.text:
            return r.text
        return None
    except Exception:
        return None


def extract_deals_from_page(html: str, page_url: str, store_name: str, category: str, addr: str, lat, lon) -> List[DealRow]:
    soup = BeautifulSoup(html, "lxml")

    page_text = soup.get_text(" ", strip=True)
    conf = confidence_from_text(page_text)

    # Try to find multiple “tiles” first
    candidates: List[DealRow] = []
    tile_selectors = [
        "[class*='product']",
        "[class*='tile']",
        "[class*='item']",
        "[class*='card']",
    ]

    seen_titles = set()
    for sel in tile_selectors:
        tiles = soup.select(sel)
        if len(tiles) < 4:
            continue

        for t in tiles[:40]:
            txt = t.get_text(" ", strip=True)
            if len(txt) < 20:
                continue
            new_p, old_p, disc = extract_prices(txt)
            if new_p is None and old_p is None and disc is None:
                continue

            # title guess: first strong text chunk
            title = None
            for hsel in ["h3", "h2", "h4", "a", "span"]:
                el = t.select_one(hsel)
                if el and el.get_text(strip=True):
                    title = el.get_text(" ", strip=True)[:160]
                    break
            title = title or best_title(soup, fallback="Offer")

            key = (title.lower(), new_p, old_p, disc)
            if key in seen_titles:
                continue
            seen_titles.add(key)

            candidates.append(
                DealRow(
                    store_name=store_name,
                    category=category,
                    website_domain=domain_of(page_url),
                    source_url=page_url,
                    title=title,
                    new_price=new_p,
                    old_price=old_p,
                    discount_percent=disc,
                    in_store_confidence=conf,
                    needs_review=True,  # default true per your requirement
                    addr=addr,
                    lat=lat,
                    lon=lon,
                    captured_at=now_iso(),
                    publish=None,
                )
            )

        if candidates:
            break

    # Fallback: one deal per page
    if not candidates:
        title = best_title(soup, fallback=store_name)
        new_p, old_p, disc = extract_prices(page_text)
        candidates.append(
            DealRow(
                store_name=store_name,
                category=category,
                website_domain=domain_of(page_url),
                source_url=page_url,
                title=title,
                new_price=new_p,
                old_price=old_p,
                discount_percent=disc,
                in_store_confidence=conf,
                needs_review=True,
                addr=addr,
                lat=lat,
                lon=lon,
                captured_at=now_iso(),
                publish=None,
            )
        )

    # Cap per page
    return candidates[:12]


def main() -> int:
    promo_path = "promo_urls.csv"
    stores_path = "stores_with_websites.csv"
    if not os.path.exists(promo_path):
        print(f"[fatal] missing {promo_path}. Run promo_discover first.")
        return 2

    # Build store lookup for addr/lat/lon by website_domain (best-effort)
    store_meta: Dict[str, Dict[str, str]] = {}
    if os.path.exists(stores_path):
        with open(stores_path, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                dom = (r.get("website") or "").strip()
                if dom:
                    d = domain_of(dom)
                    store_meta[d] = {
                        "addr": (r.get("addr") or "").strip(),
                        "lat": (r.get("lat") or "").strip(),
                        "lon": (r.get("lon") or "").strip(),
                    }

    timeout = env_int("REQUEST_TIMEOUT_SECS", 25)
    max_pages = env_int("MAX_PAGES_PER_RUN", 350)

    promo_rows = []
    with open(promo_path, "r", encoding="utf-8") as f:
        promo_rows = list(csv.DictReader(f))

    # Sort by priority_score desc
    def score(r):
        try:
            return int(r.get("priority_score") or 0)
        except Exception:
            return 0

    promo_rows.sort(key=score, reverse=True)
    promo_rows = promo_rows[:max_pages]

    print(f"[info] promo_urls loaded={len(promo_rows)} timeout={timeout}s")

    deals: List[DealRow] = []
    for i, pr in enumerate(promo_rows, start=1):
        store_name = (pr.get("store_name") or "").strip() or "(unnamed)"
        category = (pr.get("category") or "").strip() or "unknown"
        url = (pr.get("promo_url") or "").strip()
        dom = (pr.get("website_domain") or domain_of(url)).strip().lower()

        meta = store_meta.get(dom, {})
        addr = meta.get("addr", "")
        lat = float(meta["lat"]) if meta.get("lat") not in (None, "", "nan") else None
        lon = float(meta["lon"]) if meta.get("lon") not in (None, "", "nan") else None

        html = fetch(url, timeout=timeout)
        sleep_polite()
        if not html:
            print(f"[warn] fetch failed {i}/{len(promo_rows)} url={url}")
            continue

        extracted = extract_deals_from_page(html, url, store_name, category, addr, lat, lon)
        deals.extend(extracted)

        if i % 20 == 0:
            print(f"[progress] pages={i}/{len(promo_rows)} deals_so_far={len(deals)}")

    out_csv = "deals.csv"
    # Always include publish column (optional usage later)
    fieldnames = [
        "store_name","category","website_domain","source_url","title",
        "new_price","old_price","discount_percent","in_store_confidence",
        "needs_review","addr","lat","lon","captured_at","publish"
    ]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for d in deals:
            row = asdict(d)
            # ensure bool serialization is consistent
            row["needs_review"] = bool(row["needs_review"])
            w.writerow(row)
    print(f"[write] {out_csv} rows={len(deals)}")

    df = pd.DataFrame([asdict(d) for d in deals])
    out_xlsx = "deals.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="deals")
    print(f"[write] {out_xlsx} rows={len(df)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
