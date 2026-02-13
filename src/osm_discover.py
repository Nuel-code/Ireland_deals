from __future__ import annotations

import csv
import os
import sys
import time
import math
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd


OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter",
]


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class StoreRow:
    osm_type: str
    osm_id: str
    name: str
    category: str
    website: str
    phone: str
    opening_hours: str
    addr: str
    lat: Optional[float]
    lon: Optional[float]
    source: str
    captured_at: str


def normalize_website(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return "https://" + url


def build_addr(tags: Dict[str, Any]) -> str:
    parts = []
    for k in ["addr:housenumber", "addr:street", "addr:suburb", "addr:city", "addr:postcode"]:
        v = tags.get(k)
        if v:
            parts.append(str(v).strip())
    return ", ".join([p for p in parts if p])


def infer_category(tags: Dict[str, Any]) -> str:
    # Keep categories aligned with your app’s focus
    # home appliances, gadgets/electronics, clothes, jewellery, perfumes/beauty.
    shop = (tags.get("shop") or "").lower()
    amenity = (tags.get("amenity") or "").lower()
    if shop in {"electronics", "mobile_phone", "computer", "hifi", "radiotechnics"}:
        return "gadgets/electronics"
    if shop in {"clothes", "fashion", "shoes", "boutique"}:
        return "clothes"
    if shop in {"jewelry", "jewellery"}:
        return "jewellery"
    if shop in {"perfume", "beauty", "cosmetics", "chemist", "hairdresser"}:
        return "perfumes/beauty"
    if shop in {"appliance", "houseware", "furniture"}:
        return "home appliances"
    if amenity in {"department_store"}:
        # department stores can contain multiple categories; treat as broad
        return "gadgets/electronics"
    # default to electronics-ish to keep in scope but still review later
    return "gadgets/electronics"


def overpass_query(bbox: Tuple[float, float, float, float]) -> str:
    s, w, n, e = bbox
    # Query POIs with common relevant tags (shop + amenity)
    return f"""
[out:json][timeout:{env_int("OVERPASS_TIMEOUT", 180)}];
(
  node["shop"~"electronics|mobile_phone|computer|hifi|radiotechnics|clothes|fashion|shoes|boutique|jewelry|jewellery|perfume|beauty|cosmetics|chemist|appliance|houseware|furniture"]({s},{w},{n},{e});
  way["shop"~"electronics|mobile_phone|computer|hifi|radiotechnics|clothes|fashion|shoes|boutique|jewelry|jewellery|perfume|beauty|cosmetics|chemist|appliance|houseware|furniture"]({s},{w},{n},{e});
  relation["shop"~"electronics|mobile_phone|computer|hifi|radiotechnics|clothes|fashion|shoes|boutique|jewelry|jewellery|perfume|beauty|cosmetics|chemist|appliance|houseware|furniture"]({s},{w},{n},{e});

  node["amenity"="department_store"]({s},{w},{n},{e});
  way["amenity"="department_store"]({s},{w},{n},{e});
  relation["amenity"="department_store"]({s},{w},{n},{e});
);
out center tags;
""".strip()


def request_with_retry(urls: List[str], data: str, max_retries: int = 6, backoff_base: float = 2.0) -> Optional[Dict[str, Any]]:
    timeout = env_int("OVERPASS_TIMEOUT", 180)
    headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}

    for attempt in range(1, max_retries + 1):
        endpoint = urls[(attempt - 1) % len(urls)]
        try:
            print(f"[overpass] attempt {attempt}/{max_retries} endpoint={endpoint}")
            resp = requests.post(endpoint, data={"data": data}, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            print(f"[overpass] non-200 status={resp.status_code} body_snip={resp.text[:180]!r}")
        except Exception as e:
            print(f"[overpass] error: {e}")

        sleep_s = (backoff_base ** min(6, attempt)) + random.random()
        sleep_s = min(90.0, sleep_s)
        print(f"[overpass] backoff sleeping {sleep_s:.1f}s")
        time.sleep(sleep_s)

    return None


def element_center(el: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    if "lat" in el and "lon" in el:
        return float(el["lat"]), float(el["lon"])
    if "center" in el and isinstance(el["center"], dict):
        c = el["center"]
        if "lat" in c and "lon" in c:
            return float(c["lat"]), float(c["lon"])
    return None, None


def main() -> int:
    bbox_raw = os.getenv("DUBLIN_BBOX", "53.245, -6.385, 53.427, -6.065")
    parts = [p.strip() for p in bbox_raw.split(",")]
    if len(parts) != 4:
        print("[fatal] DUBLIN_BBOX must be 'south, west, north, east'")
        return 2

    s, w, n, e = map(float, parts)
    bbox = (s, w, n, e)

    q = overpass_query(bbox)
    max_retries = env_int("OVERPASS_MAX_RETRIES", 6)
    backoff_base = env_float("OVERPASS_BACKOFF_BASE", 2.0)

    data = request_with_retry(OVERPASS_ENDPOINTS, q, max_retries=max_retries, backoff_base=backoff_base)
    if not data:
        print("[fatal] Overpass failed across all endpoints.")
        return 1

    elements = data.get("elements", [])
    print(f"[overpass] got elements={len(elements)}")

    rows: List[StoreRow] = []
    captured = now_iso()

    seen = set()
    for idx, el in enumerate(elements, start=1):
        tags = el.get("tags", {}) or {}
        osm_type = el.get("type", "")
        osm_id = str(el.get("id", ""))
        key = f"{osm_type}:{osm_id}"
        if key in seen:
            continue
        seen.add(key)

        name = str(tags.get("name") or tags.get("brand") or "").strip()
        category = infer_category(tags)
        website = normalize_website(str(tags.get("website") or tags.get("contact:website") or "").strip())
        phone = str(tags.get("phone") or tags.get("contact:phone") or "").strip()
        opening = str(tags.get("opening_hours") or "").strip()
        addr = build_addr(tags)
        lat, lon = element_center(el)

        rows.append(
            StoreRow(
                osm_type=osm_type,
                osm_id=osm_id,
                name=name or "(unnamed)",
                category=category,
                website=website,
                phone=phone,
                opening_hours=opening,
                addr=addr,
                lat=lat,
                lon=lon,
                source="OpenStreetMap Overpass",
                captured_at=captured,
            )
        )

        if idx % 250 == 0:
            print(f"[progress] processed {idx}/{len(elements)} elements; rows={len(rows)}")

    # Write stores_dublin.csv (repo root)
    out_csv_all = "stores_dublin.csv"
    with open(out_csv_all, "w", newline="", encoding="utf-8") as f:
        wcsv = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()) if rows else [
            "osm_type","osm_id","name","category","website","phone","opening_hours","addr","lat","lon","source","captured_at"
        ])
        wcsv.writeheader()
        for r in rows:
            wcsv.writerow(asdict(r))
    print(f"[write] {out_csv_all} rows={len(rows)}")

    # Filter stores_with_websites.csv
    rows_web = [r for r in rows if r.website]
    out_csv_web = "stores_with_websites.csv"
    with open(out_csv_web, "w", newline="", encoding="utf-8") as f:
        wcsv = csv.DictWriter(f, fieldnames=list(asdict(rows_web[0]).keys()) if rows_web else [
            "osm_type","osm_id","name","category","website","phone","opening_hours","addr","lat","lon","source","captured_at"
        ])
        wcsv.writeheader()
        for r in rows_web:
            wcsv.writerow(asdict(r))
    print(f"[write] {out_csv_web} rows={len(rows_web)}")

    # Excel with clickable Website + Maps
    df = pd.DataFrame([asdict(r) for r in rows])
    if not df.empty:
        df["maps_url"] = df.apply(
            lambda x: f"https://www.google.com/maps/search/?api=1&query={x['lat']},{x['lon']}" if pd.notna(x.get("lat")) and pd.notna(x.get("lon")) else "",
            axis=1,
        )
    xlsx_path = "dublin_stores.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="stores")
    print(f"[write] {xlsx_path} rows={len(df)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
