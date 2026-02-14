from __future__ import annotations

import csv
import json
import os
import re
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_iso(s: str) -> datetime:
    try:
        return datetime.fromisoformat((s or "").replace("Z", "+00:00"))
    except Exception:
        return datetime.min


def norm_title(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 €£%.\-_/]", "", s)
    return s[:220]


def deterministic_id(store_domain: str, title: str, new_price: Any, source_url: str) -> str:
    base = f"{(store_domain or '').lower()}|{norm_title(title)}|{new_price}|{(source_url or '').strip()}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "" or s.lower() == "nan":
            return None
        return float(s)
    except Exception:
        return None


def to_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "" or s.lower() == "nan":
            return None
        return int(float(s))
    except Exception:
        return None


def to_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("true", "1", "yes", "y"):
        return True
    if s in ("false", "0", "no", "n"):
        return False
    return None


def write_csv(path: str, items: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "id",
        "title",
        "store_name",
        "category",
        "new_price",
        "old_price",
        "discount_percent",
        "in_store_confidence",
        "needs_review",
        "source_url",
        "addr",
        "lat",
        "lon",
        "captured_at",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for it in items:
            row = {k: it.get(k) for k in fieldnames}
            w.writerow(row)


def main() -> int:
    deals_csv = "deals.csv"
    if not os.path.exists(deals_csv):
        print("[warn] deals.csv missing; skipping feed export.")
        return 0

    with open(deals_csv, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = list(r)

    # Optional publish filter: if column exists and any row has publish=True, export only publish=True
    has_publish = "publish" in (rows[0].keys() if rows else [])
    publish_values = [to_bool(x.get("publish")) for x in rows] if has_publish else []
    should_filter_publish = has_publish and any(v is True for v in publish_values)

    latest_by_id: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        if should_filter_publish and to_bool(row.get("publish")) is not True:
            continue

        store_domain = (row.get("website_domain") or "").strip().lower()
        title = (row.get("title") or "").strip() or "(untitled)"
        source_url = (row.get("source_url") or "").strip()

        new_price = to_float(row.get("new_price"))
        old_price = to_float(row.get("old_price"))
        discount_percent = to_int(row.get("discount_percent"))

        it = {
            "id": deterministic_id(store_domain, title, new_price, source_url),
            "title": title,
            "store_name": (row.get("store_name") or "").strip(),
            "category": (row.get("category") or "").strip(),
            "new_price": new_price,
            "old_price": old_price,
            "discount_percent": discount_percent,
            "in_store_confidence": (row.get("in_store_confidence") or "LOW").strip().upper(),
            "needs_review": bool(to_bool(row.get("needs_review")) if to_bool(row.get("needs_review")) is not None else True),
            "source_url": source_url,
            "addr": (row.get("addr") or "").strip(),
            "lat": to_float(row.get("lat")),
            "lon": to_float(row.get("lon")),
            "captured_at": (row.get("captured_at") or now_iso()).strip(),
        }

        existing = latest_by_id.get(it["id"])
        if not existing:
            latest_by_id[it["id"]] = it
        else:
            if parse_iso(it.get("captured_at", "")) > parse_iso(existing.get("captured_at", "")):
                latest_by_id[it["id"]] = it

    items = list(latest_by_id.values())
    items.sort(key=lambda x: x["id"])  # stable diffs

    feed = {
        "generated_at": now_iso(),
        "count": len(items),
        "items": items,
    }

    os.makedirs("data", exist_ok=True)

    json_path = os.path.join("data", "published_deals.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)
    print(f"[write] {json_path} items={len(items)}")

    csv_path = os.path.join("data", "published_deals.csv")
    write_csv(csv_path, items)
    print(f"[write] {csv_path} rows={len(items)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
