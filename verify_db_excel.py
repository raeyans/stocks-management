#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Compare Supabase `stocks` data vs Excel files in ./files (Sheet 1),
dengan strategi: AMBIL DATA DB PER HARI penuh, lalu bandingkan dengan Excel per hari.

- Supabase client (env: SUPABASE_URL, SUPABASE_SERVICE_ROLE)
- Baca Excel by POSISI KOLOM (tanpa header):
    B issuer_code
    C issuer_name (for issuers table, tidak dipakai untuk compare)
    G stock_date
    E stock_open_price (INT)
    K stock_close_price (INT)
    I stock_high_price (INT)
    J stock_low_price  (INT)
    L stock_diff_price (INT)
    M stock_volume     (BIGINT)
    N stock_trx_value  (BIGINT)
    O stock_frequency  (INT)
    Q stock_offer      (INT)
    R stock_offer_volume (BIGINT)
    S stock_bid        (INT)
    T stock_bid_volume (BIGINT)
    U stock_listed_shares (BIGINT)
    V stock_tradeble_shares (BIGINT)
    X stock_foreign_sell (BIGINT)
    Y stock_foreign_buy  (BIGINT)

Output:
- ./reports/diff_new.csv                → ada di Excel, belum ada di DB (per key)
- ./reports/diff_changed_long.csv       → perbedaan per-field (long)
- ./reports/diff_changed_wide.csv       → satu baris per key, nilai _db vs _excel
- ./reports/diff_missing_in_excel.csv   → ada di DB (untuk tanggal yang dicakup) tapi tidak ada di Excel

Cara jalan:
  python -m pip install pandas openpyxl supabase
  export SUPABASE_URL='https://<project>.supabase.co'
  export SUPABASE_SERVICE_ROLE='<service-role-key>'
  python compare_db_vs_excel.py
"""

import os
import re
import logging
import numpy as np
import pandas as pd

from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv

# --------------------------- Konfigurasi ---------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "files"
REPORT_DIR = SCRIPT_DIR / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv()

# Posisi kolom (0-based: A=0, B=1, ...)
COL = {
    "issuer_code": 1,            # B
    "issuer_name": 2,            # C
    "stock_date": 6,             # G
    "stock_open_price": 4,       # E (INT)
    "stock_close_price": 10,     # K (INT)
    "stock_high_price": 8,       # I (INT)
    "stock_low_price": 9,        # J (INT)
    "stock_diff_price": 11,      # L (INT)
    "stock_volume": 12,          # M (BIGINT)
    "stock_trx_value": 13,       # N (BIGINT)
    "stock_frequency": 14,       # O (INT)
    "stock_offer": 16,           # Q (INT)
    "stock_offer_volume": 17,    # R (BIGINT)
    "stock_bid": 18,             # S (INT)
    "stock_bid_volume": 19,      # T (BIGINT)
    "stock_listed_shares": 20,   # U (BIGINT)
    "stock_tradeble_shares": 21, # V (BIGINT)
    "stock_foreign_sell": 23,    # X (BIGINT)
    "stock_foreign_buy": 24,     # Y (BIGINT)
}

FIELDS = [
    "stock_open_price","stock_close_price","stock_high_price","stock_low_price",
    "stock_diff_price","stock_volume","stock_trx_value","stock_frequency",
    "stock_offer","stock_offer_volume","stock_bid","stock_bid_volume",
    "stock_listed_shares","stock_tradeble_shares","stock_foreign_sell","stock_foreign_buy",
]

MONTH_REPLACEMENTS = {"Agt": "Aug", "Okt": "Oct", "Des": "Dec", "Mei": "May"}

# --------------------------- Helper Tanggal & Angka ---------------------------

def normalize_month(date_str: str) -> str:
    for indo, eng in MONTH_REPLACEMENTS.items():
        if indo in date_str:
            date_str = date_str.replace(indo, eng)
    return date_str

def parse_excel_date(date_str: str) -> Optional[str]:
    try:
        normalized = normalize_month(date_str.strip())
        return datetime.strptime(normalized, "%d %b %Y").date().isoformat()
    except Exception:
        return None

def coerce_to_iso_date(value: Any) -> Optional[str]:
    if isinstance(value, (datetime, date)):
        d = value if isinstance(value, date) and not isinstance(value, datetime) else value.date()
        return d.isoformat()
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        iso = parse_excel_date(s)
        if iso:
            return iso
        if s.isdigit() and len(s) in (10, 13):
            unit = "ms" if len(s) == 13 else "s"
            return pd.to_datetime(int(s), unit=unit, utc=True).date().isoformat()
        try:
            d = pd.to_datetime(s, errors="raise", dayfirst=True)
            return d.date().isoformat()
        except Exception:
            return None
    if isinstance(value, (int, float, np.integer, np.floating)) and not pd.isna(value):
        n = float(value)
        if n > 1e12:  # epoch ms
            return pd.to_datetime(int(n), unit="ms", utc=True).date().isoformat()
        if n > 1e9:   # epoch s
            return pd.to_datetime(int(n), unit="s", utc=True).date().isoformat()
        # Excel serial
        return pd.to_datetime(n, unit="d", origin="1899-12-30").date().isoformat()
    return None

def to_int_or_none(v) -> Optional[int]:
    if v is None:
        return None
    try:
        if isinstance(v, float) and pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, (int, np.integer)):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        s = re.sub(r"[^\d-]", "", s)  # hanya angka & minus
        if s in ("", "-"):
            return None
        try:
            return int(s)
        except Exception:
            return None
    try:
        return int(v)
    except Exception:
        return None

# --------------------------- Baca Excel ---------------------------

def ensure_col_bounds(df: pd.DataFrame) -> bool:
    max_needed = max(COL.values())
    return df.shape[1] > max_needed

def read_excel_records(path: Path) -> List[Dict[str, Any]]:
    try:
        df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    except Exception as e:
        logging.error("Gagal membaca %s: %s", path.name, e)
        return []
    if not ensure_col_bounds(df):
        logging.error("%s: sheet kurang kolom hingga Y.", path.name)
        return []

    recs: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        issuer_code = str(row.iloc[COL["issuer_code"]]).strip()
        date_iso = coerce_to_iso_date(row.iloc[COL["stock_date"]])
        if not issuer_code or not date_iso:
            continue

        recs.append({
            "issuer_code": issuer_code,
            "stock_date": date_iso,
            "stock_open_price": to_int_or_none(row.iloc[COL["stock_open_price"]]),
            "stock_close_price": to_int_or_none(row.iloc[COL["stock_close_price"]]),
            "stock_high_price": to_int_or_none(row.iloc[COL["stock_high_price"]]),
            "stock_low_price": to_int_or_none(row.iloc[COL["stock_low_price"]]),
            "stock_diff_price": to_int_or_none(row.iloc[COL["stock_diff_price"]]),
            "stock_volume": to_int_or_none(row.iloc[COL["stock_volume"]]),
            "stock_trx_value": to_int_or_none(row.iloc[COL["stock_trx_value"]]),
            "stock_frequency": to_int_or_none(row.iloc[COL["stock_frequency"]]),
            "stock_offer": to_int_or_none(row.iloc[COL["stock_offer"]]),
            "stock_offer_volume": to_int_or_none(row.iloc[COL["stock_offer_volume"]]),
            "stock_bid": to_int_or_none(row.iloc[COL["stock_bid"]]),
            "stock_bid_volume": to_int_or_none(row.iloc[COL["stock_bid_volume"]]),
            "stock_listed_shares": to_int_or_none(row.iloc[COL["stock_listed_shares"]]),
            "stock_tradeble_shares": to_int_or_none(row.iloc[COL["stock_tradeble_shares"]]),
            "stock_foreign_sell": to_int_or_none(row.iloc[COL["stock_foreign_sell"]]),
            "stock_foreign_buy": to_int_or_none(row.iloc[COL["stock_foreign_buy"]]),
        })
    return recs

def load_all_excels_grouped_by_date() -> Dict[str, List[Dict[str, Any]]]:
    """
    Baca per FILE. Tiap file diasumsikan hanya memuat 1 tanggal (1 hari).
    - Ambil semua baris via read_excel_records(p)
    - Tanggal file = stock_date dari baris valid pertama
    - (Opsional) jika terdeteksi >1 tanggal, kasih warning lalu tetap pakai tanggal pertama
    """
    files = sorted(
        p for p in (DATA_DIR.iterdir() if DATA_DIR.exists() else [])
        if p.suffix.lower() == ".xlsx" and not p.name.startswith("~$")
    )

    by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for p in files:
        rows = read_excel_records(p)
        logging.info("Parsed %-40s -> %d rows", p.name, len(rows))
        if not rows:
            continue

        # Tanggal file = tanggal dari baris pertama yang valid
        day = rows[0]["stock_date"]

        # Validasi ringan: jika ada >1 tanggal di file, beri warning
        unique_days = {r["stock_date"] for r in rows}
        if len(unique_days) > 1:
            logging.warning(
                "%s: ditemukan %d tanggal berbeda (%s). Karena 1 file = 1 hari, pakai tanggal pertama: %s",
                p.name, len(unique_days), sorted(unique_days)[:5], day
            )

        by_date[day].extend(rows)

    return by_date

# --------------------------- Supabase ---------------------------

def create_supabase_client() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE")
    if not url or not key:
        raise RuntimeError("Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE in environment.")
    return create_client(url, key)

def fetch_db_rows_for_date(client: Client, stock_date: str, page_size: int = 10000) -> List[Dict[str, Any]]:
    """
    Ambil SEMUA baris stocks untuk 1 tanggal (stock_date) dengan paginasi.
    Tidak membatasi issuer_code; DB mengembalikan semua emiten pada hari itu.
    """
    rows: List[Dict[str, Any]] = []
    start = 0
    while True:
        # PostgREST range adalah inklusif (start..end)
        end = start + page_size - 1
        resp = (
            client.table("stocks")
            .select(",".join(["issuer_code","stock_date"] + FIELDS))
            .eq("stock_date", stock_date)
            .order("issuer_code", desc=False)   # stabil untuk pagination
            .range(start, end)
            .execute()
        )
        data = getattr(resp, "data", None) or []
        rows.extend(data)
        if len(data) < page_size:
            break
        start += page_size
    return rows

# --------------------------- Perbandingan ---------------------------

def compare_day(excel_rows: List[Dict[str, Any]], db_rows: List[Dict[str, Any]]):
    """
    Bandingkan untuk 1 TANGGAL.
    excel_rows: list baris excel untuk tanggal tsb
    db_rows   : hasil fetch DB untuk tanggal tsb
    """
    excel_by_code = {r["issuer_code"]: r for r in excel_rows}
    db_by_code = {r["issuer_code"]: r for r in db_rows}

    excel_codes = set(excel_by_code.keys())
    db_codes = set(db_by_code.keys())

    new_codes = sorted(list(excel_codes - db_codes))
    common_codes = sorted(list(excel_codes & db_codes))
    missing_codes = sorted(list(db_codes - excel_codes))

    new_rows = [excel_by_code[c] for c in new_codes]

    changed_long = []
    changed_wide = []
    for code in common_codes:
        ex = excel_by_code[code]
        db = db_by_code[code]
        row_changed = False
        wide_entry = {"issuer_code": code, "stock_date": ex["stock_date"]}
        for f in FIELDS:
            db_val = db.get(f)
            ex_val = ex.get(f)
            if pd.isna(db_val) if db_val is not None else False:
                db_val = None
            if pd.isna(ex_val) if ex_val is not None else False:
                ex_val = None
            wide_entry[f + "_db"] = db_val
            wide_entry[f + "_excel"] = ex_val
            if db_val != ex_val:
                row_changed = True
                changed_long.append({
                    "issuer_code": code,
                    "stock_date": ex["stock_date"],
                    "field": f,
                    "db_value": db_val,
                    "excel_value": ex_val
                })
        if row_changed:
            changed_wide.append(wide_entry)

    missing_rows = []
    for code in missing_codes:
        r = db_by_code[code]
        entry = {"issuer_code": code, "stock_date": r["stock_date"]}
        for f in FIELDS:
            entry[f] = r.get(f)
        missing_rows.append(entry)

    return new_rows, changed_long, changed_wide, missing_rows

# --------------------------- Main ---------------------------

def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # 1) Baca Excel → kelompokkan per tanggal
    try:
        excel_by_date = load_all_excels_grouped_by_date()
    except Exception as e:
        logging.error("Gagal membaca Excel: %s", e)
        return 1

    if not excel_by_date:
        logging.error("Tidak ada baris valid dari Excel.")
        return 1

    # 2) Siapkan Supabase client
    try:
        client = create_supabase_client()
    except Exception as e:
        logging.error("Gagal membuat Supabase client: %s", e)
        return 2

    # 3) Untuk setiap tanggal: ambil DB per-hari, bandingkan per-hari
    all_new, all_changed_long, all_changed_wide, all_missing = [], [], [], []
    all_keys = set()
    db_keys_seen = set()

    for day in sorted(excel_by_date.keys()):
        excel_rows = excel_by_date[day]
        # simpan key Excel untuk summary
        for r in excel_rows:
            all_keys.add((r["issuer_code"], r["stock_date"]))

        try:
            db_rows = fetch_db_rows_for_date(client, day)
        except Exception as e:
            logging.error("Gagal fetch DB untuk tanggal %s: %s", day, e)
            continue

        for r in db_rows:
            db_keys_seen.add((r["issuer_code"], r["stock_date"]))

        new_rows, changed_long, changed_wide, missing_rows = compare_day(excel_rows, db_rows)
        all_new.extend(new_rows)
        all_changed_long.extend(changed_long)
        all_changed_wide.extend(changed_wide)
        all_missing.extend(missing_rows)

        logging.info("Tanggal %s → excel:%d db:%d | new:%d changed:%d missing:%d",
                     day, len(excel_rows), len(db_rows), len(new_rows),
                     len({(x['issuer_code'], x['stock_date']) for x in changed_long}),
                     len(missing_rows))

    # 4) Tulis laporan
    pd.DataFrame(all_new).to_csv(REPORT_DIR / "diff_new.csv", index=False)
    pd.DataFrame(all_changed_long).to_csv(REPORT_DIR / "diff_changed_long.csv", index=False)
    pd.DataFrame(all_changed_wide).to_csv(REPORT_DIR / "diff_changed_wide.csv", index=False)
    pd.DataFrame(all_missing).to_csv(REPORT_DIR / "diff_missing_in_excel.csv", index=False)

    # 5) Summary keseluruhan
    print("=== SUMMARY ===")
    print(f"Excel unique keys : {len(all_keys)}")
    print(f"DB matched keys   : {len(db_keys_seen)} (akumulasi semua hari yang dibaca)")
    print(f"New rows          : {len(all_new)} (lihat reports/diff_new.csv)")
    print(f"Changed rows      : {len({(r['issuer_code'], r['stock_date']) for r in all_changed_long})} "
          f"(lihat reports/diff_changed_wide.csv & diff_changed_long.csv)")
    print(f"Missing in Excel  : {len(all_missing)} (lihat reports/diff_missing_in_excel.csv)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
