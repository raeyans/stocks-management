#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sync Excel files (Sheet 1) into Supabase Postgres using Supabase Python client.
- Reads all .xlsx in ./files (next to this script).
- Uses COLUMN POSITIONS (no headers):
    B issuer_code
    C issuer_name (for issuers table)
    G stock_date
    E stock_open_price (INT)
    K stock_close_price (INT)
    I stock_high_price (INT)
    J stock_low_price  (INT)
    L stock_diff_price (INT)
    M stock_volume     (BIGINT)
    N stock_trx_value   (BIGINT)
    O stock_frequency (INT)
    Q stock_offer      (INT)
    R stock_offer_volume (BIGINT)
    S stock_bid        (INT)
    T stock_bid_volume (BIGINT)
    U stock_listed_shares (BIGINT)
    V stock_tradeble_shares (BIGINT)
    X stock_foreign_sell (BIGINT)
    Y stock_foreign_buy (BIGINT)
- Date parser supports Indonesian month abbreviations (Agt/Okt/Des/Mei), epoch sec/ms, and Excel serial dates.
- Manual upsert for `stocks`: check existence -> update or insert (no unique constraint needed).
- If inserting into `stocks` fails due to missing issuer FK, insert into `issuers` then retry once.

Env:
  SUPABASE_URL, SUPABASE_SERVICE_ROLE
"""

import os
import re
import logging
import argparse
import math
import numpy as np
import pandas as pd

from typing import List, Any, Optional, Tuple, Dict
from datetime import datetime, date
from supabase import create_client, Client
from dotenv import load_dotenv


# --------------------------- Config ---------------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "files")

load_dotenv()

COL = {
    "issuer_code": 1,         # B
    "issuer_name": 2,         # C
    "stock_date": 6,          # G
    "stock_open_price": 4,    # E (INT)
    "stock_close_price": 10,  # K (INT)
    "stock_high_price": 8,    # I (INT)
    "stock_low_price": 9,     # J (INT)
    "stock_diff_price": 11,   # L (INT)
    "stock_volume": 12,       # M (BIGINT)
    "stock_trx_value": 13,    # N (BIGINT)
    "stock_frequency": 14,    # O (INT)
    "stock_offer": 16,        # Q (INT)
    "stock_offer_volume": 17, # R (BIGINT)
    "stock_bid": 18,          # S (INT)
    "stock_bid_volume": 19,   # T (BIGINT)
    "stock_listed_shares": 20,    # U (BIGINT)
    "stock_tradeble_shares": 21,  # V (BIGINT)
    "stock_foreign_sell": 23,  # X (BIGINT)
    "stock_foreign_buy": 24,   # Y (BIGINT)
}

LOG_EVERY = 200  # log progress every N rows

# --------------------------- Date Helpers ---------------------------

MONTH_REPLACEMENTS = {"Agt": "Aug", "Okt": "Oct", "Des": "Dec", "Mei": "May"}

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
        if n > 1e12:
            return pd.to_datetime(int(n), unit="ms", utc=True).date().isoformat()
        if n > 1e9:
            return pd.to_datetime(int(n), unit="s", utc=True).date().isoformat()
        return pd.to_datetime(n, unit="d", origin="1899-12-30").date().isoformat()
    return None

# --------------------------- Number Helpers ---------------------------

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
        s = re.sub(r"[^\d-]", "", s)
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

# --------------------------- IO helpers ---------------------------

def ensure_col_bounds(df: pd.DataFrame) -> bool:
    max_needed = max(COL.values())
    return df.shape[1] > max_needed

def read_sheet_records(path: str) -> List[Dict[str, Any]]:
    try:
        df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    except Exception as e:
        print(f"Gagal membaca { path }: { e }")
        # logging.error("Gagal membaca %s: %s", path, e)
        return []
    if not ensure_col_bounds(df):
        print(f"{ os.path.basename(path) }: sheet kurang kolom (butuh hingga kolom T).")
        # logging.error("%s: sheet kurang kolom (butuh hingga kolom T).", os.path.basename(path))
        return []
    recs: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        issuer_code = str(row.iloc[COL["issuer_code"]]).strip().upper()
        issuer_name_raw = row.iloc[COL["issuer_name"]].upper()
        issuer_name = (str(issuer_name_raw).strip().upper() if issuer_name_raw is not None else None) or None

        date_iso = coerce_to_iso_date(row.iloc[COL["stock_date"]])
        if not issuer_code or not date_iso:
            continue

        recs.append({
            "issuer_code": issuer_code,
            "issuer_name": issuer_name,
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
            "stock_is_arb": False,
            "stock_is_ara": False,
            "is_active": True,
        })
    return recs

def find_excel_files() -> List[str]:
    if not os.path.isdir(DATA_DIR):
        return []
    return sorted(
        os.path.join(DATA_DIR, name)
        for name in os.listdir(DATA_DIR)
        if name.lower().endswith(".xlsx") and not name.startswith("~$")
    )

# --------------------------- Supabase helpers ---------------------------

def create_supabase_client() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE")
    if not url or not key:
        raise RuntimeError("Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE in environment.")
    return create_client(url, key)

def issuer_exists(client: Client, issuer_code: str) -> bool:
    resp = client.table("issuers").select("issuer_id").eq("issuer_code", issuer_code).limit(1).execute()
    data = getattr(resp, "data", None) or []
    return len(data) > 0

def insert_issuer_if_needed(client: Client, issuer_code: str, issuer_name: Optional[str]) -> None:
    if issuer_exists(client, issuer_code):
        return
    payload = {
        "issuer_code": issuer_code,
        "issuer_name": issuer_name or issuer_code,
        "is_active": True,
    }
    client.table("issuers").insert(payload).execute()

def stock_exists(client: Client, issuer_code: str, stock_date: str) -> bool:
    resp = client.table("stocks").select("stock_id").eq("issuer_code", issuer_code).eq("stock_date", stock_date).limit(1).execute()
    data = getattr(resp, "data", None) or []
    return len(data) > 0

def update_stock(client: Client, r: Dict[str, Any]) -> None:
    fields = {
        "stock_open_price": r["stock_open_price"],
        "stock_close_price": r["stock_close_price"],
        "stock_high_price": r["stock_high_price"],
        "stock_low_price": r["stock_low_price"],
        "stock_diff_price": r["stock_diff_price"],
        "stock_volume": r["stock_volume"],
        "stock_trx_value": r["stock_trx_value"],
        "stock_frequency": r["stock_frequency"],
        "stock_offer": r["stock_offer"],
        "stock_offer_volume": r["stock_offer_volume"],
        "stock_bid": r["stock_bid"],
        "stock_bid_volume": r["stock_bid_volume"],
        "stock_listed_shares": r["stock_listed_shares"],
        "stock_tradeble_shares": r["stock_tradeble_shares"],
        "stock_foreign_sell": r["stock_foreign_sell"],
        "stock_foreign_buy": r["stock_foreign_buy"],
        "stock_is_arb": r["stock_is_arb"],
        "stock_is_ara": r["stock_is_ara"],
        "is_active": True,
    }
    client.table("stocks").update(fields).eq("issuer_code", r["issuer_code"]).eq("stock_date", r["stock_date"]).execute()

def insert_stock(client: Client, r: Dict[str, Any]) -> None:
    payload = {
        "issuer_code": r["issuer_code"],
        "stock_date": r["stock_date"],
        "stock_open_price": r["stock_open_price"],
        "stock_close_price": r["stock_close_price"],
        "stock_high_price": r["stock_high_price"],
        "stock_low_price": r["stock_low_price"],
        "stock_diff_price": r["stock_diff_price"],
        "stock_volume": r["stock_volume"],
        "stock_trx_value": r["stock_trx_value"],
        "stock_frequency": r["stock_frequency"],
        "stock_offer": r["stock_offer"],
        "stock_offer_volume": r["stock_offer_volume"],
        "stock_bid": r["stock_bid"],
        "stock_bid_volume": r["stock_bid_volume"],
        "stock_listed_shares": r["stock_listed_shares"],
        "stock_tradeble_shares": r["stock_tradeble_shares"],
        "stock_foreign_sell": r["stock_foreign_sell"],
        "stock_foreign_buy": r["stock_foreign_buy"],
        "stock_is_arb": r["stock_is_arb"],
        "stock_is_ara": r["stock_is_ara"],
        "is_active": True,
    }
    client.table("stocks").insert(payload).execute()

# ===== Tick size Indonesia (umum) =====
def get_tick_size(price: int) -> int:
    if price < 200:
        return 1
    if price < 500:
        return 2
    if price < 2000:
        return 5
    if price < 5000:
        return 10
    return 25

def round_to_tick(raw_price: float, ref_price_for_tick: Optional[int] = None) -> int:
    base = int(ref_price_for_tick if ref_price_for_tick is not None else round(raw_price))
    tick = get_tick_size(base)
    return int(round(raw_price / tick) * tick)

def floor_to_tick(x: float, ref: int) -> int:
    t = get_tick_size(ref)
    return int(math.floor(x / t) * t)

def ceil_to_tick(x: float, ref: int) -> int:
    t = get_tick_size(ref)
    return int(math.ceil(x / t) * t)

# ===== ARA / ARB limits =====
def ara_arb_percent(prev_close: int) -> Tuple[float, float]:
    # ARA bertingkat: 35% / 25% / 20% (berdasar prev_close); ARB flat 15%
    if prev_close < 200:
        ara = 0.35
    elif prev_close <= 5000:
        ara = 0.25
    else:
        ara = 0.20
    arb = 0.15
    return ara, arb

def ara_arb_limit_prices(prev_close: int, use_tick_rounding: bool = True) -> Tuple[int, int]:
    ara_pct, arb_pct = ara_arb_percent(prev_close)
    ara_raw = prev_close * (1 + ara_pct)
    arb_raw = prev_close * (1 - arb_pct)
    if use_tick_rounding:
        ara_px = max(50, round_to_tick(ara_raw, ref_price_for_tick=prev_close))
        arb_px = max(50, round_to_tick(arb_raw, ref_price_for_tick=prev_close))
    else:
        ara_px = int(round(ara_raw))
        arb_px = int(round(arb_raw))
    return ara_px, arb_px

# ===== Deteksi ARA / ARB (return Tuple[Optional[str], int]) =====
def detect_ara_arb(
    prev_close: int,
    high_price: int,
    low_price: int,
    close_price: int,
    use_tick_rounding: bool = True,
) -> Tuple[Optional[str], int]:
    """
    Heuristik (tanpa order book):
      - ARA jika {high == ara_price and close == ara_price}
      - ARB jika {low  == arb_price and close == arb_price}
    Return:
      ("ARA", ara_price) atau ("ARB", arb_price) atau (None, 0)
    """
    ara_price, arb_price = ara_arb_limit_prices(prev_close, use_tick_rounding=use_tick_rounding)

    if high_price == ara_price and close_price == ara_price:
        return "ARA", ara_price
    if low_price == arb_price and close_price == arb_price:
        return "ARB", arb_price
    return None, 0

    # ara_pct = 0.35 if prev_close < 200 else (0.25 if prev_close <= 5000 else 0.20)
    # arb_pct = 0.15
    # ara_px = max(50, floor_to_tick(prev_close*(1+ara_pct), prev_close))
    # arb_px = max(50,  ceil_to_tick(prev_close*(1-arb_pct), prev_close))
    # if high_price == ara_px and close_price == ara_px: 
    #     return "ARA", ara_px
    # if low_price == arb_px and close_price == arb_px: 
    #     return "ARB", arb_px
    # return None, 0

# --------------------------- Main ---------------------------

def main():
    # parser = argparse.ArgumentParser(description="Sync issuers & stocks from Excel files in ./files (Supabase client).")
    # parser.add_argument("--quiet", action="store_true", help="Reduce logging.")
    # args = parser.parse_args()
    # logging.basicConfig(level=logging.ERROR if args.quiet else logging.WARNING, format="%(levelname)s: %(message)s")

    files = find_excel_files()
    if not files:
        print(f"Tidak ada file .xlsx di { DATA_DIR }")
        # logging.error("Tidak ada file .xlsx di %s", DATA_DIR)
        return 1

    all_rows: List[Dict[str, Any]] = []
    counter = 0

    for p in files:
        rows = read_sheet_records(p)
        counter = counter + len(rows)
        print(f"Parsed { os.path.basename(p) } -> { len(rows) } rows -> counter: { counter }")
        # logging.info("Parsed %-40s -> %d rows", os.path.basename(p), len(rows))
        all_rows.extend(rows)
    if not all_rows:
        print(f"Tidak ada baris valid yang terbaca.")
        # logging.error("Tidak ada baris valid yang terbaca.")
        return 1

    # print(f"Total baris -> { counter } rows")
    # return 4

    try:
        client = create_supabase_client()
    except Exception as e:
        print(f"Gagal membuat Supabase client: { e }")
        # logging.error("Gagal membuat Supabase client: %s", e)
        return 2

    inserted_issuers = 0
    updated_stocks = 0
    inserted_stocks = 0
    errors = 0

    start_time = datetime.now()
    print(f"Start time: {start_time}")

    for idx, r in enumerate(all_rows, start=1):
        try:
            status, px = detect_ara_arb(r["stock_open_price"], 
                                        r["stock_high_price"], 
                                        r["stock_low_price"],
                                        r["stock_close_price"])
            
            if status == "ARA":
                r["stock_is_ara"] = True
            elif status == "ARB":
                r["stock_is_arb"] = True
            
            # Manual upsert
            if stock_exists(client, r["issuer_code"], r["stock_date"]):
                update_stock(client, r)
                updated_stocks += 1
            else:
                try:
                    insert_stock(client, r)
                    inserted_stocks += 1
                except Exception as e:
                    msg = str(e)
                    fk_related = ("foreign key" in msg.lower()) or ("violates foreign key" in msg.lower()) or ("23503" in msg)
                    if fk_related:
                        try:
                            before = issuer_exists(client, r["issuer_code"])
                            insert_issuer_if_needed(client, r["issuer_code"], r.get("issuer_name"))
                            after = issuer_exists(client, r["issuer_code"])
                            if (not before) and after:
                                inserted_issuers += 1
                            insert_stock(client, r)  # retry
                            inserted_stocks += 1
                            print(f"Issuer '{ r['issuer_code'] }' ditambahkan dan stock berhasil di-insert ulang.")
                            # logging.info("Issuer '%s' ditambahkan dan stock berhasil di-insert ulang.", r["issuer_code"])
                        except Exception as e2:
                            errors += 1
                            print(f"Gagal insert issuer/stock untuk { r['issuer_code'] } { r['stock_date'] }: { e2 }")
                            # logging.error("Gagal insert issuer/stock untuk %s %s: %s", r['issuer_code'], r['stock_date'], e2)
                    else:
                        errors += 1
                        print(f"Insert stock gagal untuk { r['issuer_code'] } { r['stock_date'] }: { e }")
                        # logging.error("Insert stock gagal untuk %s %s: %s", r['issuer_code'], r['stock_date'], e)
        except Exception as e:
            errors += 1
            print(f"Row gagal diproses ({ r.get('issuer_code') } { r.get('stock_date') }): { e }")
            # logging.error("Row gagal diproses (%s %s): %s", r.get('issuer_code'), r.get('stock_date'), e)

        if idx % LOG_EVERY == 0:
            print(f"Progress: { idx } rows | updated={ updated_stocks } "
                  f"inserted={ inserted_stocks } issuers_new={ inserted_issuers } "
                  f"errors={ errors } | { datetime.now() }")
            # logging.info("Progress: %d rows | updated=%d inserted=%d issuers_new=%d errors=%d",
            #              idx, updated_stocks, inserted_stocks, inserted_issuers, errors)

    print(f"SUCCESS - updated_stocks={updated_stocks} inserted_stocks={inserted_stocks} "
          f"new_issuers={inserted_issuers} errors={errors}")
    
    finish_time = datetime.now()
    print(f"Finish time: {finish_time}")
    latency_seconds = (finish_time - start_time).total_seconds()
    print(f"Latency: {latency_seconds:.3f} seconds")
    
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
