# pip install requests
import os
import requests
import time

from requests.adapters import HTTPAdapter
from dotenv import load_dotenv
from datetime import date, datetime, timedelta
from supabase import create_client, Client
from typing import Any, Dict, List, Optional, Iterable, Set
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

load_dotenv()

# Atur rentang tanggal di sini (format YYYY-MM-DD), inklusif
START_DATE = "2025-09-01"
END_DATE   = "2025-11-28"

FILE_TICKER = "issuer_code.txt"
FILE_HOL    = "holiday.txt"

BASE_URL = os.getenv("MARKET_DETECTOR_API_URL")
TOKEN = os.getenv("AUTH_TOKEN")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
}
BASE_PARAMS = {
    "transaction_type": "TRANSACTION_TYPE_NET",
    "market_board": "MARKET_BOARD_ALL",
    "investor_type": "INVESTOR_TYPE_ALL",
    "limit": 25,
}

# today = date.today()
# str_today = today.strftime("%Y-%m-%d")
# str_yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# ============================== Utilitas I/O ==============================

def create_supabase_client() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE")
    
    if not url or not key:
        raise RuntimeError("Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE in environment.")
    
    return create_client(url, key)

def date_range_inclusive_skip(start_str: str, end_str: str, holidays: Set[date]) -> Iterable[str]:
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end   = datetime.strptime(end_str,   "%Y-%m-%d").date()
    if end < start:
        raise ValueError("END_DATE harus >= START_DATE")

    cur = start
    while cur <= end:
        # 0=Mon ... 5=Sat 6=Sun
        if cur.weekday() < 5 and cur not in holidays:
            yield cur.strftime("%Y-%m-%d")
        cur += timedelta(days=1)

def read_lines(path: str) -> List[str]:
    items: List[str] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#"):
                    items.append(s)
    except FileNotFoundError:
        pass
    return items

def read_tickers(path: str) -> List[str]:
    return read_lines(path)

def read_holidays(path: str) -> Set[date]:
    out: Set[date] = set()
    for s in read_lines(path):
        try:
            d = datetime.strptime(s, "%Y-%m-%d").date()
            out.add(d)
        except ValueError:
            # lewati baris yang tidak valid
            continue
    return out

def to_int(x: Any) -> Optional[int]:
    if x is None or x == "":
        return None
    if isinstance(x, bool):
        return int(x)
    if isinstance(x, int):
        return x
    
    try:
        d = Decimal(str(x))
        return int(d.to_integral_value(rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError, TypeError):
        return None

def to_float(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return float(x)
    
    try:
        d = Decimal(str(x))
        return float(d)
    except (InvalidOperation, ValueError, TypeError):
        return None

# ============================== API Call ==============================

def fetch_market_detector_for_day(ticker: str, day_str: str) -> Optional[Dict[str, Any]]:
    params = {"from": day_str, "to": day_str, **BASE_PARAMS}
    url = f"{BASE_URL}/{ticker}"

    r = requests.get(url, headers=HEADERS, params=params, timeout=(5, 30))
    r.raise_for_status()
    
    if r.headers.get("content-type", "").startswith("application/json"):
        payload = r.json()
    else:
        payload = {}
    
    return payload.get("data")

# ============================== Parser: data → rows ==============================

def safe_get(d: Dict[str, Any], path: str, default=None):
    cur = d

    for key in path.split("."):
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            return default
    
    return cur

def parse_broker_highlight(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(data, dict):
        return None

    bd = data.get("bandar_detector") or {}
    bs = data.get("broker_summary") or {}

    row = {
        "issuer_code":        bs.get("symbol"),
        "brox_date":          data.get("from"),
        "brox_highlight":     bd.get("broker_accdist"),

        "top1_highlight":     safe_get(bd, "top1.accdist"),
        "top1_amount":        to_int(safe_get(bd, "top1.amount")),
        "top1_pct":           to_float(safe_get(bd, "top1.percent")),
        "top1_volume":        to_int(safe_get(bd, "top1.vol")),

        "top3_highlight":     safe_get(bd, "top3.accdist"),
        "top3_amount":        to_int(safe_get(bd, "top3.amount")),
        "top3_pct":           to_float(safe_get(bd, "top3.percent")),
        "top3_volume":        to_int(safe_get(bd, "top3.vol")),

        "top5_highlight":     safe_get(bd, "top5.accdist"),
        "top5_amount":        to_int(safe_get(bd, "top5.amount")),
        "top5_pct":           to_float(safe_get(bd, "top5.percent")),
        "top5_volume":        to_int(safe_get(bd, "top5.vol")),

        "top10_highlight":    safe_get(bd, "top10.accdist"),
        "top10_amount":       to_int(safe_get(bd, "top10.amount")),
        "top10_pct":          to_float(safe_get(bd, "top10.percent")),
        "top10_volume":       to_int(safe_get(bd, "top10.vol")),

        "brox_total_buyer":   to_int(bd.get("total_buyer")),
        "brox_total_seller":  to_int(bd.get("total_seller")),
        "brox_value":         to_int(bd.get("value")),
        "brox_volume":        to_int(bd.get("volume")),
    }

    if not row["issuer_code"] or not row["brox_date"]:
        return None
    
    return row

def parse_broker_summary(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    bs = data.get("broker_summary") or {}
    symbol = bs.get("symbol")
    the_date = data.get("from")

    if not symbol or not the_date:
        return rows

    # brokers_buy
    for b in (bs.get("brokers_buy") or []):
        rows.append({
            "issuer_code": symbol,
            "broksum_date": the_date,
            "broker_code": b.get("netbs_broker_code"),
            "broksum_trx_type": "BUY",
            "broksum_lot": to_int(b.get("blot")),
            "broksum_value": to_int(b.get("bval")),
            "broksum_avg_price": to_float(b.get("netbs_buy_avg_price"))
        })

    # brokers_sell
    for s in (bs.get("brokers_sell") or []):
        rows.append({
            "issuer_code": symbol,
            "broksum_date": the_date,
            "broker_code": s.get("netbs_broker_code"),
            "broksum_trx_type": "SELL",
            "broksum_lot": to_int(s.get("slot")),
            "broksum_value": to_int(s.get("sval")),
            "broksum_avg_price": to_float(s.get("netbs_sell_avg_price"))
        })

    return rows

# ============================== Save (bulk UPSERT) ==============================

def chunked(seq, n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def upsert_rows(client: Client, table: str, rows, on_conflict: str, chunk: int = 500):
    if not rows:
        return
    
    for part in chunked(rows, chunk):
        client.table(table).upsert(
            part,
            on_conflict=on_conflict,
            ignore_duplicates=False
        ).execute()

# ============================== Main Flow ==============================

def main():
    tickers = read_tickers(FILE_TICKER)
    if not tickers:
        print("issuer_code.txt kosong / tidak berisi ticker.")
        return

    holidays = read_holidays(FILE_HOL)
    client = create_supabase_client()

    for day in date_range_inclusive_skip(START_DATE, END_DATE, holidays):
        print(f"\n=== Proses hari {day} ===")

        # 1) Fetch semua ticker hari ini -> simpan 'data' di memori
        raw_payloads: List[Dict[str, Any]] = []
        for i, t in enumerate(tickers, 1):
            try:
                data = fetch_market_detector_for_day(t, day)
                if data:
                    # pastikan tanggal sesuai hari yg diproses
                    data["from"] = day
                    raw_payloads.append({"ticker": t, "data": data})
                    print(f"[{i}/{len(tickers)}] OK {t}")
                else:
                    print(f"[{i}/{len(tickers)}] EMPTY {t}")
            except requests.HTTPError as e:
                body = e.response.text[:400] if e.response is not None else ""
                print(f"[{i}/{len(tickers)}] HTTPError {t}: {e}\n{body}")
            except requests.RequestException as e:
                print(f"[{i}/{len(tickers)}] RequestError {t}: {e}")
            
            time.sleep(0.12)

        # 2) Parse → rows dua tabel
        broker_highlight_rows: List[Dict[str, Any]] = []
        broker_summary_rows:  List[Dict[str, Any]] = []

        for item in raw_payloads:
            d = item["data"]
            bh = parse_broker_highlight(d)
            if bh:
                broker_highlight_rows.append(bh)
            broker_summary_rows.extend(parse_broker_summary(d))

        print(f"[{day}] Parsed -> broker_highlight: {len(broker_highlight_rows)}, broker_summary: {len(broker_summary_rows)}")

        # 3) UPSERT
        upsert_rows(client, "broker_highlight",
                    broker_highlight_rows,
                    on_conflict="issuer_code,brox_date",
                    chunk=500)
        upsert_rows(client, "broker_summary",
                    broker_summary_rows,
                    on_conflict="issuer_code,broksum_date,broker_code,broksum_trx_type",
                    chunk=1000)

        print(f"[{day}] Selesai UPSERT. Lanjut ke hari berikutnya...")

    print("\nSelesai semua tanggal.")

if __name__ == "__main__":
    main()
