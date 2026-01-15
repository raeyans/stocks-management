import os
import pandas as pd

from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import date, timedelta, datetime

# --------------------------- Konfigurasi ---------------------------

load_dotenv()

# --------------------------- Supabase ---------------------------

def create_supabase_client() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE")
    
    if not url or not key:
        raise RuntimeError("Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE in environment.")
    
    return create_client(url, key)

# --------------------------- Helpers ---------------------------

THRESHOLD_HAS_ARB_ARA = date(2025, 4, 8)

def include_arb_ara(end_date_str: str) -> bool:
    """Kembalikan True jika end_date >= 2025-04-08 (kolom Is ARB/Is ARA boleh ditampilkan)."""
    try:
        d = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except ValueError:
        # Jika format tidak valid, abaikan (anggap tidak include)
        return False
    
    return d >= THRESHOLD_HAS_ARB_ARA

# --------------------------- Utilities ---------------------------

def dump_dataframe_to_excel(df: pd.DataFrame, xlsx_path: str, sheet_name: str = "Stocks") -> str:
    """
    Simpan DataFrame ke Excel (.xlsx) dengan header yang rapi dan autosize kolom.
    """
    if df.empty:
        # Tetap buat file dengan header agar konsisten
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        return xlsx_path

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

        # Autosize kolom
        ws = writer.book[sheet_name]
        for col_idx, col_name in enumerate(df.columns, start=1):
            series = df[col_name].astype(str).fillna("")
            max_len = max([len(col_name)] + series.map(len).tolist())
            ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = min(max_len + 2, 60)

    return xlsx_path

def normalize_tickers(tickers_input) -> list:
    """
    Terima ticker sebagai:
      - None  -> []
      - "BBCA" -> ["BBCA"]
      - "BBCA, BBTN, BBNI" -> ["BBCA","BBTN","BBNI"]
      - ["BBCA","BBTN"] -> ["BBCA","BBTN"]
    """
    if tickers_input is None:
        return []
    if isinstance(tickers_input, str):
        parts = [p.strip().upper() for p in tickers_input.split(",") if p.strip()]
        return parts
    if isinstance(tickers_input, (list, tuple, set)):
        return [str(x).strip().upper() for x in tickers_input if str(x).strip()]
    # tipe lain diabaikan
    return []

# --------------------------- Fetch Data ---------------------------

def fetch_stocks_dataframe(client: Client, start_date: str, end_date: str, tickers: list | None = None, page_size: int = 1000) -> pd.DataFrame:
    base_select = (
        "date:stock_date,"
        "ticker:issuer_code,"
        "open:stock_open_price,"
        "high:stock_high_price,"
        "low:stock_low_price,"
        "close:stock_close_price,"
        "diff:stock_diff_price,"
        "volume:stock_volume,"
        "transaction_value:stock_trx_value,"
        "frequency:stock_frequency,"
        "listed_shares:stock_listed_shares,"
        "tradeble_shares:stock_tradeble_shares,"
        "foreign_buy:stock_foreign_buy,"
        "foreign_sell:stock_foreign_sell"
    )

    # Tambahkan kolom ARB/ARA hanya jika end_date >= 2025-04-08
    if include_arb_ara(end_date):
        SELECT = base_select + "," + "is_arb:stock_is_arb,is_ara:stock_is_ara"
    else:
        SELECT = base_select  # tanpa is_arb/is_ara

    rows = []
    start_idx = 0

    while True:
        end_idx = start_idx + page_size - 1
        q = (
            client.table("stocks")
                  .select(SELECT)
                  .eq("is_active", True)
                  .gte("stock_date", start_date)
                  .lte("stock_date", end_date)
        )

        # Terapkan filter ticker jika ada
        if tickers:
            q = q.in_("issuer_code", tickers)

        q = (
            q.order("stock_date")
             .order("issuer_code")
             .range(start_idx, end_idx)
        )
        resp = q.execute()

        data = getattr(resp, "data", []) or []
        rows.extend(data)

        fetched = len(data)
        if fetched < page_size:
            break
        start_idx += fetched

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Pastikan foreign_* numerik, isi NaN -> 0
    for col in ["foreign_buy", "foreign_sell"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Hitung foreign_net
    df["foreign_net"] = df["foreign_buy"] - df["foreign_sell"]

    # Hitung Diff % dari Diff/Open (aman dari pembagian nol)
    if "diff" in df.columns and "open" in df.columns:
        open_nonzero = pd.to_numeric(df["open"], errors="coerce").replace({0: pd.NA})
        diff_numeric = pd.to_numeric(df["diff"], errors="coerce")
        df["diff_pct"] = (diff_numeric / open_nonzero) * 100
        df["diff_pct"] = df["diff_pct"].round(2)

    # Rename ke label final
    rename_map = {
        "date": "Date",
        "ticker": "Ticker",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "diff": "Diff",
        "diff_pct": "Diff Pct",
        "volume": "Volume",
        "transaction_value": "Transaction Value",
        "frequency": "Frequency",
        "listed_shares": "Listed Shares",
        "tradeble_shares": "Tradeble Shares",
        "foreign_buy": "Foreign Buy",
        "foreign_sell": "Foreign Sell",
        "foreign_net": "Foreign Net",
        "is_arb": "Is ARB",
        "is_ara": "Is ARA",
    }
    df.rename(columns=rename_map, inplace=True)

    # Reorder kolom
    desired_full_order = [
        "Date", "Ticker", "Open", "High", "Low", "Close",
        "Diff", "Diff Pct", "Volume", "Transaction Value", 
        "Frequency", "Listed Shares", "Tradeble Shares",
        "Foreign Buy", "Foreign Sell", "Foreign Net", "Is ARB", "Is ARA"
    ]
    ordered = [c for c in desired_full_order if c in df.columns]
    remaining = [c for c in df.columns if c not in ordered]
    df = df[ordered + remaining]

    return df

def main():
    client = create_supabase_client()
    
    today = date.today()

    # Default: last 1 year s/d today (inklusif)
    # one_year_ago = today - timedelta(days=365)
    # start_date = one_year_ago.strftime("%Y-%m-%d")
    # end_date = today.strftime("%Y-%m-%d")

    # YTD:
    # start_date = date(today.year, 1, 1).strftime("%Y-%m-%d")
    # end_date   = today.strftime("%Y-%m-%d")
    
    # Tahun 2024 penuh:
    start_date = "2025-06-01"
    end_date   = "2026-01-09"

    # ----------------- FILTER TICKER -----------------
    # Bisa string satu ticker:
    # ticker_filter = "INDS"
    # Bisa string beberapa ticker dipisah koma:
    ticker_filter = "VIVA, TOBA, PICO, OPMS, ESTI, COCO, DADA"
    # Atau list python:
    # ticker_filter = ["COIN", "PIPA", "CBRE", "CDIA", "ESTI", "SSIA", "COCO", "VIVA", "DADA"]
    # ticker_filter = None  # <-- set di sini jika ingin filter
    tickers = normalize_tickers(ticker_filter)
    # -------------------------------------------------

    df = fetch_stocks_dataframe(
        client,
        start_date=start_date,
        end_date=end_date,
        tickers=tickers,
    )

    # Pastikan folder output ada
    output_dir = "output" if not tickers else "views"
    os.makedirs(output_dir, exist_ok=True)

    base = f"stocks_data_{start_date}_to_{end_date}"
    if tickers:
        base += "_" + "-".join(tickers)  # tambahkan info ticker di nama file bila ada filter

    xlsx_path = os.path.join(output_dir, f"{base}.xlsx")
    dump_dataframe_to_excel(df, xlsx_path)

    # csv_path = os.path.join(output_dir, f"{base}.csv")
    # json_path = os.path.join(output_dir, f"{base}.json")
    # df.to_csv(csv_path, index=False, encoding="utf-8")
    # df.to_json(json_path, orient="records", date_format="iso", force_ascii=False, indent=2)

    print("Selesai. File tersimpan di:")
    print(f"- {xlsx_path}")
    # print(f"- {csv_path}")
    # print(f"- {json_path}")
    
    return 0

if __name__ == "__main__":
    raise SystemExit(main())