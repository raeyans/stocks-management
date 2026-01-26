#!/usr/bin/env python3

import os
import pandas as pd
import sys

from datetime import date, datetime
from decimal import Decimal, getcontext
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import Any, Dict, List, Optional


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TRADE_DIR = os.path.join(SCRIPT_DIR, "trade")

load_dotenv()
getcontext().prec = 28

VALID_TRX_TYPES = {"DEPOSIT", "BELI", "JUAL", "WITHDRAW", "MATERAI"}

# Broker Fee:        0.0961% (BUY and SELL)
# VAT Broker Fee:    0.0106% (BUY and SELL)
# Exchange Fee:      0.0400% (BUY and SELL)
# VAT Exchange Fee:  0.0033% (BUY and SELL)
# Income Tax:        0.1000% (SELL only)

BROKER_FEE_RATE = Decimal("0.000961")
VAT_BROKER_FEE_RATE = Decimal("0.000106")
EXCH_FEE_RATE = Decimal("0.000400")
VAT_EXCH_FEE_RATE = Decimal("0.000033")
INCOME_TAX_RATE = Decimal("0.001000")


def create_supabase_client() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL and/or SUPABASE_SERVICE_ROLE environment variables.")
    return create_client(url, key)

def find_single_trade_excel() -> str:
    if not os.path.isdir(TRADE_DIR):
        raise FileNotFoundError(f"Trade folder not found: {TRADE_DIR}")

    files = sorted(
        os.path.join(TRADE_DIR, f)
        for f in os.listdir(TRADE_DIR)
        if f.lower().endswith(".xlsx") and not f.startswith("~$")
    )

    if len(files) == 0:
        raise FileNotFoundError(f"No .xlsx file found in trade folder: {TRADE_DIR}")
    if len(files) > 1:
        raise RuntimeError(f"trade/ must contain exactly 1 .xlsx file, found {len(files)}: {files}")

    return files[0]

def calc_buy_sell_fields(price: int, lot: int, trx_type: str) -> Dict[str, Decimal]:
    base = Decimal(price) * Decimal(lot) * Decimal(100)

    broker_fee = base * BROKER_FEE_RATE
    vat_broker_fee = base * VAT_BROKER_FEE_RATE
    exch_fee = base * EXCH_FEE_RATE
    vat_exch_fee = base * VAT_EXCH_FEE_RATE

    income_tax = base * INCOME_TAX_RATE if trx_type == "JUAL" else Decimal("0")
    total_fee = broker_fee + vat_broker_fee + exch_fee + vat_exch_fee + income_tax
    trx_total = base + total_fee if trx_type == "BELI" else base - total_fee

    return {
        "trx_broker_fee": broker_fee,
        "trx_vat_broker_fee": vat_broker_fee,
        "trx_exch_fee": exch_fee,
        "trx_vat_exch_fee": vat_exch_fee,
        "trx_income_tax": income_tax,
        "trx_total_fee": total_fee,
        "trx_total": trx_total,
    }

def row_to_payload(row: pd.Series) -> Optional[Dict[str, Any]]:
    tanggal = row["Tanggal"]
    trx_type = str(row["Transaksi"]).strip().upper()

    if trx_type not in VALID_TRX_TYPES:
        raise ValueError(f"Invalid transaction type: {trx_type}. Valid values: {sorted(VALID_TRX_TYPES)}")

    trx_date = tanggal.date().isoformat() if hasattr(tanggal, "date") else str(tanggal)

    payload: Dict[str, Any] = {
        "is_active": True,
        "trx_date": trx_date,
        "trx_type": trx_type,
    }

    if trx_type in {"DEPOSIT", "WITHDRAW", "MATERAI"}:
        jumlah = row["Jumlah"]
        if pd.isna(jumlah):
            raise ValueError(f"Missing 'Jumlah' for {trx_type} on {trx_date}")

        payload.update({
            "trx_ticker": None,
            "trx_total": str(Decimal(str(jumlah))),
            "trx_holding_period": -1,
            "trx_is_hold": False,
            "trx_is_calc_pnl": True,
            "trx_broker_fee": "0",
            "trx_vat_broker_fee": "0",
            "trx_exch_fee": "0",
            "trx_vat_exch_fee": "0",
            "trx_income_tax": "0",
            "trx_total_fee": "0",
        })
        return payload

    if trx_type in {"BELI", "JUAL"}:
        ticker_raw = row["Ticker"]
        ticker = "" if pd.isna(ticker_raw) else str(ticker_raw).strip().upper()
        if not ticker:
            raise ValueError(f"Missing 'Ticker' for {trx_type} on {trx_date}")

        harga = row["Harga"]
        lot = row["Lot"]
        if pd.isna(harga) or pd.isna(lot):
            raise ValueError(f"Missing 'Harga' and/or 'Lot' for {trx_type} {ticker} on {trx_date}")

        price_int = int(harga)
        lot_int = int(lot)

        calc = calc_buy_sell_fields(price_int, lot_int, trx_type)

        payload.update({
            "trx_ticker": ticker,
            "trx_price": price_int,
            "trx_lot": lot_int,
            "trx_broker_fee": str(calc["trx_broker_fee"]),
            "trx_vat_broker_fee": str(calc["trx_vat_broker_fee"]),
            "trx_exch_fee": str(calc["trx_exch_fee"]),
            "trx_vat_exch_fee": str(calc["trx_vat_exch_fee"]),
            "trx_income_tax": str(calc["trx_income_tax"]),
            "trx_total_fee": str(calc["trx_total_fee"]),
            "trx_total": str(calc["trx_total"]),
        })
        return payload

    return None

def insert_trans(client: Client, payload: Dict[str, Any]) -> None:
    client.table("trans").insert(payload).execute()

def insert_trans_from_trade_folder() -> Dict[str, int]:
    excel_path = find_single_trade_excel()
    df = pd.read_excel(excel_path, sheet_name=0, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]

    required = ["Tanggal", "Transaksi", "Ticker", "Harga", "Lot", "Jumlah"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing Excel columns: {missing}. Required: {required}")

    client = create_supabase_client()

    inserted = 0
    skipped = 0
    errors = 0

    for i, row in df.iterrows():
        try:
            if pd.isna(row["Tanggal"]) and pd.isna(row["Transaksi"]):
                skipped += 1
                continue

            payload = row_to_payload(row)
            if not payload:
                skipped += 1
                continue

            insert_trans(client, payload)
            inserted += 1

        except Exception as e:
            errors += 1
            print(f"[ERROR] Failed at Excel row {i+2}: {e}")

    print(f"Processed file: {os.path.basename(excel_path)}")
    return {"inserted": inserted, "skipped": skipped, "errors": errors}

def update_buy_holding_period_and_close_hold(client: Client, buy_trx_id: Any, holding_period_days: int) -> None:
    client.table("trans").update(
        {
            "trx_holding_period": int(holding_period_days),
            "trx_is_hold": False,
        }
    ).eq("trx_id", buy_trx_id).execute()

def close_hold_for_sells(client: Client, buy_trx_id: Any) -> None:
    client.table("trans").update(
        {
            "trx_is_hold": False,
        }
    ).eq("trx_type", "JUAL").eq("trx_ref", buy_trx_id).execute()

def fetch_trans_bs_detail_trx_ids(client: Client, buy_trx_id: Any) -> List[int]:
    resp = (
        client.table("trans_bs_detail")
        .select("trx_id")
        .eq("trx_ref", buy_trx_id)
        .eq("is_active", True)
        .execute()
    )

    rows = resp.data or []
    trx_ids: List[int] = []

    for r in rows:
        trx_id = r.get("trx_id")
        if trx_id is not None:
            trx_ids.append(int(trx_id))

    seen = set()
    deduped = []
    for x in trx_ids:
        if x not in seen:
            seen.add(x)
            deduped.append(x)

    return deduped

def fetch_trans_rows_by_ids(client: Client, trx_ids: List[int]) -> List[Dict[str, Any]]:
    if not trx_ids:
        return []

    resp = (
        client.table("trans")
        .select("trx_id,trx_date,trx_lot,trx_type,trx_is_hold,trx_ref,is_active")
        .in_("trx_id", trx_ids)
        .eq("is_active", True)
        .execute()
    )
    return resp.data or []

def close_hold_for_sells_by_ids(client: Client, sell_trx_ids: List[int]) -> None:
    if not sell_trx_ids:
        return

    client.table("trans").update(
        {"trx_is_hold": False}
    ).in_("trx_id", sell_trx_ids).execute()

def fetch_root_hold_buys(client: Client) -> List[Dict[str, Any]]:
    resp = (
        client.table("trans")
        .select("trx_id,trx_date,trx_type,trx_lot,trx_ref,trx_is_hold,is_active")
        .eq("is_active", True)
        .eq("trx_type", "BELI")
        .eq("trx_is_hold", True)
        .is_("trx_ref", "null")
        .execute()
    )
    return resp.data or []

def fetch_related_trx_by_root_id(client: Client, root_buy_id: int) -> List[Dict[str, Any]]:
    resp = (
        client.table("trans")
        .select("trx_id,trx_date,trx_type,trx_lot,trx_ref,trx_is_hold,is_active")
        .eq("is_active", True)
        .in_("trx_type", ["BELI", "JUAL"])
        .eq("trx_ref", root_buy_id)
        .execute()
    )
    return resp.data or []

def compute_group_metrics(group_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    buy_ids: List[int] = []
    sell_ids: List[int] = []
    total_buy_lot = 0
    total_sell_lot = 0

    first_buy_date: Optional[date] = None
    last_sell_date: Optional[date] = None

    for r in group_rows:
        trx_id = r.get("trx_id")
        trx_type = str(r.get("trx_type") or "").strip().upper()
        trx_lot_raw = r.get("trx_lot")
        trx_date_raw = r.get("trx_date")

        if trx_id is None:
            raise ValueError("A group row is missing trx_id.")
        if trx_lot_raw is None:
            raise ValueError(f"Transaction {trx_id} is missing trx_lot.")
        if trx_date_raw is None:
            raise ValueError(f"Transaction {trx_id} is missing trx_date.")

        d = date.fromisoformat(trx_date_raw)
        lot = int(trx_lot_raw)

        if trx_type == "BELI":
            buy_ids.append(int(trx_id))
            total_buy_lot += lot
            if first_buy_date is None or d < first_buy_date:
                first_buy_date = d

        elif trx_type == "JUAL":
            sell_ids.append(int(trx_id))
            total_sell_lot += lot
            if last_sell_date is None or d > last_sell_date:
                last_sell_date = d

    if first_buy_date is None:
        raise ValueError("Cannot determine first BUY date because no BUY rows exist in the group.")

    return {
        "buy_ids": buy_ids,
        "sell_ids": sell_ids,
        "total_buy_lot": total_buy_lot,
        "total_sell_lot": total_sell_lot,
        "first_buy_date": first_buy_date,
        "last_sell_date": last_sell_date,
    }

def update_close_group(client: Client, buy_ids: List[int], sell_ids: List[int], holding_days: int,) -> None:
    if buy_ids:
        client.table("trans").update(
            {
                "trx_holding_period": int(holding_days),
                "trx_is_hold": False,
            }
        ).in_("trx_id", buy_ids).execute()

    if sell_ids:
        client.table("trans").update(
            {
                "trx_is_hold": False,
            }
        ).in_("trx_id", sell_ids).execute()

def finalize_closed_positions_from_holds() -> Dict[str, int]:
    client = create_supabase_client()

    stats = {
        "root_buys_checked": 0,
        "root_buys_skipped_no_related": 0,
        "root_buys_skipped_no_sells": 0,
        "root_buys_skipped_lot_mismatch": 0,
        "groups_closed": 0,
        "errors": 0,
    }

    root_buys = fetch_root_hold_buys(client)

    for rb in root_buys:
        stats["root_buys_checked"] += 1

        try:
            root_id = rb.get("trx_id")
            if root_id is None:
                raise ValueError("Root BUY transaction is missing trx_id.")

            related = fetch_related_trx_by_root_id(client, int(root_id))
            if not related:
                stats["root_buys_skipped_no_related"] += 1
                continue

            group_rows = [rb] + related

            metrics = compute_group_metrics(group_rows)

            if not metrics["sell_ids"] or metrics["last_sell_date"] is None:
                stats["root_buys_skipped_no_sells"] += 1
                continue

            if metrics["total_buy_lot"] != metrics["total_sell_lot"]:
                stats["root_buys_skipped_lot_mismatch"] += 1
                continue

            holding_days = (metrics["last_sell_date"] - metrics["first_buy_date"]).days
            if holding_days < 0:
                holding_days = 0

            update_close_group(
                client=client,
                buy_ids=metrics["buy_ids"],
                sell_ids=metrics["sell_ids"],
                holding_days=holding_days,
            )

            stats["groups_closed"] += 1

        except Exception as e:
            stats["errors"] += 1
            print(f"[ERROR] Failed to finalize root BUY group (trx_id={rb.get('trx_id')}): {e}")

    return stats

def fetch_root_buys_for_pnl_calc(client: Client) -> List[Dict[str, Any]]:
    resp = (
        client.table("trans")
        .select(
            "trx_id,trx_date,trx_type,trx_ticker,trx_lot,"
            "trx_broker_fee,trx_vat_broker_fee,trx_exch_fee,trx_vat_exch_fee,trx_income_tax,"
            "trx_total,trx_ref,trx_is_hold,trx_is_calc_pnl,is_active"
        )
        .eq("is_active", True)
        .eq("trx_is_hold", False)
        .eq("trx_is_calc_pnl", False)
        .eq("trx_type", "BELI")
        .is_("trx_ref", "null")
        .execute()
    )
    return resp.data or []

def fetch_group_trx_for_pnl_calc(client: Client, root_buy_id: int) -> List[Dict[str, Any]]:
    resp = (
        client.table("trans")
        .select(
            "trx_id,trx_date,trx_type,trx_ticker,trx_lot,"
            "trx_broker_fee,trx_vat_broker_fee,trx_exch_fee,trx_vat_exch_fee,trx_income_tax,"
            "trx_total,trx_ref,trx_is_hold,trx_is_calc_pnl,is_active"
        )
        .eq("is_active", True)
        .eq("trx_is_hold", False)
        .eq("trx_is_calc_pnl", False)
        .in_("trx_type", ["BELI", "JUAL"])
        .eq("trx_ref", root_buy_id)
        .execute()
    )
    return resp.data or []

def build_pnl_summary_payload(root_buy_id: int, group_rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    buy_rows = [r for r in group_rows if str(r.get("trx_type") or "").upper() == "BELI"]
    sell_rows = [r for r in group_rows if str(r.get("trx_type") or "").upper() == "JUAL"]

    if not sell_rows:
        return None

    ticker = None
    for r in group_rows:
        t = r.get("trx_ticker")
        if t:
            ticker = str(t).strip().upper()
            break
    if not ticker:
        raise ValueError("Cannot determine pnl_ticker because trx_ticker is missing in the group.")

    first_buy_date: Optional[date] = None
    for r in buy_rows:
        d = date.fromisoformat(r.get("trx_date"))
        if first_buy_date is None or d < first_buy_date:
            first_buy_date = d

    last_sell_date: Optional[date] = None
    for r in sell_rows:
        d = date.fromisoformat(r.get("trx_date"))
        if last_sell_date is None or d > last_sell_date:
            last_sell_date = d

    if first_buy_date is None or last_sell_date is None:
        raise ValueError("Cannot determine pnl date range because BUY/SELL dates are incomplete.")

    total_buy_lot = sum(int(r["trx_lot"]) for r in buy_rows if r.get("trx_lot") is not None)
    total_sell_lot = sum(int(r["trx_lot"]) for r in sell_rows if r.get("trx_lot") is not None)
    if total_buy_lot != total_sell_lot:
        return None

    sum_broker_fee = Decimal("0")
    sum_vat_broker_fee = Decimal("0")
    sum_exch_fee = Decimal("0")
    sum_vat_exch_fee = Decimal("0")
    sum_income_tax = Decimal("0")

    for r in group_rows:
        trx_id = r.get("trx_id")
        sum_broker_fee += Decimal(str(r.get("trx_broker_fee")))
        sum_vat_broker_fee += Decimal(str(r.get("trx_vat_broker_fee")))
        sum_exch_fee += Decimal(str(r.get("trx_exch_fee")))
        sum_vat_exch_fee += Decimal(str(r.get("trx_vat_exch_fee")))
        sum_income_tax += Decimal(str(r.get("trx_income_tax")))

    total_fee = sum_broker_fee + sum_vat_broker_fee + sum_exch_fee + sum_vat_exch_fee + sum_income_tax

    sum_sell_total = Decimal("0")
    for r in sell_rows:
        trx_id = r.get("trx_id")
        sum_sell_total += Decimal(str(r.get("trx_total")))

    sum_buy_total = Decimal("0")
    for r in buy_rows:
        trx_id = r.get("trx_id")
        sum_buy_total += Decimal(str(r.get("trx_total")))

    pnl_amount = sum_sell_total - sum_buy_total

    return {
        "is_active": True,
        "pnl_trx_ref": int(root_buy_id),
        "pnl_ticker": ticker,
        "pnl_first_buy_date": first_buy_date.isoformat(),
        "pnl_last_sell_date": last_sell_date.isoformat(),
        "pnl_processed_lot": int(total_buy_lot),
        "pnl_total_broker_fee": str(sum_broker_fee),
        "pnl_vat_broker_fee": str(sum_vat_broker_fee),
        "pnl_total_exch_fee": str(sum_exch_fee),
        "pnl_vat_exch_fee": str(sum_vat_exch_fee),
        "pnl_income_tax": str(sum_income_tax),
        "pnl_total_fee": str(total_fee),
        "pnl_amount": str(pnl_amount),
    }

def pnl_summary_exists(client: Client, payload: Dict[str, Any]) -> bool:
    # pnl_trx_ref = payload.get("pnl_trx_ref")

    # if pnl_trx_ref is None:
    pnl_trx_ref = int(payload.get("pnl_trx_ref"))
    
    if pnl_trx_ref > 0:
        resp = (
            client.table("pnl_summary")
            .select("pnl_id")
            .eq("is_active", True)
            .eq("pnl_trx_ref", int(pnl_trx_ref))
            .limit(1)
            .execute()
        )
        if (resp.data):
            return True
        
        return False
    
    # print(payload)
    resp = (
        client.table("pnl_summary")
        .select("pnl_id")
        .eq("is_active", True)
        .eq("pnl_ticker", payload["pnl_ticker"])
        .eq("pnl_first_buy_date", payload["pnl_first_buy_date"])
        .eq("pnl_last_sell_date", payload["pnl_last_sell_date"])
        .eq("pnl_processed_lot", payload["pnl_processed_lot"])
        .limit(1)
        .execute()
    )
    # return len(resp.data or []) > 0
    if (resp.data):
        return True
    
    return False

def insert_pnl_summary(client: Client, payload: Dict[str, Any]) -> None:
    client.table("pnl_summary").insert(payload).execute()

def calc_pnl_summary() -> Dict[str, int]:
    client = create_supabase_client()

    stats = {
        "root_buys_checked": 0,
        "groups_skipped_no_related": 0,
        "groups_skipped_not_closed": 0,
        "groups_inserted": 0,
        "groups_already_exists": 0,
        "groups_marked_calc": 0,
        "errors": 0,
    }

    roots = fetch_root_buys_for_pnl_calc(client)

    for rb in roots:
        stats["root_buys_checked"] += 1

        try:
            root_id = rb.get("trx_id")
            if root_id is None:
                raise ValueError("Root BUY transaction is missing trx_id.")

            related = fetch_group_trx_for_pnl_calc(client, int(root_id))
            if not related:
                stats["groups_skipped_no_related"] += 1
                continue

            group_rows = [rb] + related

            # payload = build_pnl_summary_payload(group_rows)
            payload = build_pnl_summary_payload(int(root_id), group_rows)
            if payload is None:
                stats["groups_skipped_not_closed"] += 1
                continue

            group_trx_ids = [int(r["trx_id"]) for r in group_rows if r.get("trx_id") is not None]

            if pnl_summary_exists(client, payload):
                mark_group_trans_as_calc_pnl(client, group_trx_ids)
                stats["groups_already_exists"] += 1
                stats["groups_marked_calc"] += 1
                continue

            insert_pnl_summary(client, payload)
            stats["groups_inserted"] += 1

            mark_group_trans_as_calc_pnl(client, group_trx_ids)
            stats["groups_marked_calc"] += 1

        except Exception as e:
            stats["errors"] += 1
            print(f"[ERROR] Failed to calculate pnl_summary for root BUY trx_id={rb.get('trx_id')}: {e}")

    return stats

def mark_group_trans_as_calc_pnl(client: Client, trx_ids: List[int]) -> None:
    if not trx_ids:
        return

    client.table("trans").update(
        {"trx_is_calc_pnl": True}
    ).in_("trx_id", trx_ids).execute()


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python trading_record.py [record|process]")

    mode = sys.argv[1].strip().lower()

    if mode == "record":
        print(insert_trans_from_trade_folder())
    elif mode == "process":
        print(finalize_closed_positions_from_holds())
    elif mode == "calc":
        print(calc_pnl_summary())
    else:
        raise SystemExit("Invalid mode. Use: record, process, or calc")

if __name__ == "__main__":
    main()
