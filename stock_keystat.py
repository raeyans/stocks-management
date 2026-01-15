#!/usr/bin/env python3

from __future__ import annotations

import copy
import json
import os
import sys
import requests

from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from supabase import Client, create_client

# -------------------------- Config & helpers --------------------------

load_dotenv()

AUTH_TOKEN = (os.getenv("AUTH_TOKEN") or "").strip()
EMITTEN_INFO_API = (os.getenv("EMITTEN_INFO_API_URL") or "").strip()
KEYSTATS_RATIO_API = (os.getenv("KEYSTATS_RATIO_API_URL") or "").strip()

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip()
SUPABASE_SERVICE_ROLE = (os.getenv("SUPABASE_SERVICE_ROLE") or "").strip()

API_TIMEOUT_CONNECT = 5
API_TIMEOUT_READ = 30

HEADERS = {
    "Authorization": f"Bearer {AUTH_TOKEN}",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
}

if AUTH_TOKEN:
    HEADERS["authorization"] = f"Bearer {AUTH_TOKEN}"


def die(msg: str, code: int = 2) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)

def safe_get(d: Any, path: str, default=None):
    cur = d
    for key in path.split("."):
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            return default
    return cur

def fetch_json(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=(API_TIMEOUT_CONNECT, API_TIMEOUT_READ))
    r.raise_for_status()
    if r.headers.get("content-type", "").startswith("application/json"):
        return r.json()
    return {}

_MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

def parse_date_dmy_mon_yyyy(s: Any) -> Optional[date]:
    """Parse strings like '14 Jan 2026'."""
    if not isinstance(s, str):
        return None
    s = s.strip()
    if not s or s in {"-", "NA"}:
        return None
    try:
        return datetime.strptime(s, "%d %b %Y").date()
    except Exception:
        # Fallback: manual parse
        parts = s.split()
        if len(parts) != 3:
            return None
        try:
            d = int(parts[0])
            m = _MONTHS.get(parts[1][:3].title())
            y = int(parts[2])
            if not m:
                return None
            return date(y, m, d)
        except Exception:
            return None

def parse_iso_dt(s: Any) -> Optional[datetime]:
    if not isinstance(s, str):
        return None
    s = s.strip()
    if not s or s in {"-", "NA"}:
        return None
    try:
        # fromisoformat supports +07:00
        return datetime.fromisoformat(s)
    except Exception:
        # last resort
        try:
            from dateutil.parser import isoparse  # type: ignore
            return isoparse(s)
        except Exception:
            return None

_SUFFIX = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000, "T": 1_000_000_000_000}

def parse_number(value: Any) -> Optional[float]:
    """Parse strings like:
    - '1,154 B' -> 1154e9
    - '(94 B)'  -> -94e9
    - '44.24%'  -> 44.24
    - '340.000000' -> 340
    - '3.880' (IDR price format) -> 3880
    Returns float (for ratios/percent) or absolute numeric.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None

    s = value.strip()
    if not s or s in {"-", "NA"}:
        return None

    is_pct = s.endswith("%")
    if is_pct:
        s = s[:-1].strip()

    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1].strip()

    # Remove currency symbols if any
    s = s.replace("IDR", "").replace("Rp", "").strip()

    # Unit suffix (e.g., '105 B')
    mult = 1
    parts = s.split()
    if len(parts) == 2 and parts[1] in _SUFFIX:
        mult = _SUFFIX[parts[1]]
        s = parts[0]

    # Handle Indonesian thousands separator for integer-like numbers (e.g., '3.880')
    # If it contains only digits + separators and the last group has 3 digits, treat '.' as thousands.
    if all(ch.isdigit() or ch in {",", "."} for ch in s) and "." in s and "," not in s:
        groups = s.split(".")
        if len(groups[-1]) == 3:
            s = "".join(groups)

    # Normalize thousands separators
    s = s.replace(",", "")

    try:
        num = float(s) * mult
    except Exception:
        return None

    if negative:
        num = -num

    if is_pct:
        return num

    return num

def to_int(value: Any) -> Optional[int]:
    n = parse_number(value)
    if n is None:
        return None
    try:
        return int(round(n))
    except Exception:
        return None

def to_float(value: Any) -> Optional[float]:
    n = parse_number(value)
    if n is None:
        return None
    return float(n)

def format_idr_thousands(n: Any) -> Optional[str]:
    """3880 -> '3.880'."""
    if n is None:
        return None
    try:
        i = int(round(float(n)))
    except Exception:
        return None
    s = f"{i:,}"  # 3,880
    return s.replace(",", ".")

def format_ratio(n: Optional[float], decimals: int = 2) -> str:
    if n is None:
        return "-"
    return f"{n:.{decimals}f}"

def format_pct(n: Optional[float], decimals: int = 2) -> str:
    if n is None:
        return "-"
    return f"{n:.{decimals}f}%"

def format_compact_id(n: Optional[float]) -> str:
    """Format big numbers to '2,976 B' style."""
    if n is None:
        return "-"
    sign = ""
    x = float(n)
    if x < 0:
        sign = "("
        x = -x

    # Prefer B for stock metrics in IDR
    if x >= 1_000_000_000:
        val = round(x / 1_000_000_000)
        s = f"{val:,} B"
    elif x >= 1_000_000:
        val = round(x / 1_000_000)
        s = f"{val:,} M"
    elif x >= 1_000:
        val = round(x / 1_000)
        s = f"{val:,} K"
    else:
        s = f"{round(x):,}"

    if sign:
        return f"({s})"
    return s

# -------------------------- Supabase helpers --------------------------

def create_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
        die("SUPABASE_URL / SUPABASE_SERVICE_ROLE env vars are required for DB mode.")
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)

def upsert_row(client: Client, table: str, row: Dict[str, Any], on_conflict: str) -> None:
    client.table(table).upsert(row, on_conflict=on_conflict).execute()

# -------------------------- Output helpers --------------------------

def ensure_keystat_dir() -> Path:
    """Ensure ./keystat folder exists (next to this script) and return its Path."""
    out_dir = Path(__file__).resolve().parent / 'keystat'
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir

def write_json_to_keystat(filename: str, data: Dict[str, Any]) -> Path:
    out_dir = ensure_keystat_dir()
    out_path = out_dir / filename
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    return out_path

def write_txt_to_keystat(filename: str, text: str) -> Path:
    out_dir = ensure_keystat_dir()
    out_path = out_dir / filename
    out_path.write_text(text, encoding='utf-8')
    return out_path

# -------------------------- API fetchers --------------------------

def build_url(tpl: str, symbol: str) -> str:
    if "{symbol}" in tpl:
        return tpl.format(symbol=symbol)
    # allow query param style
    if "?" in tpl:
        return f"{tpl}&symbol={symbol}"
    return f"{tpl.rstrip('/')}/{symbol}"

def fetch_api1(symbol: str) -> Dict[str, Any]:
    if not EMITTEN_INFO_API:
        die("EMITTEN_INFO_API env var is required.")
    url = build_url(EMITTEN_INFO_API, symbol)
    payload = fetch_json(url)
    data = payload.get("data")
    return data if isinstance(data, dict) else {}

def fetch_api2(symbol: str) -> Dict[str, Any]:
    if not KEYSTATS_RATIO_API:
        die("KEYSTATS_RATIO_API env var is required.")
    url = build_url(KEYSTATS_RATIO_API, symbol)
    payload = fetch_json(url)
    data = payload.get("data")
    return data if isinstance(data, dict) else {}

# -------------------------- Parsers (non-redundant DB fields) --------------------------

def index_closure_items(closure_fin_items_results: Any) -> Dict[Tuple[str, str], Any]:
    """Return mapping: (keystats_name, fitem.name) -> fitem.value"""
    out: Dict[Tuple[str, str], Any] = {}
    if not isinstance(closure_fin_items_results, list):
        return out

    for group in closure_fin_items_results:
        if not isinstance(group, dict):
            continue
        gname = group.get("keystats_name")
        if not isinstance(gname, str):
            continue
        for item in group.get("fin_name_results") or []:
            if not isinstance(item, dict):
                continue
            fitem = item.get("fitem") or {}
            if not isinstance(fitem, dict):
                continue
            fname = fitem.get("name")
            if not isinstance(fname, str):
                continue
            out[(gname, fname)] = fitem.get("value")
    return out

def extract_financial_year_series(financial_year_parent: Any) -> Dict[str, Any]:
    """Store time-series in a JSON-friendly compact form.

    Output keys: revenue, net_income, eps
    Each value includes most_recent_quarter and list of years.

    For each year, we store:
      - year (int)
      - annualised_value_raw / ttm_value_raw
      - annualised_value_num / ttm_value_num
      - period_values: list of {period, quarter_value_raw, quarter_value_num}
    """
    series: Dict[str, Any] = {}
    groups = safe_get(financial_year_parent, "financial_year_groups", [])
    if not isinstance(groups, list):
        return series

    def key_of(name: str) -> str:
        n = name.strip().lower()
        n = n.replace(" ", "_")
        if n == "net_income":
            return "net_income"
        if n == "revenue":
            return "revenue"
        if n == "eps":
            return "eps"
        return n

    for g in groups:
        if not isinstance(g, dict):
            continue
        fitem_name = g.get("fitem_name")
        if not isinstance(fitem_name, str):
            continue
        k = key_of(fitem_name)
        fy_vals = g.get("financial_year_values")
        if not isinstance(fy_vals, list):
            continue

        years: List[Dict[str, Any]] = []
        for y in fy_vals:
            if not isinstance(y, dict):
                continue
            year_str = y.get("year")
            try:
                year_int = int(year_str)
            except Exception:
                continue

            pv_out: List[Dict[str, Any]] = []
            for pv in y.get("period_values") or []:
                if not isinstance(pv, dict):
                    continue
                pv_out.append({
                    "period": pv.get("period"),
                    "quarter_value_raw": pv.get("quarter_value"),
                    "quarter_value_num": to_float(pv.get("quarter_value")),
                })

            years.append({
                "year": year_int,
                "annualised_value_raw": y.get("annualised_value"),
                "ttm_value_raw": y.get("ttm_value"),
                "annualised_value_num": to_float(y.get("annualised_value")),
                "ttm_value_num": to_float(y.get("ttm_value")),
                "period_values": pv_out,
            })

        series[k] = {
            "most_recent_quarter": g.get("most_recent_quarter"),
            "years": years,
        }

    return series

def pick_latest_ttm(series: Dict[str, Any], key: str) -> Optional[float]:
    blk = series.get(key)
    if not isinstance(blk, dict):
        return None
    years = blk.get("years")
    if not isinstance(years, list) or not years:
        return None
    # pick the max year entry
    best = max((y for y in years if isinstance(y, dict) and isinstance(y.get("year"), int)), key=lambda x: x["year"], default=None)
    if not best:
        return None
    return to_float(best.get("ttm_value_raw"))

def pick_latest_annualised(series: Dict[str, Any], key: str) -> Optional[float]:
    blk = series.get(key)
    if not isinstance(blk, dict):
        return None
    years = blk.get("years")
    if not isinstance(years, list) or not years:
        return None
    best = max((y for y in years if isinstance(y, dict) and isinstance(y.get("year"), int)), key=lambda x: x["year"], default=None)
    if not best:
        return None
    return to_float(best.get("annualised_value_raw"))

def build_db_row(profile: Dict[str, Any], keystats: Dict[str, Any]) -> Dict[str, Any]:
    closure_idx = index_closure_items(keystats.get("closure_fin_items_results"))

    # Profile fields
    symbol = profile.get("symbol")
    if not symbol:
        die("API1 data missing 'symbol'.")

    observed_date = parse_date_dmy_mon_yyyy(profile.get("date"))
    updated_at = parse_iso_dt(profile.get("updated"))

    series = extract_financial_year_series(keystats.get("financial_year_parent") or {})

    # Store "raw" payloads, but stripped from redundant fields so the table truly
    # doesn't persist redundancy anywhere.
    raw_profile = strip_redundant_profile(profile)
    raw_keystats = strip_redundant_keystats(keystats)

    row: Dict[str, Any] = {
        "symbol": symbol,
        "name": profile.get("name"),
        "country": profile.get("country"),
        "exchange": profile.get("exchange"),
        "sector": profile.get("sector"),
        "sub_sector": profile.get("sub_sector"),

        "price": to_int(profile.get("price")),
        "volume": to_int(profile.get("volume")),
        "observed_date": observed_date.isoformat() if observed_date else None,
        "observed_time": profile.get("time"),
        "updated_at": updated_at.isoformat() if updated_at else None,
        "market_status": safe_get(profile, "market_hour.status"),

        # Growth
        "growth_revenue_qyoy_pct": to_float(closure_idx.get(("Growth", "Revenue (Quarter YoY Growth)"))),
        "growth_gross_profit_qyoy_pct": to_float(closure_idx.get(("Growth", "Gross Profit (Quarter YoY Growth)"))),
        "growth_net_income_qyoy_pct": to_float(closure_idx.get(("Growth", "Net Income (Quarter YoY Growth)"))),

        # Margin
        "gross_margin_q_pct": to_float(closure_idx.get(("Profitability", "Gross Profit Margin (Quarter)"))),
        "operating_margin_q_pct": to_float(closure_idx.get(("Profitability", "Operating Profit Margin (Quarter)"))),
        "net_margin_q_pct": to_float(closure_idx.get(("Profitability", "Net Profit Margin (Quarter)"))),

        # Cash flow
        "cfo_ttm": to_int(closure_idx.get(("Cash Flow Statement", "Cash From Operations (TTM)"))),
        "capex_ttm": to_int(closure_idx.get(("Cash Flow Statement", "Capital expenditure (TTM)"))),
        "fcf_ttm": to_int(closure_idx.get(("Cash Flow Statement", "Free cash flow (TTM)"))),

        # Solvency / liquidity (non-redundant)
        "current_ratio_q": to_float(closure_idx.get(("Solvency", "Current Ratio (Quarter)"))),
        "quick_ratio_q": to_float(closure_idx.get(("Solvency", "Quick Ratio (Quarter)"))),
        "interest_coverage_ttm": to_float(closure_idx.get(("Solvency", "Interest Coverage (TTM)"))),
        "altman_z_modified": to_float(closure_idx.get(("Solvency", "Altman Z-Score (Modified)"))),

        # Balance sheet (store raw components; ratios can be derived)
        "cash_q": to_int(closure_idx.get(("Balance Sheet", "Cash (Quarter)"))),
        "total_assets_q": to_int(closure_idx.get(("Balance Sheet", "Total Assets (Quarter)"))),
        "total_liabilities_q": to_int(closure_idx.get(("Balance Sheet", "Total Liabilities (Quarter)"))),
        "total_equity": to_int(closure_idx.get(("Balance Sheet", "Total Equity"))),
        "st_debt_q": to_int(closure_idx.get(("Balance Sheet", "Short-term Debt (Quarter)"))),
        "lt_debt_q": to_int(closure_idx.get(("Balance Sheet", "Long-term Debt (Quarter)"))),
        "total_debt_q": to_int(closure_idx.get(("Balance Sheet", "Total Debt (Quarter)"))),
        "working_capital_q": to_int(closure_idx.get(("Balance Sheet", "Working Capital (Quarter)"))),

        # Profitability / efficiency (not strictly derivable w/o average balances)
        "roa_ttm_pct": to_float(closure_idx.get(("Management Effectiveness", "Return on Assets (TTM)"))),
        "roe_ttm_pct": to_float(closure_idx.get(("Management Effectiveness", "Return on Equity (TTM)"))),
        "roic_ttm_pct": to_float(closure_idx.get(("Management Effectiveness", "Return On Invested Capital (TTM)"))),
        "roce_ttm_pct": to_float(closure_idx.get(("Management Effectiveness", "Return on Capital Employed (TTM)"))),
        "asset_turnover_ttm": to_float(closure_idx.get(("Management Effectiveness", "Asset Turnover (TTM)"))),

        # Valuation (keep the ones we cannot derive from stored primitives)
        "forward_pe": to_float(closure_idx.get(("Current Valuation", "Forward PE Ratio"))),
        "ev_to_ebit_ttm": to_float(closure_idx.get(("Current Valuation", "EV to EBIT (TTM)"))),
        "ev_to_ebitda_ttm": to_float(closure_idx.get(("Current Valuation", "EV to EBITDA (TTM)"))),
        "ihsg_pe_median_ttm": to_float(closure_idx.get(("Current Valuation", "IHSG PE Ratio TTM (Median)"))),

        # Stats (non-redundant)
        "shares_outstanding": to_float(safe_get(keystats, "stats.current_share_outstanding")),
        "free_float_pct": to_float(safe_get(keystats, "stats.free_float")),
        "currency": (safe_get(keystats, "financial_report_currency", []) or [None])[0],

        # Time series
        "financial_year_series": series,

        # Raw payloads for audit/debug (redundancy stripped)
        "raw_profile": raw_profile,
        "raw_keystats": raw_keystats,

        "fetched_at": datetime.now().isoformat(),
    }

    return row

def build_profile_only_row(profile: Dict[str, Any]) -> Dict[str, Any]:
    symbol = profile.get("symbol")
    if not symbol:
        die("API1 data missing 'symbol'.")

    observed_date = parse_date_dmy_mon_yyyy(profile.get("date"))
    updated_at = parse_iso_dt(profile.get("updated"))

    return {
        "symbol": symbol,
        "name": profile.get("name"),
        "country": profile.get("country"),
        "exchange": profile.get("exchange"),
        "sector": profile.get("sector"),
        "sub_sector": profile.get("sub_sector"),
        "price": to_int(profile.get("price")),
        "volume": to_int(profile.get("volume")),
        "observed_date": observed_date.isoformat() if observed_date else None,
        "observed_time": profile.get("time"),
        "updated_at": updated_at.isoformat() if updated_at else None,
        "market_status": safe_get(profile, "market_hour.status"),
        "raw_profile": strip_redundant_profile(profile),
        "fetched_at": datetime.now().isoformat(),
    }

# -------------------------- Output object (merged + redundancies reconstructed) --------------------------

REDUNDANT_PROFILE_FIELDS = {"symbol_2", "symbol_3", "formatted_price"}

REDUNDANT_KEYSTAT_ITEMS = {
    # Current Valuation
    ("Current Valuation", "Current PE Ratio (TTM)"),
    ("Current Valuation", "Current PE Ratio (Annualised)"),
    ("Current Valuation", "Earnings Yield (TTM)"),
    ("Current Valuation", "Current Price to Sales (TTM)"),
    ("Current Valuation", "Current Price to Book Value"),
    ("Current Valuation", "Current Price To Cashflow (TTM)"),
    ("Current Valuation", "Current Price To Free Cashflow (TTM)"),

    # Per Share
    ("Per Share", "Current EPS (TTM)"),
    ("Per Share", "Current EPS (Annualised)"),
    ("Per Share", "Revenue Per Share (TTM)"),
    ("Per Share", "Cash Per Share (Quarter)"),
    ("Per Share", "Current Book Value Per Share"),
    ("Per Share", "Free Cashflow Per Share (TTM)"),

    # Solvency ratios derivable from totals
    ("Solvency", "Debt to Equity Ratio (Quarter)"),
    ("Solvency", "LT Debt/Equity (Quarter)"),
    ("Solvency", "Total Liabilities/Equity (Quarter)"),
    ("Solvency", "Total Debt/Total Assets (Quarter)"),
    ("Solvency", "Financial Leverage (Quarter)"),

    # Balance Sheet
    ("Balance Sheet", "Net Debt (Quarter)"),
}

def strip_redundant_profile(profile: Dict[str, Any]) -> Dict[str, Any]:
    """Remove redundant keys from API1 profile payload before persisting to DB."""
    if not isinstance(profile, dict):
        return {}
    p = copy.deepcopy(profile)
    for k in REDUNDANT_PROFILE_FIELDS:
        p.pop(k, None)
    return p

def strip_redundant_keystats(keystats: Dict[str, Any]) -> Dict[str, Any]:
    """Remove redundant sub-fields/items from API2 payload before persisting to DB.

    We remove:
      - stats.market_cap and stats.enterprise_value
      - closure_fin_items_results entries listed in REDUNDANT_KEYSTAT_ITEMS
    """
    if not isinstance(keystats, dict):
        return {}
    k = copy.deepcopy(keystats)

    stats = k.get("stats")
    if isinstance(stats, dict):
        stats.pop("market_cap", None)
        stats.pop("enterprise_value", None)
        k["stats"] = stats

    cfi = k.get("closure_fin_items_results")
    if isinstance(cfi, list):
        for grp in cfi:
            if not isinstance(grp, dict):
                continue
            gname = grp.get("keystats_name")
            if not isinstance(gname, str):
                continue
            fin_list = grp.get("fin_name_results")
            if not isinstance(fin_list, list):
                continue
            new_fin_list = []
            for item in fin_list:
                if not isinstance(item, dict):
                    continue
                fitem = item.get("fitem")
                if not isinstance(fitem, dict):
                    new_fin_list.append(item)
                    continue
                fname = fitem.get("name")
                if not isinstance(fname, str):
                    new_fin_list.append(item)
                    continue
                if (gname, fname) in REDUNDANT_KEYSTAT_ITEMS:
                    # drop redundant entry
                    continue
                new_fin_list.append(item)
            grp["fin_name_results"] = new_fin_list
        k["closure_fin_items_results"] = cfi

    return k

def _set_fitem_value(closure_fin_items_results: Any, keystats_name: str, fitem_name: str, new_value: str) -> None:
    if not isinstance(closure_fin_items_results, list):
        return
    for group in closure_fin_items_results:
        if not isinstance(group, dict):
            continue
        if group.get("keystats_name") != keystats_name:
            continue
        for item in group.get("fin_name_results") or []:
            if not isinstance(item, dict):
                continue
            fitem = item.get("fitem")
            if not isinstance(fitem, dict):
                continue
            if fitem.get("name") == fitem_name:
                fitem["value"] = new_value

def reconstruct_redundant_fields(profile: Dict[str, Any], keystats: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Return (profile_patched, keystats_patched) where redundant fields are reconstructed
    from other non-redundant fields.

    We intentionally override redundant fields even if they exist in API response.
    """
    p = copy.deepcopy(profile) if isinstance(profile, dict) else {}
    k = copy.deepcopy(keystats) if isinstance(keystats, dict) else {}

    symbol = p.get("symbol")
    price = to_float(p.get("price"))
    if symbol:
        p["symbol_2"] = symbol
        p["symbol_3"] = symbol
    if price is not None:
        p["formatted_price"] = format_idr_thousands(price)

    # Pull base numbers from keystats totals
    closure_idx = index_closure_items(k.get("closure_fin_items_results"))
    cash = to_float(closure_idx.get(("Balance Sheet", "Cash (Quarter)")))
    total_debt = to_float(closure_idx.get(("Balance Sheet", "Total Debt (Quarter)")))
    lt_debt = to_float(closure_idx.get(("Balance Sheet", "Long-term Debt (Quarter)")))
    st_debt = to_float(closure_idx.get(("Balance Sheet", "Short-term Debt (Quarter)")))
    assets = to_float(closure_idx.get(("Balance Sheet", "Total Assets (Quarter)")))
    liabilities = to_float(closure_idx.get(("Balance Sheet", "Total Liabilities (Quarter)")))
    equity = to_float(closure_idx.get(("Balance Sheet", "Total Equity")))

    cfo = to_float(closure_idx.get(("Cash Flow Statement", "Cash From Operations (TTM)")))
    fcf = to_float(closure_idx.get(("Cash Flow Statement", "Free cash flow (TTM)")))

    # Shares from stats
    shares = to_float(safe_get(k, "stats.current_share_outstanding"))

    # TTM & annualised from financial_year_series (canonical for revenue & net income)
    series = extract_financial_year_series(k.get("financial_year_parent") or {})
    revenue_ttm = pick_latest_ttm(series, "revenue")
    revenue_ann = pick_latest_annualised(series, "revenue")
    net_income_ttm = pick_latest_ttm(series, "net_income")
    net_income_ann = pick_latest_annualised(series, "net_income")

    # Derived metrics
    net_debt = None
    if total_debt is not None and cash is not None:
        net_debt = total_debt - cash

    market_cap = None
    if price is not None and shares is not None:
        market_cap = price * shares

    enterprise_value = None
    if market_cap is not None and net_debt is not None:
        enterprise_value = market_cap + net_debt

    eps_ttm = None
    eps_ann = None
    if shares and shares != 0:
        if net_income_ttm is not None:
            eps_ttm = net_income_ttm / shares
        if net_income_ann is not None:
            eps_ann = net_income_ann / shares

    pe_ttm = None
    pe_ann = None
    if price is not None:
        if eps_ttm not in (None, 0):
            pe_ttm = price / eps_ttm
        if eps_ann not in (None, 0):
            pe_ann = price / eps_ann

    earnings_yield = None
    if pe_ttm not in (None, 0):
        earnings_yield = 100.0 / pe_ttm

    ps_ttm = None
    if market_cap is not None and revenue_ttm not in (None, 0):
        ps_ttm = market_cap / revenue_ttm

    pbv = None
    if market_cap is not None and equity not in (None, 0):
        pbv = market_cap / equity

    pcf = None
    if market_cap is not None and cfo not in (None, 0):
        pcf = market_cap / cfo

    pfcf = None
    if market_cap is not None and fcf not in (None, 0):
        pfcf = market_cap / fcf

    rps_ttm = None
    if revenue_ttm is not None and shares not in (None, 0):
        rps_ttm = revenue_ttm / shares

    cash_ps = None
    if cash is not None and shares not in (None, 0):
        cash_ps = cash / shares

    bvps = None
    if equity is not None and shares not in (None, 0):
        bvps = equity / shares

    fcf_ps = None
    if fcf is not None and shares not in (None, 0):
        fcf_ps = fcf / shares

    dte = None
    ltdte = None
    lte = None
    dta = None
    flv = None
    if equity not in (None, 0):
        if total_debt is not None:
            dte = total_debt / equity
        if lt_debt is not None:
            ltdte = lt_debt / equity
        if liabilities is not None:
            lte = liabilities / equity
        if assets not in (None, 0):
            flv = assets / equity
    if assets not in (None, 0) and total_debt is not None:
        dta = total_debt / assets

    # Patch closure_fin_items_results values
    cfi = k.get("closure_fin_items_results")

    # Current Valuation
    _set_fitem_value(cfi, "Current Valuation", "Current PE Ratio (TTM)", format_ratio(pe_ttm, 2))
    _set_fitem_value(cfi, "Current Valuation", "Current PE Ratio (Annualised)", format_ratio(pe_ann, 2))
    _set_fitem_value(cfi, "Current Valuation", "Earnings Yield (TTM)", format_pct(earnings_yield, 2))
    _set_fitem_value(cfi, "Current Valuation", "Current Price to Sales (TTM)", format_ratio(ps_ttm, 2))
    _set_fitem_value(cfi, "Current Valuation", "Current Price to Book Value", format_ratio(pbv, 2))
    _set_fitem_value(cfi, "Current Valuation", "Current Price To Cashflow (TTM)", format_ratio(pcf, 2))
    _set_fitem_value(cfi, "Current Valuation", "Current Price To Free Cashflow (TTM)", format_ratio(pfcf, 2))

    # Per Share
    _set_fitem_value(cfi, "Per Share", "Current EPS (TTM)", format_ratio(eps_ttm, 2))
    _set_fitem_value(cfi, "Per Share", "Current EPS (Annualised)", format_ratio(eps_ann, 2))
    _set_fitem_value(cfi, "Per Share", "Revenue Per Share (TTM)", format_ratio(rps_ttm, 2))
    _set_fitem_value(cfi, "Per Share", "Cash Per Share (Quarter)", format_ratio(cash_ps, 2))
    _set_fitem_value(cfi, "Per Share", "Current Book Value Per Share", format_ratio(bvps, 2))
    _set_fitem_value(cfi, "Per Share", "Free Cashflow Per Share (TTM)", format_ratio(fcf_ps, 2))

    # Solvency (ratios derivable)
    _set_fitem_value(cfi, "Solvency", "Debt to Equity Ratio (Quarter)", format_ratio(dte, 2))
    _set_fitem_value(cfi, "Solvency", "LT Debt/Equity (Quarter)", format_ratio(ltdte, 2))
    _set_fitem_value(cfi, "Solvency", "Total Liabilities/Equity (Quarter)", format_ratio(lte, 2))
    _set_fitem_value(cfi, "Solvency", "Total Debt/Total Assets (Quarter)", format_ratio(dta, 2))
    _set_fitem_value(cfi, "Solvency", "Financial Leverage (Quarter)", format_ratio(flv, 2))

    # Balance sheet
    _set_fitem_value(cfi, "Balance Sheet", "Net Debt (Quarter)", format_compact_id(net_debt))

    # Patch stats (market cap & EV are redundant vs price/shares + net debt)
    stats = k.get("stats")
    if isinstance(stats, dict):
        stats["market_cap"] = format_compact_id(market_cap)
        stats["enterprise_value"] = format_compact_id(enterprise_value)

    # Keep patched closure items
    k["closure_fin_items_results"] = cfi

    return p, k

def merge_one_object(profile: Dict[str, Any], keystats: Dict[str, Any]) -> Dict[str, Any]:
    """No separate api1/api2 objects: a single merged object."""
    out: Dict[str, Any] = {}
    if isinstance(profile, dict):
        out.update(profile)
    if isinstance(keystats, dict):
        # merge selected top-level keys from API2 "data"
        for k in [
            "closure_fin_items_results",
            "financial_year_parent",
            "stats",
            "dividend_group",
            "financial_report_currency",
            "info",
        ]:
            if k in keystats:
                out[k] = keystats.get(k)
    return out

def render_txt(merged_obj: Dict[str, Any]) -> str:
    """Human-readable text; not an object."""
    lines: List[str] = []

    def add(k: str, v: Any) -> None:
        if isinstance(v, (dict, list)):
            lines.append(f"{k}: {json.dumps(v, ensure_ascii=False)}")
        else:
            lines.append(f"{k}: {v}")

    # Header / core
    for key in [
        "symbol", "name", "country", "exchange", "sector", "sub_sector", "price", "volume",
        # "symbol", "symbol_2", "symbol_3", "name", "country", "exchange", "sector", "sub_sector",
        # "price", "formatted_price", "volume", "date", "time", "updated",
    ]:
        if key in merged_obj:
            add(key, merged_obj.get(key))

    # mh = merged_obj.get("market_hour")
    # if isinstance(mh, dict):
    #     add("market_hour.status", mh.get("status"))

    stats = merged_obj.get("stats")
    if isinstance(stats, dict):
        # lines.append("\n[stats]")
        for k in ["current_share_outstanding", "free_float", "market_cap", "enterprise_value"]:
            if k in stats:
                add(k, stats.get(k))

    # Closure items in sections
    cfi = merged_obj.get("closure_fin_items_results")
    if isinstance(cfi, list):
        # lines.append("\n[closure_fin_items_results]")
        for grp in cfi:
            if not isinstance(grp, dict):
                continue
            gname = grp.get("keystats_name")
            if not isinstance(gname, str):
                continue
            lines.append(f"\n## {gname}")
            for item in grp.get("fin_name_results") or []:
                if not isinstance(item, dict):
                    continue
                fitem = item.get("fitem")
                if not isinstance(fitem, dict):
                    continue
                nm = fitem.get("name")
                val = fitem.get("value")
                if nm:
                    lines.append(f"- {nm}: {val}")

    # Financial year series (raw)
    # fyp = merged_obj.get("financial_year_parent")
    # if isinstance(fyp, dict):
    #     lines.append("\n[financial_year_parent]")
    #     lines.append(json.dumps(fyp, ensure_ascii=False))

    return "\n".join(lines) + "\n"

# -------------------------- CLI --------------------------

def parse_cli(argv: List[str]) -> Tuple[str, str]:
    """Returns (mode, symbol)

    Supported:
      - python3 stock_keystat.py
      - python3 stock_keystat.py json
      - python3 stock_keystat.py txt

    Optional symbol:
      - python3 stock_keystat.py ANTM
      - python3 stock_keystat.py json ANTM
    """
    mode = "db"
    symbol = "RAJA"

    if len(argv) >= 2:
        a1 = argv[1].strip().lower()
        if a1 in {"json", "txt"}:
            mode = a1
            if len(argv) >= 3:
                symbol = argv[2].strip().upper()
        else:
            # treat as symbol
            symbol = argv[1].strip().upper()

    return mode, symbol

def main() -> None:
    mode, symbol = parse_cli(sys.argv)

    profile = fetch_api1(symbol)

    if mode == "db":
        client = create_supabase_client()
        # Save after API1 (profile only)
        row1 = build_profile_only_row(profile)
        upsert_row(client, "stocks_keystat", row1, on_conflict="symbol,updated_at")

    keystats = fetch_api2(symbol)

    # Build patched outputs (redundant fields reconstructed)
    profile_patched, keystats_patched = reconstruct_redundant_fields(profile, keystats)
    merged_obj = merge_one_object(profile_patched, keystats_patched)

    if mode == "db":
        row2 = build_db_row(profile_patched, keystats_patched)
        upsert_row(client, "stocks_keystat", row2, on_conflict="symbol,updated_at")
        print(f"OK: saved to Supabase table 'stocks_keystat' for {symbol}")
        return

    # json/txt modes
    obs_date = merged_obj.get("date") or datetime.now().strftime("%Y-%m-%d")
    safe_date = str(obs_date).replace(" ", "_").replace(":", "-")

    if mode == "json":
        fname = f"stock_keystat_{symbol}_{safe_date}.json"
        out_path = write_json_to_keystat(fname, merged_obj)
        print(f"OK: wrote {out_path}")
        return

    if mode == "txt":
        fname = f"stock_keystat_{symbol}_{safe_date}.txt"
        out_path = write_txt_to_keystat(fname, render_txt(merged_obj))
        print(f"OK: wrote {out_path}")
        return

    die(f"Unknown mode: {mode}")

if __name__ == "__main__":
    main()
