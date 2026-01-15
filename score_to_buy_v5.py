"""
Stock Score-to-Buy Framework v5
===============================

Fokus: Spike Hunter dengan label "top X% return" (percentile),
lebih dekat ke daftar top gainer harian.
"""

from __future__ import annotations
import os, glob
from typing import List, Dict, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from sklearn.linear_model import LinearRegression, LogisticRegression
    SKLEARN_AVAILABLE = True
except Exception:
    SKLEARN_AVAILABLE = False


# =========================
# CONFIG YANG BISA DIUBAH
# =========================

# Mode:
# - "balanced"      -> skor expected return (Score_to_buy)
# - "spike_hunter"  -> skor spike (Score_to_buy_spike)
# - "both"          -> hitung keduanya, ranking pakai spike
MODE = "spike_hunter"

DATA_DIR = "data"
DATA_FILE_PATTERN = "stocks_data_*.xlsx"
SHEET_NAME = "Stocks"

FORECAST_HORIZON = 1   # 1 atau 2 hari perdagangan

# Cara definisikan spike:
# - "absolute"   -> spike_threshold absolut (mis. +15%)
# - "percentile" -> spike = top SPIKE_PERCENTILE return historis
SPIKE_MODE = "percentile"
SPIKE_THRESHOLD = 0.15      # dipakai kalau SPIKE_MODE == "absolute"
SPIKE_PERCENTILE = 0.98     # top 2% return = spike

TOP_N = 15

# TARGET_DATE:
# - None → tanggal terakhir di data
# - "YYYY-MM-DD" → tanggal spesifik
TARGET_DATE_STR: Optional[str] = None


# ======================
# VALIDASI BASIC CONFIG
# ======================

if FORECAST_HORIZON not in (1, 2):
    raise ValueError("FORECAST_HORIZON hanya boleh 1 atau 2.")

if MODE not in ("balanced", "spike_hunter", "both"):
    raise ValueError("MODE harus 'balanced', 'spike_hunter', atau 'both'.")

if SPIKE_MODE not in ("absolute", "percentile"):
    raise ValueError("SPIKE_MODE harus 'absolute' atau 'percentile'.")

if MODE in ("spike_hunter", "both") and not SKLEARN_AVAILABLE:
    raise ImportError("MODE spike_hunter membutuhkan scikit-learn.")


# =================================
# FEATURE ENGINEERING PER TICKER
# =================================

def _rolling_regression_slope(series: pd.Series, window: int = 20) -> pd.Series:
    values = series.values
    n = len(values)
    slopes = np.full(n, np.nan)

    x = np.arange(window)
    x_mean = x.mean()
    denom = np.sum((x - x_mean) ** 2)

    for i in range(window - 1, n):
        y = values[i - window + 1 : i + 1]
        if np.isnan(y).any():
            continue
        y_mean = y.mean()
        num = np.sum((x - x_mean) * (y - y_mean))
        slopes[i] = num / denom

    return pd.Series(slopes, index=series.index)


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("date").reset_index(drop=True).copy()

    if "value" not in df.columns:
        df["value"] = df["close"] * df["volume"]

    df["log_ret"] = np.log(df["close"] / df["close"].shift(1))

    df["ma_short"] = df["close"].rolling(10).mean()
    df["ma_long"]  = df["close"].rolling(20).mean()
    df["ma_spread"] = df["ma_short"] - df["ma_long"]

    df["momentum_10"] = df["close"] / df["close"].shift(10) - 1

    df["trend_slope"] = _rolling_regression_slope(df["close"], window=20)

    df["vol_20"] = df["log_ret"].rolling(20).std() * np.sqrt(252)

    df["avg_vol_20"] = df["volume"].rolling(20).mean()
    df["avg_val_20"] = df["value"].rolling(20).mean()
    df["rvol_20"] = df["volume"] / df["avg_vol_20"]
    df["rval_20"] = df["value"]  / df["avg_val_20"]

    df["avg_freq_20"] = df["freq"].rolling(20).mean()
    df["rfreq_20"] = df["freq"] / df["avg_freq_20"]
    df["avg_trade_size"] = df["volume"] / df["freq"].replace(0, np.nan)

    df["hh_20"] = df["high"].rolling(20).max()
    df["breakout_20"] = (
        (df["close"] > df["hh_20"].shift(1)) & (df["rvol_20"] > 1.5)
    ).astype(int)

    return df


def normalize_features_per_ticker(
    df: pd.DataFrame, feature_cols: List[str], lookback: int = 252
) -> pd.DataFrame:
    df = df.copy()

    def _norm(g: pd.DataFrame) -> pd.DataFrame:
        for col in feature_cols:
            m = g[col].rolling(lookback, min_periods=30).mean()
            s = g[col].rolling(lookback, min_periods=30).std()
            g[f"{col}_z"] = (g[col] - m) / s.replace(0, np.nan)
        return g

    return df.groupby("ticker", group_keys=False).apply(_norm)


def z_to_score(z: pd.Series, k: float = 1.0) -> pd.Series:
    z_clip = z.clip(-10, 10)
    return 100.0 * (1.0 / (1.0 + np.exp(-k * z_clip)))


def compute_pillar_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    trend = []
    if "trend_slope_z" in df.columns:
        trend.append(z_to_score(df["trend_slope_z"]) * 0.4)
    if "momentum_10_z" in df.columns:
        trend.append(z_to_score(df["momentum_10_z"]) * 0.3)
    if "ma_spread_z" in df.columns:
        trend.append(z_to_score(df["ma_spread_z"]) * 0.3)
    df["TrendScore"] = np.nansum(trend, axis=0) if trend else np.nan

    vol = []
    if "rvol_20_z" in df.columns:
        vol.append(z_to_score(df["rvol_20_z"]) * 0.5)
    if "rval_20_z" in df.columns:
        vol.append(z_to_score(df["rval_20_z"]) * 0.5)
    df["VolumeScore"] = np.nansum(vol, axis=0) if vol else np.nan

    liq = []
    if "rfreq_20_z" in df.columns:
        liq.append(z_to_score(df["rfreq_20_z"]) * 0.6)
    if "avg_trade_size_z" in df.columns:
        liq.append(z_to_score(df["avg_trade_size_z"]) * 0.4)
    df["LiquidityScore"] = np.nansum(liq, axis=0) if liq else np.nan

    if "vol_20_z" in df.columns:
        df["RiskScore"] = z_to_score(-df["vol_20_z"])
    else:
        df["RiskScore"] = np.nan

    if "breakout_20" in df.columns:
        df["EventScore"] = 50.0 + 30.0 * df["breakout_20"]
    else:
        df["EventScore"] = 50.0

    return df


# =============================
# GLOBAL LINEAR REGRESSION
# =============================

def _fit_global_linear_regression(
    df: pd.DataFrame, feature_cols: List[str], horizon: int = 1
) -> Tuple[LinearRegression, float, List[str]]:
    if not SKLEARN_AVAILABLE:
        raise ImportError("Butuh scikit-learn.")

    df = df.copy()
    df["future_log_ret"] = (
        df.groupby("ticker")["close"]
        .shift(-horizon)
        .div(df["close"])
        .pipe(np.log)
    )

    feature_lag_cols = [f"{c}_lag1" for c in feature_cols]
    for c, lc in zip(feature_cols, feature_lag_cols):
        df[lc] = df.groupby("ticker")[c].shift(1)

    cols_needed = feature_lag_cols + ["future_log_ret"]
    data = df[cols_needed].dropna()
    if len(data) < 50:
        raise ValueError("Data global tidak cukup untuk regresi (balanced).")

    X = data[feature_lag_cols].values
    y = data["future_log_ret"].values

    model = LinearRegression()
    model.fit(X, y)

    y_hat = model.predict(X)
    sigma = float(np.std(y - y_hat, ddof=1))

    return model, sigma, feature_lag_cols


def add_global_forecast_score(
    df: pd.DataFrame,
    model: LinearRegression,
    feature_lag_cols: List[str],
    sigma: float,
) -> pd.DataFrame:
    df = df.copy()
    if sigma <= 0 or np.isnan(sigma):
        sigma = 1.0

    mask = df[feature_lag_cols].notna().all(axis=1)
    y_hat = np.full(len(df), np.nan)
    if mask.any():
        X = df.loc[mask, feature_lag_cols].values
        y_hat[mask.values] = model.predict(X)

    df["y_hat"] = y_hat
    df["y_hat_sharpe"] = df["y_hat"] / sigma
    df["ForecastScore"] = z_to_score(df["y_hat_sharpe"])
    return df


def compute_score_to_buy_balanced(
    df: pd.DataFrame,
    weights: Dict[str, float] | None = None,
    adjust_with_risk: bool = False,
) -> pd.DataFrame:
    df = df.copy()
    w = {
        "trend": 0.25,
        "volume": 0.20,
        "liq": 0.15,
        "risk": 0.10,
        "fcst": 0.25,
        "event": 0.05,
    }
    if weights:
        w.update(weights)

    df["Score_to_buy_raw"] = (
        w["trend"] * df.get("TrendScore", np.nan)
        + w["volume"] * df.get("VolumeScore", np.nan)
        + w["liq"]   * df.get("LiquidityScore", np.nan)
        + w["risk"]  * df.get("RiskScore", np.nan)
        + w["fcst"]  * df.get("ForecastScore", np.nan)
        + w["event"] * df.get("EventScore", np.nan)
    )

    if adjust_with_risk and "RiskScore" in df.columns:
        df["Score_to_buy"] = df["Score_to_buy_raw"] * (df["RiskScore"] / 100.0)
    else:
        df["Score_to_buy"] = df["Score_to_buy_raw"]

    return df


# ========================
# GLOBAL SPIKE CLASSIFIER
# ========================

def _build_spike_labels(
    df: pd.DataFrame,
    feature_cols: List[str],
    horizon: int,
    spike_mode: str,
    spike_threshold: float,
    spike_percentile: float,
) -> Tuple[pd.DataFrame, List[str], float]:
    df = df.copy()
    df["future_ret_h"] = (
        df.groupby("ticker")["close"]
        .shift(-horizon)
        .div(df["close"])
        .sub(1.0)
    )

    # pilih threshold
    if spike_mode == "absolute":
        thr = spike_threshold
    else:
        valid = df["future_ret_h"].dropna()
        if len(valid) < 50:
            raise ValueError("Data global kurang untuk hitung percentile.")
        thr = float(np.quantile(valid, spike_percentile))

    df["spike_label"] = (df["future_ret_h"] >= thr).astype(int)

    feature_lag_cols = [f"{c}_lag1" for c in feature_cols]
    for c, lc in zip(feature_cols, feature_lag_cols):
        df[lc] = df.groupby("ticker")[c].shift(1)

    return df, feature_lag_cols, thr


def fit_global_spike_classifier(
    df: pd.DataFrame,
    feature_cols: List[str],
    horizon: int = 1,
    spike_mode: str = SPIKE_MODE,
    spike_threshold: float = SPIKE_THRESHOLD,
    spike_percentile: float = SPIKE_PERCENTILE,
) -> Tuple[LogisticRegression, List[str], float]:
    if not SKLEARN_AVAILABLE:
        raise ImportError("Butuh scikit-learn.")

    df_lab, feature_lag_cols, eff_thr = _build_spike_labels(
        df, feature_cols, horizon, spike_mode, spike_threshold, spike_percentile
    )

    cols_needed = feature_lag_cols + ["spike_label"]
    data = df_lab[cols_needed].dropna()
    if len(data) < 50:
        raise ValueError("Data global tidak cukup untuk spike classifier.")

    y = data["spike_label"].values
    if len(np.unique(y)) < 2:
        raise ValueError(
            "Label spike global hanya 1 kelas."
        )

    X = data[feature_lag_cols].values

    clf = LogisticRegression(max_iter=2000, class_weight="balanced")
    clf.fit(X, y)
    return clf, feature_lag_cols, eff_thr


def add_global_spike_score(
    df: pd.DataFrame,
    clf: LogisticRegression,
    feature_lag_cols: List[str],
) -> pd.DataFrame:
    """
    Tambahkan SpikeProb & SpikeScore ke df.

    Catatan penting:
    - Pastikan kolom fitur lag (mis. trend_slope_z_lag1) ADA.
      Kalau belum ada, kita bikin dari base col-nya (mis. trend_slope_z)
      dengan shift(1) per ticker → konsisten dengan saat training.
    """
    df = df.copy()

    # Pastikan fitur lag tersedia
    for lag_col in feature_lag_cols:
        if lag_col not in df.columns:
            base_col = lag_col.replace("_lag1", "")
            if base_col not in df.columns:
                raise KeyError(
                    f"Kolom dasar '{base_col}' untuk '{lag_col}' tidak ditemukan di df."
                )
            df[lag_col] = df.groupby("ticker")[base_col].shift(1)

    # Baru setelah semua lag siap, kita hitung probabilitas spike
    mask = df[feature_lag_cols].notna().all(axis=1)

    spike_prob = np.full(len(df), np.nan)
    if mask.any():
        X = df.loc[mask, feature_lag_cols].values
        prob = clf.predict_proba(X)[:, 1]
        spike_prob[mask.values] = prob

    df["SpikeProb"] = spike_prob
    df["SpikeScore"] = 100.0 * df["SpikeProb"]
    return df

def compute_score_to_buy_spike(
    df: pd.DataFrame,
    weights: Dict[str, float] | None = None,
    adjust_with_risk: bool = False,
) -> pd.DataFrame:
    df = df.copy()
    w = {
        "spike": 0.60,
        "trend": 0.15,
        "volume": 0.15,
        "liq":   0.05,
        "risk":  0.05,
        "event": 0.00,
    }
    if weights:
        w.update(weights)

    df["Score_to_buy_spike_raw"] = (
        w["spike"] * df.get("SpikeScore", np.nan)
        + w["trend"] * df.get("TrendScore", np.nan)
        + w["volume"] * df.get("VolumeScore", np.nan)
        + w["liq"]   * df.get("LiquidityScore", np.nan)
        + w["risk"]  * df.get("RiskScore", np.nan)
        + w["event"] * df.get("EventScore", np.nan)
    )

    if adjust_with_risk and "RiskScore" in df.columns:
        df["Score_to_buy_spike"] = df["Score_to_buy_spike_raw"] * (df["RiskScore"] / 100.0)
    else:
        df["Score_to_buy_spike"] = df["Score_to_buy_spike_raw"]

    return df


# ===========================
# LOAD DATA & BUILD PANEL
# ===========================

def find_latest_data_file(
    data_dir: str = DATA_DIR, pattern: str = DATA_FILE_PATTERN
) -> str:
    files = glob.glob(os.path.join(data_dir, pattern))
    if not files:
        raise FileNotFoundError(f"Tidak ada file '{pattern}' di folder '{data_dir}'.")
    files = sorted(files, key=os.path.getmtime, reverse=True)
    return files[0]


def load_and_prepare_data(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=SHEET_NAME)

    rename_map = {
        "Date": "date",
        "Ticker": "ticker",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
        "Transaction Value": "value",
        "Frequency": "freq",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    req = ["date", "ticker", "open", "high", "low", "close", "volume", "freq"]
    miss = [c for c in req if c not in df.columns]
    if miss:
        raise ValueError(f"Kolom wajib hilang: {miss}")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df


def build_feature_panel(df_all: pd.DataFrame) -> pd.DataFrame:
    dfs = []
    for ticker, g in df_all.groupby("ticker"):
        g_feat = compute_features(g)
        g_feat["ticker"] = ticker
        dfs.append(g_feat)
    df_feat = pd.concat(dfs, ignore_index=True)

    norm_cols = [
        "trend_slope",
        "momentum_10",
        "ma_spread",
        "rvol_20",
        "rval_20",
        "rfreq_20",
        "avg_trade_size",
        "vol_20",
    ]
    df_feat = normalize_features_per_ticker(df_feat, norm_cols, lookback=252)
    df_feat = compute_pillar_scores(df_feat)
    return df_feat


def get_top_n_for_date(
    df: pd.DataFrame, target_date: pd.Timestamp, top_n: int, score_col: str
) -> pd.DataFrame:
    sub = df[df["date"] == target_date].copy()
    if sub.empty:
        raise ValueError(f"Tidak ada data untuk {target_date.date()}")
    if score_col not in sub.columns:
        raise ValueError(f"Kolom {score_col} tidak ada.")
    return sub.sort_values(score_col, ascending=False).head(top_n)


# ==============
# MAIN
# ==============

def main():
    print("=== Stock Score-to-Buy Framework v5 (Spike Percentile) ===")
    print(f"MODE        : {MODE}")
    print(f"SPIKE_MODE  : {SPIKE_MODE}")
    if SPIKE_MODE == "absolute":
        print(f"THRESHOLD   : {SPIKE_THRESHOLD*100:.1f}%")
    else:
        print(f"PERCENTILE  : {SPIKE_PERCENTILE*100:.1f}% (top quantile)")
    print(f"HORIZON     : {FORECAST_HORIZON} hari")
    print(f"TOP_N       : {TOP_N}")

    path = find_latest_data_file()
    print(f"File data   : {path}")

    df_all = load_and_prepare_data(path)
    min_date, max_date = df_all["date"].min(), df_all["date"].max()
    print(f"Rentang data: {min_date.date()} s.d. {max_date.date()}")

    print("Bangun panel fitur...")
    df_feat = build_feature_panel(df_all)

    # target date
    if TARGET_DATE_STR is None:
        target_date = max_date
        print(f"TARGET_DATE : {target_date.date()} (max data)")
    else:
        target_date = pd.to_datetime(TARGET_DATE_STR)
        print(f"TARGET_DATE : {target_date.date()}")
        if target_date > max_date:
            raise ValueError("TARGET_DATE di luar range data.")

    model_features = [
        "trend_slope_z",
        "momentum_10_z",
        "ma_spread_z",
        "rvol_20_z",
        "rval_20_z",
        "rfreq_20_z",
        "vol_20_z",
    ]
    feat_avail = [c for c in model_features if c in df_feat.columns]

    # balanced (opsional)
    if MODE in ("balanced", "both"):
        print("Latih global regression (balanced)...")
        reg_model, sigma, reg_cols = _fit_global_linear_regression(
            df_feat, feat_avail, horizon=FORECAST_HORIZON
        )
        df_feat = add_global_forecast_score(df_feat, reg_model, reg_cols, sigma)
        df_feat = compute_score_to_buy_balanced(df_feat, adjust_with_risk=True)

    # spike hunter (opsional)
    if MODE in ("spike_hunter", "both"):
        print("Latih global spike classifier...")
        clf, spike_cols, eff_thr = fit_global_spike_classifier(
            df_feat,
            feat_avail,
            horizon=FORECAST_HORIZON,
            spike_mode=SPIKE_MODE,
            spike_threshold=SPIKE_THRESHOLD,
            spike_percentile=SPIKE_PERCENTILE,
        )
        if SPIKE_MODE == "percentile":
            print(f"Threshold efektif dari percentile: {eff_thr*100:.2f}%")
        else:
            print(f"Threshold spike absolut: {eff_thr*100:.2f}%")

        df_feat = add_global_spike_score(df_feat, clf, spike_cols)
        df_feat = compute_score_to_buy_spike(df_feat, adjust_with_risk=True)

    if MODE == "balanced":
        score_col = "Score_to_buy"
    else:
        score_col = "Score_to_buy_spike"

    print(f"Ranking pakai skor: {score_col}")

    top_df = get_top_n_for_date(df_feat, target_date, TOP_N, score_col)

    print(f"\n=== TOP_{TOP_N} saham dengan {score_col} tertinggi ===")
    print(top_df[["date", "ticker", "close", score_col]].to_string(index=False))

    os.makedirs(DATA_DIR, exist_ok=True)
    all_path = os.path.join(DATA_DIR, f"score_to_buy_v5_all_{max_date.date()}.xlsx")
    top_path = os.path.join(
        DATA_DIR, f"score_to_buy_v5_top{TOP_N}_{score_col}_{target_date.date()}.xlsx"
    )
    df_feat.to_excel(all_path, index=False)
    top_df.to_excel(top_path, index=False)
    print(f"\nSimpan full panel : {all_path}")
    print(f"Simpan TOP_{TOP_N}: {top_path}")
    print("Selesai.")


if __name__ == "__main__":
    main()
