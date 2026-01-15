import re
import glob
import os
import pdfplumber
import pandas as pd

# =========================
# HARD-CODED INPUT (ubah kalau perlu)
# =========================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "foreign")

if not os.path.isdir(DATA_DIR):
    raise FileNotFoundError(f"Folder tidak ditemukan: {DATA_DIR}")

pdfs = sorted(glob.glob(os.path.join(DATA_DIR, "*.pdf")))

# Pastikan hanya ada 1 file PDF
if len(pdfs) == 0:
    raise FileNotFoundError(f"Tidak ada file PDF di folder: {DATA_DIR}")
if len(pdfs) > 1:
    names = "\n - " + "\n - ".join(os.path.basename(p) for p in pdfs)
    raise RuntimeError(
        "Harus ada tepat 1 file PDF di folder 'foreign'. "
        f"Saat ini ditemukan {len(pdfs)} file:{names}"
    )

PDF_PATH = pdfs[0]

# =========================
# Helper formatting (ID style)
# =========================
def fmt_int_id(n: int) -> str:
    # 10607300 -> "10.607.300"
    return f"{n:,}".replace(",", ".")

def fmt_ratio_id(x: float) -> str:
    # 20.8149 -> "20,81 x"
    return f"{x:.2f}".replace(".", ",") + " x"

def to_int(num_str: str) -> int:
    # "10,607,300" -> 10607300
    return int(num_str.replace(",", "").strip())

# =========================
# Extract & parse rows from PDF text
# Expected row pattern (from Stockbit PDF):
# "12 SGER 10,097,700 509,600 10,607,300"
#        Code  NetForeign  Sell   Buy
# =========================
row_re = re.compile(
    r"^\s*(\d+)\s+([A-Z0-9]+)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)\s*$"
)

records = []
with pdfplumber.open(PDF_PATH) as pdf:
    for page in pdf.pages:
        text = page.extract_text() or ""
        for line in text.splitlines():
            m = row_re.match(line)
            if not m:
                continue
            no = int(m.group(1))
            code = m.group(2).strip()
            net_foreign = to_int(m.group(3))
            foreign_sell = to_int(m.group(4))
            foreign_buy = to_int(m.group(5))

            records.append(
                {
                    "No": no,
                    "Code": code,
                    "NetForeign": net_foreign,
                    "ForeignSell": foreign_sell,
                    "ForeignBuy": foreign_buy,
                }
            )

df = pd.DataFrame(records)
if df.empty:
    raise ValueError("Gagal parsing tabel dari PDF. Pola baris tidak terdeteksi.")

# =========================
# FILTER RULES (sesuai kebutuhan Anda)
# 1) Kode saham reguler: 4 huruf kapital A-Z
# 2) ForeignSell > 0 (hindari division by zero)
# 3) NetForeign > 0 (opsional, tapi sesuai contoh Anda)
# =========================
df = df[df["Code"].str.match(r"^[A-Z]{4}$", na=False)]
df = df[df["ForeignSell"] > 0]
df = df[df["NetForeign"] > 0]  # bila mau net buy saja (sesuai contoh)

# Validasi (opsional): cek konsistensi NetForeign = Buy - Sell
# Jika ada mismatch, tetap gunakan Buy/Sell dari PDF sebagai sumber utama.
df["CheckNet"] = df["ForeignBuy"] - df["ForeignSell"]
df["NetMismatch"] = (df["CheckNet"] != df["NetForeign"])

# =========================
# Ambil TOP 20 berdasarkan NetForeign (desc) -> universe seperti contoh Anda
# =========================
top20_universe = df.sort_values("NetForeign", ascending=False).head(20).copy()

# Hitung rasio Buy/Sell
top20_universe["RatioBuySell"] = top20_universe["ForeignBuy"] / top20_universe["ForeignSell"]

# Ranking berdasarkan rasio (desc)
top20_ranked = top20_universe.sort_values("RatioBuySell", ascending=False).reset_index(drop=True)
top20_ranked.insert(0, "Peringkat", range(1, len(top20_ranked) + 1))

# =========================
# Format output seperti contoh (titik ribuan, koma desimal)
# =========================
out = top20_ranked[["Peringkat", "Code", "ForeignBuy", "ForeignSell", "RatioBuySell"]].copy()
out.rename(
    columns={
        "Code": "Kode",
        "ForeignBuy": "Buy (Saham)",
        "ForeignSell": "Sell (Saham)",
        "RatioBuySell": "Rasio Buy/Sell",
    },
    inplace=True,
)

out["Buy (Saham)"] = out["Buy (Saham)"].map(fmt_int_id)
out["Sell (Saham)"] = out["Sell (Saham)"].map(fmt_int_id)
out["Rasio Buy/Sell"] = out["Rasio Buy/Sell"].map(fmt_ratio_id)

print("\nPeringkat Emiten Berdasarkan Rasio Buy : Sell")
print(out.to_string(index=False))

# Simpan ke Excel
OUT_DIR = os.path.join(SCRIPT_DIR, "foreign-out")
os.makedirs(OUT_DIR, exist_ok=True)

OUT_XLSX = os.path.join(OUT_DIR, "foreign_midday_top20_ratio.xlsx")
out.to_excel(OUT_XLSX, index=False)  # butuh openpyxl terinstall (umumnya sudah)
print(f"\nSaved: {OUT_XLSX}")

# (Opsional) info mismatch net
mismatch_count = int(top20_universe["NetMismatch"].sum())
if mismatch_count:
    print(f"\nCatatan: ditemukan {mismatch_count} baris NetForeign != (Buy-Sell) dalam universe Top20.")
