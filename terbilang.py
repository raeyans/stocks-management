#!/usr/bin/env python3
# terbilang.py

from __future__ import annotations
import sys
import re


MAX_N = 999_999_999_999_999  # 999 triliun


def terbilang(n: int) -> str:
    if n < 0:
        return "minus " + terbilang(-n)
    if n == 0:
        return "nol"
    if n > MAX_N:
        raise ValueError("Maksimum adalah 999.999.999.999.999 (999 triliun).")

    angka = ["", "satu", "dua", "tiga", "empat", "lima", "enam", "tujuh", "delapan", "sembilan"]

    def under_1000(x: int) -> str:
        parts: list[str] = []
        ratus = x // 100
        sisa = x % 100

        if ratus:
            parts.append("seratus" if ratus == 1 else f"{angka[ratus]} ratus")

        if sisa:
            if sisa < 10:
                parts.append(angka[sisa])
            elif sisa == 10:
                parts.append("sepuluh")
            elif sisa == 11:
                parts.append("sebelas")
            elif sisa < 20:
                parts.append(f"{angka[sisa % 10]} belas")
            else:
                puluh = sisa // 10
                unit = sisa % 10
                parts.append(f"{angka[puluh]} puluh")
                if unit:
                    parts.append(angka[unit])

        return " ".join(parts).strip()

    units = [
        (1_000_000_000_000, "triliun"),
        (1_000_000_000, "miliar"),
        (1_000_000, "juta"),
        (1_000, "ribu"),
        (1, ""),
    ]

    out: list[str] = []
    sisa = n

    for nilai, label in units:
        group = sisa // nilai
        sisa %= nilai
        if group == 0:
            continue

        # 1.000 => "seribu" (bukan "satu ribu")
        if nilai == 1_000 and group == 1:
            out.append("seribu")
            continue

        chunk = under_1000(group)
        out.append(f"{chunk} {label}".strip())

    result = " ".join(out).strip()
    return result[:1].upper() + result[1:]


def parse_n_arg(argv: list[str]) -> int:
    """
    Mendukung:
    - python3 terbilang.py n=9876000
    - python3 terbilang.py --n 9876000
    - python3 terbilang.py 9876000  (posisional)
    Juga toleran separator ribuan: 9_876_000 / 9.876.000 / 9,876,000
    """
    n_str = None

    # cari format n=...
    for a in argv[1:]:
        if a.startswith("n="):
            n_str = a.split("=", 1)[1]
            break

    # cari format --n  / -n
    if n_str is None:
        for i, a in enumerate(argv[1:], start=1):
            if a in ("--n", "-n") and i + 1 < len(argv):
                n_str = argv[i + 1]
                break

    # fallback: arg pertama posisional
    if n_str is None:
        for a in argv[1:]:
            if not a.startswith("-"):
                n_str = a
                break

    if not n_str:
        raise ValueError("Argumen n tidak ditemukan. Contoh: python3 terbilang.py n=9876000")

    s = n_str.strip()

    # hanya izinkan angka + separator ribuan umum
    if not re.fullmatch(r"-?[0-9][0-9_\.,]*", s):
        raise ValueError(f"Format angka tidak valid: {n_str}")

    negative = s.startswith("-")
    if negative:
        s = s[1:]

    # jika mengandung tanda desimal yang “beneran” (mis: 12.34), tolak
    # heuristik: kalau ada dua jenis pemisah ('.' dan ',') kita anggap itu ribuan; kalau hanya 1 jenis,
    # kita anggap ribuan juga selama setelah dibersihkan tetap digit semua.
    s_clean = s.replace("_", "").replace(".", "").replace(",", "")
    if not s_clean.isdigit():
        raise ValueError(f"Format angka tidak valid: {n_str}")

    n = int(s_clean)
    if negative:
        n = -n
    return n


def main() -> int:
    try:
        n = parse_n_arg(sys.argv)
        print(terbilang(n))
        return 0
    except Exception as e:
        # error ke stderr agar stdout tetap “bersih” untuk piping
        print(f"ERROR: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
