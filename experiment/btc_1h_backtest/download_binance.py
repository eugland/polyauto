"""
Download monthly BTCUSDT 1m klines from data.binance.vision and write
a single concatenated parquet at db/binance_archive/BTCUSDT_1m.parquet.

Run:
    python -m experiment.btc_1h_backtest.download_binance --months 12
"""
from __future__ import annotations

import argparse
import io
import sys
import zipfile
from datetime import date
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent.parent
CACHE_DIR = REPO / "db" / "binance_archive"
CSV_DIR = CACHE_DIR / "csv_BTCUSDT_1m"
PARQUET = CACHE_DIR / "BTCUSDT_1m.parquet"

URL = "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-{ym}.zip"

KLINE_COLS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote", "ignore",
]


def _months_back(n: int, end_year: int, end_month: int) -> list[str]:
    """Return YYYY-MM strings for the n months ending at (end_year, end_month) inclusive."""
    out: list[str] = []
    y, m = end_year, end_month
    for _ in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return list(reversed(out))


def _download_month(ym: str) -> Path | None:
    """Download zip for ym=YYYY-MM, extract CSV, return CSV path. Skip if already cached."""
    csv_path = CSV_DIR / f"BTCUSDT-1m-{ym}.csv"
    if csv_path.exists() and csv_path.stat().st_size > 0:
        return csv_path
    url = URL.format(ym=ym)
    print(f"  fetching {url}")
    try:
        with urlopen(Request(url), timeout=60) as r:
            data = r.read()
    except HTTPError as e:
        print(f"  {ym}: HTTP {e.code} (possibly month not yet published) — skipping", file=sys.stderr)
        return None
    z = zipfile.ZipFile(io.BytesIO(data))
    inner = [n for n in z.namelist() if n.endswith(".csv")]
    if not inner:
        print(f"  {ym}: no CSV in zip", file=sys.stderr)
        return None
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    with z.open(inner[0]) as src, open(csv_path, "wb") as dst:
        dst.write(src.read())
    return csv_path


def _build_parquet(csv_paths: list[Path]) -> int:
    """
    Concatenate CSVs into a single parquet keyed by minute_open_time_utc.
    Returns row count.
    """
    if not csv_paths:
        raise SystemExit("no CSVs to combine")
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    csv_glob = str(CSV_DIR / "BTCUSDT-1m-*.csv")
    # Binance CSVs from May 2025 onward have a header row; older ones don't.
    # We sniff the first byte of each file to decide. Easiest: use pandas with header=None,
    # then drop any row that looks like a header.
    print(f"  building parquet from {len(csv_paths)} CSV files...")
    frames: list[pd.DataFrame] = []
    for p in sorted(csv_paths):
        df = pd.read_csv(p, header=None, names=KLINE_COLS, low_memory=False)
        # Some Binance monthly files include a header row in the data; drop if open_time is non-numeric.
        df = df[pd.to_numeric(df["open_time"], errors="coerce").notna()]
        df = df.astype({
            "open_time": "int64", "close_time": "int64",
            "open": "float64", "high": "float64", "low": "float64", "close": "float64",
            "volume": "float64", "quote_volume": "float64",
            "trades": "int64",
            "taker_buy_base": "float64", "taker_buy_quote": "float64",
        })
        frames.append(df.drop(columns=["ignore"]))
    full = pd.concat(frames, ignore_index=True)
    # Some months produce timestamps in microseconds (newer files); normalize to ms.
    if full["open_time"].max() > 10**14:
        full["open_time"]  = full["open_time"]  // 1000
        full["close_time"] = full["close_time"] // 1000
    full = full.drop_duplicates(subset=["open_time"]).sort_values("open_time").reset_index(drop=True)
    full["open_time_utc"] = pd.to_datetime(full["open_time"], unit="ms", utc=True)
    duckdb.from_df(full).to_parquet(str(PARQUET))
    return len(full)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=12)
    ap.add_argument("--end-year", type=int, default=date.today().year)
    ap.add_argument("--end-month", type=int, default=date.today().month - 1 or 12)
    args = ap.parse_args()

    # If today is in early month and the prior month archive isn't published yet, fall back further.
    yms = _months_back(args.months, args.end_year, args.end_month)
    print(f"target months: {yms[0]} .. {yms[-1]}  ({len(yms)} months)")

    csv_paths: list[Path] = []
    for ym in yms:
        p = _download_month(ym)
        if p is not None:
            csv_paths.append(p)

    n = _build_parquet(csv_paths)
    print(f"\nwrote {PARQUET}  ({n:,} rows)")
    span_days = (n / 60 / 24)
    print(f"~{span_days:.1f} calendar days of 1m bars")


if __name__ == "__main__":
    main()
