"""Universe loader — S&P 500 list + pair tickers + sector ETFs + macro + vol indices.

S&P 500 is fetched from Wikipedia once a week (stored in DuckDB). A static fallback
snapshot (~120 largest names by market cap) is used if the network fetch fails.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

log = logging.getLogger("stock.universe")


@dataclass(frozen=True)
class Ticker:
    symbol: str
    name: str
    sector: str
    is_sp500: bool = False
    is_pair: bool = False
    is_sector_etf: bool = False
    is_macro: bool = False
    is_vol: bool = False


PAIR_TICKERS: list[Ticker] = [
    Ticker("SVIX", "-1x Short VIX Futures ETF", "Volatility", is_pair=True),
    Ticker("UVIX", "2x Long VIX Futures ETF", "Volatility", is_pair=True),
    Ticker("TSLL", "Direxion 2x Long TSLA ETF", "Consumer Discretionary", is_pair=True),
    Ticker("TSLZ", "Direxion -1x Short TSLA ETF", "Consumer Discretionary", is_pair=True),
    Ticker("TSLA", "Tesla Inc.", "Consumer Discretionary", is_pair=True),
]

SECTOR_ETFS: list[Ticker] = [
    Ticker("XLF",  "Financials SPDR",            "Financials",            is_sector_etf=True),
    Ticker("XLE",  "Energy SPDR",                "Energy",                is_sector_etf=True),
    Ticker("XLK",  "Technology SPDR",            "Technology",            is_sector_etf=True),
    Ticker("XLV",  "Health Care SPDR",           "Health Care",           is_sector_etf=True),
    Ticker("XLY",  "Consumer Discretionary SPDR","Consumer Discretionary",is_sector_etf=True),
    Ticker("XLI",  "Industrials SPDR",           "Industrials",           is_sector_etf=True),
    Ticker("XLP",  "Consumer Staples SPDR",      "Consumer Staples",      is_sector_etf=True),
    Ticker("XLU",  "Utilities SPDR",             "Utilities",             is_sector_etf=True),
    Ticker("XLB",  "Materials SPDR",             "Materials",             is_sector_etf=True),
    Ticker("XLRE", "Real Estate SPDR",           "Real Estate",           is_sector_etf=True),
    Ticker("XLC",  "Communication Services SPDR","Communication Services",is_sector_etf=True),
]

VOL_TICKERS: list[Ticker] = [
    Ticker("^VIX",   "CBOE Volatility Index",            "Volatility", is_vol=True),
    Ticker("^VIX3M", "3-Month Volatility Index",         "Volatility", is_vol=True),
    Ticker("^VIX6M", "6-Month Volatility Index",         "Volatility", is_vol=True),
    Ticker("^VVIX",  "CBOE VVIX (VIX of VIX)",           "Volatility", is_vol=True),
]

MACRO_TICKERS: list[Ticker] = [
    Ticker("SPY",     "S&P 500 ETF",             "Equity",       is_macro=True),
    Ticker("QQQ",     "Nasdaq 100 ETF",          "Equity",       is_macro=True),
    Ticker("IWM",     "Russell 2000 ETF",        "Equity",       is_macro=True),
    Ticker("UUP",     "US Dollar Bullish ETF",   "FX",           is_macro=True),
    Ticker("^TNX",    "10-Year Treasury Yield",  "Rates",        is_macro=True),
    Ticker("GLD",     "Gold ETF",                "Commodities",  is_macro=True),
    Ticker("USO",     "US Oil Fund",             "Commodities",  is_macro=True),
    Ticker("BTC-USD", "Bitcoin",                 "Crypto",       is_macro=True),
]


# Fallback snapshot — largest ~120 S&P 500 names by index weight. Used if the Wikipedia
# fetch fails. The collector's nightly refresh attempts to replace this with the full 500.
SP500_FALLBACK: list[tuple[str, str, str]] = [
    ("AAPL",  "Apple Inc.",                       "Technology"),
    ("MSFT",  "Microsoft Corp.",                  "Technology"),
    ("NVDA",  "NVIDIA Corp.",                     "Technology"),
    ("AMZN",  "Amazon.com Inc.",                  "Consumer Discretionary"),
    ("GOOGL", "Alphabet Inc. Class A",            "Communication Services"),
    ("GOOG",  "Alphabet Inc. Class C",            "Communication Services"),
    ("META",  "Meta Platforms Inc.",              "Communication Services"),
    ("TSLA",  "Tesla Inc.",                       "Consumer Discretionary"),
    ("BRK-B", "Berkshire Hathaway Class B",       "Financials"),
    ("LLY",   "Eli Lilly & Co.",                  "Health Care"),
    ("AVGO",  "Broadcom Inc.",                    "Technology"),
    ("JPM",   "JPMorgan Chase & Co.",             "Financials"),
    ("V",     "Visa Inc.",                        "Financials"),
    ("WMT",   "Walmart Inc.",                     "Consumer Staples"),
    ("XOM",   "Exxon Mobil Corp.",                "Energy"),
    ("UNH",   "UnitedHealth Group",               "Health Care"),
    ("MA",    "Mastercard Inc.",                  "Financials"),
    ("PG",    "Procter & Gamble",                 "Consumer Staples"),
    ("JNJ",   "Johnson & Johnson",                "Health Care"),
    ("HD",    "Home Depot",                       "Consumer Discretionary"),
    ("COST",  "Costco Wholesale",                 "Consumer Staples"),
    ("ORCL",  "Oracle Corp.",                     "Technology"),
    ("BAC",   "Bank of America",                  "Financials"),
    ("NFLX",  "Netflix Inc.",                     "Communication Services"),
    ("ABBV",  "AbbVie Inc.",                      "Health Care"),
    ("CVX",   "Chevron Corp.",                    "Energy"),
    ("CRM",   "Salesforce Inc.",                  "Technology"),
    ("KO",    "Coca-Cola Co.",                    "Consumer Staples"),
    ("MRK",   "Merck & Co.",                      "Health Care"),
    ("WFC",   "Wells Fargo & Co.",                "Financials"),
    ("AMD",   "Advanced Micro Devices",           "Technology"),
    ("CSCO",  "Cisco Systems",                    "Technology"),
    ("ACN",   "Accenture plc",                    "Technology"),
    ("PEP",   "PepsiCo Inc.",                     "Consumer Staples"),
    ("ADBE",  "Adobe Inc.",                       "Technology"),
    ("LIN",   "Linde plc",                        "Materials"),
    ("TMO",   "Thermo Fisher Scientific",         "Health Care"),
    ("MCD",   "McDonald's Corp.",                 "Consumer Discretionary"),
    ("DIS",   "Walt Disney Co.",                  "Communication Services"),
    ("ABT",   "Abbott Laboratories",              "Health Care"),
    ("TMUS",  "T-Mobile US Inc.",                 "Communication Services"),
    ("AXP",   "American Express",                 "Financials"),
    ("CAT",   "Caterpillar Inc.",                 "Industrials"),
    ("IBM",   "International Business Machines",  "Technology"),
    ("GE",    "General Electric",                 "Industrials"),
    ("QCOM",  "Qualcomm Inc.",                    "Technology"),
    ("TXN",   "Texas Instruments",                "Technology"),
    ("MS",    "Morgan Stanley",                   "Financials"),
    ("GS",    "Goldman Sachs Group",              "Financials"),
    ("INTU",  "Intuit Inc.",                      "Technology"),
    ("ISRG",  "Intuitive Surgical",               "Health Care"),
    ("VZ",    "Verizon Communications",           "Communication Services"),
    ("RTX",   "RTX Corp.",                        "Industrials"),
    ("CMCSA", "Comcast Corp.",                    "Communication Services"),
    ("PFE",   "Pfizer Inc.",                      "Health Care"),
    ("BKNG",  "Booking Holdings",                 "Consumer Discretionary"),
    ("NOW",   "ServiceNow Inc.",                  "Technology"),
    ("PM",    "Philip Morris International",      "Consumer Staples"),
    ("SPGI",  "S&P Global Inc.",                  "Financials"),
    ("BLK",   "BlackRock Inc.",                   "Financials"),
    ("AMGN",  "Amgen Inc.",                       "Health Care"),
    ("UBER",  "Uber Technologies",                "Industrials"),
    ("NEE",   "NextEra Energy",                   "Utilities"),
    ("LOW",   "Lowe's Companies",                 "Consumer Discretionary"),
    ("HON",   "Honeywell International",          "Industrials"),
    ("T",     "AT&T Inc.",                        "Communication Services"),
    ("UNP",   "Union Pacific",                    "Industrials"),
    ("SCHW",  "Charles Schwab",                   "Financials"),
    ("SYK",   "Stryker Corp.",                    "Health Care"),
    ("BA",    "Boeing Co.",                       "Industrials"),
    ("LMT",   "Lockheed Martin",                  "Industrials"),
    ("COP",   "ConocoPhillips",                   "Energy"),
    ("C",     "Citigroup Inc.",                   "Financials"),
    ("PLD",   "Prologis Inc.",                    "Real Estate"),
    ("DHR",   "Danaher Corp.",                    "Health Care"),
    ("GILD",  "Gilead Sciences",                  "Health Care"),
    ("ETN",   "Eaton Corp.",                      "Industrials"),
    ("SBUX",  "Starbucks Corp.",                  "Consumer Discretionary"),
    ("TJX",   "TJX Companies",                    "Consumer Discretionary"),
    ("NKE",   "Nike Inc.",                        "Consumer Discretionary"),
    ("MDT",   "Medtronic plc",                    "Health Care"),
    ("DE",    "Deere & Co.",                      "Industrials"),
    ("ADP",   "Automatic Data Processing",        "Industrials"),
    ("AMAT",  "Applied Materials",                "Technology"),
    ("MU",    "Micron Technology",                "Technology"),
    ("LRCX",  "Lam Research",                     "Technology"),
    ("BMY",   "Bristol-Myers Squibb",             "Health Care"),
    ("VRTX",  "Vertex Pharmaceuticals",           "Health Care"),
    ("PANW",  "Palo Alto Networks",               "Technology"),
    ("CB",    "Chubb Ltd.",                       "Financials"),
    ("MDLZ",  "Mondelez International",           "Consumer Staples"),
    ("ADI",   "Analog Devices",                   "Technology"),
    ("KLAC",  "KLA Corp.",                        "Technology"),
    ("PGR",   "Progressive Corp.",                "Financials"),
    ("SO",    "Southern Co.",                     "Utilities"),
    ("INTC",  "Intel Corp.",                      "Technology"),
    ("REGN",  "Regeneron Pharmaceuticals",        "Health Care"),
    ("ELV",   "Elevance Health",                  "Health Care"),
    ("CI",    "Cigna Group",                      "Health Care"),
    ("DUK",   "Duke Energy",                      "Utilities"),
    ("FI",    "Fiserv Inc.",                      "Financials"),
    ("CME",   "CME Group",                        "Financials"),
    ("MO",    "Altria Group",                     "Consumer Staples"),
    ("EQIX",  "Equinix Inc.",                     "Real Estate"),
    ("ZTS",   "Zoetis Inc.",                      "Health Care"),
    ("SHW",   "Sherwin-Williams",                 "Materials"),
    ("ITW",   "Illinois Tool Works",              "Industrials"),
    ("AON",   "Aon plc",                          "Financials"),
    ("NOC",   "Northrop Grumman",                 "Industrials"),
    ("GD",    "General Dynamics",                 "Industrials"),
    ("CSX",   "CSX Corp.",                        "Industrials"),
    ("BSX",   "Boston Scientific",                "Health Care"),
    ("TGT",   "Target Corp.",                     "Consumer Discretionary"),
    ("FDX",   "FedEx Corp.",                      "Industrials"),
    ("WM",    "Waste Management",                 "Industrials"),
    ("CL",    "Colgate-Palmolive",                "Consumer Staples"),
    ("MMC",   "Marsh & McLennan",                 "Financials"),
    ("EOG",   "EOG Resources",                    "Energy"),
    ("SLB",   "Schlumberger NV",                  "Energy"),
    ("APD",   "Air Products & Chemicals",         "Materials"),
    ("MAR",   "Marriott International",           "Consumer Discretionary"),
    ("NSC",   "Norfolk Southern",                 "Industrials"),
    ("ORLY",  "O'Reilly Automotive",              "Consumer Discretionary"),
    ("FCX",   "Freeport-McMoRan",                 "Materials"),
    ("PYPL",  "PayPal Holdings",                  "Financials"),
]


def fetch_sp500_from_wikipedia(timeout: int = 15) -> list[tuple[str, str, str]]:
    """Return [(symbol, name, sector), ...] from the Wikipedia S&P 500 page."""
    import pandas as pd  # noqa: F401  — required by read_html
    import requests
    from io import StringIO

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0 (stock-dashboard)"}
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    import pandas as pd
    tables = pd.read_html(StringIO(resp.text))
    if not tables:
        raise RuntimeError("No tables on Wikipedia S&P 500 page")
    df = tables[0]
    out: list[tuple[str, str, str]] = []
    for _, row in df.iterrows():
        sym = str(row.get("Symbol", "")).strip().replace(".", "-")
        name = str(row.get("Security", "")).strip()
        sector = str(row.get("GICS Sector", "")).strip()
        if sym and name:
            out.append((sym, name, sector))
    return out


def load_sp500(logger: logging.Logger | None = None) -> list[tuple[str, str, str]]:
    """Try Wikipedia, fall back to the static snapshot on failure."""
    lg = logger or log
    try:
        rows = fetch_sp500_from_wikipedia()
        if len(rows) < 400:
            raise RuntimeError(f"Only {len(rows)} rows parsed from Wikipedia")
        lg.info("Loaded %d S&P 500 names from Wikipedia", len(rows))
        return rows
    except Exception as exc:
        lg.warning("Wikipedia S&P 500 fetch failed (%s) — using %d-name fallback",
                   exc, len(SP500_FALLBACK))
        return list(SP500_FALLBACK)


def all_static_tickers() -> list[Ticker]:
    """Non-S&P tickers: pairs, sector ETFs, vol, macro."""
    return PAIR_TICKERS + SECTOR_ETFS + VOL_TICKERS + MACRO_TICKERS


def all_symbols(sp500: Iterable[tuple[str, str, str]]) -> list[str]:
    symbols: set[str] = set()
    for sym, _, _ in sp500:
        symbols.add(sym)
    for t in all_static_tickers():
        symbols.add(t.symbol)
    return sorted(symbols)
