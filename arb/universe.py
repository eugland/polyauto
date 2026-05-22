"""Tickers grouped into the three scopes we backtest."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple


@dataclass(frozen=True)
class Pair:
    leader: str
    follower: str
    scope: str   # "mega_vs_etf" | "intra_sector" | "sector_vs_spy"
    note: str = ""

    @property
    def key(self) -> str:
        return f"{self.leader}/{self.follower}"


# Mega-cap heavyweights paired with their sector SPDR ETF.
# Pattern: does the single name's move predict (or co-move strongly with) the basket?
MEGA_VS_ETF: List[Pair] = [
    Pair("AAPL",  "XLK",  "mega_vs_etf", "Apple vs Tech"),
    Pair("MSFT",  "XLK",  "mega_vs_etf", "Microsoft vs Tech"),
    Pair("NVDA",  "XLK",  "mega_vs_etf", "Nvidia vs Tech"),
    Pair("JPM",   "XLF",  "mega_vs_etf", "JPMorgan vs Financials"),
    Pair("BAC",   "XLF",  "mega_vs_etf", "Bank of America vs Financials"),
    Pair("XOM",   "XLE",  "mega_vs_etf", "Exxon vs Energy"),
    Pair("CVX",   "XLE",  "mega_vs_etf", "Chevron vs Energy"),
    Pair("UNH",   "XLV",  "mega_vs_etf", "UnitedHealth vs Healthcare"),
    Pair("JNJ",   "XLV",  "mega_vs_etf", "J&J vs Healthcare"),
    Pair("GOOGL", "XLC",  "mega_vs_etf", "Alphabet vs Comm"),
    Pair("META",  "XLC",  "mega_vs_etf", "Meta vs Comm"),
    Pair("AMZN",  "XLY",  "mega_vs_etf", "Amazon vs Cons. Disc."),
    Pair("TSLA",  "XLY",  "mega_vs_etf", "Tesla vs Cons. Disc."),
]

# Cointegration candidates inside a sector — classic stat-arb pairs.
INTRA_SECTOR: List[Pair] = [
    Pair("KO",   "PEP",  "intra_sector", "Coke vs Pepsi"),
    Pair("MA",   "V",    "intra_sector", "Mastercard vs Visa"),
    Pair("GS",   "MS",   "intra_sector", "Goldman vs Morgan Stanley"),
    Pair("HD",   "LOW",  "intra_sector", "Home Depot vs Lowe's"),
    Pair("AAPL", "MSFT", "intra_sector", "Apple vs Microsoft"),
    Pair("XOM",  "CVX",  "intra_sector", "Exxon vs Chevron"),
    Pair("WMT",  "TGT",  "intra_sector", "Walmart vs Target"),
    Pair("F",    "GM",   "intra_sector", "Ford vs GM"),
    Pair("UPS",  "FDX",  "intra_sector", "UPS vs FedEx"),
    Pair("BAC",  "JPM",  "intra_sector", "BAC vs JPM"),
]

# Sector ETFs vs broad market. Tests whether sector leadership flips precede SPY.
SECTOR_VS_SPY: List[Pair] = [
    Pair("XLK",  "SPY", "sector_vs_spy", "Tech vs SPY"),
    Pair("XLF",  "SPY", "sector_vs_spy", "Financials vs SPY"),
    Pair("XLE",  "SPY", "sector_vs_spy", "Energy vs SPY"),
    Pair("XLV",  "SPY", "sector_vs_spy", "Healthcare vs SPY"),
    Pair("XLC",  "SPY", "sector_vs_spy", "Comm vs SPY"),
    Pair("XLY",  "SPY", "sector_vs_spy", "Cons. Disc. vs SPY"),
    Pair("XLP",  "SPY", "sector_vs_spy", "Cons. Staples vs SPY"),
    Pair("XLI",  "SPY", "sector_vs_spy", "Industrials vs SPY"),
    Pair("XLU",  "SPY", "sector_vs_spy", "Utilities vs SPY"),
    Pair("XLB",  "SPY", "sector_vs_spy", "Materials vs SPY"),
    Pair("XLRE", "SPY", "sector_vs_spy", "Real Estate vs SPY"),
]


def all_pairs() -> List[Pair]:
    return MEGA_VS_ETF + INTRA_SECTOR + SECTOR_VS_SPY


def all_tickers() -> List[str]:
    seen = set()
    out: List[str] = []
    for p in all_pairs():
        for t in (p.leader, p.follower):
            if t not in seen:
                seen.add(t)
                out.append(t)
    return out
