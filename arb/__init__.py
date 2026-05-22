"""Statistical arbitrage research module.

Three lead/lag scopes:
  - Mega-cap constituents vs their sector ETF (does AAPL drag XLK?)
  - Intra-sector pairs (KO/PEP, MA/V, GS/MS) for spread mean reversion
  - Sector ETFs vs SPY (rotation signals)

Two backtest styles run side-by-side per pair:
  - Spread mean-reversion: cointegrated spread, z-score entry/exit
  - Lead-lag momentum: leader's last-bar return signs next-bar follower trade
"""
