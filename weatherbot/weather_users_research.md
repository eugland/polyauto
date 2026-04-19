# Weather Market User Strategy Research

**Date:** April 19, 2026  
**Wallets studied:** 3 active weather-market traders from `users.txt`

---

## Overview

Three traders were identified and analysed across their Polymarket profiles, live positions, and full trade history via `data-api.polymarket.com`. All three focus heavily on daily temperature prediction markets, but each runs a distinct strategy with meaningfully different risk profiles, entry mechanics, and geographic focus.

---

## User 1 — @haerder

**Wallet:** `0x8dec027d883949a6bfe79842d0ae6b80347e46e0`  
**Joined:** December 2025  
**Portfolio Value:** $6,567.72  
**Total Predictions:** 580  
**Biggest Win:** $2,122.81  
**Daily P/L (Apr 19):** +$33.90

### Strategy: High-Conviction Asian NO Accumulator

haerder runs a concentrated, research-backed strategy of betting **NO** on temperature thresholds they believe won't be reached. Positions are almost exclusively in East Asian cities with high-confidence outcomes — the market already prices these at 99¢+ and haerder is entering at the tail end to capture the last 0.1–0.7% edge before resolution.

#### Current Active Positions (Apr 19, 2026)

| Market | Side | Entry | Current | Size | P&L |
|---|---|---|---|---|---|
| Tokyo ≥25°C | NO | 0.9960 | 0.9995 | 853.5 | +$2.94 |
| Tokyo = 24°C | NO | 0.9926 | 0.9995 | 1,581.1 | +$10.84 |
| Tokyo = 23°C | YES | 0.9964 | 0.9995 | 1,450.8 | +$4.39 |
| Busan ≥27°C | NO | 0.9979 | 0.9995 | 1,997.6 | +$3.05 |
| Shanghai = 22°C | NO | 0.9939 | 0.9995 | 602.8 | +$3.32 |
| Beijing = 23°C | YES | 0.9980 | 0.9995 | 85.2 | +$0.13 |

#### Entry Price Distribution
- **Typical range:** 0.989–0.999
- **Modal entry:** ~0.994–0.997
- Entries are consistently near the top of the price range — haerder is **not hunting discounts**, they are entering after a market has largely settled and capturing the residual 0.1–0.5¢ premium before resolution.

#### City Coverage
Primarily **East Asia**: Tokyo (dominant), Busan, Shanghai, Beijing, with occasional exposure to Ankara, Istanbul, and Lucknow. The narrowness of geographic focus implies either direct weather data access or a strong prior about which cities have predictable April temperature profiles.

#### Position Sizing
- Large positions in highest-conviction markets: 1,000–2,000 shares
- Smaller refinement trades: <100 shares (used to adjust exposure within a band)
- Micro-trades (<10 shares) appear to be portfolio balancing artefacts

#### YES vs NO Mix
~90% NO bets. The handful of YES bets (e.g. Tokyo 23°C, Beijing 23°C) are not contrarian — they represent the **opposite side of the same temperature band thesis**: betting the temperature lands at a moderate threshold rather than going above it. This is effectively a coordinated bracket strategy on a temperature range.

#### Timing Pattern
- Executes in bursts — multiple trades within minutes during active sessions
- Bulk redemptions on April 16–17 indicate clean position exits at resolution
- No evidence of intraday repositioning; positions are entered and held to expiry

#### Risk Profile
**Low variance, low yield.** Each position earns fractions of a cent per share, but with sizes in the 1,000–2,000 range this generates $3–$15 per market per day. Across 580 lifetime predictions, the cumulative yield is meaningful. Downside risk is small but non-zero — a surprise heat event that breaks the threshold destroys the full position value.

---

## User 2 — @sin3000

**Wallet:** `0x8d71ff86701227bb479b2039edd92b08f73115d8`  
**Joined:** October 2025  
**Portfolio Value:** $5,971.79  
**Total Predictions:** 555  
**Biggest Win:** $230.79  
**Daily P/L (Apr 19):** +$30.29

### Strategy: Systematic Asian NO Accumulator (haerder-adjacent, broader geography)

sin3000 runs a near-identical strategy to haerder — high-probability NO bets on Asian temperature markets at 98.4–99.9¢ entries — but with a wider city footprint and a more disciplined scaling-in approach.

#### Current Active Positions (Apr 19, 2026)

| Market | Side | Entry | Current | Size | P&L |
|---|---|---|---|---|---|
| Shanghai ≥23°C | NO | 0.9970 | 0.9995 | 2,005.7 | +$4.99 |
| Shanghai ≥22°C | NO | 0.9872 | 0.9995 | 1,104.4 | +$13.58 |
| Tokyo = 23°C | YES | 0.9938 | 0.9995 | 1,063.6 | +$6.04 |
| Busan ≥27°C | NO | 0.9979 | 0.9995 | 1,000.0 | +$1.50 |
| Beijing = 23°C | YES | 0.9979 | 0.9995 | 801.1 | +$1.20 |

#### Entry Price Distribution
- **Range:** 0.984–0.999
- The Shanghai 22°C position at **0.9872** is a notable outlier — this suggests sin3000 found and entered the position when it was relatively mispriced (earlier in the day or before the market settled), capturing a larger spread than haerder's typical entry.
- Most trades: 0.994–0.999

#### City Coverage
Broader than haerder: **Shanghai, Tokyo, Beijing, Busan, Seoul, Chengdu, Chongqing, Ankara, Tel Aviv, Lucknow.** The inclusion of Chinese Tier-2 cities (Chengdu, Chongqing) and Middle East/South Asia (Ankara, Tel Aviv, Lucknow) suggests sin3000 is scanning a wider universe of temperature markets for mispriced probabilities.

#### Scaling Behaviour
Unlike haerder who enters with a single large order, sin3000 **scales into positions** — multiple small buys (5–50 shares) on the same market spread across a session before consolidating into a large position. This is visible in the April 15 Tokyo data where numerous 5-share buys preceded a 2,000+ share position.

This approach serves two purposes:
1. Price discovery — testing market response before committing size
2. Averaging — acquiring shares across a slightly wider price range

#### Timing Pattern
- Heaviest activity April 15–17, with batch redemptions following
- Multi-trade sequences in the same market within minutes suggests semi-automated or scripted execution
- Bulk redemptions (single transaction hash, multiple positions) points to an automated withdrawal process post-resolution

#### Risk Profile
**Very similar to haerder.** Marginally higher variance due to the wider city footprint (less-liquid markets in Chengdu/Chongqing may have wider spreads and higher tail risk). The $230.79 biggest win vs haerder's $2,122.81 suggests sin3000 either runs smaller absolute sizes or has a shorter track record of catching larger mispriced outliers. Joined two months earlier than haerder, but smaller portfolio value — may be more capital-constrained.

---

## User 3 — @aapang

**Wallet:** `0x104171232971a6db8cf938f76fdbebbb81c5f452`  
**Joined:** December 2025  
**Portfolio Value:** $35,200  
**Total Predictions:** 3,803  
**Biggest Win:** $19,800  
**Daily P/L (Apr 19):** -$302.35

### Strategy: NegRisk Split + Convert (free lottery tickets on impossible buckets)

aapang is not simply buying cheap YES tokens on the open market. The actual mechanic — confirmed by CONVERSION transactions in the on-chain data — exploits Polymarket's **NegRiskAdapter** on categorical "Highest temperature in X" markets.

#### How It Works — The NegRisk Mechanic

Polymarket's categorical temperature markets (e.g. "Highest temperature in Hong Kong on Apr 21?") use the NegRisk framework, where every bucket is a YES/NO binary but all buckets are mutually exclusive and backed by the same USDC pool. This enables a powerful conversion:

> **A NO token in any one market can be converted into 1 YES token in every other market.**

aapang exploits this in two steps:

**Step 1 — SPLIT on the expected bucket:**
> Pick the most likely temperature outcome (e.g. HK 28°C, which the market prices at ~99¢).  
> Pay $200 USDC → receive 200 YES(28°C) + 200 NO(28°C).  
> YES(28°C) is worth ~99¢. NO(28°C) is worth ~1¢.

**Step 2 — CONVERT the NO to all YES's:**
> Convert the 200 NO(28°C) tokens via the NegRiskAdapter.  
> Result: receive 200 YES(26°C) + 200 YES(27°C) + 200 YES(29°C) + 200 YES(30°C+) + ... (one for each remaining bucket).  
> These impossible-bucket YES tokens are each worth 0.1–0.5¢.

**Net position after both steps:**
- 200 YES(28°C) — the near-certain outcome, worth ~99¢ each → ~$198 value  
- 200 YES(each impossible bucket) — essentially free, acquired as a by-product of the split

**Step 3 — Optional: sell off some impossible bucket tokens:**
> The SELL at 0.001–0.005 transactions in the data are aapang offloading the least interesting impossible bucket tokens for whatever marginal recovery they can get.

**Step 4 — Top up the near-certain position via CLOB:**
> The BUY at 0.99 transactions are additional direct purchases of the expected bucket (28°C) on the CLOB, layered on top of the split position.

#### Why This Is Clever

The split costs $200 and delivers ~$198 in near-certain YES tokens **plus** a portfolio of impossible-bucket YES tokens almost for free. Effectively:

- Cost of 200 YES(expected): ~$200 (same as buying directly)
- Cost of 200 YES(impossible) × N buckets: **near zero** — they came from converting the 1¢ NO tokens

If there are 7 impossible buckets, aapang has 1,400 free lottery tickets across them all. Every city, every day. At scale across 20 cities this generates tens of thousands of near-zero-cost lottery tickets simultaneously.

When an unexpected weather event occurs — a surprise heat wave, a cold snap, an anomalous reading — one of those "impossible" buckets resolves YES, and 200 tokens × $1 = **$200 on a near-zero cost basis**. That is the Denver +1,892%.

#### Confirmed by Transaction Data

CONVERSION transactions were found on 17+ categorical "Highest temperature" markets on April 21, 2026, spanning:
- Hong Kong ($1,000), Seattle ($200), Jeddah ($200), Beijing ($100), Munich ($200), Busan ($200), Jakarta ($200), Houston ($200), Atlanta ($200), Mexico City ($200), Los Angeles ($200), São Paulo ($200/$200), Shanghai ($200), Helsinki ($200), Milan ($200), Chicago ($200)

All within a ~90-minute window (timestamps 1776573022–1776576208), indicating automated bot execution doing mass splits across the full city universe simultaneously.

#### Economic Structure

| Component | Mechanism | Cost |
|---|---|---|
| YES(expected bucket) | Split or direct CLOB buy | ~99¢/share |
| YES(impossible buckets) | Convert NO from split | ~0¢/share |
| Impossible bucket sell-off | SELL at 0.001–0.005 | Recovers ~$0.20 per $200 split |
| Net cost basis on impossible YES | Near zero | Free |

#### City Coverage
**20+ cities globally** — Hong Kong, Tokyo, Denver, Chicago, Dallas, Toronto, London, Paris, Singapore, Moscow, Wellington, Munich, Austin, Miami, Panama City, Seattle, Jakarta, Jeddah, São Paulo, Helsinki, Milan, Atlanta, Mexico City, Busan, Beijing, Shanghai. The breadth is intentional: maximum city coverage = maximum lottery ticket coverage per day.

#### Notable Trade — Denver (1,892% Return)
A Denver temperature market resolved in an unexpected bucket that aapang held from a split/convert. 1,000 tokens acquired near-free (via NO conversion) resolved at $1.00 → ~$946 profit on effectively $0 cost basis. This is the strategy working exactly as designed.

#### Non-Weather Exposure
Dota 2 and League of Legends esports positions appear in the data and seem to be a **separate discretionary strategy**, not related to the NegRisk split mechanic. The esports markets are standard binary YES/NO bets at typical prices (0.93–0.99), consistent with haerder/sin3000 style near-certainty plays.

#### P&L Reality Check
The -100% P&L on most historical positions is expected and by design — the impossible buckets are *supposed* to lose most of the time. The strategy is profitable because:
1. The impossible YES tokens cost near zero
2. The near-certain YES position covers capital deployment (or is sold for near-full recovery)
3. Occasional surprise weather events produce outsized payouts from the free lottery tickets
4. At 3,803 total predictions across 20 cities, the daily lottery ticket count is enormous

#### Timing Pattern
- CONVERSION transactions batch-executed in ~90-minute windows — clearly bot-driven
- 17 cities processed in sequence within 90 minutes
- Rapid sequential trades across many cities
- Activity spans all hours — no geographic/timezone constraint

#### Risk Profile
**Low capital risk per city, high opportunity cost of the spread.** The primary cost is the ~1¢ effective price of the NO tokens that get converted (negligible). The near-certain YES position is either held to resolution or sold on the CLOB to recover capital. True risk is near zero per cycle — the downside is simply that all impossible bucket tickets expire worthless, which happens most days.

---

## Comparative Analysis

| Dimension | haerder | sin3000 | aapang |
|---|---|---|---|
| Portfolio size | $6,568 | $5,972 | $35,200 |
| Predictions | 580 | 555 | 3,803 |
| Primary side | NO | NO | YES (impossible) + YES (expected) |
| Entry price range | 0.989–0.999 | 0.984–0.999 | ~0¢ (converted) + 0.99 (expected) |
| Cities | East Asia focused | East Asia + broader | Global (20+ cities) |
| Biggest win | $2,123 | $231 | $19,800 |
| Daily yield | ~$34 | ~$30 | Volatile |
| Approach | Manual / semi-auto | Semi-auto / scaled | Fully automated bot |
| Risk per impossible token | Low | Low | ~Zero (free via conversion) |
| Strategy type | Near-certainty yield | Near-certainty yield | NegRisk split + free lottery tickets |
| Volume style | Concentrated | Scaled | Mass simultaneous (17+ cities/session) |

---

## Strategy Patterns Relevant to Our Bot

### What haerder/sin3000 do that our bot doesn't

1. **YES bets on moderate thresholds** — both users place YES bets on the *expected* temperature bucket (e.g. Tokyo 23°C YES) alongside NO bets on thresholds above/below. This is a bracket strategy capturing value on both sides of the forecast range. Our weather bot only bets NO.

2. **Wider city coverage** — sin3000 trades Chengdu, Chongqing, Tel Aviv, Lucknow. These may have less liquidity and more edge available. Worth investigating whether `polymarket.fetch_temperature_markets_payload()` is surfacing all available temperature markets or filtering some out.

3. **Scaling into positions** — sin3000's pattern of 5-share probes before committing size is interesting. Could be manual discipline or a bot feature. Could reduce slippage on larger orders.

4. **Entry timing** — both users show batch entry activity rather than continuous re-entry. They appear to wait for a settlement price to form and then enter near the day's final price, rather than entering at any time. Our bot's `--interval 60` polling is compatible with this but could be optimised to enter later in the day when prices have settled.

### What aapang does that's worth monitoring

5. **NegRisk split + convert is a separate playbook entirely** — aapang is not competing with our NO bets. They're exploiting the NegRisk categorical market mechanic to acquire impossible-bucket YES tokens for free. The markets they're active in ("Highest temperature in X") are *categorical* markets, which our bot doesn't touch (we trade the daily binary threshold markets like "Will Tokyo be above 25°C?"). Worth investigating whether the categorical highest-temp markets have a bot-accessible split/convert path via py-clob-client.

6. **US city coverage** — Denver, Dallas, Chicago, Miami, Atlanta, Houston, Seattle, LA, Austin. Our bot is predominantly European/Asian city focused. aapang's success in US markets (Denver +1,892%) suggests edge exists there.

### Key risk observation

All three users are **buying near-resolution** at 98–99¢. This is the same regime our weather bot operates in. The concentrated exposure means a single surprise heat event (late-breaking weather data contradicting the forecast) can wipe out a full position. haerder's $2,123 biggest win implies they've also had losses in that range. Positions should not be sized assuming 99¢ is "safe" — it is high-probability, not certain.

---

## Open Questions for Further Research

1. **Do haerder/sin3000 use weather APIs or pure market signal?** Their entry prices suggest they enter after the market has already priced the outcome high — they may be pure price-followers rather than weather-data users. This would mean they're trading on market consensus, not meteorological edge.

2. **Is aapang running a model?** 3,803 predictions with systematic 5¢ entries across 20 cities is almost certainly programmatic. Understanding their forecast source would be valuable.

3. **Are haerder and sin3000 related?** Both joined within 2 months of each other, trade identical city subsets on identical dates, run near-identical strategies. Could be the same operator with two wallets, or two users who coordinated. Worth checking whether they ever trade the same market on opposite sides.

4. **What's haerder's $2,123 biggest win from?** That's a large gain for a 99¢-entry strategy — either a very large position size, or an early entry at a significantly lower price. Identifying that trade would reveal when haerder takes larger speculative positions.
