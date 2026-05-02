"""
Pre-candle strategies — fire 70-100 min before target candle's close
(equivalently 10-40 min before the candle opens). Each function takes the
feature dataframe from features_pre_candle and returns a signals dataframe
with columns [target_hour, fire_offset_min, side].
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def _sub(fire: pd.DataFrame, mask: pd.Series, side: pd.Series) -> pd.DataFrame:
    out = fire.loc[mask, ["target_hour", "fire_offset_min"]].copy()
    out["side"] = side[mask].values
    return out.reset_index(drop=True)


def p1_trend_3h(fire: pd.DataFrame, threshold: float = 0.005) -> pd.DataFrame:
    """3-hour momentum continuation: bet sign(ret_3h) when |ret_3h| > threshold."""
    mask = fire["ret_3h"].abs() > threshold
    side = pd.Series(np.where(fire["ret_3h"] > 0, "up", "down"), index=fire.index)
    return _sub(fire, mask, side)


def p2_reversion_1h(fire: pd.DataFrame, threshold: float = 0.01) -> pd.DataFrame:
    """1-hour mean reversion: bet AGAINST sign(ret_1h) when |ret_1h| > threshold."""
    mask = fire["ret_1h"].abs() > threshold
    side = pd.Series(np.where(fire["ret_1h"] > 0, "down", "up"), index=fire.index)
    return _sub(fire, mask, side)


def p3_rsi_mr(fire: pd.DataFrame, hi: float = 70.0, lo: float = 30.0) -> pd.DataFrame:
    """RSI(14) on 1H bars: overbought → bet down, oversold → bet up."""
    overbought = fire["rsi_14"] > hi
    oversold = fire["rsi_14"] < lo
    side = pd.Series(np.where(oversold, "up", np.where(overbought, "down", "")), index=fire.index)
    mask = overbought | oversold
    return _sub(fire, mask, side)


def p4_flow_2h(fire: pd.DataFrame, hi: float = 0.55, lo: float = 0.45) -> pd.DataFrame:
    """Sustained 2-hour taker-buy ratio: > hi → up, < lo → down."""
    up_m = fire["taker_ratio_2h"] >= hi
    dn_m = fire["taker_ratio_2h"] <= lo
    mask = up_m | dn_m
    side = pd.Series(np.where(up_m, "up", np.where(dn_m, "down", "")), index=fire.index)
    return _sub(fire, mask, side)


def p5_combined(fire: pd.DataFrame, ret_thr: float = 0.005, flow_hi: float = 0.55, flow_lo: float = 0.45) -> pd.DataFrame:
    """P1 ∧ P4 agreement: trend AND sustained flow agree on direction."""
    up_trend = fire["ret_3h"] > ret_thr
    dn_trend = fire["ret_3h"] < -ret_thr
    up_flow = fire["taker_ratio_2h"] >= flow_hi
    dn_flow = fire["taker_ratio_2h"] <= flow_lo
    up_m = up_trend & up_flow
    dn_m = dn_trend & dn_flow
    mask = up_m | dn_m
    side = pd.Series(np.where(up_m, "up", np.where(dn_m, "down", "")), index=fire.index)
    return _sub(fire, mask, side)


def p6_tod_outsample(fire: pd.DataFrame, outcomes: pd.DataFrame, train_frac: float = 0.5) -> pd.DataFrame:
    """
    UTC time-of-day bias, OUT-OF-SAMPLE.

    Train: first `train_frac` of target_hours. Compute per-(utc_hour) historical
    Up rate. Side = Up if Up rate > 0.5, else Down. Bet only on the test set
    (last 1-train_frac of target_hours).
    """
    df = fire.merge(outcomes[["target_hour", "winner"]], on="target_hour", how="left")
    target_hours = sorted(df["target_hour"].unique())
    cutoff = target_hours[int(len(target_hours) * train_frac)]
    train = df[df["target_hour"] < cutoff]
    test = df[df["target_hour"] >= cutoff]

    # Per-UTC-hour up rate, computed only on first-fire-per-hour to avoid weighting
    train_one = train.drop_duplicates("target_hour")
    rates = train_one.groupby("target_utc_hour")["winner"].apply(lambda s: (s == "up").mean())
    side_per_hour = (rates > 0.5).map({True: "up", False: "down"})

    out = test[["target_hour", "fire_offset_min", "target_utc_hour"]].copy()
    out["side"] = out["target_utc_hour"].map(side_per_hour).values
    out = out.dropna(subset=["side"])  # drop hours with no training signal
    return out[["target_hour", "fire_offset_min", "side"]].reset_index(drop=True)


def baseline_always_up(fire: pd.DataFrame) -> pd.DataFrame:
    out = fire[["target_hour", "fire_offset_min"]].copy()
    out["side"] = "up"
    return out


def baseline_always_down(fire: pd.DataFrame) -> pd.DataFrame:
    out = fire[["target_hour", "fire_offset_min"]].copy()
    out["side"] = "down"
    return out


def baseline_random(fire: pd.DataFrame, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    out = fire[["target_hour", "fire_offset_min"]].copy()
    out["side"] = rng.choice(["up", "down"], size=len(out))
    return out


# ── Extended strategies (P7-P14) ─────────────────────────────────────────────

def p7_bb_mr(fire: pd.DataFrame, threshold: float = 1.0) -> pd.DataFrame:
    """Bollinger band mean reversion: bb_pct >= +threshold → bet down (above upper),
    bb_pct <= -threshold → bet up (below lower). threshold=1.0 = touching ±2σ band."""
    valid = fire["bb_pct"].notna()
    up_m = valid & (fire["bb_pct"] <= -threshold)
    dn_m = valid & (fire["bb_pct"] >= threshold)
    mask = up_m | dn_m
    side = pd.Series(np.where(up_m, "up", np.where(dn_m, "down", "")), index=fire.index)
    return _sub(fire, mask, side)


def p8_rsi_plus_1h(fire: pd.DataFrame, hi: float = 70.0, lo: float = 30.0,
                   ret1h_thr: float = 0.01) -> pd.DataFrame:
    """P3 (RSI MR) AND the most recent 1H bar agreed the move was extreme.
    Stricter intersection: fewer fires, expected higher hit rate."""
    overbought = (fire["rsi_14"] > hi) & (fire["ret_1h"] > ret1h_thr)
    oversold   = (fire["rsi_14"] < lo) & (fire["ret_1h"] < -ret1h_thr)
    mask = overbought | oversold
    side = pd.Series(np.where(oversold, "up", np.where(overbought, "down", "")), index=fire.index)
    return _sub(fire, mask, side)


def p9_rsi_lowvol(fire: pd.DataFrame, hi: float = 70.0, lo: float = 30.0) -> pd.DataFrame:
    """P3 only when current σ_24h is BELOW its 30-day rolling median.
    Idea: mean-reversion is more reliable in low-vol regimes."""
    lowvol = fire["sigma_24h"].notna() & fire["sigma_24h_med_30d"].notna() \
             & (fire["sigma_24h"] < fire["sigma_24h_med_30d"])
    overbought = lowvol & (fire["rsi_14"] > hi)
    oversold   = lowvol & (fire["rsi_14"] < lo)
    mask = overbought | oversold
    side = pd.Series(np.where(oversold, "up", np.where(overbought, "down", "")), index=fire.index)
    return _sub(fire, mask, side)


def p10_rsi_slow(fire: pd.DataFrame, hi: float = 65.0, lo: float = 35.0) -> pd.DataFrame:
    """RSI(28) — slower, smoother. Slightly tighter thresholds since slow RSI
    rarely reaches 70/30."""
    overbought = fire["rsi_28"] > hi
    oversold   = fire["rsi_28"] < lo
    mask = overbought | oversold
    side = pd.Series(np.where(oversold, "up", np.where(overbought, "down", "")), index=fire.index)
    return _sub(fire, mask, side)


def p11_rsi_fast(fire: pd.DataFrame, hi: float = 75.0, lo: float = 25.0) -> pd.DataFrame:
    """RSI(7) — fast, noisier. Wider thresholds since fast RSI overshoots more."""
    overbought = fire["rsi_7"] > hi
    oversold   = fire["rsi_7"] < lo
    mask = overbought | oversold
    side = pd.Series(np.where(oversold, "up", np.where(overbought, "down", "")), index=fire.index)
    return _sub(fire, mask, side)


def p12_macd_revert(fire: pd.DataFrame, z_threshold: float = 1.5) -> pd.DataFrame:
    """MACD-histogram reversion: when |MACD-hist z-score| ≥ threshold,
    bet against the histogram's sign (extreme histograms tend to revert)."""
    valid = fire["macd_hist_z"].notna()
    overshoot_up = valid & (fire["macd_hist_z"] >= z_threshold)
    overshoot_dn = valid & (fire["macd_hist_z"] <= -z_threshold)
    mask = overshoot_up | overshoot_dn
    side = pd.Series(np.where(overshoot_dn, "up", np.where(overshoot_up, "down", "")), index=fire.index)
    return _sub(fire, mask, side)


def p13_24h_extreme(fire: pd.DataFrame, threshold: float = 0.0) -> pd.DataFrame:
    """24h-extreme reversion: at 24h-high (or within `threshold` pct of it), bet down;
    at 24h-low (or within threshold pct), bet up. threshold=0.0 = strict touch."""
    at_high = fire["dist_to_24h_hi_pct"].notna() & (fire["dist_to_24h_hi_pct"] >= -threshold)
    at_low  = fire["dist_to_24h_lo_pct"].notna() & (fire["dist_to_24h_lo_pct"] <= threshold)
    mask = at_high | at_low
    side = pd.Series(np.where(at_low, "up", np.where(at_high, "down", "")), index=fire.index)
    return _sub(fire, mask, side)


def p14_rsi_strict(fire: pd.DataFrame, hi: float = 80.0, lo: float = 20.0) -> pd.DataFrame:
    """RSI(14) with strict 80/20 thresholds — fewer trades, expect higher hit rate."""
    overbought = fire["rsi_14"] > hi
    oversold   = fire["rsi_14"] < lo
    mask = overbought | oversold
    side = pd.Series(np.where(oversold, "up", np.where(overbought, "down", "")), index=fire.index)
    return _sub(fire, mask, side)


def p15_triple_confluence(fire: pd.DataFrame) -> pd.DataFrame:
    """Three independent mean-reversion signals all agree:
       RSI(14) extreme, BB extreme, AND 1H bar extreme — stricter intersection."""
    overbought = (fire["rsi_14"] > 70) & (fire["bb_pct"] >= 1.0) & (fire["ret_1h"] > 0.01)
    oversold   = (fire["rsi_14"] < 30) & (fire["bb_pct"] <= -1.0) & (fire["ret_1h"] < -0.01)
    mask = overbought | oversold
    side = pd.Series(np.where(oversold, "up", np.where(overbought, "down", "")), index=fire.index)
    return _sub(fire, mask, side)


# Registry — name -> callable. The callable takes the feature df (and optionally
# outcomes for OOS strategies); we pass outcomes via partial in the runner.
def get_registry(outcomes: pd.DataFrame) -> list[tuple[str, callable]]:
    return [
        ("P1_trend_3h_0.5%",   lambda f: p1_trend_3h(f, threshold=0.005)),
        ("P1_trend_3h_1.0%",   lambda f: p1_trend_3h(f, threshold=0.010)),
        ("P2_reversion_1h_1%", lambda f: p2_reversion_1h(f, threshold=0.010)),
        ("P2_reversion_1h_2%", lambda f: p2_reversion_1h(f, threshold=0.020)),
        ("P3_rsi_mr_70_30",    lambda f: p3_rsi_mr(f, hi=70, lo=30)),
        ("P3_rsi_mr_75_25",    lambda f: p3_rsi_mr(f, hi=75, lo=25)),
        ("P4_flow_2h_55_45",   lambda f: p4_flow_2h(f, hi=0.55, lo=0.45)),
        ("P4_flow_2h_52_48",   lambda f: p4_flow_2h(f, hi=0.52, lo=0.48)),
        ("P5_trend+flow",      lambda f: p5_combined(f, ret_thr=0.005, flow_hi=0.55, flow_lo=0.45)),
        ("P6_tod_OOS",         lambda f: p6_tod_outsample(f, outcomes, train_frac=0.5)),
        ("P7_BB_MR_1.0",       lambda f: p7_bb_mr(f, threshold=1.0)),
        ("P7_BB_MR_1.5",       lambda f: p7_bb_mr(f, threshold=1.5)),
        ("P8_RSI_x_1H_70_30",  lambda f: p8_rsi_plus_1h(f, hi=70, lo=30, ret1h_thr=0.010)),
        ("P9_RSI_lowvol",      lambda f: p9_rsi_lowvol(f, hi=70, lo=30)),
        ("P10_RSI28_65_35",    lambda f: p10_rsi_slow(f, hi=65, lo=35)),
        ("P10_RSI28_70_30",    lambda f: p10_rsi_slow(f, hi=70, lo=30)),
        ("P11_RSI7_75_25",     lambda f: p11_rsi_fast(f, hi=75, lo=25)),
        ("P11_RSI7_80_20",     lambda f: p11_rsi_fast(f, hi=80, lo=20)),
        ("P12_MACD_z1.5",      lambda f: p12_macd_revert(f, z_threshold=1.5)),
        ("P12_MACD_z2.0",      lambda f: p12_macd_revert(f, z_threshold=2.0)),
        ("P13_24h_extreme_0",  lambda f: p13_24h_extreme(f, threshold=0.0)),
        ("P13_24h_extreme_0.2",lambda f: p13_24h_extreme(f, threshold=0.002)),
        ("P14_RSI_strict_80_20", lambda f: p14_rsi_strict(f, hi=80, lo=20)),
        ("P15_triple_conf",    lambda f: p15_triple_confluence(f)),
        ("baseline_always_up",   baseline_always_up),
        ("baseline_always_down", baseline_always_down),
        ("baseline_random",      baseline_random),
    ]
