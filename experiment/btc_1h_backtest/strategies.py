"""
Strategy definitions. Each is a pure function:
    (fire_features_df, params) -> signals_df with columns
        [hour_id_et, fire_minute, side]
    where side ∈ {'up','down'}. A row exists only when the strategy fires.

Strategies operate on the per-(hour, fire-minute) features built by features.py.
They never look at hour_close or future bars.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def s1_late_displacement_momentum(
    fire: pd.DataFrame,
    z_threshold: float = 1.5,
    minutes: tuple[int, int] = (30, 50),
) -> pd.DataFrame:
    """
    Continuation: if |displacement_z| >= z_threshold at minute t in [minutes],
    bet same direction as displacement.
    """
    lo, hi = minutes
    mask = (
        fire["fire_minute"].between(lo, hi, inclusive="both")
        & fire["displacement_z"].notna()
        & (fire["displacement_z"].abs() >= z_threshold)
    )
    sub = fire.loc[mask, ["hour_id_et", "fire_minute"]].copy()
    sub["side"] = np.where(fire.loc[mask, "displacement_z"] > 0, "up", "down")
    return sub.reset_index(drop=True)


def s1_bsm_filter(
    fire: pd.DataFrame,
    z_threshold: float = 1.5,
    minutes: tuple[int, int] = (30, 50),
    p_bs_buffer: float = 0.05,
) -> pd.DataFrame:
    """
    S1 trigger AND BSM digital P_up agrees with the side:
      - if side==up:   p_bs_up >= 0.5 + buffer
      - if side==down: p_bs_up <= 0.5 - buffer
    """
    s1 = s1_late_displacement_momentum(fire, z_threshold=z_threshold, minutes=minutes)
    if s1.empty:
        return s1
    feats = fire.set_index(["hour_id_et", "fire_minute"])["p_bs_up"]
    keys = list(zip(s1["hour_id_et"], s1["fire_minute"]))
    p_bs = feats.reindex(keys).values
    keep = np.where(
        s1["side"].values == "up",
        p_bs >= 0.5 + p_bs_buffer,
        p_bs <= 0.5 - p_bs_buffer,
    )
    return s1.loc[keep].reset_index(drop=True)


def s4_s1_lowvol(
    fire: pd.DataFrame,
    z_threshold: float = 1.5,
    minutes: tuple[int, int] = (30, 50),
) -> pd.DataFrame:
    """
    S1 trigger AND no recent vol burst (last 5 min max range <= 3 x 60-min median range).
    """
    s1 = s1_late_displacement_momentum(fire, z_threshold=z_threshold, minutes=minutes)
    if s1.empty:
        return s1
    feats = fire.set_index(["hour_id_et", "fire_minute"])["vol_burst"]
    keys = list(zip(s1["hour_id_et"], s1["fire_minute"]))
    vb = feats.reindex(keys).values
    keep = vb == 0
    return s1.loc[keep].reset_index(drop=True)


def s6_orderflow(
    fire: pd.DataFrame,
    high: float = 0.55,
    low: float = 0.45,
    minutes: tuple[int, int] = (30, 50),
) -> pd.DataFrame:
    """
    Order-flow imbalance: 5-min rolling taker-buy ratio.
      ratio_5m >= high → bet 'up'
      ratio_5m <= low  → bet 'down'
    """
    lo, hi = minutes
    valid = (
        fire["fire_minute"].between(lo, hi, inclusive="both")
        & fire["taker_ratio_5m"].notna()
    )
    up_mask = valid & (fire["taker_ratio_5m"] >= high)
    dn_mask = valid & (fire["taker_ratio_5m"] <= low)
    up = fire.loc[up_mask, ["hour_id_et", "fire_minute"]].copy()
    up["side"] = "up"
    dn = fire.loc[dn_mask, ["hour_id_et", "fire_minute"]].copy()
    dn["side"] = "down"
    return pd.concat([up, dn], ignore_index=True)


# Registry: name -> (callable, default kwargs). The CLI sweeps over a few z-values.
STRATEGY_VARIANTS = [
    ("S1_z1.0", lambda f: s1_late_displacement_momentum(f, z_threshold=1.0)),
    ("S1_z1.5", lambda f: s1_late_displacement_momentum(f, z_threshold=1.5)),
    ("S1_z2.0", lambda f: s1_late_displacement_momentum(f, z_threshold=2.0)),
    ("S1_z2.5", lambda f: s1_late_displacement_momentum(f, z_threshold=2.5)),
    ("S1+BSM_z1.5", lambda f: s1_bsm_filter(f, z_threshold=1.5)),
    ("S1+BSM_z2.0", lambda f: s1_bsm_filter(f, z_threshold=2.0)),
    ("S4_z1.5", lambda f: s4_s1_lowvol(f, z_threshold=1.5)),
    ("S4_z2.0", lambda f: s4_s1_lowvol(f, z_threshold=2.0)),
    ("S6_55_45", lambda f: s6_orderflow(f, high=0.55, low=0.45)),
    ("S6_60_40", lambda f: s6_orderflow(f, high=0.60, low=0.40)),
]
