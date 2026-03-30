from __future__ import annotations

from automata.models import BetOrder, ParsedMarket


def should_bet_no(
    pm: ParsedMarket,
    daily_high: float,       # in the same unit as pm.unit
    is_after_cutoff: bool,
    temp_margin: float,
    min_no_price: float,
    max_no_price: float,
) -> tuple[bool, str]:
    """
    Returns (should_bet, reason).

    "higher" — bet No if daily_high + margin is still below threshold
    "below"  — bet No if daily_high already exceeded threshold
    "range"  — bet No if daily_high clearly outside [lo, hi]
    """
    price = pm.market.no_price

    if not (min_no_price <= price <= max_no_price):
        return False, f"No price {price:.2f} outside [{min_no_price:.2f}, {max_no_price:.2f}]"

    if pm.direction == "higher":
        if not is_after_cutoff:
            return False, "not past cutoff — temperature could still rise"
        gap = pm.threshold_lo - daily_high
        if gap > temp_margin:
            return True, f"high={daily_high:.1f} gap={gap:.1f} > margin={temp_margin:.1f}"
        return False, f"gap {gap:.1f} ≤ margin {temp_margin:.1f} — too close"

    if pm.direction == "below":
        if daily_high > pm.threshold_lo:
            return True, f"high={daily_high:.1f} already exceeds threshold={pm.threshold_lo:.1f}"
        return False, f"high={daily_high:.1f} still at or below {pm.threshold_lo:.1f}"

    # "range"
    if daily_high > pm.threshold_hi:
        return True, f"high={daily_high:.1f} already above range hi={pm.threshold_hi:.1f}"
    if is_after_cutoff:
        gap = pm.threshold_lo - daily_high
        if gap > temp_margin:
            return True, f"high={daily_high:.1f} still {gap:.1f} below range lo={pm.threshold_lo:.1f}"
    return False, f"high={daily_high:.1f} within/approaching range [{pm.threshold_lo:.1f}, {pm.threshold_hi:.1f}]"


def build_order(pm: ParsedMarket, size_usdc: float, reason: str) -> BetOrder:
    return BetOrder(market=pm.market, size_usdc=size_usdc, reason=reason)
