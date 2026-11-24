"""Independent funding opportunity model. No order placement."""
from dataclasses import dataclass
from math import isfinite
from itertools import permutations

@dataclass(frozen=True)
class Venue:
    name: str
    bid: float
    ask: float
    rate: float  # decimal per settlement
    next_ms: int
    interval_ms: int
    taker_fee: float

def settlements(venue, now_ms, horizon_ms):
    if venue.interval_ms <= 0 or venue.next_ms <= now_ms:
        raise ValueError("Missing or stale funding schedule")
    return max(0, 1 + (now_ms + horizon_ms - venue.next_ms) // venue.interval_ms)

def evaluate(long, short, now_ms, horizon_ms, qty, slippage_bps=15):
    if long.name == short.name or horizon_ms <= 0 or qty <= 0:
        raise ValueError("Invalid hedge")
    for v in (long, short):
        if not all(isfinite(x) for x in (v.bid,v.ask,v.rate,v.taker_fee)) or not 0 < v.bid <= v.ask or v.taker_fee < 0:
            raise ValueError("Invalid quote")
    if not isfinite(qty) or not isfinite(slippage_bps) or slippage_bps < 0:
        raise ValueError("Invalid sizing")
    lc = settlements(long, now_ms, horizon_ms)
    sc = settlements(short, now_ms, horizon_ms)
    lm, sm = (long.bid+long.ask)/2, (short.bid+short.ask)/2
    funding = qty * (sm*short.rate*sc - lm*long.rate*lc)
    # Flat future quotes scenario, not a prediction of basis convergence.
    crossing = qty * (long.ask-long.bid+short.ask-short.bid)
    fees = qty * ((long.ask+long.bid)*long.taker_fee+(short.ask+short.bid)*short.taker_fee)
    slip = qty * (long.ask+long.bid+short.ask+short.bid)*slippage_bps/10000
    return dict(long=long.name, short=short.name, long_cycles=lc, short_cycles=sc,
                funding_usd=funding, crossing_usd=crossing, fees_usd=fees,
                slippage_usd=slip, net_flat_basis_usd=funding-crossing-fees-slip,
                entry_basis_usd=qty*(short.bid-long.ask))

def rank(venues, now_ms, horizon_ms, qty, slippage_bps=15):
    results = []
    for long, short in permutations(venues, 2):
        try:
            results.append(evaluate(long, short, now_ms, horizon_ms, qty, slippage_bps))
        except ValueError:
            continue
    return sorted(results, key=lambda r:r["net_flat_basis_usd"], reverse=True)
