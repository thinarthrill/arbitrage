"""Paper-only funding lifecycle. Replay chronological executable quotes and settlement events."""
from dataclasses import dataclass, field
from funding_strategy import Venue, evaluate, rank
import math

@dataclass
class Policy:
    min_net_usd: float = 0.25
    max_basis_bps: float = 30
    max_loss_usd: float = 2
    horizon_ms: int = 8*3600*1000
    slippage_bps: float = 5

@dataclass
class PaperPosition:
    symbol: str
    long: Venue
    short: Venue
    qty: float
    opened_ms: int
    policy: Policy
    funding: float = 0
    events: set = field(default_factory=set)
    closed: bool = False

    def settle(self, venue, timestamp_ms, rate, mark):
        if self.closed or timestamp_ms <= self.opened_ms or mark <= 0 or not all(map(math.isfinite,(rate,mark))):
            raise ValueError("Invalid settlement")
        if venue not in (self.long.name,self.short.name):
            raise ValueError("Unrelated venue")
        key = (venue,timestamp_ms)
        if key in self.events:
            return
        self.events.add(key)
        self.funding += self.qty*mark*rate*(1 if venue == self.short.name else -1)

    def pnl(self, long, short):
        if (long.name,short.name)!=(self.long.name,self.short.name):
            raise ValueError("Wrong venues")
        q=self.qty
        prices=(self.long.ask,self.short.bid,long.bid,short.ask)
        gross=q*(prices[2]-prices[0]+prices[1]-prices[3])
        fees=q*(prices[0]*self.long.taker_fee+prices[1]*self.short.taker_fee+prices[2]*long.taker_fee+prices[3]*short.taker_fee)
        slip=q*sum(prices)*self.policy.slippage_bps/10000
        return dict(price_pnl=gross,funding=self.funding,fees=fees,slippage=slip,net=gross+self.funding-fees-slip)

    def exit_reason(self,long,short,now_ms):
        if self.pnl(long,short)["net"] <= -self.policy.max_loss_usd:
            return "loss_limit"
        if now_ms >= self.opened_ms+self.policy.horizon_ms:
            return "horizon"
        if short.rate/short.interval_ms <= long.rate/long.interval_ms:
            return "funding_flip"
        return None

    def close(self,long,short):
        if self.closed:
            raise ValueError("Already closed")
        result=self.pnl(long,short)
        self.closed=True
        return result

def select(symbol, venues, now_ms, qty, policy):
    for candidate in rank(venues,now_ms,policy.horizon_ms,qty,policy.slippage_bps):
        by_name={v.name:v for v in venues}
        long,short=by_name[candidate["long"]],by_name[candidate["short"]]
        mid=(long.bid+long.ask+short.bid+short.ask)/4
        basis=abs((short.bid+short.ask-long.bid-long.ask)/2)/mid*10000
        if candidate["net_flat_basis_usd"]>=policy.min_net_usd and basis<=policy.max_basis_bps:
            return PaperPosition(symbol,long,short,qty,now_ms,policy)
    return None
