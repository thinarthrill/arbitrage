"""Public market scanner; requires ccxt. No credentials or private endpoints."""
import argparse
import json
import time
import re
from funding_strategy import Venue, rank

def scan(symbol, venues, fee_bps, qty, horizon_hours):
    import ccxt
    quotes=[]
    errors={}
    for name in venues:
        try:
            ex=getattr(ccxt,name)({"enableRateLimit":True,"options":{"defaultType":"swap"}})
            market=ex.load_markets()[symbol]
            if not market.get("linear") or not market.get("swap") or market.get("settle")!="USDT":
                raise ValueError("Not a linear USDT perpetual")
            f=ex.fetch_funding_rate(symbol)
            book=ex.fetch_order_book(symbol)
            if not book["bids"] or not book["asks"]:
                raise ValueError("Empty book")
            interval=f.get("interval")
            match=re.fullmatch(r"(\d+)([hm])",str(interval))
            if not match:
                raise ValueError("Unverified funding interval")
            interval_ms=int(match[1])*(3600000 if match[2]=="h" else 60000)
            next_ms=f.get("nextFundingTimestamp") or f.get("fundingTimestamp")
            if not next_ms or not f.get("fundingRate") is not None:
                raise ValueError("Missing funding fields")
            # CCXT derivatives books usually report contracts, not base units.
            size=float(market["contractSize"])
            if min(book["bids"][0][1],book["asks"][0][1])*size < qty:
                raise ValueError("Insufficient top-of-book depth")
            quotes.append(Venue(name,float(book["bids"][0][0]),float(book["asks"][0][0]),float(f["fundingRate"]),int(next_ms),interval_ms,fee_bps/10000))
        except Exception as exc:
            errors[name]=str(exc)
    return {"symbol":symbol,"errors":errors,"opportunities":rank(quotes,int(time.time()*1000),int(horizon_hours*3600000),qty)}

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--symbol",default="BTC/USDT:USDT")
    p.add_argument("--venues",default="binanceusdm,bybit,okx,gateio")
    p.add_argument("--fee-bps",type=float,required=True)
    p.add_argument("--qty",type=float,required=True)
    p.add_argument("--hours",type=float,default=8)
    a=p.parse_args()
    print(json.dumps(scan(a.symbol,a.venues.split(","),a.fee_bps,a.qty,a.hours),indent=2))
