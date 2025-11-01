#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cross-Exchange PRICE Arbitrage (Perp Futures) — with external Spread Stats (z-score)

Дополнения:
- MEXC использует фьючерсный контрактный API (contract.mexc.com) — apples-to-apples с остальными.
- Вход по аномалии спреда: z >= Z_IN и positive economics (net_usd_adj > 0).
- Выход по z <= Z_OUT (или по EXIT_SPREAD_BPS).
- Статистика лог-спредов читается из SPREAD_STATS_PATH (обновляется сервисом spread_stats_collector.py).
"""

import os, time, json, math, hmac, hashlib, logging, uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd

# ----------------- ENV helpers -----------------
# ...existing code...
from dotenv import load_dotenv, find_dotenv
load_dotenv()

def getenv_str(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return d if v is None or v.strip()=="" else v.strip()
def getenv_bool(k: str, d: bool=False) -> bool:
    v = os.getenv(k)
    if v is None: return d
    return v.strip().lower() in ("1","true","t","yes","y","on")
def getenv_float(k: str, d: float) -> float:
    v = os.getenv(k)
    try: return float(v) if v is not None and v.strip()!="" else d
    except: return d
def getenv_list(k: str, default_list: List[str]) -> List[str]:
    v = os.getenv(k)
    if v is None or v.strip()=="":
        return default_list
    return [x.strip() for x in v.split(",") if x.strip()]

# ----------------- logging -----------------
logging.basicConfig(
    level=logging.DEBUG if getenv_bool("DEBUG", False) else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ----------------- HTTP -----------------
import certifi, requests
os.environ.setdefault("SSL_CERT_FILE", certifi.where())
REQUEST_TIMEOUT = int(getenv_float("REQUEST_TIMEOUT", 8))
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": getenv_str("USER_AGENT", "ArbBot/1.2-price-z")})
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
_retry = Retry(total=3, backoff_factor=0.4, status_forcelist=[429,500,502,503,504], allowed_methods=["GET","POST"], raise_on_status=False)
_adapter = HTTPAdapter(max_retries=_retry)
SESSION.mount("https://", _adapter); SESSION.mount("http://", _adapter)

# порог для мгновенного алерта и защита от спама
ALERT_SPREAD_PCT   = float(getenv_float("ALERT_SPREAD_PCT", 2.0))     # 2% по умолчанию
ALERT_COOLDOWN_SEC = int(getenv_float("ALERT_COOLDOWN_SEC", 60))      # не чаще раза в 60с на символ
_LAST_ALERT_TS: Dict[str, float] = {}

def _get(u, params=None):
    try:
        r = SESSION.get(u, params=params or {}, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200: return None
        return r.json()
    except Exception:
        return None

# ----------------- utils -----------------
def utc_ms_now() -> int: return int(datetime.now(timezone.utc).timestamp()*1000)
def iso_utc(ms: Optional[int]) -> str:
    if not ms: return ""
    return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat()

def to_float(x) -> Optional[float]:
    try:
        if x is None: return None
        return float(x)
    except: return None

def _round_step(value: float, step: float) -> float:
    if not step or step <= 0: return value
    return math.floor(value/step)*step

def per_leg_notional_from_capital(capital_usd: float, leverage: float) -> float:
    if leverage <= 0: return max(0.0, float(capital_usd))
    return float(capital_usd) / (1.0 + 1.0/float(leverage))

# ----------------- Telegram -----------------
def format_signal_card(r: dict, per_leg_notional_usd: float, price_source: str) -> str:
    sym      = str(r["symbol"])
    long_ex  = str(r["long_ex"]).upper()
    short_ex = str(r["short_ex"]).upper()
    px_low   = float(r["px_low"])
    px_high  = float(r["px_high"])
    sp_pct   = float(r["spread_pct"])
    sp_bps   = float(r["spread_bps"])
    net_usd  = float(r["net_usd"])
    gross    = float(r["gross_usd"])
    fees_rt  = float(r["fees_roundtrip_usd"])
    ts       = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    price_lbl = {"mid":"MID","last":"LAST","mark":"MARK","bid":"BID","ask":"ASK","book":"BBO"}.get(price_source.lower(),"MID")
    z = r.get("z", None)
    std = r.get("std", None)
    net_usd_adj = r.get("net_usd_adj", None)

    a_long  = _anchor(long_ex.lower(),  sym)
    a_short = _anchor(short_ex.lower(), sym)

    z_line = ""
    if "z" in r and r["z"] is not None:
        try: z_line = f"📏 <b>Z-score</b>: <b>{float(r['z']):.2f}</b>\n"
        except: pass

    lines = [
        "──────────────",
         f"🧭 PRICE ARB • <b>{sym}</b>",
         f"{_anchor(long_ex, sym)} BUY  ↔  {_anchor(short_ex, sym)} SELL",
         f"🧮 Spread: {sp_pct:.2f}%  ({sp_bps:.0f} bps)",
         f"💵 Gross: ${gross:.2f}",
         f"💸 Fees RT: ${fees_rt:.2f}",
         f"✅ Net: ${net_usd:.2f}",
    ]
    if net_usd_adj is not None:
        lines.append(f"🧮 Net after slippage: ${float(net_usd_adj):.2f}")
    if z is not None and z == z:  # not NaN
        if std is not None and std == std:
            lines.append(f"📈 Z-score: {float(z):.2f} (σ={float(std):.2f})")
        else:
            lines.append(f"📈 Z-score: {float(z):.2f}")
    lines.extend([
        f"📊 Prices [{price_lbl}]",
        f"   Low @ {long_ex}:  {px_low:.6f}",
        f"   High @ {short_ex}: {px_high:.6f}",
        f"🕒 {ts}"
    ])
    return "\n".join(lines)

def maybe_send_telegram(text: str) -> None:
    token = getenv_str("TELEGRAM_BOT_TOKEN","")
    chat_id = getenv_str("TELEGRAM_CHAT_ID","")
    if not token or not chat_id: return
    try:
        r = SESSION.post(f"https://api.telegram.org/bot{token}/sendMessage",
                         json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True, "parse_mode": "HTML"},
                         timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logging.warning("Telegram send failed: %s %s", r.status_code, r.text[:200])
    except Exception as e:
        logging.warning("Telegram exception: %s", e)

# ----------------- CSV / GCS -----------------
def is_gs(path: Optional[str]) -> bool: return bool(path) and str(path).startswith("gs://")
def bucketize_path(path: Optional[str]) -> Optional[str]:
    if not path or path.strip()=="": return path
    p = path.strip()
    if is_gs(p) or os.path.isabs(p):
        return p
    backet = getenv_str("BACKET","")
    if backet:
        norm = p.lstrip('/').replace('\\', '/')
        return f"gs://{backet}/{norm}"
    return p

GCS_AVAILABLE = True
try:
    from google.cloud import storage  # type: ignore
    from google.oauth2 import service_account  # type: ignore
except Exception:
    GCS_AVAILABLE = False

def gcs_client():
    if not GCS_AVAILABLE: raise RuntimeError("google-cloud-storage not installed")
    key_str = os.getenv("GCS_KEY_JSON","").strip()
    if key_str:
        info = json.loads(key_str)
        creds = service_account.Credentials.from_service_account_info(info)
        return storage.Client(project=info.get("project_id"), credentials=creds)
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS","").strip()
    if key_path:
        return storage.Client.from_service_account_json(key_path)
    return storage.Client()

def gcs_read_csv(gs_path: str, expected_columns: List[str]) -> pd.DataFrame:
    try:
        client = gcs_client()
        bucket_name = gs_path[5:].split("/",1)[0]; blob_name = gs_path[5+len(bucket_name)+1:]
        bucket = client.lookup_bucket(bucket_name); blob = bucket.blob(blob_name)
        if not blob.exists(): return pd.DataFrame(columns=expected_columns)
        data = blob.download_as_bytes()
        df = pd.read_csv(pd.io.common.BytesIO(data))
        for c in expected_columns:
            if c not in df.columns: df[c] = None
        return df[expected_columns]
    except Exception as e:
        logging.warning("GCS read error %s: %s", gs_path, e)
        return pd.DataFrame(columns=expected_columns)

def gcs_write_csv(gs_path: str, df: pd.DataFrame):
    try:
        client = gcs_client()
        bucket_name = gs_path[5:].split("/",1)[0]; blob_name = gs_path[5+len(bucket_name)+1:]
        bucket = client.lookup_bucket(bucket_name); blob = bucket.blob(blob_name)
        from io import StringIO
        buf = StringIO(); df.to_csv(buf, index=False)
        blob.upload_from_string(buf.getvalue(), content_type="text/csv")
    except Exception as e:
        logging.warning("GCS write error %s: %s", gs_path, e)

def write_csv(path: Optional[str], df: pd.DataFrame) -> None:
    if not path or path.strip()=="": return
    p = bucketize_path(path)
    if is_gs(p): gcs_write_csv(p, df); return
    import os
    os.makedirs(os.path.dirname(os.path.abspath(p)), exist_ok=True)
    df.to_csv(p, index=False)

def read_csv(path: Optional[str], columns: List[str]) -> pd.DataFrame:
    if not path or path.strip()=="": return pd.DataFrame(columns=columns)
    p = bucketize_path(path)
    if is_gs(p): return gcs_read_csv(p, columns)
    import os
    if not os.path.exists(p): return pd.DataFrame(columns=columns)
    try:
        df = pd.read_csv(p)
        for c in columns:
            if c not in df.columns: df[c]=None
        return df[columns]
    except: return pd.DataFrame(columns=columns)

# ----------------- Exchanges bases -----------------
def bybit_base() -> str:
    return "https://api.bybit.com"
def bybit_data_base() -> str:
    return "https://api.bybit.com"
def binance_fapi_base() -> str:
    return "https://fapi.binance.com"
def binance_data_base() -> str:
    return "https://fapi.binance.com"
def okx_base() -> str: return "https://www.okx.com"
def gate_base() -> str:
    return "https://api.gateio.ws"
def gate_contract_from_symbol(symbol: str) -> str:
    sym = symbol.upper()
    if sym.endswith("USDT"): return f"{sym[:-4]}_USDT"
    if sym.endswith("USD"):  return f"{sym[:-3]}_USD"
    return sym

# ----------------- Market data: bid/ask/mid -----------------
def binance_book_ticker(symbol: str) -> Optional[Dict[str, Any]]:
    j = _get(f"{binance_data_base()}/fapi/v1/ticker/bookTicker", {"symbol": symbol.upper()})
    if not j: return None
    bid = to_float(j.get("bidPrice")); ask = to_float(j.get("askPrice"))
    if bid is None or ask is None: return None
    mid = (bid+ask)/2.0 if (bid and ask) else None
    return {"exchange":"binance","symbol":symbol.upper(),"bid":bid,"ask":ask,"mid":mid,"ts":utc_ms_now()}

def bybit_book_ticker(symbol: str) -> Optional[Dict[str, Any]]:
    j = _get(f"{bybit_data_base()}/v5/market/tickers", {"category":"linear","symbol":symbol.upper()}) or {}
    lst = (j.get("result") or {}).get("list") or []
    if not lst: return None
    row = lst[0]
    bid = to_float(row.get("bid1Price")) or to_float(row.get("bidPrice"))
    ask = to_float(row.get("ask1Price")) or to_float(row.get("askPrice"))
    if bid is None or ask is None: return None
    mid = (bid+ask)/2.0
    return {"exchange":"bybit","symbol":symbol.upper(),"bid":bid,"ask":ask,"mid":mid,"ts":utc_ms_now()}

def okx_book_ticker(symbol: str) -> Optional[Dict[str, Any]]:
    inst = f"{symbol[:-4]}-USDT-SWAP" if symbol.upper().endswith("USDT") else f"{symbol}-USDT-SWAP"
    j = _get(f"{okx_base()}/api/v5/market/ticker", {"instId": inst})
    data = (j or {}).get("data") or []
    if not data: return None
    row = data[0]
    bid = to_float(row.get("bidPx")); ask = to_float(row.get("askPx"))
    if bid is None or ask is None: return None
    mid = (bid+ask)/2.0
    return {"exchange":"okx","symbol":symbol.upper(),"bid":bid,"ask":ask,"mid":mid,"ts":utc_ms_now()}

# ----------------- Market data: bid/ask/last/mark -----------------
def binance_quote(symbol: str) -> Optional[Dict[str, Any]]:
    sym = symbol.upper()
    bt = _get(f"{binance_data_base()}/fapi/v1/ticker/bookTicker", {"symbol": sym}) or {}
    bid = to_float(bt.get("bidPrice")); ask = to_float(bt.get("askPrice"))
    lt = _get(f"{binance_data_base()}/fapi/v1/ticker/price", {"symbol": sym}) or {}
    last = to_float(lt.get("price"))
    pi = _get(f"{binance_data_base()}/fapi/v1/premiumIndex", {"symbol": sym}) or {}
    mark = to_float(pi.get("markPrice"))
    mid = (bid+ask)/2.0 if (bid and ask) else None
    return {"exchange":"binance","symbol":sym,"bid":bid,"ask":ask,"mid":mid,"last":last,"mark":mark,"ts":utc_ms_now()}

def bybit_quote(symbol: str) -> Optional[Dict[str, Any]]:
    sym = symbol.upper()
    t = _get(f"{bybit_data_base()}/v5/market/tickers", {"category":"linear","symbol":sym}) or {}
    lst = (t.get("result") or {}).get("list") or []
    if not lst: return None
    row = lst[0]
    bid = to_float(row.get("bid1Price") or row.get("bidPrice"))
    ask = to_float(row.get("ask1Price") or row.get("askPrice"))
    last = to_float(row.get("lastPrice"))
    mark = to_float(row.get("markPrice")) or to_float(row.get("indexPrice"))
    mid = (bid+ask)/2.0 if (bid and ask) else None
    return {"exchange":"bybit","symbol":sym,"bid":bid,"ask":ask,"mid":mid,"last":last,"mark":mark,"ts":utc_ms_now()}

def okx_quote(symbol: str) -> Optional[Dict[str, Any]]:
    sym = symbol.upper()
    inst = f"{sym[:-4]}-USDT-SWAP" if sym.endswith("USDT") else f"{sym}-USDT-SWAP"
    j = _get(f"{okx_base()}/api/v5/market/ticker", {"instId": inst}) or {}
    data = (j.get("data") or [])
    if not data: return None
    row = data[0]
    bid = to_float(row.get("bidPx")); ask = to_float(row.get("askPx"))
    last = to_float(row.get("last"))
    mark = to_float(row.get("markPx"))
    mid = (bid+ask)/2.0 if (bid and ask) else None
    return {"exchange":"okx","symbol":sym,"bid":bid,"ask":ask,"mid":mid,"last":last,"mark":mark,"ts":utc_ms_now()}

def mexc_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """Перпетуалы MEXC: depth + fair_price из contract.mexc.com"""
    try:
        contract = symbol.upper().replace("USDT","_USDT")
        depth = _get(f"https://contract.mexc.com/api/v1/contract/depth/{contract}") or {}
        bids = depth.get("bids") or []; asks = depth.get("asks") or []
        bid = to_float(bids[0][0]) if bids else None
        ask = to_float(asks[0][0]) if asks else None
        if bid is None or ask is None: return None
        mid = (bid + ask) / 2.0

        fair = _get(f"https://contract.mexc.com/api/v1/contract/fair_price/{contract}") or {}
        fair_data = fair.get("data") or {}
        mark = to_float(fair_data.get("fairPrice"))

        return {"exchange":"mexc","symbol":symbol.upper(),
                "bid":bid,"ask":ask,"mid":mid,"last":None,"mark":mark,"ts":utc_ms_now()}
    except Exception:
        return None

def gate_quote(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        contract = gate_contract_from_symbol(symbol)
        j = _get(f"{gate_base()}/api/v4/futures/usdt/tickers", {"contract": contract}) or []
        if not j: return None
        row = j[0] if isinstance(j, list) else j
        bid = to_float(row.get("best_bid"))
        ask = to_float(row.get("best_ask"))
        last = to_float(row.get("last"))
        mark = to_float(row.get("mark_price"))
        if bid is None or ask is None:
            pxs = [p for p in [to_float(row.get("last")), to_float(row.get("index_price")), mark] if p]
            if not pxs: return None
            mid = sum(pxs)/len(pxs)
            return {"exchange":"gate","symbol":symbol.upper(),"bid":None,"ask":None,"mid":mid,"last":last,"mark":mark,"ts":utc_ms_now()}
        mid = (bid + ask) / 2.0
        return {"exchange":"gate","symbol":symbol.upper(),"bid":bid,"ask":ask,"mid":mid,"last":last,"mark":mark,"ts":utc_ms_now()}
    except Exception:
        return None

# ----------------- Scanners -----------------
def scan_all_with_instant_alerts(
    exchanges: List[str],
    symbols: List[str],
    per_leg_notional_usd: float,
    taker_fee: float,
    price_source: str,
    alert_spread_pct: float,
    cooldown_sec: int,
) -> pd.DataFrame:
    import pandas as pd
    rows_all = []
    now = time.time()

    stats_df = read_spread_stats()
    Z_IN = float(getenv_float("Z_IN", 2.5))
    SLIPPAGE_BPS = float(getenv_float("SLIPPAGE_BPS", 1.0))

    for sym in symbols:
        per_symbol_rows = []
        for ex in exchanges:
            if ex == "binance":
                r = binance_quote(sym)
            elif ex == "bybit":
                r = bybit_quote(sym)
            elif ex == "okx":
                r = okx_quote(sym)
            elif ex=="mexc":
                r = mexc_quote(sym)
            elif ex=="gate":
                r = gate_quote(sym)
            else:
                r = None
            if r:
                rows_all.append(r)
                per_symbol_rows.append(r)

        best = best_pair_for_symbol(per_symbol_rows, per_leg_notional_usd, taker_fee, price_source)
        if best and best["spread_pct"] >= float(alert_spread_pct):
            # считаем Z и net_usd_adj; INSTANT шлем только если z>=Z_IN и экономика положительная
            x, z, std = get_z_for_pair(
                stats_df,
                symbol=str(best["symbol"]),
                ex_low=str(best["long_ex"]),
                ex_high=str(best["short_ex"]),
                px_low=float(best["px_low"]),
                px_high=float(best["px_high"]),
            )
            net_usd_adj = float(best["net_usd"]) - (4.0 * (SLIPPAGE_BPS/1e4) * per_leg_notional_usd)
            best["z"] = z
            best["std"] = std
            best["net_usd_adj"] = net_usd_adj
            z_ok = (z == z) and (z >= Z_IN)  # z not NaN and above threshold
            eco_ok = net_usd_adj > 0.0

            last_ts = _LAST_ALERT_TS.get(best["symbol"], 0.0)
            if z_ok and eco_ok and (now - last_ts >= cooldown_sec):
                card = format_signal_card(best, per_leg_notional_usd, price_source)
                maybe_send_telegram("🚨 <b>INSTANT ALERT</b>\n" + card)
                _LAST_ALERT_TS[best["symbol"]] = now

    cols = ["exchange","symbol","bid","ask","mid","last","mark","ts"]
    import pandas as pd
    return pd.DataFrame(rows_all) if rows_all else pd.DataFrame(columns=cols)

def scan_all(exchanges: List[str], symbols: List[str]) -> pd.DataFrame:
    import pandas as pd
    rows=[]
    for ex in exchanges:
        for sym in symbols:
            if ex=="binance": r=binance_quote(sym)
            elif ex=="bybit": r=bybit_quote(sym)
            elif ex=="okx":   r=okx_quote(sym)
            elif ex=="mexc":  r=mexc_quote(sym)
            elif ex=="gate":  r=gate_quote(sym)
            else:             r=None
            if r: rows.append(r)
    cols=["exchange","symbol","bid","ask","mid","last","mark","ts"]
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=cols)

# ----------------- Trading API (Binance/Bybit) -----------------
_BINANCE_TIME_OFFSET_MS = 0
_BINANCE_TIME_SYNCED_AT = 0
def binance_sync_time():
    global _BINANCE_TIME_OFFSET_MS, _BINANCE_TIME_SYNCED_AT
    try:
        r = SESSION.get(f"{binance_data_base()}/fapi/v1/time", timeout=REQUEST_TIMEOUT)
        if r.status_code==200:
            srv = int(r.json().get("serverTime")); now = int(time.time()*1000)
            _BINANCE_TIME_OFFSET_MS = srv - now; _BINANCE_TIME_SYNCED_AT = now
    except: pass

from urllib.parse import urlencode
def _fmt_val(v):
    if isinstance(v, bool): return "true" if v else "false"
    if isinstance(v, float):
        s = ("%.10f" % v).rstrip("0").rstrip("."); return s if s else "0"
    return str(v)

def binance_signed_post(params: dict) -> dict:
    api_key = getenv_str("BINANCE_API_KEY",""); api_secret = getenv_str("BINANCE_API_SECRET","")
    now = int(time.time()*1000)
    if now - (_BINANCE_TIME_SYNCED_AT or 0) > 60_000: binance_sync_time()
    base = dict(params); base["timestamp"] = now + (_BINANCE_TIME_OFFSET_MS or 0)
    items = sorted((k, _fmt_val(v)) for k,v in base.items()); qs = urlencode(items, doseq=True)
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    body = qs + "&signature=" + sig
    return {"headers":{"X-MBX-APIKEY":api_key, "Content-Type":"application/x-www-form-urlencoded"}, "data":body}

_BINANCE_SYMBOL_META = {}
def _binance_symbol_info(symbol: str) -> Optional[dict]:
    sym = symbol.upper()
    if sym in _BINANCE_SYMBOL_META: return _BINANCE_SYMBOL_META[sym]
    try:
        r = SESSION.get(f"{binance_fapi_base()}/fapi/v1/exchangeInfo", timeout=REQUEST_TIMEOUT)
        j = r.json()
        for s in j.get("symbols", []): _BINANCE_SYMBOL_META[s["symbol"]] = s
        return _BINANCE_SYMBOL_META.get(sym)
    except: return None

def binance_feasible(symbol: str, qty: float, price: float) -> Tuple[bool,str,float]:
    meta = _binance_symbol_info(symbol) or {}
    filters = {f["filterType"]: f for f in meta.get("filters", [])}
    lot = filters.get("LOT_SIZE") or {}
    min_qty = float(lot.get("minQty") or 0); step = float(lot.get("stepSize") or 0)
    q = _round_step(qty, step)
    if q < min_qty: return False, f"minQty {min_qty} not met (rounded {q})", q
    notional = filters.get("MIN_NOTIONAL") or {}
    min_not = float(notional.get("minNotional") or notional.get("notional") or 0)
    if price*q < min_not: return False, f"minNotional {min_not} not met (px*qty={price*q})", q
    return True, "ok", q

def _bybit_ts() -> str: return str(int(time.time()*1000))
def bybit_signed_post(payload: dict, recv_window: str="5000") -> dict:
    api_key = getenv_str("BYBIT_API_KEY",""); api_secret = getenv_str("BYBIT_API_SECRET","")
    ts = _bybit_ts(); body = json.dumps(payload or {}, ensure_ascii=False, separators=(",",":"))
    origin = f"{ts}{api_key}{recv_window}{body}"
    sign = hmac.new(api_secret.encode(), origin.encode(), hashlib.sha256).hexdigest()
    headers = {"X-BAPI-API-KEY": api_key, "X-BAPI-TIMESTAMP": ts, "X-BAPI-RECV-WINDOW": recv_window,
               "X-BAPI-SIGN": sign, "X-BAPI-SIGN-TYPE":"2", "Content-Type":"application/json",
               "User-Agent": getenv_str("USER_AGENT","ArbBot/1.2-price-z")}
    return {"headers": headers, "data": body}

_BYBIT_SYMBOL_META = {}
def _bybit_symbol_info(symbol: str) -> Optional[dict]:
    try:
        j = _get(f"{bybit_base()}/v5/market/instruments-info", {"category":"linear","symbol":symbol.upper()}) or {}
        lst = (j.get("result") or {}).get("list") or []
        if lst:
            _BYBIT_SYMBOL_META[symbol.upper()] = lst[0]
            return lst[0]
    except: pass
    return _BYBIT_SYMBOL_META.get(symbol.upper())

def bybit_feasible(symbol: str, qty: float, price: float) -> Tuple[bool,str,float]:
    meta = _bybit_symbol_info(symbol) or {}
    lot_step = float((meta.get("lotSizeFilter") or {}).get("qtyStep") or 0)
    min_qty  = float((meta.get("lotSizeFilter") or {}).get("minOrderQty") or 0)
    q = _round_step(qty, lot_step or 1)
    if q < min_qty: return False, f"minOrderQty {min_qty} not met (rounded {q})", q
    min_notional = float((meta.get("lotSizeFilter") or {}).get("minOrderAmt") or 0)
    if min_notional and price*q < min_notional:
        return False, f"minOrderAmt {min_notional} not met (px*qty={price*q})", q
    return True, "ok", q

def _place_perp_market_order(exchange: str, symbol: str, side: str, qty: float, paper: bool=False) -> dict:
    if paper:
        return {"status":"FILLED","avg_price":0.0,"order_id": f"paper-{uuid.uuid4().hex[:8]}"}
    ex = exchange.lower()
    if ex == "binance":
        binance_sync_time()
        signed = binance_signed_post({"symbol":symbol.upper(),"side":side.upper(),"type":"MARKET","quantity":qty,"recvWindow":10000})
        r = SESSION.post(f"{binance_fapi_base()}/fapi/v1/order", headers=signed["headers"], data=signed["data"], timeout=REQUEST_TIMEOUT)
        j = r.json()
        if r.status_code==200:
            px = 0.0
            try: px = float((j.get("fills") or [{}])[0].get("price") or 0.0)
            except: pass
            return {"status":"FILLED","avg_price":px,"order_id":str(j.get("orderId"))}
        raise RuntimeError(f"Binance order failed: {r.status_code} {str(j)[:200]}")
    if ex == "bybit":
        base = bybit_base()
        params = {"category":"linear","symbol":symbol.upper(),"side":side.capitalize(),
                  "orderType":"Market","qty":qty,"timeInForce":"IOC"}
        signed = bybit_signed_post(params)
        r = SESSION.post(f"{base}/v5/order/create", headers=signed["headers"], data=signed["data"], timeout=REQUEST_TIMEOUT)
        j = r.json()
        if j.get("retCode")==0:
            avg = 0.0
            try: avg = float((j["result"].get("avgPrice") or 0.0))
            except: pass
            return {"status":"FILLED","avg_price":avg,"order_id":j["result"].get("orderId")}
        raise RuntimeError(f"Bybit order failed: {j}")
    raise ValueError(f"unsupported exchange {exchange}")

# ----------------- Candidate builder -----------------
import pandas as pd

def exchange_trade_url(exchange: str, symbol: str) -> str:
    ex = exchange.lower(); base = symbol.upper().replace("USDT",""); quote = "USDT"
    if ex=="binance": return f"https://www.binance.com/en/futures/{base}{quote}?type=perpetual"
    if ex=="bybit":   return f"https://www.bybit.com/trade/usdt/{base}{quote}"
    if ex=="okx":     return f"https://www.okx.com/trade-swap/{base.lower()}-{quote.lower()}-swap"
    if ex == "mexc":
        contract = symbol.replace("USDT", "_USDT")
        return f"https://futures.mexc.com/exchange/{contract}"
    if ex == "gate":
        contract = gate_contract_from_symbol(symbol)
        return f"https://www.gate.com/ru/futures/USDT/{contract}"
    return ""

def _anchor(exchange: str, symbol: str) -> str:
    url = exchange_trade_url(exchange, symbol)
    return f'<a href="{url}">{symbol}@{exchange.upper()}</a>' if url else f"{symbol}@{exchange.upper()}"

def select_px(row: pd.Series, price_source: str) -> Optional[float]:
    ps = price_source.lower()
    if ps in ("mid",""):  return to_float(row.get("mid"))
    if ps == "last":      return to_float(row.get("last"))
    if ps == "mark":      return to_float(row.get("mark"))
    if ps == "bid":       return to_float(row.get("bid"))
    if ps == "ask":       return to_float(row.get("ask"))
    return to_float(row.get("mid"))

def best_pair_for_symbol(rows: List[Dict[str, Any]], per_leg_notional_usd: float, taker_fee: float, price_source: str) -> Optional[dict]:
    if not rows: return None
    def pick_px(r):
        if price_source == "last": return to_float(r.get("last"))
        if price_source == "mark": return to_float(r.get("mark"))
        if price_source == "bid":  return to_float(r.get("bid"))
        if price_source == "ask":  return to_float(r.get("ask"))
        return to_float(r.get("mid"))

    if price_source == "book":
        asks = [r for r in rows if to_float(r.get("ask")) is not None]
        bids = [r for r in rows if to_float(r.get("bid")) is not None]
        if len(asks) < 1 or len(bids) < 1: return None
        row_low  = min(asks, key=lambda r: float(to_float(r.get("ask"))))
        row_high = max(bids, key=lambda r: float(to_float(r.get("bid"))))
        if row_low["exchange"] == row_high["exchange"]:
            # если совпали, альтернативы нет — возвращаем None, чтобы не строить фальшивую пару
            return None
        sym = str(row_low["symbol"])
        px_low, ex_low   = float(row_low["ask"]),   str(row_low["exchange"])
        px_high, ex_high = float(row_high["bid"]),  str(row_high["exchange"])
        spread = px_high - px_low
        spread_pct = (spread/px_low)*100.0 if px_low>0 else 0.0
        spread_bps = spread_pct*100.0
        qty = per_leg_notional_usd / px_low if px_low>0 else 0.0
        gross = spread * qty
        fees_rt = 4.0 * taker_fee * per_leg_notional_usd
        net = gross - fees_rt
        return {
            "symbol": sym,
            "long_ex": ex_low, "short_ex": ex_high,
            "px_low": px_low, "px_high": px_high,
            "spread": spread, "spread_pct": spread_pct, "spread_bps": spread_bps,
            "qty_est": qty, "gross_usd": gross,
            "fees_roundtrip_usd": fees_rt,
            "net_usd": net
        }
    # Обычные режимы: одна метрика цены на обеих биржах
    usable = []
    for r in rows:
        px = pick_px(r)
        if px is not None:
            usable.append({"exchange": r["exchange"], "symbol": r["symbol"], "px": float(px)})
    if len(usable) < 2: return None
    cheapest = min(usable, key=lambda x:x["px"])
    priciest = max(usable, key=lambda x:x["px"])

    px_low, px_high = float(cheapest["px"]), float(priciest["px"])
    spread = px_high - px_low
    spread_pct = (spread/px_low)*100.0 if px_low>0 else 0.0
    spread_bps = spread_pct*100.0
    qty = per_leg_notional_usd / px_low if px_low>0 else 0.0
    gross = spread * qty
    fees_rt = 4.0 * taker_fee * per_leg_notional_usd
    net = gross - fees_rt
    return {
        "symbol": rows[0]["symbol"].upper(),
        "long_ex": cheapest["exchange"],
        "short_ex": priciest["exchange"],
        "px_low": px_low,
        "px_high": px_high,
        "spread": spread,
        "spread_pct": spread_pct,
        "spread_bps": spread_bps,
        "qty_est": qty,
        "gross_usd": gross,
        "fees_roundtrip_usd": fees_rt,
        "net_usd": net
    }

def build_price_arbitrage(df_raw: pd.DataFrame, per_leg_notional_usd: float, taker_fee: float, price_source: str) -> pd.DataFrame:
    if df_raw.empty: return pd.DataFrame()
    use = df_raw.copy()
    ps = price_source.lower()
    if ps == "book":
        # Для книги заявок работаем сразу с bid/ask, не создаём единую "px"
        pass
    else:
        use["px"] = use.apply(lambda r: select_px(r, price_source), axis=1)
        use = use.dropna(subset=["px"])
    out_rows=[]
    for sym in sorted(use["symbol"].unique()):
        sub = use[use["symbol"]==sym]
        if ps == "book":
            sub_ask = sub.dropna(subset=["ask"])
            sub_bid = sub.dropna(subset=["bid"])
            if len(sub_ask) < 1 or len(sub_bid) < 1: continue
            cheapest = sub_ask.loc[sub_ask["ask"].astype(float).idxmin()]
            priciest = sub_bid.loc[sub_bid["bid"].astype(float).idxmax()]
            if str(cheapest["exchange"]) == str(priciest["exchange"]):
                continue
            px_low, ex_low   = float(cheapest["ask"]),  str(cheapest["exchange"])
            px_high, ex_high = float(priciest["bid"]),  str(priciest["exchange"])
        else:
            if len(sub) < 2: continue
            cheapest = sub.loc[sub["px"].idxmin()]
            priciest = sub.loc[sub["px"].idxmax()]
            px_low, ex_low = float(cheapest["px"]), str(cheapest["exchange"])
            px_high, ex_high = float(priciest["px"]), str(priciest["exchange"])

        spread = px_high - px_low
        spread_pct = (spread/px_low)*100.0 if px_low>0 else 0.0
        spread_bps = spread_pct*100.0
        qty = per_leg_notional_usd / px_low if px_low>0 else 0.0
        gross = spread * qty
        fees_rt = 4.0 * taker_fee * per_leg_notional_usd
        net = gross - fees_rt
        out_rows.append({
            "symbol": sym,
            "long_ex": ex_low, "short_ex": ex_high,
            "px_low": px_low, "px_high": px_high,
            "spread": spread, "spread_pct": spread_pct, "spread_bps": spread_bps,
            "qty_est": qty, "gross_usd": gross,
            "fees_roundtrip_usd": fees_rt,
            "net_usd": net
        })
    out = pd.DataFrame(out_rows)
    if not out.empty:
        out = out.sort_values(["net_usd","spread_bps"], ascending=[False,False]).reset_index(drop=True)
    return out

# ----------------- Z-score: reading spread stats -----------------
SPREAD_STATS_PATH = bucketize_path(getenv_str("SPREAD_STATS_PATH", "spread_stats.csv"))
STATS_COLS = ["symbol","ex_low","ex_high","ema_mean","ema_var","count","updated_ms"]

def read_spread_stats() -> pd.DataFrame:
    return read_csv(SPREAD_STATS_PATH, STATS_COLS)
ALPHA = float(getenv_float("SPREAD_EMA_ALPHA", 0.05))
SAVE_EVERY_SEC = int(getenv_float("SAVE_EVERY_SEC", 30))

class StatsStore:
    """
    Локальный стор статистики спредов: обновляет EMA и периодически сохраняет CSV (локально или gs://).
    """
    def __init__(self, path: Optional[str], alpha: float):
        self.path = path
        self.alpha = alpha
        self.df = read_csv(self.path, STATS_COLS)
        self._last_save = time.time()

    def _mask(self, symbol: str, ex_low: str, ex_high: str):
        return (self.df["symbol"]==symbol.upper()) & (self.df["ex_low"]==ex_low.lower()) & (self.df["ex_high"]==ex_high.lower())

    def _ema_update(self, mean: Optional[float], var: Optional[float], x: float) -> Tuple[float,float]:
        if mean is None or var is None or mean != mean or var != var:
            return x, 0.0
        new_mean = self.alpha * x + (1 - self.alpha) * mean
        new_var  = self.alpha * (x - new_mean) ** 2 + (1 - self.alpha) * var
        return new_mean, new_var

    def update_pair(self, symbol: str, ex_low: str, ex_high: str, x: float):
        m = self._mask(symbol, ex_low, ex_high)
        now_ms = int(datetime.now(timezone.utc).timestamp()*1000)
        if not self.df[m].empty:
            i = self.df[m].index[0]
            mean = to_float(self.df.at[i,"ema_mean"]); var = to_float(self.df.at[i,"ema_var"])
            cnt  = int(self.df.at[i,"count"] or 0)
            mean2, var2 = self._ema_update(mean, var, x)
            self.df.at[i,"ema_mean"]=mean2; self.df.at[i,"ema_var"]=var2
            self.df.at[i,"count"]=cnt+1;   self.df.at[i,"updated_ms"]=now_ms
        else:
            row = {"symbol":symbol.upper(),"ex_low":ex_low.lower(),"ex_high":ex_high.lower(),
                   "ema_mean":x,"ema_var":1e-6,"count":1,"updated_ms":now_ms}
            self.df = pd.concat([self.df, pd.DataFrame([row])], ignore_index=True)

    def maybe_save(self, force: bool=False):
        if not self.path: return
        now = time.time()
        if force or (now - self._last_save) >= SAVE_EVERY_SEC:
            write_csv(self.path, self.df)
            self._last_save = now
            logging.info("Spread stats saved: rows=%d", len(self.df))

def get_z_for_pair(stats: pd.DataFrame, symbol: str, ex_low: str, ex_high: str, px_low: float, px_high: float) -> Tuple[float,float,float]:
    """Возвращает (x, z, std). Если нужной строки в stats нет — z=nan."""
    if px_low <= 0 or px_high <= 0:
        return float('nan'), float('nan'), float('nan')
    x = math.log(px_high/px_low)
    s, a, b = symbol.upper(), ex_low.lower(), ex_high.lower()
    sub = stats[(stats["symbol"]==s) & (stats["ex_low"]==a) & (stats["ex_high"]==b)]
    if sub.empty:
        return x, float('nan'), float('nan')
    mean = to_float(sub.iloc[0].get("ema_mean"))
    var  = to_float(sub.iloc[0].get("ema_var"))
    std  = math.sqrt(max(var or 0.0, 1e-12))
    z    = (x - (mean or 0.0)) / std if std>0 else float('nan')
    return x, z, std

# ----------------- Atomic open with rollback -----------------
def new_attempt_id() -> str: return uuid.uuid4().hex[:12]

def atomic_cross_open(symbol: str, cheap_ex: str, rich_ex: str,
                      qty: float, price_low: float, price_high: float,
                      paper: bool) -> Tuple[bool,str,dict]:
    attempt_id = new_attempt_id()
    if cheap_ex=="bybit": okA, msgA, qA = bybit_feasible(symbol, qty, price_low)
    else:                 okA, msgA, qA = binance_feasible(symbol, qty, price_low)
    if rich_ex=="bybit":  okB, msgB, qB = bybit_feasible(symbol, qty, price_high)
    else:                 okB, msgB, qB = binance_feasible(symbol, qty, price_high)
    if not okA or not okB:
        return False, attempt_id, {"error": f"not feasible: {cheap_ex}({msgA}), {rich_ex}({msgB})"}
    qty_final = min(qA, qB)
    if qty_final<=0: return False, attempt_id, {"error":"qty_final<=0 after rounding"}

    try:
        oa = _place_perp_market_order(cheap_ex, symbol, "BUY", qty_final, paper=paper)
        if oa.get("status")!="FILLED": raise RuntimeError(f"legA not filled: {oa}")
    except Exception as e:
        return False, attempt_id, {"error": f"legA error: {e}"}
    try:
        ob = _place_perp_market_order(rich_ex, symbol, "SELL", qty_final, paper=paper)
        if ob.get("status")!="FILLED": raise RuntimeError(f"legB not filled: {ob}")
    except Exception as e:
        try: _ = _place_perp_market_order(cheap_ex, symbol, "SELL", qty_final, paper=paper)
        except Exception as e2: logging.error("Rollback failed: %s", e2)
        return False, attempt_id, {"error": f"legB error: {e}"}
    return True, attempt_id, {"qty": qty_final}

# ----------------- Positions -----------------
POS_COLS = ["id","attempt_id","symbol","long_ex","short_ex","opened_ms","last_ms","held_h",
            "size_usd","entry_spread_bps","entry_px_low","entry_px_high",
            "status","opened_at","closed_at","close_reason","validated_qty","note_net_usd"]

def load_positions(path: str) -> pd.DataFrame:
    return read_csv(path, POS_COLS)

def save_positions(path: str, df: pd.DataFrame):
    write_csv(path, df)

# ----------------- Core loop: open/close/rotate -----------------
def positions_once(
    quotes_df: pd.DataFrame,
    per_leg_notional_usd: float,
    entry_bps: float,
    exit_bps: float,
    taker_fee: float,
    pos_path: str,
    paper: bool,
    top3_to_tg: int,
    rotate_on: bool,
    rotate_delta_usd: float,
    stats_df: pd.DataFrame,
):
    price_source = getenv_str("PRICE_SOURCE", "mid")
    cands = build_price_arbitrage(quotes_df, per_leg_notional_usd, taker_fee, price_source)

    # --- читаем статистику спредов и считаем z для кандидатов ---
    Z_IN  = float(getenv_float("Z_IN", 2.5))
    Z_OUT = float(getenv_float("Z_OUT", 0.8))
    SLIPPAGE_BPS = float(getenv_float("SLIPPAGE_BPS", 1.0))  # консервативный запас

    stats = stats_df if stats_df is not None else pd.DataFrame(columns=STATS_COLS)
    if cands is not None and not cands.empty:
        zs=[]; xs=[]; stds=[]
        for _, r in cands.iterrows():
            x, z, std = get_z_for_pair(
                stats, symbol=str(r["symbol"]),
                ex_low=str(r["long_ex"]), ex_high=str(r["short_ex"]),
                px_low=float(r["px_low"]), px_high=float(r["px_high"])
            )
            xs.append(x); zs.append(z); stds.append(std)
        cands["log_spread"] = xs; cands["z"] = zs; cands["std"] = stds
        # штраф за проскальзывание (4 ноги round-trip)
        cands["net_usd_adj"] = cands["net_usd"] - (4.0 * (SLIPPAGE_BPS/1e4) * per_leg_notional_usd)
        # фильтр по аномалии и экономике
        cands = cands[(cands["net_usd_adj"]>0.0) & (cands["z"]>=Z_IN)].copy()
        cands = cands.sort_values(["z","net_usd_adj","spread_bps"], ascending=[False,False,False]).reset_index(drop=True)

    best = None
    if cands is not None and not cands.empty:
        topN = cands.head(top3_to_tg).copy()
        for _, r in topN.iterrows():
            maybe_send_telegram(format_signal_card(r, per_leg_notional_usd, price_source))
        best = cands.iloc[0]
    else:
        logging.info("No z-filtered candidates this cycle.")

    df_pos = load_positions(pos_path)
    now_ms = utc_ms_now()
    has_open = (not df_pos.empty) and any(df_pos["status"]=="open")
    if has_open:
        do_close = False
        reason = ""
        i = df_pos.index[df_pos["status"]=="open"][0]
        sym = str(df_pos.at[i,"symbol"]).upper()
        long_ex = str(df_pos.at[i,"long_ex"]); short_ex = str(df_pos.at[i,"short_ex"])

        sub = quotes_df[(quotes_df["symbol"]==sym)]
        px_low = None; px_high=None
        try:
            row_low  = sub[sub["exchange"]==long_ex].iloc[0]
            row_high = sub[sub["exchange"]==short_ex].iloc[0]
            px_low  = float(select_px(row_low,  getenv_str("PRICE_SOURCE", "mid")) or 0.0)
            px_high = float(select_px(row_high, getenv_str("PRICE_SOURCE", "mid")) or 0.0)
        except Exception:
            pass

        spread_bps_now = None
        z_now = None
        if px_low is not None and px_high is not None:
            spread_bps_now = ((px_high - px_low)/px_low)*1e4 if px_low > 0 else 0.0
            _, z_now, _ = get_z_for_pair(stats, sym, long_ex, short_ex, px_low, px_high)

        if z_now is not None and z_now==z_now and z_now <= Z_OUT:
            do_close = True; reason = f"Z-out: z={z_now:.2f} <= {Z_OUT:.2f}"
        elif spread_bps_now is not None and spread_bps_now <= exit_bps:
            do_close = True; reason = f"Spread converged: {spread_bps_now:.2f} bps <= {exit_bps:.2f} bps"

        if rotate_on and (not do_close) and best is not None:
            try:
                current_expected = float(df_pos.at[i,"note_net_usd"] or 0.0)
                new_expected = float(best["net_usd"])
                if new_expected - current_expected >= float(rotate_delta_usd):
                    do_close=True; reason=f"Rotate: new net ${new_expected:.2f} ≥ current + ${rotate_delta_usd:.2f}"
            except: pass

        if do_close:
            px_any = px_low or px_high or 1.0
            stored_qty = df_pos.at[i,"validated_qty"]
            qty = float(stored_qty) if (stored_qty is not None and not pd.isna(stored_qty) and float(stored_qty)>0) else per_leg_notional_usd/px_any
            if not paper:
                try:
                    _place_perp_market_order(long_ex,  sym, "SELL", qty, paper=False)
                    _place_perp_market_order(short_ex, sym, "BUY",  qty, paper=False)
                except Exception as e:
                    logging.warning("Close error %s: %s", sym, e)
            df_pos.at[i,"status"]="closed"
            df_pos.at[i,"closed_at"]=iso_utc(now_ms)
            df_pos.at[i,"close_reason"]=reason
            df_pos.at[i,"last_ms"]=now_ms
            maybe_send_telegram(f"✅ <b>Closed</b> {sym}\n{_anchor(long_ex,sym)} / {_anchor(short_ex,sym)}\nReason: {reason}")
            save_positions(pos_path, df_pos)
            has_open = False

    if (not has_open) and best is not None:
        spread_bps = float(best["spread_bps"])
        if float(best.get("z", 0.0)) >= Z_IN and float(best.get("net_usd_adj", 0.0))>0.0:
            sym = str(best["symbol"]).upper()
            cheap_ex = str(best["long_ex"]); rich_ex=str(best["short_ex"])
            px_low = float(best["px_low"]); px_high=float(best["px_high"])
            qty = float(best["qty_est"]) if best["qty_est"] and best["qty_est"]>0 else per_leg_notional_usd/max(px_low,1.0)

            ok, attempt_id, meta = atomic_cross_open(
                symbol=sym, cheap_ex=cheap_ex, rich_ex=rich_ex,
                qty=qty, price_low=px_low, price_high=px_high, paper=paper
            )
            if ok:
                cur_max = pd.to_numeric(df_pos["id"], errors="coerce").max() if not df_pos.empty else None
                next_id = int(cur_max)+1 if cur_max==cur_max and cur_max is not None else 1
                new = {
                    "id": next_id, "attempt_id": attempt_id, "symbol": sym,
                    "long_ex": cheap_ex, "short_ex": rich_ex,
                    "opened_ms": now_ms, "last_ms": now_ms, "held_h": 0.0,
                    "size_usd": per_leg_notional_usd,
                    "entry_spread_bps": spread_bps,
                    "entry_px_low": px_low, "entry_px_high": px_high,
                    "status":"open",
                    "opened_at": iso_utc(now_ms), "closed_at":"", "close_reason":"",
                    "validated_qty": meta.get("qty", qty),
                    "note_net_usd": float(best["net_usd"])
                }
                df_pos = pd.concat([df_pos, pd.DataFrame([new])], ignore_index=True)
                maybe_send_telegram("✅ <b>OPENED</b>\n" + format_signal_card(best, per_leg_notional_usd, price_source))
            else:
                logging.warning("Open failed: %s", meta.get("error"))
    save_positions(pos_path, df_pos)

# ===== Symbols & matrix helpers =====
DEFAULT_EXCHANGES = ["binance","bybit","okx","gate","mexc"]
COMMON_SYMBOLS = ["BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","LINKUSDT","BNBUSDT","ADAUSDT","TONUSDT","OPUSDT","ARBUSDT","PEPEUSDT"]

def load_matrix_df(matrix_path: str) -> pd.DataFrame:
    p = bucketize_path(matrix_path)
    if not p: return pd.DataFrame()
    try:
        if is_gs(p):
            client = gcs_client()
            bucket_name = p[5:].split("/",1)[0]; blob_name = p[5+len(bucket_name)+1:]
            bucket = client.lookup_bucket(bucket_name); blob = bucket.blob(blob_name)
            if not blob.exists(): return pd.DataFrame()
            data = blob.download_as_bytes()
            return pd.read_csv(pd.io.common.BytesIO(data))
        else:
            return pd.read_csv(p)
    except Exception:
        logging.warning("Matrix read error: %s", p); return pd.DataFrame()

def symbols_from_matrix(matrix_path: str, exchanges: List[str], mode: str = "union") -> List[str]:
    df = load_matrix_df(matrix_path)
    if df.empty: return []
    df.columns = [c.strip() for c in df.columns]
    if "symbol" not in df.columns: return []
    ex_cols = [ex for ex in exchanges if ex in df.columns]
    if not ex_cols: return []
    sub = df[["symbol"] + ex_cols].copy()
    if mode.startswith("atleast:"):
        try: k = int(mode.split(":",1)[1])
        except: k = 1
        mask = sub[ex_cols].sum(axis=1) >= k
    elif mode == "intersection":
        mask = sub[ex_cols].all(axis=1)
    else:
        mask = sub[ex_cols].any(axis=1)
    out = sorted(sub.loc[mask, "symbol"].dropna().astype(str).str.upper().unique().tolist())
    return out

def binance_fapi_base_data() -> str:
    return "https://fapi.binance.com"

def binance_top_perp_usdt(top_n: int = 200, min_quote_usdt: float = 0.0) -> List[str]:
    try:
        exinfo = SESSION.get(f"{binance_fapi_base_data()}/fapi/v1/exchangeInfo", timeout=REQUEST_TIMEOUT)
        exinfo.raise_for_status()
        info = exinfo.json()
        perp_usdt = {
            s["symbol"] for s in info.get("symbols", [])
            if s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
        }
        t24 = SESSION.get(f"{binance_fapi_base_data()}/fapi/v1/ticker/24hr", timeout=REQUEST_TIMEOUT)
        items = []
        for r in t24.json():
            sym = r.get("symbol")
            if sym in perp_usdt:
                qv = to_float(r.get("quoteVolume")) or 0.0
                if qv >= float(min_quote_usdt):
                    items.append((sym, qv))
        items.sort(key=lambda x: x[1], reverse=True)
        return [sym for sym, _ in items[: int(top_n)]]
    except Exception as e:
        logging.warning("binance_top_perp_usdt error: %s", e)
        return []

# ----------------- Main loop -----------------
def main():
    exchanges = [x.lower() for x in getenv_list("EXCHANGES", DEFAULT_EXCHANGES)]

    src = getenv_str("SYMBOLS_SOURCE", "common").lower()
    symbols_env = getenv_list("SYMBOLS", [])
    top_n = int(getenv_float("TOP_N", 200))
    min_quote = float(getenv_float("MIN_QUOTE_USDT", 10_000_000))
    matrix_path = getenv_str("MATRIX_READ_PATH", "")
    matrix_mode = getenv_str("MATRIX_MODE", "union")
    use_per_ex = getenv_bool("MATRIX_USE_PER_EXCHANGE", True)

    if src == "manual" and symbols_env:
        symbols = [s.upper() for s in symbols_env]
    elif src == "binance-top":
        symbols = binance_top_perp_usdt(top_n=top_n, min_quote_usdt=min_quote)
    elif src == "union":
        if matrix_path:
            symbols = symbols_from_matrix(matrix_path, exchanges, mode="union")
        else:
            symbols = COMMON_SYMBOLS
    elif src == "common":
        symbols = COMMON_SYMBOLS
    elif src == "matrix":
        if not matrix_path:
            logging.error("SYMBOLS_SOURCE=matrix, но MATRIX_READ_PATH не задан — беру COMMON")
            symbols = COMMON_SYMBOLS
        else:
            symbols = symbols_from_matrix(matrix_path, exchanges, mode=matrix_mode)
    else:
        symbols = COMMON_SYMBOLS

    logging.info("Symbols selected (%d): %s", len(symbols), symbols[:20])

    pos_cross_path = bucketize_path(getenv_str("POS_CROSS_PATH", "positions_price_cross.csv"))

    
    
    entry_bps = float(getenv_float("ENTRY_SPREAD_BPS", getenv_float("ENTRY_APR", 10.0)))
    exit_bps  = float(getenv_float("EXIT_SPREAD_BPS",  getenv_float("EXIT_APR", 2.0)))
    taker_fee = float(getenv_float("TAKER_FEE", 0.0005))
    paper = getenv_bool("PAPER", True)
    sleep_s = int(getenv_float("SLEEP_SEC", 15))

    notional_env = getenv_str("NOTIONAL","")
    notional = float(notional_env) if notional_env else None
    capital = float(getenv_float("CAPITAL", 1000.0))
    leverage = float(getenv_float("PERP_LEVERAGE", 5.0))
    per_leg_notional = float(notional) if notional is not None else per_leg_notional_from_capital(capital, leverage)

    rotate_on = getenv_bool("ROTATE", False)
    rotate_delta_usd = float(getenv_float("ROTATE_DELTA_USD", 5.0))

    stats_store = StatsStore(SPREAD_STATS_PATH, ALPHA)

    logging.info("PriceArb started | exchanges=%s | symbols=%s", exchanges, symbols)
    logging.info("PAPER=%s | per-leg notional=$%.2f | ENTRY=%.2f bps | EXIT=%.2f bps | taker=%.4f",
                 paper, per_leg_notional, entry_bps, exit_bps, taker_fee)
    logging.info("ROTATE=%s | ROTATE_DELTA_USD=%.2f", rotate_on, rotate_delta_usd)

    price_source = getenv_str("PRICE_SOURCE", "mid").lower()

    while True:
        try:
            quotes_df = scan_all_with_instant_alerts(
                exchanges=exchanges,
                symbols=symbols,
                per_leg_notional_usd=per_leg_notional,
                taker_fee=taker_fee,
                price_source=price_source,
                alert_spread_pct=ALERT_SPREAD_PCT,
                cooldown_sec=ALERT_COOLDOWN_SEC,
            )

            # ---- ОБНОВЛЯЕМ EMA-статистику из свежих котировок (на лету) ----
            # Берём по каждому символу все доступные биржи в quotes_df и считаем лог-спред для каждой пары.
            try:
                if quotes_df is not None and not quotes_df.empty:
                    df = quotes_df.copy()
                    # выбираем использованную цену согласно PRICE_SOURCE
                    if price_source == "book":
                        # Обновляем EMA на основе BBO: ask как дешёвая цена, bid как дорогая
                        for sym in sorted(df["symbol"].unique()):
                            sub = df[df["symbol"]==sym]
                            sub_ask = sub.dropna(subset=["ask"])
                            sub_bid = sub.dropna(subset=["bid"])
                            if sub_ask.empty or sub_bid.empty:
                                continue
                            # Для стабильности возьмём глобальный мин ASK и макс BID (реалистичнее не бывает)
                            row_low  = sub_ask.loc[sub_ask["ask"].astype(float).idxmin()]
                            row_high = sub_bid.loc[sub_bid["bid"].astype(float).idxmax()]
                            if str(row_low["exchange"]) == str(row_high["exchange"]):
                                continue
                            px_low  = float(row_low["ask"]);  ex_low  = str(row_low["exchange"])
                            px_high = float(row_high["bid"]); ex_high = str(row_high["exchange"])
                            if px_low>0 and px_high>0:
                                x = math.log(px_high/px_low)
                                stats_store.update_pair(sym, ex_low, ex_high, x)
                    else:
                        # как и было: EMA на основе выбранной единой цены (mid/last/mark/bid/ask)
                        def _sel(r):
                            ps = price_source
                            if ps == "last": return to_float(r.get("last"))
                            if ps == "mark": return to_float(r.get("mark"))
                            if ps == "bid":  return to_float(r.get("bid"))
                            if ps == "ask":  return to_float(r.get("ask"))
                            return to_float(r.get("mid"))
                        df["px"] = df.apply(_sel, axis=1)
                        df = df.dropna(subset=["px"])
                        for sym in sorted(df["symbol"].unique()):
                            sub = df[df["symbol"]==sym].sort_values("px")
                            exs = list(sub["exchange"].values); pxs = list(sub["px"].values)
                            n = len(pxs)
                            for i in range(n):
                                for j in range(i+1, n):
                                    px_low, px_high = float(pxs[i]), float(pxs[j])
                                    if px_low<=0 or px_high<=0: continue
                                    ex_low, ex_high = str(exs[i]), str(exs[j])
                                    x = math.log(px_high/px_low)
                                    stats_store.update_pair(sym, ex_low, ex_high, x)
                # сохраняем не чаще, чем раз в SAVE_EVERY_SEC
                stats_store.maybe_save(force=False)
            except Exception as e:
                logging.warning("Stats inline update error: %s", e)

            positions_once(
                quotes_df=quotes_df,
                per_leg_notional_usd=per_leg_notional,
                entry_bps=entry_bps,
                exit_bps=exit_bps,
                taker_fee=taker_fee,
                pos_path=pos_cross_path,
                paper=paper,
                top3_to_tg=int(getenv_float("TOP_N_TELEGRAM",5)),
                rotate_on=rotate_on,
                rotate_delta_usd=rotate_delta_usd,
                stats_df=stats_store.df
            )
        except Exception as e:
            logging.exception("Cycle error: %s", e)
            maybe_send_telegram(f"❌ PriceArb cycle error: <code>{str(e)[:1600]}</code>")
        time.sleep(max(3, sleep_s))

if __name__ == "__main__":
    main()
