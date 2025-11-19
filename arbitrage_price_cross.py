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

import os, time, json, math, hmac, hashlib, logging, uuid, base64
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd

# ----------------- ENV helpers -----------------
from dotenv import load_dotenv, find_dotenv
load_dotenv()

 # === Env helpers: куда ходим за котировками и куда отправляем ордера ===
def price_feed_env() -> str:
    # Котировки всегда читаем с mainnet
    return "mainnet"

def _public_base(exchange: str) -> str:
    # Публичные котировки — только MAINNET
    if exchange == "binance":
        return "https://fapi.binance.com"
    if exchange == "bybit":
        return "https://api.bybit.com"
    if exchange == "mexc":
        return "https://contract.mexc.com"
    if exchange == "gate":
        return "https://api.gateio.ws"
    raise ValueError(f"Unknown exchange: {exchange}")

# === BULK PATCH START ===
import time
import traceback

def _bulk_binance():
    url = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
    try:
        r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return {}
        out = {}
        for j in r.json():
            sym = j.get("symbol")
            if not sym:
                continue
            bid = float(j["bidPrice"])
            ask = float(j["askPrice"])
            if bid <= 0 or ask <= 0:
                continue
            out[sym.upper()] = {
                "bid": bid,
                "ask": ask,
                "mark": None,
                "last": None
            }
        return out
    except:
        traceback.print_exc()
        return {}

def _bulk_bybit():
    url = "https://api.bybit.com/v5/market/tickers?category=linear"
    try:
        r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return {}
        data = r.json().get("result", {}).get("list", [])
        out = {}
        for j in data:
            sym = j.get("symbol")
            if not sym:
                continue
            bid = float(j["bid1Price"])
            ask = float(j["ask1Price"])
            mark = float(j.get("markPrice", 0)) if j.get("markPrice") else None
            last = float(j.get("lastPrice", 0)) if j.get("lastPrice") else None
            if bid <= 0 or ask <= 0:
                continue
            out[sym.upper()] = {
                "bid": bid,
                "ask": ask,
                "mark": mark,
                "last": last
            }
        return out
    except:
        traceback.print_exc()
        return {}

def _bulk_okx():
    url = "https://www.okx.com/api/v5/market/tickers?instType=SWAP"
    try:
        r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return {}
        data = r.json().get("data", [])
        out = {}
        for j in data:
            inst = j.get("instId", "")
            if not inst.endswith("-USDT-SWAP"):
                continue
            sym = inst.replace("-", "").replace("SWAP", "")
            bid = float(j["bidPx"])
            ask = float(j["askPx"])
            mark = float(j.get("markPx", 0)) if j.get("markPx") else None
            last = float(j.get("last", 0)) if j.get("last") else None
            if bid <= 0 or ask <= 0:
                continue
            out[sym.upper()] = {
                "bid": bid,
                "ask": ask,
                "mark": mark,
                "last": last
            }
        return out
    except:
        traceback.print_exc()
        return {}

def _bulk_gate():
    base = "https://api.gateio.ws" if not _is_true("GATE_API_TESTNET", False) else "https://api-testnet.gateapi.io"
    url = f"{base}/api/v4/futures/usdt/tickers"
    try:
        r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return {}
        data = r.json()
        out = {}
        for j in data:
            inst = j.get("contract")
            if not inst or not inst.endswith("_USDT"):
                continue
            sym = inst.replace("_", "")
            bid = float(j["bid1"])
            ask = float(j["ask1"])
            mark = float(j.get("mark_price", 0)) if j.get("mark_price") else None
            last = float(j.get("last", 0)) if j.get("last") else None
            if bid <= 0 or ask <= 0:
                continue
            out[sym.upper()] = {
                "bid": bid,
                "ask": ask,
                "mark": mark,
                "last": last
            }
        return out
    except:
        traceback.print_exc()
        return {}

def load_all_bulk_quotes(exchanges):
    quotes = {}
    for ex in exchanges:
        ex = ex.lower()
        if ex == "binance":
            quotes["binance"] = _bulk_binance()
        elif ex == "bybit":
            quotes["bybit"] = _bulk_bybit()
        elif ex == "okx":
            quotes["okx"] = _bulk_okx()
        elif ex == "gate":
            quotes["gate"] = _bulk_gate()
        else:
            quotes[ex] = {}
    return quotes
# === BULK PATCH END ===

# === Per-exchange private base ===
def _private_base(exchange: str) -> str:
    ex = exchange.lower()
    if ex == "binance":
        # только флаг BINANCE_API_TESTNET управляет Binance
        return "https://testnet.binancefuture.com" if _is_true("BINANCE_API_TESTNET", False) else "https://fapi.binance.com"
    if ex == "bybit":
        # Bybit environments:
        # - BYBIT_API_TESTNET=1 → api-testnet.bybit.com
        # - BYBIT_DEMO=1 → api-demo.bybit.com
        # - иначе → api.bybit.com
        if _is_true("BYBIT_API_TESTNET", False):
            return "https://api-testnet.bybit.com"
        if _is_true("BYBIT_DEMO", False):
            return "https://api-demo.bybit.com"
        return "https://api.bybit.com"
    if ex == "mexc":
        # у MEXC фьюч-тестнета нет
        return "https://contract.mexc.com"
    if ex == "gate":
        # Gate: переключаемся между mainnet и testnet по флагам GATE_TESTNET / GATE_PAPER
        # Для демо по документации:
        #   API domain for demo trading: https://api-testnet.gateapi.io/api/v4
        use_testnet = _is_true("GATE_TESTNET", False) or _is_true("GATE_PAPER", False)
        if use_testnet:
            return "https://api-testnet.gateapi.io"
        return "https://api.gateio.ws"
    if ex == "okx":
        # у OKX отдельного фьюч-тестнета для USDT-SWAP нет, «демо» режим делается заголовком x-simulated-trading
        # поэтому всегда используем основной хост
        return "https://www.okx.com"
    raise ValueError(f"Unknown exchange: {exchange}")

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
# --- Отключаем подробные HTTP DEBUG-логи от urllib3 и requests ---
#logging.getLogger("urllib3").setLevel(logging.WARNING)
#logging.getLogger("requests").setLevel(logging.WARNING)

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
INSTANT_ALERT = os.getenv("INSTANT_ALERT", "True").lower() == "true"

def now_ms() -> int:
    return int(time.time() * 1000)

def _hmac_sha256_hex(secret: str, msg: str) -> str:
    return hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest()

def _hmac_sha512_hex(secret: str, msg: str) -> str:
    return hmac.new(secret.encode(), msg.encode(), hashlib.sha512).hexdigest()

def _b64_sha256(msg: bytes) -> str:
    return base64.b64encode(hashlib.sha256(msg).digest()).decode()

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

# округление цены и qty по шагу
def _round_to_step(x: float, step: float, mode="round"):
    if step <= 0:
        return x
    q = x / step
    if mode == "down":
        q = math.floor(q)
    elif mode == "up":
        q = math.ceil(q)
    else:
        q = round(q)
    return q * step

def _fmt_qty_str(q: float) -> str:
    s = ("%.10f" % float(q)).rstrip("0").rstrip(".")
    return s if s else "0"

def _gen_cloid(prefix: str, attempt_id: str, leg: str) -> str:
    """
    Генерируем client order id (совместим с Binance/Bybit).
    Binance допускает до 36 символов. Формат: pfx-attempt-leg-utc
    """
    base = f"{prefix}-{attempt_id[:8]}-{leg}-{int(time.time())}"
    return base[:36]

def _wavg(px_list, qty_list) -> float:
    num = sum(p*q for p,q in zip(px_list, qty_list))
    den = sum(qty_list) or 0.0
    return (num/den) if den>0 else 0.0

def _sum(x): 
    return float(sum(x)) if x else 0.0

def _fmt_price_str(p: float) -> str:
    s = ("%.10f" % float(p)).rstrip("0").rstrip(".")
    return s if s else "0"

def _read_lock() -> dict:
    try:
        p = _OPEN_LOCK_PATH
        if not p:
            return {}
        if is_gs(p):
            client = gcs_client()
            bname = p[5:].split("/",1)[0]; oname = p[5+len(bname)+1:]
            blob = client.lookup_bucket(bname).blob(oname)
            if not blob.exists():
                return {}
            data = blob.download_as_bytes()
            return json.loads(data.decode("utf-8"))
        else:
            if not os.path.exists(p):
                return {}
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return {}

def _write_lock(obj: dict):
    try:
        p = _OPEN_LOCK_PATH
        if not p:
            return
        data = json.dumps(obj, ensure_ascii=False)
        if is_gs(p):
            client = gcs_client()
            bname = p[5:].split("/",1)[0]; oname = p[5+len(bname)+1:]
            client.lookup_bucket(bname).blob(oname).upload_from_string(data, content_type="application/json")
        else:
            os.makedirs(os.path.dirname(os.path.abspath(p)), exist_ok=True)
            with open(p, "w", encoding="utf-8") as f:
                f.write(data)
    except Exception as e:
        logging.warning("open-lock write error: %s", e)

def _clear_lock():
    try:
        p = _OPEN_LOCK_PATH
        if not p:
           return
        if is_gs(p):
            client = gcs_client()
            bname = p[5:].split("/",1)[0]; oname = p[5+len(bname)+1:]
            client.lookup_bucket(bname).blob(oname).delete(if_generation_match=None)
        else:
            if os.path.exists(p):
                os.remove(p)
    except Exception:
        pass

def open_lock_check_or_set(symbol: str) -> bool:
    """
    True  -> можно открывать (и лок установлен)
    False -> нельзя (уже кто-то открывает/лок свежий)
   """
    now = time.time()
    lk = _read_lock()
    if lk:
        sym = lk.get("symbol")
        ts  = float(lk.get("ts", 0.0))
        if (now - ts) < _OPEN_LOCK_TTL_SEC:
            # свежий лок — запрещаем другой тикер
            if sym and sym != symbol:
                logging.debug("Pair-lock: opening %s is in progress, skip %s", sym, symbol)
                return False
            # тот же тикер — обновим таймштамп
    _write_lock({"symbol": symbol, "ts": now})
    return True

def open_lock_clear():
    _clear_lock()

def cap_qty_by_capital(price: float, qty_step: float, capital_usd: float, leverage: float) -> float:
    """
    Жёсткий лимит: нотионал одной ноги ≤ CAPITAL*LEVERAGE/2
    """
    if price <= 0 or capital_usd <= 0 or leverage <= 0:
        return 0.0
    max_leg_notional = (capital_usd * leverage) / 2.0
    raw_qty = max_leg_notional / price
    if qty_step and qty_step > 0:
        steps = math.floor(raw_qty / qty_step)
        return max(0.0, steps * qty_step)
    return max(0.0, raw_qty)

def _rotate_need_abs(current_expected_net: float, per_leg_notional_usd: float) -> float:
    """
    Вычисляет минимально нужный прирост прибыли (в USD), чтобы разрешить ротацию.
    Порог = max( current_expected * ROTATE_DELTA_PCT,
                 per_leg_notional * ROTATE_MIN_NOTIONAL_PCT,
                 CAPITAL * ROTATE_MIN_CAP_PCT )
    Все проценты задаются в .env, например 0.30 (т.е. 0.30%).
    """
    delta_pct = getenv_float("ROTATE_DELTA_PCT", 0.30) / 100.0
    min_notional_pct = getenv_float("ROTATE_MIN_NOTIONAL_PCT", 0.20) / 100.0
    min_cap_pct = getenv_float("ROTATE_MIN_CAP_PCT", 0.05) / 100.0

    cap = max(0.0, float(getenv_float("CAPITAL", 1000.0)))
    leg = max(0.0, float(per_leg_notional_usd))
    cur = max(0.0, float(current_expected_net))

    part1 = cur * delta_pct
    part2 = leg * min_notional_pct
    part3 = cap * min_cap_pct
    return max(part1, part2, part3)

def bybit_get_filters(symbol: str):
    # instruments-info уже дергаешь — используем один и тот же эндпоинт
    url = bybit_base() + "/v5/market/instruments-info"
    params = {"category": "linear", "symbol": symbol.upper()}
    r = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
    j = r.json()
    if j.get("retCode") != 0 or not j.get("result", {}).get("list"):
        raise RuntimeError(f"Bybit filters fetch failed: {j}")
    it = j["result"]["list"][0]
    pf = it.get("priceFilter", {})
    lot = it.get("lotSizeFilter", {})
    tick = float(pf.get("tickSize", "0.0001"))
    min_price = float(pf.get("minPrice", "0"))
    max_price = float(pf.get("maxPrice", "1e20"))
    qty_step = float(lot.get("qtyStep", "1"))
    return tick, min_price, max_price, qty_step

def bybit_best_bid_ask(symbol: str):
    url = bybit_base() + "/v5/market/orderbook"
    params = {"category": "linear", "symbol": symbol.upper(), "limit": 1}
    r = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
    j = r.json()
    if j.get("retCode") != 0:
        raise RuntimeError(f"Bybit orderbook failed: {j}")
    bids = j["result"]["b"][0] if j["result"]["b"] else None
    asks = j["result"]["a"][0] if j["result"]["a"] else None
    best_bid = float(bids[0]) if bids else None
    best_ask = float(asks[0]) if asks else None
    return best_bid, best_ask


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
    ts       = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    price_lbl = {"mid":"MID","last":"LAST","mark":"MARK","bid":"BID","ask":"ASK","book":"BBO"}.get(price_source.lower(),"MID")
    z = r.get("z", None)
    std = r.get("std", None)
    net_usd_adj = r.get("net_usd_adj", None)

    lines = [
        "──────────────",
        f"<b>{sym}</b>",
        f"{_anchor(long_ex, sym)} BUY  ↔  {_anchor(short_ex, sym)} SELL",
    ]
    if net_usd_adj is not None:
        lines.append(f"🧮 Net after slippage: ${float(net_usd_adj):.2f}")
    if z is not None and z == z:  # not NaN
        if std is not None and std == std:
            lines.append(f"\n📈 <code>Z-score: {float(z):.2f} (σ={float(std):.2f})</code>")
        else:
            lines.append(f"\n📈 <code>Z-score: {float(z):.2f}</code>")

    if r.get("entry_bps_sugg") is not None:
        lines.append(f"\n🎯 <code>Entry ≥ {float(r['entry_bps_sugg']):.0f} bps</code>")
    lines.extend([
        f"\n🧮 SPREAD: {sp_pct:.2f}% ({sp_bps:.0f} bps)",
        f"💵 Gross: ${gross:.2f}",
        f"💸 Fees RT: ${fees_rt:.2f}",
        f"✅ Net: ${net_usd:.2f}",
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

# ----------------- Simple open-lock (pair-lock) -----------------
_OPEN_LOCK_PATH = bucketize_path(getenv_str("OPEN_LOCK_PATH", "open_lock.json"))
_OPEN_LOCK_TTL_SEC = int(getenv_float("OPEN_LOCK_TTL_SEC", 30))

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
def _bybit_headers_base(api_key: str, api_secret: str, body: str = "{}", recv: str = "5000") -> dict:
    ts = str(int(time.time() * 1000))
    origin = f"{ts}{api_key}{recv}{body}"
    sign = hmac.new(api_secret.encode(), origin.encode(), hashlib.sha256).hexdigest()
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv,
        "X-BAPI-SIGN": sign,
        "X-BAPI-SIGN-TYPE": "2",
        "Content-Type": "application/json",
    }
    if _is_true("BYBIT_DEMO", False):
        headers["X-BAPI-SIMULATED-TRADING"] = "1"
    return headers

def _is_true(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return str(v).strip().lower() in ("1","true","t","yes","y","on")

def bybit_base() -> str:
    # для ордеров — тестнет
    return "https://api-testnet.bybit.com" if _is_true("BYBIT_API_TESTNET", False) else "https://api.bybit.com"

def bybit_data_base() -> str:
    """
    Возвращает базу для публичной маркет-данаты Bybit Linear.
    При BYBIT_DEMO=1 -> api-demo.bybit.com
    При BYBIT_API_TESTNET=1 -> api-testnet.bybit.com
    Иначе -> api.bybit.com
    """
    if _is_true("BYBIT_DEMO", False):
        return "https://api-demo.bybit.com"
    if _is_true("BYBIT_API_TESTNET", False):
        return "https://api-testnet.bybit.com"
    return "https://api.bybit.com"

def binance_fapi_base() -> str:
    # Трейдинг
    return "https://testnet.binancefuture.com" if _is_true("BINANCE_API_TESTNET", False) else "https://fapi.binance.com"

def binance_data_base() -> str:
    """
    Возвращает базу для публичной маркет-данаты Binance Futures.
    Если BINANCE_API_TESTNET=1 -> testnet, иначе mainnet.
    """
    return "https://testnet.binancefuture.com" if _is_true("BINANCE_API_TESTNET", False) else "https://fapi.binance.com"


def okx_base() -> str: return "https://www.okx.com"

def gate_base() -> str:
    # приватная база для Gate (mainnet/testnet по флагам)
    return _private_base("gate")

def gate_contract_from_symbol(symbol: str) -> str:
    sym = symbol.upper()
    if sym.endswith("USDT"): return f"{sym[:-4]}_USDT"
    if sym.endswith("USD"):  return f"{sym[:-3]}_USD"
    return sym

# ----------------- Connectivity checks -----------------
def _http_ok(url: str, params: dict|None=None, expect_key: str|None=None) -> bool:
    try:
        r = SESSION.get(url, params=params or {}, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logging.error("Connectivity fail: %s %s -> HTTP %s", url, params or "", r.status_code)
            return False
        if expect_key is not None:
            j = r.json()
            ok = expect_key in j or (isinstance(j, dict) and expect_key in (j.get("result") or {}))
            if not ok:
                logging.error("Connectivity fail (missing key '%s'): %s %s", expect_key, url, params or "")
            return ok
        return True
    except Exception as e:
        logging.error("Connectivity exception: %s %s -> %s", url, params or "", e)
        return False

def check_connectivity(exchanges: list[str], probe_symbol: str="BTCUSDT") -> bool:
    """
    Возвращает True, если все выбранные биржи отвечают.
    Дополнительно пытается вывести USDT-баланс по каждому доступному аккаунту.
    """
    ok_all = True
    probe_symbol = probe_symbol.upper()

    for ex in exchanges:
        ex_l = ex.lower().strip()

        # -------------------------------
        # BINANCE
        # -------------------------------
        if ex_l == "binance":
            data_base = binance_data_base()
            ok = (
                _http_ok(f"{data_base}/fapi/v1/time")
                and _http_ok(f"{data_base}/fapi/v1/ticker/bookTicker", {"symbol": probe_symbol})
            )
            env_label = "testnet" if "testnet" in data_base else "mainnet"
            logging.info("[CHECK] Binance (%s) -> %s", env_label, "OK" if ok else "FAIL")

            # Баланс Binance Futures (если заданы ключи)
            key = os.getenv("BINANCE_API_KEY", "")
            sec = os.getenv("BINANCE_API_SECRET", "")
            if not key or not sec:
                logging.info("[BALANCE] Binance USDT: (no API keys)")
            else:
                bal = "-"
                try:
                    base = _private_base("binance")
                    ts = now_ms()
                    qs = f"timestamp={ts}&recvWindow=20000"
                    sig = _hmac_sha256_hex(sec, qs)
                    headers = {"X-MBX-APIKEY": key}
                    url = f"{base}/fapi/v2/balance?{qs}&signature={sig}"
                    r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                    if r.status_code == 200:
                        j = r.json()
                        if isinstance(j, list):
                            usdt = next((x for x in j if str(x.get("asset")) == "USDT"), None)
                            if usdt:
                                bal = usdt.get("balance", "-")
                        else:
                            logging.warning("[CHECK] Binance balance: unexpected JSON %s", str(j)[:200])
                    else:
                        logging.warning("[CHECK] Binance balance HTTP %s: %s", r.status_code, r.text[:200])
                except Exception as e:
                    logging.warning("[CHECK] Binance balance error: %s", e)
                logging.info("[BALANCE] Binance USDT: %s", bal)

            ok_all &= ok
            continue

        # -------------------------------
        # BYBIT
        # -------------------------------
        if ex_l == "bybit":
            data_base = bybit_data_base()
            ok = _http_ok(
                f"{data_base}/v5/market/tickers",
                {"category": "linear", "symbol": probe_symbol},
                expect_key="result",
            )
            env_label = "demo" if "api-demo" in data_base else ("testnet" if "testnet" in data_base else "mainnet")
            logging.info("[CHECK] Bybit (%s) -> %s", env_label, "OK" if ok else "FAIL")

            key = os.getenv("BYBIT_API_KEY", "")
            sec = os.getenv("BYBIT_API_SECRET", "")
            if not key or not sec:
                logging.info("[BALANCE] Bybit USDT: (no API keys)")
            else:
                bal = "-"
                try:
                    # ✅ Используем уже рабочую функцию
                    info = bybit_unified_usdt_balance()
                    if info:
                        bal = info.get("wallet", "-")
                    else:
                        bal = "-"
                except Exception as e:
                    logging.warning("[CHECK] Bybit balance error: %s", e)
                logging.info("[BALANCE] Bybit USDT: %s", bal)

            ok_all &= ok
            continue

        # -------------------------------
        # OKX
        # -------------------------------
        if ex_l == "okx":
            ok = _http_ok("https://www.okx.com/api/v5/public/time")
            env_label = "demo" if _is_true("OKX_TESTNET", False) or _is_true("OKX_PAPER", False) else "mainnet"
            logging.info("[CHECK] OKX (%s) -> %s", env_label, "OK" if ok else "FAIL")

            key = os.getenv("OKX_API_KEY", "")
            sec = os.getenv("OKX_API_SECRET", "")
            passphrase = os.getenv("OKX_PASSPHRASE", "")
            if not key or not sec or not passphrase:
                logging.info("[BALANCE] OKX USDT: (no API keys)")
            else:
                bal = "-"
                try:
                    base = okx_base()
                    endpoint = "/api/v5/account/balance"
                    ts = time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime())
                    method = "GET"
                    body = ""
                    prehash = f"{ts}{method}{endpoint}{body}"
                    sign = base64.b64encode(
                        hmac.new(sec.encode(), prehash.encode(), hashlib.sha256).digest()
                    ).decode()
                    headers = {
                        "OK-ACCESS-KEY": key,
                        "OK-ACCESS-SIGN": sign,
                        "OK-ACCESS-TIMESTAMP": ts,
                        "OK-ACCESS-PASSPHRASE": passphrase,
                        "x-simulated-trading": "1"
                            if _is_true("OKX_TESTNET", False) or _is_true("OKX_PAPER", False)
                            else "0",
                    }
                    url = f"{base}{endpoint}"
                    r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                    if r.status_code == 200:
                        j = r.json()
                        data = j.get("data") or []
                        details = (data[0] or {}).get("details", []) if data else []
                        usdt_row = next((x for x in details if x.get("ccy") == "USDT"), None)
                        if usdt_row:
                            bal = usdt_row.get("cashBal") or usdt_row.get("eq", "-")
                    else:
                        logging.warning("[CHECK] OKX balance HTTP %s: %s",
                                        r.status_code, r.text[:200])
                except Exception as e:
                    logging.warning("[CHECK] OKX balance error: %s", e)
                logging.info("[BALANCE] OKX USDT: %s", bal)

            ok_all &= ok
            continue

        # -------------------------------
        # GATE
        # -------------------------------
        if ex_l == "gate":
            base_pub = gate_base()
            ok = _http_ok(
                f"{base_pub}/api/v4/futures/usdt/tickers",
                {"contract": "BTC_USDT"},
            )
            env_label = "testnet" if "api-testnet" in base_pub else "mainnet"
            logging.info("[CHECK] Gate (%s) -> %s", env_label, "OK" if ok else "FAIL")

            key = os.getenv("GATE_API_KEY", "")
            sec = os.getenv("GATE_API_SECRET", "")
            if not key or not sec:
                logging.info("[BALANCE] Gate USDT: (no API keys)")
            else:
                bal = "-"
                try:
                    method = "GET"
                    path = "/api/v4/futures/usdt/accounts"
                    query = ""
                    body = ""
                    ts = str(int(time.time()))
                    body_hash = hashlib.sha512(body.encode()).hexdigest()
                    msg = "\n".join([method, path, query, body_hash, ts])
                    sign = _hmac_sha512_hex(sec, msg)
                    headers = {
                        "KEY": key,
                        "Timestamp": ts,
                        "SIGN": sign,
                    }
                    url = f"{gate_base()}{path}"
                    r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                    if r.status_code == 200:
                        j = r.json()
                        if isinstance(j, dict):
                            # у Gate futures в ответе есть поле available
                            bal = j.get("available", "-")
                    else:
                        logging.warning("[CHECK] Gate balance HTTP %s: %s",
                                        r.status_code, r.text[:200])
                except Exception as e:
                    logging.warning("[CHECK] Gate balance error: %s", e)
                logging.info("[BALANCE] Gate USDT: %s", bal)

            ok_all &= ok
            continue

        # -------------------------------
        # MEXC
        # -------------------------------
        if ex_l == "mexc":
            ok = _http_ok(
                f"https://contract.mexc.com/api/v1/contract/depth/{probe_symbol.replace('USDT','_USDT')}",
                {"limit": 1},
            )
            logging.info("[CHECK] MEXC (mainnet) -> %s", "OK" if ok else "FAIL")

            key = os.getenv("MEXC_API_KEY", "")
            sec = os.getenv("MEXC_API_SECRET", "")
            if not key or not sec:
                logging.info("[BALANCE] MEXC USDT: (no API keys)")
            else:
                bal = "-"
                try:
                    base = "https://contract.mexc.com"
                    endpoint = "/api/v1/private/account/asset"
                    rt = str(now_ms())
                    query = ""
                    string_to_sign = rt + key + query
                    sign = _hmac_sha256_hex(sec, string_to_sign)
                    headers = {
                        "ApiKey": key,
                        "Request-Time": rt,
                        "Signature": sign,
                    }
                    url = f"{base}{endpoint}"
                    r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                    if r.status_code == 200:
                        j = r.json()
                        data = j.get("data") or {}
                        bal = data.get("usdtBalance", "-")
                    else:
                        logging.warning("[CHECK] MEXC balance HTTP %s: %s",
                                        r.status_code, r.text[:200])
                except Exception as e:
                    logging.warning("[CHECK] MEXC balance error: %s", e)
                logging.info("[BALANCE] MEXC USDT: %s", bal)

            ok_all &= ok
            continue

        # -------------------------------
        # UNKNOWN
        # -------------------------------
        logging.warning("[CHECK] %s: не поддержана проверка коннекта — пропускаю", ex)

    return ok_all

# ----------------- AUTH connectivity checks (private endpoints) -----------------
def _check_binance_auth() -> bool:
    key = os.getenv("BINANCE_API_KEY", "")
    sec = os.getenv("BINANCE_API_SECRET", "")
    if not key or not sec:
        logging.warning("[AUTH] Binance: ключи не заданы — пропускаю приватную проверку")
        return True
    base = _private_base("binance")
    ts = now_ms()
    qs = f"timestamp={ts}&recvWindow=20000"
    sig = _hmac_sha256_hex(sec, qs)
    headers = {"X-MBX-APIKEY": key}
    url = f"{base}/fapi/v2/balance?{qs}&signature={sig}"
    try:
        r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logging.error("[AUTH] Binance FAIL HTTP %s: %s", r.status_code, r.text[:200])
            return False
        j = r.json()
        if isinstance(j, list):
            logging.info("[AUTH] Binance OK (balances entries=%d)", len(j))
            return True
        logging.error("[AUTH] Binance FAIL: неожиданный ответ %s", str(j)[:200])
        return False
    except Exception as e:
        logging.exception("[AUTH] Binance exception: %s", e)
        return False

def _check_bybit_auth() -> bool:
    key = os.getenv("BYBIT_API_KEY", "")
    sec = os.getenv("BYBIT_API_SECRET", "")
    if not key or not sec:
        logging.warning("[AUTH] Bybit: ключи не заданы — пропускаю приватную проверку")
        return True

    base = _private_base("bybit")
    endpoint = "/v5/account/wallet-balance"
    params = "accountType=UNIFIED"
    if _is_true("BYBIT_DEMO", False):
        # для demo режима на api.bybit.com нужно добавить simulateTrading=true
        params += "&simulateTrading=true"

    recv = "20000"
    ts = str(now_ms())
    prehash = ts + key + recv + params  # GET: timestamp+apiKey+recvWindow+queryString
    sign = _hmac_sha256_hex(sec, prehash)
    headers = {
        "X-BAPI-API-KEY": key,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv,
        "X-BAPI-SIGN": sign,
        "X-BAPI-SIGN-TYPE": "2",
    }
    if _is_true("BYBIT_DEMO", False):
        headers["X-BAPI-SIMULATED-TRADING"] = "1"

    url = f"{base}{endpoint}?{params}"
    try:
        r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logging.error("[AUTH] Bybit FAIL HTTP %s: %s", r.status_code, r.text[:200])
            return False
        j = r.json()
        if j.get("retCode") == 0:
            data = ((j.get("result") or {}).get("list") or [])
            coins = (data[0] or {}).get("coin", []) if data else []
            usdt = next((c for c in coins if str(c.get("coin")) == "USDT"), {})

            equity = float(usdt.get("equity") or 0.0)
            wallet = float(usdt.get("walletBalance") or 0.0)
            avail  = float(
                usdt.get("availableToWithdraw")
                or usdt.get("availableBalance")
                or 0.0
            )
            upnl   = float(usdt.get("unrealisedPnl") or 0.0)

            logging.info(
                "[AUTH] Bybit OK — equity=%.2f, wallet=%.2f, available=%.2f, uPnL=%.2f",
                equity, wallet, avail, upnl
            )
            return True

        logging.error("[AUTH] Bybit FAIL retCode=%s: %s", j.get("retCode"), j.get("retMsg"))
        return False
    except Exception as e:
        logging.exception("[AUTH] Bybit exception: %s", e)
        return False

def _check_gate_auth() -> bool:
    key = os.getenv("GATE_API_KEY", "")
    sec = os.getenv("GATE_API_SECRET", "")
    if not key or not sec:
        logging.warning("[AUTH] Gate: ключи не заданы — пропускаю приватную проверку")
        return True

    method = "GET"
    # В Gate API v4 sign_string использует prefix + url: "/api/v4" + "/futures/usdt/accounts"
    path = "/api/v4/futures/usdt/accounts"
    query = ""
    body = ""

    # timestamp — целое число секунд
    ts = str(int(time.time()))

    # body_hash = sha512(body).hexdigest(), даже если body пустой
    body_hash = hashlib.sha512(body.encode()).hexdigest()

    # Согласно официальной схеме:
    # sign_string = method + "\n" + path + "\n" + query + "\n" + body_hash + "\n" + timestamp
    msg = "\n".join([method, path, query, body_hash, ts])
    sign = _hmac_sha512_hex(sec, msg)

    headers = {
        "KEY": key,
        "Timestamp": ts,
        "SIGN": sign,
    }

    # Используем gate_base(), чтобы поддерживать и mainnet, и testnet
    url = f"{gate_base()}{path}"

    try:
        r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logging.error("[AUTH] Gate FAIL HTTP %s: %s", r.status_code, r.text[:200])
            return False
        j = r.json()
        # Простой sanity-check ответа
        if isinstance(j, dict):
            logging.info("[AUTH] Gate OK (futures account)")
            return True
        logging.error("[AUTH] Gate: неожиданный формат ответа: %s", str(j)[:200])
        return False
    except Exception as e:
        logging.exception("[AUTH] Gate exception: %s", e)
        return False

def _check_mexc_auth() -> bool:
    key = os.getenv("MEXC_API_KEY", "")
    sec = os.getenv("MEXC_API_SECRET", "")
    if not key or not sec:
        logging.warning("[AUTH] MEXC: ключи не заданы — пропускаю приватную проверку")
        return True
    # Futures (contract) private spec:
    # headers: ApiKey, Request-Time (ms), Signature (HMAC_SHA256 hex of query/body with secret)
    # simplest private GET: /api/v1/private/account/asset
    base = "https://contract.mexc.com"
    endpoint = "/api/v1/private/account/asset"
    rt = str(now_ms())
    query = ""  # no params
    string_to_sign = rt + key + query
    sign = _hmac_sha256_hex(sec, string_to_sign)
    headers = {
        "ApiKey": key,
        "Request-Time": rt,
        "Signature": sign,
    }
    url = f"{base}{endpoint}"
    try:
        r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logging.error("[AUTH] MEXC FAIL HTTP %s: %s", r.status_code, r.text[:200])
            return False
        j = r.json()
        if j.get("success") or j.get("code") in (0, "0"):
            logging.info("[AUTH] MEXC OK (assets)")
            return True
        logging.error("[AUTH] MEXC FAIL: %s", str(j)[:200])
        return False
    except Exception as e:
        logging.exception("[AUTH] MEXC exception: %s", e)
        return False

def _check_okx_auth() -> bool:
    key = os.getenv("OKX_API_KEY", "")
    sec = os.getenv("OKX_API_SECRET", "")
    passphrase = os.getenv("OKX_PASSPHRASE", "")
    if not key or not sec or not passphrase:
        logging.warning("[AUTH] OKX: ключи не заданы — пропускаю приватную проверку")
        return True
    # OKX sign: sign = base64(hmac_sha256(secret, prehash)), prehash = ts + method + path + body
    base = okx_base()
    endpoint = "/api/v5/account/balance"
    ts = str(time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime()))
    method = "GET"
    body = ""
    prehash = f"{ts}{method}{endpoint}{body}"
    sign = base64.b64encode(hmac.new(sec.encode(), prehash.encode(), hashlib.sha256).digest()).decode()
    headers = {
        "OK-ACCESS-KEY": key,
        "OK-ACCESS-SIGN": sign,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "x-simulated-trading": "1" if _is_true("OKX_TESTNET", False) or _is_true("OKX_PAPER", False) else "0",
    }
    url = f"{base}{endpoint}"
    try:
        r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logging.error("[AUTH] OKX FAIL HTTP %s: %s", r.status_code, r.text[:200])
            return False
        j = r.json()
        if j.get("code") == "0":
            logging.info("[AUTH] OKX OK (balance)")
            return True
        logging.error("[AUTH] OKX FAIL code=%s: %s", j.get("code"), j.get("msg"))
        return False
    except Exception as e:
        logging.exception("[AUTH] OKX exception: %s", e)
        return False

def check_auth_connectivity(exchanges: list[str]) -> bool:
    ok_all = True
    for ex in exchanges:
        exl = ex.lower().strip()
        if exl == "binance":
            ok = _check_binance_auth()
        elif exl == "bybit":
            ok = _check_bybit_auth()
        elif exl == "gate":
            ok = _check_gate_auth()
        elif exl == "mexc":
            ok = _check_mexc_auth()
        elif exl == "okx":
            ok = _check_okx_auth()
        else:
            logging.warning("[AUTH] %s: приватная проверка не реализована — пропускаю", ex)
            ok = True
        logging.info("[AUTH] %s -> %s", ex.upper(), "OK" if ok else "FAIL")
        ok_all &= ok
    return ok_all

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
def binance_quote(symbol: str, price_source: str = "mid") -> Optional[Dict[str, Any]]:
    sym = symbol.upper()
    ps = (price_source or "mid").lower()

    bid = ask = last = mark = mid = None

    # Нужен стакан (mid/bid/ask/book) — берём bookTicker (1 запрос)
    if ps in ("mid", "bid", "ask", "book"):
        bt = _get(f"{binance_data_base()}/fapi/v1/ticker/bookTicker", {"symbol": sym}) or {}
        bid = to_float(bt.get("bidPrice")); ask = to_float(bt.get("askPrice"))
        if bid is not None and ask is not None:
            mid = (bid + ask) / 2.0

    # Нужна last — берём ticker/price (1 запрос)
    if ps == "last":
        lt = _get(f"{binance_data_base()}/fapi/v1/ticker/price", {"symbol": sym}) or {}
        last = to_float(lt.get("price"))

    # Нужна mark — берём premiumIndex (1 запрос)
    if ps == "mark":
        pi = _get(f"{binance_data_base()}/fapi/v1/premiumIndex", {"symbol": sym}) or {}
        mark = to_float(pi.get("markPrice"))

    # Фоллбеки: если ps="mid" не получили bid/ask — можно мягко подстраховаться mark/last при желании

    return {"exchange":"binance","symbol":sym,"bid":bid,"ask":ask,"mid":mid,"last":last,"mark":mark,"ts":utc_ms_now()}

def bybit_quote(symbol: str, price_source: str = "mid"):
    sym = symbol.upper()
    ps = (price_source or "mid").lower()
    url = "https://api.bybit.com/v5/market/tickers"
    params = {"category": "linear", "symbol": sym}
    j = _get(f"{bybit_data_base()}/v5/market/tickers", {"category":"linear","symbol":sym}) or {}
    lst = (((j.get("result") or {}).get("list")) or [])
    if not lst: return None
    x = lst[0]

    bid = to_float(x.get("bid1Price"))
    ask = to_float(x.get("ask1Price"))
    last = to_float(x.get("lastPrice"))
    mark = to_float(x.get("markPrice"))
    mid = (bid + ask) / 2.0 if (bid is not None and ask is not None) else None

    # отдадим только то, что запросили
    if ps == "bid":   ask = last = mark = None
    if ps == "ask":   bid = last = mark = None
    if ps == "last":  bid = ask = mid = mark = None
    if ps == "mark":  bid = ask = mid = last = None
    if ps == "mid":   last = mark = None
    if ps == "book":  last = mark = mid = None  # чисто best bid/ask

    return {"exchange":"bybit","symbol":sym,"bid":bid,"ask":ask,"mid":mid,"last":last,"mark":mark,"ts":utc_ms_now()}

def okx_inst_from_symbol(symbol: str) -> str:
    """
    Преобразует 'BTCUSDT' -> 'BTC-USDT-SWAP', '1000PEPEUSDT' -> '1000PEPE-USDT-SWAP'
    """
    s = symbol.upper().replace("_", "").replace("/", "")
    if not s.endswith("USDT"):
        # при желании тут можно бросить исключение или вернуть исходное
        return s
    base = s[:-4]
    return f"{base}-USDT-SWAP"


def okx_quote(symbol: str, price_source: str = "mid"):
    """
    price_source: mid | bid | ask | book | last | mark
    Для bid/ask/mid/book/last используем один тикер-эндпойнт.
    Для mark — публичный mark-price.
    """
    ps = (price_source or "mid").lower()
    inst_id = okx_inst_from_symbol(symbol)

    bid = ask = last = mark = mid = None

    # 1) bid/ask/mid/book/last — 1 запрос к тикеру
    if ps in ("mid", "bid", "ask", "book", "last"):
        tj = _get(f"{okx_base()}/api/v5/market/ticker", {"instId": inst_id}) or {}
        arr = (tj.get("data") or [])
        if arr:
            x = arr[0]
            bid = to_float(x.get("bidPx"))
            ask = to_float(x.get("askPx"))
            last = to_float(x.get("last"))
            if bid is not None and ask is not None:
                mid = (bid + ask) / 2.0

    # 2) mark — 1 запрос к mark-price (с мягким фоллбеком на last из тикера)
    if ps == "mark":
        mj = _get(f"{okx_base()}/api/v5/public/mark-price", {"instType": "SWAP", "instId": inst_id}) or {}

        arr = (mj.get("data") or [])
        if arr:
            mark = to_float(arr[0].get("markPx"))
        if mark is None:
            # фоллбек: возьмём last из тикера (доп.запрос не нужен, но можно сделать, если хочешь «чище»)
            tj = _get(f"{okx_base()}/api/v5/market/ticker", {"instId": inst_id}) or {}
            arr = (tj.get("data") or [])
            if arr:
                last = to_float(arr[0].get("last"))

    # обнуляем лишнее под выбранный источник
    if ps == "bid":
        ask = last = mark = mid = None
    elif ps == "ask":
        bid = last = mark = mid = None
    elif ps == "last":
        bid = ask = mid = mark = None
    elif ps == "mark":
        bid = ask = mid = last = None
    elif ps == "mid":
        last = mark = None
    elif ps == "book":
        last = mark = mid = None  # чисто best bid/ask

    return {
        "exchange": "okx",
        "symbol": symbol.upper(),
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "last": last,
        "mark": mark,
        "ts": utc_ms_now(),
    }

def mexc_quote(symbol: str, price_source: str = "mid"):
    sym = symbol.upper().replace("/", "_")  # формат  BTC_USDT
    ps = (price_source or "mid").lower()

    bid = ask = last = mark = mid = None

    # стакан (один запрос)
    if ps in ("mid", "bid", "ask", "book"):
        d = _get(f"https://contract.mexc.com/api/v1/contract/depth/{sym}", {"limit": 1}) or {}
        bids = (d.get("data") or {}).get("bids") or []
        asks = (d.get("data") or {}).get("asks") or []
        bid = to_float(bids[0][0]) if bids else None
        ask = to_float(asks[0][0]) if asks else None
        if bid is not None and ask is not None:
            mid = (bid + ask) / 2.0

    # last (один запрос)
    if ps == "last":
        t = _get("https://contract.mexc.com/api/v1/contract/ticker", {"symbol": sym}) or {}
        x = ((t.get("data") or [None]) or [None])[0] or {}
        last = to_float(x.get("lastPrice"))

    # mark (один запрос, с фоллбеком)
    if ps == "mark":
        fp = _get(f"https://contract.mexc.com/api/v1/contract/fair_price/{sym}") or {}
        x = (fp.get("data") or {})
        mark = to_float(x.get("fairPrice")) or to_float(x.get("markPrice"))
        if mark is None:
            # мягкий фоллбек на last
            t = _get("https://contract.mexc.com/api/v1/contract/ticker", {"symbol": sym}) or {}
            y = ((t.get("data") or [None]) or [None])[0] or {}
            last = to_float(y.get("lastPrice"))

    if ps == "bid":   ask = last = mark = mid = None
    if ps == "ask":   bid = last = mark = mid = None
    if ps == "last":  bid = ask = mid = mark = None
    if ps == "mark":  bid = ask = mid = last = None
    if ps == "mid":   last = mark = None
    if ps == "book":  last = mark = mid = None

    return {"exchange":"mexc","symbol":symbol.upper(),"bid":bid,"ask":ask,"mid":mid,"last":last,"mark":mark,"ts":utc_ms_now()}

def gate_quote(symbol: str, price_source: str = "mid"):
    # contract формат у Gate: BTC_USDT
    contract = gate_contract_from_symbol(symbol)  # твоя функция маппинга
    ps = (price_source or "mid").lower()
    url = f"{gate_base()}/api/v4/futures/usdt/tickers"
    j = _get(url, {"contract": contract}) or []
    if not j: return None
    x = j[0]

    bid = to_float(x.get("bid1"))
    ask = to_float(x.get("ask1"))
    last = to_float(x.get("last"))
    mark = to_float(x.get("mark_price"))
    mid = (bid + ask) / 2.0 if (bid is not None and ask is not None) else None

    if ps == "bid":   ask = last = mark = None
    if ps == "ask":   bid = last = mark = None
    if ps == "last":  bid = ask = mid = mark = None
    if ps == "mark":  bid = ask = mid = last = None
    if ps == "mid":   last = mark = None
    if ps == "book":  last = mark = mid = None

    return {"exchange":"gate","symbol":symbol.upper(),"bid":bid,"ask":ask,"mid":mid,"last":last,"mark":mark,"ts":utc_ms_now()}

def get_z_for_pair(stats: pd.DataFrame, symbol: str, ex_low: str, ex_high: str,
                   px_low: float, px_high: float) -> Tuple[float,float,float]:
    """Возвращает (x, z, std). Если статданные недостоверны — z=nan."""
    if px_low <= 0 or px_high <= 0:
        return float('nan'), float('nan'), float('nan')

    x = math.log(px_high/px_low)
    s, a, b = symbol.upper(), ex_low.lower(), ex_high.lower()
    sub = stats[(stats["symbol"]==s) & (stats["ex_low"]==a) & (stats["ex_high"]==b)]
    if sub.empty:
        return x, float('nan'), float('nan')

    mean = to_float(sub.iloc[0].get("ema_mean"))
    var  = to_float(sub.iloc[0].get("ema_var"))
    cnt  = int(sub.iloc[0].get("count") or 0)
    upd  = float(sub.iloc[0].get("updated_ms") or 0.0) / 1000.0
    now  = time.time()

    # фильтры качества статистики
    if cnt < SPREAD_MIN_COUNT or (upd > 0 and (now - upd) > SPREAD_STALE_SEC):
        return x, float('nan'), float('nan')

    std = max(math.sqrt(max(var or 0.0, 0.0)), SPREAD_STD_FLOOR)
    z   = (x - (mean or 0.0)) / std if std > 0 else float('nan')
    return x, z, std

def get_pair_reco(stats: pd.DataFrame, symbol: str, ex_low: str, ex_high: str) -> tuple[float, float]:
    """
    Возвращает (z_in_suggested, entry_bps_suggested) ДЛЯ ПАРЫ из spread_stats.csv,
    либо (Z_IN из .env, ENTRY_SPREAD_BPS из .env), если в stats нет записи.
    ВНИМАНИЕ: этот хелпер НЕ трогает формат CSV и НЕ требует новых колонок — он просто
    возвращает дефолты, чтобы код не падал, если ты не используешь персональные рекомендации.
    """
    z_default   = float(getenv_float("Z_IN", 3.5))
    entry_bps_d = float(getenv_float("ENTRY_SPREAD_BPS", 90.0))

    # Если сейчас в твоём CSV нет персональных полей — вернём дефолты
    if stats is None or stats.empty:
        return z_default, entry_bps_d

    s, a, b = symbol.upper(), ex_low.lower(), ex_high.lower()
    sub = stats[(stats["symbol"] == s) & (stats["ex_low"] == a) & (stats["ex_high"] == b)]
    if sub.empty:
        return z_default, entry_bps_d

    # Пытаемся прочитать рекомендуемые колонки, если ты их уже пишешь;
    # если нет — мягко откатываемся к дефолтам. НОВЫХ КОЛОНОК НЕ ТРЕБУЕМ.
    z_in_sugg = to_float(sub.iloc[0].get("Z_IN_suggested"))
    entry_bps_sugg = to_float(sub.iloc[0].get("entry_spread_bps_suggested"))

    if z_in_sugg is None or z_in_sugg != z_in_sugg:
        z_in_sugg = z_default
    if entry_bps_sugg is None or entry_bps_sugg != entry_bps_sugg or entry_bps_sugg <= 0:
        entry_bps_sugg = entry_bps_d

    return float(z_in_sugg), float(entry_bps_sugg)
def try_instant_open(best: dict, per_leg_notional_usd: float, taker_fee: float, paper: bool, pos_path: str) -> bool:
    """
    Пытается открыть позицию сразу при найденном сигнале (внутри сканера),
    если сейчас нет открытых позиций, не нарушен кулдаун и pair-lock доступен.
    Возвращает True, если открытие выполнено, иначе False.
    """
    import pandas as pd
    sym      = str(best["symbol"]).upper()
    cheap_ex = str(best["long_ex"])
    rich_ex  = str(best["short_ex"])
    px_low   = float(best["px_low"])
    px_high  = float(best["px_high"])

    # 0) Проверим, нет ли уже открытых позиций
    df_pos = load_positions(pos_path)
    if (not df_pos.empty) and any(df_pos["status"] == "open"):
        return False

    # 1) Кулдаун после закрытия (чтобы не перезаходить мгновенно в тот же тикер)
    COOLDOWN_AFTER_CLOSE_SEC = int(getenv_float("COOLDOWN_AFTER_CLOSE_SEC", 30))
    _LAST_CLOSED: dict[str, float] = globals().setdefault("_LAST_CLOSED", {})
    if COOLDOWN_AFTER_CLOSE_SEC > 0 and sym in _LAST_CLOSED:
        if (time.time() - _LAST_CLOSED[sym]) < COOLDOWN_AFTER_CLOSE_SEC:
            logging.debug("Skip open %s due to cooldown after close", sym)
            return False

        # 1.5) Preflight-refresh котировок перед открытием (если включен REFRESH_CONFIRM)
    try:
        do_confirm = os.getenv("REFRESH_CONFIRM", "0").lower() in ("1", "true", "yes")
        if do_confirm:
            ps = getenv_str("PRICE_SOURCE", "mid") or "mid"

            def _bbo(ex: str) -> tuple[float, float]:
                ex_l = (ex or "").lower()
                if ex_l == "bybit":
                    bid, ask = bybit_best_bid_ask(sym)
                    return float(bid or 0.0), float(ask or 0.0)
                else:
                    q = binance_quote(sym, "book") or {}
                    bid = float(q.get("bid") or 0.0)
                    ask = float(q.get("ask") or 0.0)
                    return bid, ask

            # На дешёвой бирже мы покупаем (нужен ask), на дорогой продаём (нужен bid)
            bid_short, ask_short = _bbo(rich_ex)
            bid_long,  ask_long  = _bbo(cheap_ex)

            if ask_long <= 0.0 or bid_short <= 0.0:
                logging.debug("Preflight skip %s: no valid BBO (ask_long=%.8f, bid_short=%.8f)",
                              sym, ask_long, bid_short)
                return False

            # Требуемый спред: либо персональный из best["entry_bps_sugg"], либо ENTRY_SPREAD_BPS из .env
            entry_req_env  = float(getenv_float("ENTRY_SPREAD_BPS", 0.0))
            entry_req_best = float(best.get("entry_bps_sugg") or 0.0)
            entry_bps_req  = max(entry_req_env, entry_req_best, 0.0)

            ENTRY_HYST_BPS = float(getenv_float("ENTRY_HYST_BPS", 3.0))

            entry_bps_now = (bid_short - ask_long) / ask_long * 1e4
            if entry_bps_req > 0.0 and entry_bps_now < (entry_bps_req - ENTRY_HYST_BPS):
                logging.debug(
                    "Preflight skip %s: entry_bps_now=%.2f < %.2f (req-hyst, req=%.2f)",
                    sym, entry_bps_now, entry_bps_req - ENTRY_HYST_BPS, entry_bps_req
                )
                return False
    except Exception as e:
        logging.debug("Preflight exception for %s: %s", sym, e)

    # 2) Pair-lock: чтоб другой тикер не начал открываться в этот же момент
    if not open_lock_check_or_set(sym):
        return False

    # --- (NEW) Refresh-confirm снапшота перед расчётом qty и отправкой ордеров ---
    try:
        do_confirm = os.getenv("REFRESH_CONFIRM", "0").lower() in ("1","true","yes")
        if do_confirm:
            ps = getenv_str("PRICE_SOURCE", "mid")
            # свежие котировки только для нужного символа на двух биржах
            rows = []
            try:
                rows.append(binance_quote(sym, ps))
            except Exception:
                pass
            try:
                rows.append(bybit_quote(sym, ps))
            except Exception:
                pass
            rows = [r for r in rows if r]  # убрать None

            ref_best = best_pair_for_symbol(rows, per_leg_notional_usd, taker_fee, ps)
            if (not ref_best) or \
               (str(ref_best["long_ex"]) != cheap_ex) or \
               (str(ref_best["short_ex"]) != rich_ex):
                open_lock_clear()
                logging.debug("REFRESH_CONFIRM failed: pair changed for %s", sym)
                return False

            # не даём открыть, если спред заметно деградировал
            hyst = float(getenv_float("EXIT_HYST_BPS", 3.0))
            entry_req = float(getenv_float("ENTRY_SPREAD_BPS", 70.0))
            if float(ref_best["spread_bps"]) < max(entry_req - hyst, entry_req * 0.9):
                open_lock_clear()
                logging.debug("REFRESH_CONFIRM failed: spread deteriorated for %s", sym)
                return False

            # проверка актуального Z (если доступна статистика)
            try:
                x_now, z_now, std_now = get_z_for_pair(
                    read_spread_stats(),
                    sym, str(ref_best["long_ex"]), str(ref_best["short_ex"]),
                    float(ref_best["px_low"]), float(ref_best["px_high"])
                )
                Z_IN_LOC = float(getenv_float("Z_IN", 3.0))
                STD_MIN_FOR_OPEN = float(getenv_float("STD_MIN_FOR_OPEN", 0.0))
                if (std_now is not None and std_now == std_now and std_now < STD_MIN_FOR_OPEN) or \
                   (z_now is not None and z_now == z_now and z_now < Z_IN_LOC):
                    open_lock_clear()
                    logging.debug("REFRESH_CONFIRM failed: z/std check for %s", sym)
                    return False
            except Exception:
                # если статистика недоступна — пропускаем этот чек
                pass
    except Exception as e:
        open_lock_clear()
        logging.debug("REFRESH_CONFIRM exception: %s", e)
        return False

    try:
        # 3) Рассчитываем qty как в positions_once (капитал/леверидж/шаги лота)
        # Базовая оценка
        qty = float(best.get("qty_est") or 0.0)
        if qty <= 0:
            per_leg_notional = per_leg_notional_usd
            qty = per_leg_notional / max(px_low, 1.0)

        # Шаги лота (Bybit/ Binance), жёсткое ограничение по капиталу
        try:
            _, _, _, by_qty_step = bybit_get_filters(sym)
        except Exception:
            by_qty_step = 0.0
        bn_info = _binance_symbol_info(sym) or {}
        lot_f = {f["filterType"]: f for f in bn_info.get("filters", [])}.get("LOT_SIZE") or {}
        bn_qty_step = float(lot_f.get("stepSize") or 0.0)

        capital  = float(getenv_float("CAPITAL", 1000.0))
        leverage = float(getenv_float("PERP_LEVERAGE", 5.0))
        qty_capA = cap_qty_by_capital(px_low,  by_qty_step or bn_qty_step or 0.0, capital, leverage)
        qty_capB = cap_qty_by_capital(px_high, by_qty_step or bn_qty_step or 0.0, capital, leverage)
        qty = max(0.0, min(qty, qty_capA, qty_capB))
        if qty <= 0:
            logging.debug("Qty capped to 0 by capital limits — skip %s", sym)
            return False

        # 4) Открываем атомарно
        ok, attempt_id, meta = atomic_cross_open(
            symbol=sym, cheap_ex=cheap_ex, rich_ex=rich_ex,
            qty=qty, price_low=px_low, price_high=px_high, paper=paper
        )

        now_ms = utc_ms_now()
        if ok:
            # проставим запись в positions
            cur_max = pd.to_numeric(df_pos["id"], errors="coerce").max() if not df_pos.empty else None
            next_id = int(cur_max)+1 if cur_max==cur_max and cur_max is not None else 1
            new = {
                "id": next_id, "attempt_id": attempt_id, "symbol": sym,
                "long_ex": cheap_ex, "short_ex": rich_ex,
                "opened_ms": now_ms, "last_ms": now_ms, "held_h": 0.0,
                "size_usd": per_leg_notional_usd,
                "entry_spread_bps": float(best["spread_bps"]),
                "entry_px_low": px_low, "entry_px_high": px_high,
                "status": "open",
                "opened_at": iso_utc(now_ms), "closed_at": "", "close_reason": "",
                "validated_qty": meta.get("qty", qty),
                "note_net_usd": float(best.get("net_usd") or 0.0),
                "open_long_order_id":  meta.get("open_long_order_id",""),
                "open_short_order_id": meta.get("open_short_order_id",""),
                "open_long_px":  meta.get("open_long_px", 0.0),
                "open_short_px": meta.get("open_short_px", 0.0),
                "open_fees_usd": meta.get("open_fees_usd", 0.0),
                "open_long_cloid": meta.get("open_long_cloid",""),
                "open_short_cloid": meta.get("open_short_cloid","")
            }
            df_pos = pd.concat([df_pos, pd.DataFrame([new])], ignore_index=True)
            save_positions(pos_path, df_pos)

            # Телеграм карточка «OPENED»
            price_source = getenv_str("PRICE_SOURCE", "mid")
            maybe_send_telegram("✅ <b>OPENED</b>\n" + format_signal_card(best, per_leg_notional_usd, price_source))
            return True
        else:
            logging.warning("Instant open failed: %s", meta.get("error"))
            return False
    finally:
        open_lock_clear()

# ----------------- Scanners -----------------
def scan_all_with_instant_alerts(
    exchanges: List[str],
    symbols: List[str],
    per_leg_notional_usd: float,
    taker_fee: float,
    price_source: str,
    alert_spread_pct: float,
    cooldown_sec: int,
    *,
    instant_open: bool = True,
    pos_path_for_instant: Optional[str] = None,
    paper: Optional[bool] = None,
    per_ex_symbols: Optional[Dict[str, set]] = None,
) -> pd.DataFrame:
    import pandas as pd
    rows_all = []
    now = time.time()

    stats_df = read_spread_stats()
    SLIPPAGE_BPS = float(getenv_float("SLIPPAGE_BPS", 1.0))

    for sym in symbols:
        per_symbol_rows = []
        sym_u = sym.upper()
        for ex in exchanges:
            # Если у нас есть матрица доступности символов по биржам,
            # не дёргаем котировки по тем биржам, где этого символа нет.
            if per_ex_symbols is not None:
                allowed = per_ex_symbols.get(ex)
                # Пустое множество/None трактуем как "нет информации" → не фильтруем.
                if allowed and sym_u not in allowed:
                    continue

            if ex == "binance":
                r = binance_quote(sym, price_source)
            elif ex == "bybit":
                r = bybit_quote(sym, price_source)
            elif ex == "okx":
                r = okx_quote(sym, price_source)
            elif ex == "mexc":
                r = mexc_quote(sym, price_source)
            elif ex == "gate":
                r = gate_quote(sym, price_source)
            else:
                r = None
            if r:
                rows_all.append(r)
                per_symbol_rows.append(r)

        best = best_pair_for_symbol(per_symbol_rows, per_leg_notional_usd, taker_fee, price_source)
        if not best:
            continue

        # --- локальные пороги: попытаться взять из статистики спредов, иначе дефолты .env ---
        try:
            z_in_loc, entry_bps_sugg = get_pair_reco(
                stats_df,
                str(best["symbol"]),
                str(best["long_ex"]),
                str(best["short_ex"])
            )
        except Exception:
            z_in_loc = float(getenv_float("Z_IN", 3.5))
            entry_bps_sugg = float(getenv_float("ENTRY_SPREAD_BPS", 90.0))

        Z_IN_LOC  = float(z_in_loc)
        entry_bps = float(entry_bps_sugg)
        best["entry_bps_sugg"] = entry_bps  # показываем в карточке, что именно используем

        # Считаем Z и экономику (всегда), чтобы решить вопрос об ОТКРЫТИИ
        _, z, std = get_z_for_pair(
            stats_df,
            symbol=str(best["symbol"]),
            ex_low=str(best["long_ex"]),
            ex_high=str(best["short_ex"]),
            px_low=float(best["px_low"]),
            px_high=float(best["px_high"]),
        )
        SLIPPAGE_BPS_DYNAMIC = getenv_bool("SLIPPAGE_BPS_DYNAMIC", True)
        slip_bps = SLIPPAGE_BPS
        if SLIPPAGE_BPS_DYNAMIC:
            try:
                # оценим «дырок» между лучшим ask на дешёвой и лучшим bid на дорогой
                gap_bps = max(0.0, (float(best["px_high"]) - float(best["px_low"])) / float(best["px_low"]) * 1e4)
                slip_bps = max(SLIPPAGE_BPS, 0.5 * gap_bps)
            except Exception:
                pass
        net_usd_adj = float(best["net_usd"]) - (4.0 * (slip_bps / 1e4) * per_leg_notional_usd)
        best["net_usd_adj"] = net_usd_adj
        best["z"]   = z
        best["std"] = std
        best["net_usd_adj"] = net_usd_adj
        
        # --- проверки на качество сигнала ---
        std_min_for_open = float(getenv_float("STD_MIN_FOR_OPEN", 1e-4))
        std_ok    = float(best.get("std") or 0.0) >= std_min_for_open
        spread_ok = float(best.get("spread_bps") or 0.0) >= entry_bps
        z_ok      = (z == z) and (z >= Z_IN_LOC)
        eco_ok    = net_usd_adj > 0.0
        ENTRY_MODE = getenv_str("ENTRY_MODE", "zscore").lower()
        if ENTRY_MODE not in ("zscore","price"):
            ENTRY_MODE = "zscore"

        if ENTRY_MODE == "zscore":
            cond_open = instant_open and z_ok and eco_ok and spread_ok and std_ok
        else:  # price
            cond_open = instant_open and eco_ok and spread_ok  # без z и std

        # 1) МГНОВЕННОЕ ОТКРЫТИЕ — теперь НЕ зависит от alert_spread_pct
        if cond_open:
            _pos_path = pos_path_for_instant or bucketize_path(getenv_str("POS_CROSS_PATH", "positions_price_cross.csv"))
            _paper    = bool(getenv_bool("PAPER", True)) if paper is None else bool(paper)
            
            try:
                _ = try_instant_open(best, per_leg_notional_usd, taker_fee, _paper, _pos_path)
            except Exception as e:
                logging.warning("Instant open exception for %s: %s", best.get("symbol"), e)

        # 2) ТЕЛЕГРАМ-АЛЕРТ — оставляем защиту по спреду и антиспам
        if float(best["spread_pct"]) >= float(alert_spread_pct):
            last_ts = _LAST_ALERT_TS.get(best["symbol"], 0.0)
            if (now - last_ts) >= cooldown_sec:
                card = format_signal_card(best, per_leg_notional_usd, price_source)
                if INSTANT_ALERT:
                    maybe_send_telegram("🚨 <b>INSTANT ALERT</b>\n" + card)
                    _LAST_ALERT_TS[best["symbol"]] = now

    cols = ["exchange","symbol","bid","ask","mid","last","mark","ts"]
    return pd.DataFrame(rows_all) if rows_all else pd.DataFrame(columns=cols)

# ============================================================
# =========== BULK-OPTIMIZED SCAN_SPREADS_ONCE ===============
# ============================================================

def scan_spreads_once(
    exchanges,
    symbols,
    spread_bps_min,
    spread_bps_max,
    notional_usd,
    taker_fee,
    pos_path,
    price_stats_path,
    debug=False
):
    """
    Быстрый цикл сканирования, использующий bulk-тickers для Binance, Bybit, OKX, Gate.
    Ускорение 50–200X по сравнению с прежней реализацией.
    """
    start_ts = time.time()

    # ==== 1. Загружаем котировки одним bulk-запросом ====
    bulk = load_all_bulk_quotes(exchanges)
    # bulk = {
    #    "binance": { "BTCUSDT": {bid, ask, mark, last}, ... },
    #    "bybit":   { ... },
    #    "okx":     { ... },
    #    "gate":    { ... }
    # }

    records = []     # для расчёта статистики спредов
    candidates = []  # для сигналов

    # ==== 2. Проходим по каждому символу ====
    for sym in symbols:
        su = sym.upper()
        best_long_ex  = None
        best_short_ex = None
        best_bid = 0
        best_ask = 10 ** 12

        # ==== 3. Ищем лучшую биржу для LONG (где дешевле купить) ====
        for ex in exchanges:
            q = bulk.get(ex, {}).get(su)
            if not q:
                continue
            if q["ask"] < best_ask and q["ask"] > 0:
                best_ask = q["ask"]
                best_long_ex = ex

        # ==== 4. Лучшая биржа для SHORT (где дороже продать) ====
        for ex in exchanges:
            q = bulk.get(ex, {}).get(su)
            if not q:
                continue
            if q["bid"] > best_bid and q["bid"] > 0:
                best_bid = q["bid"]
                best_short_ex = ex

        # Если нет цен — пропускаем
        if not best_long_ex or not best_short_ex:
            continue

        # ==== 5. Считаем mid и спред ====
        px_low  = best_ask
        px_high = best_bid

        if px_low <= 0 or px_high <= 0:
            continue

        mid_low  = px_low
        mid_high = px_high

        spread = mid_high - mid_low
        spread_pct = spread / mid_low if mid_low > 0 else 0
        spread_bps = spread_pct * 1e4  # в bps

        # ==== 6. Добавляем в историю (для статистики) ====
        records.append(
            {
                "symbol": su,
                "long_ex": best_long_ex,
                "short_ex": best_short_ex,
                "px_low": px_low,
                "px_high": px_high,
                "spread_bps": spread_bps,
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            }
        )

        # ==== 7. Проверяем фильтры для сигналов ====
        if spread_bps_min <= spread_bps <= spread_bps_max:
            candidates.append(
                {
                    "symbol": su,
                    "long_ex": best_long_ex,
                    "short_ex": best_short_ex,
                    "px_low": px_low,
                    "px_high": px_high,
                    "spread_bps": spread_bps
                }
            )

    # ==== 8. Записываем статистику ====
    if records:
        try:
            df = pd.DataFrame(records)
            if os.path.exists(price_stats_path):
                df0 = pd.read_csv(price_stats_path)
                df = pd.concat([df0, df], ignore_index=True)
            df.to_csv(price_stats_path, index=False)
            logging.info(f"Spread stats saved: rows={len(df)}")
        except Exception as e:
            logging.error(f"Failed to write stats: {e}")

    # ==== 9. Если нет кандидатов ====
    if not candidates:
        logging.info("No price-filtered candidates this cycle.")
        return None

    # ==== 10. Выбираем лучший сигнал ====
    best = max(candidates, key=lambda x: x["spread_bps"])
    if debug:
        logging.info(f"Best candidate: {best}")

    return best

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

# где-нибудь рядом с _fmt_val
def _fmt_qty_str(q: float) -> str:
    # формат для Bybit: убираем хвостовые нули/точку
    s = ("%.10f" % float(q)).rstrip("0").rstrip(".")
    return s if s else "0"

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
def bybit_signed_post(payload: dict, endpoint: str):
    api_key = os.getenv("BYBIT_API_KEY", "")
    api_secret = os.getenv("BYBIT_API_SECRET", "")
    recv = "5000"
    ts = str(int(time.time() * 1000))

    # JSON-строка тела ДОЛЖНА участвовать в подписи
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

    prehash = ts + api_key + recv + body  # для POST: timestamp + apiKey + recvWindow + body
    sign = hmac.new(api_secret.encode(), prehash.encode(), hashlib.sha256).hexdigest()

    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv,
        "X-BAPI-SIGN": sign,
        "X-BAPI-SIGN-TYPE": "2",
        "Content-Type": "application/json",
    }
    # ⬇️ именно здесь добавляем демо-заголовок
    if _is_true("BYBIT_DEMO", False):
        headers["X-BAPI-SIMULATED-TRADING"] = "1"

    base = _private_base("bybit")  # отдаст https://api-demo.bybit.com при BYBIT_DEMO=1
    url = f"{base}{endpoint}"
    r = SESSION.post(url, headers=headers, data=body, timeout=REQUEST_TIMEOUT)
    return r


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

def _place_perp_market_order(exchange: str, symbol: str, side: str, qty: float,
                             paper: bool=False, cl_oid: str|None=None, reduce_only: bool=False) -> dict:
    if paper:
        return {"status":"FILLED","avg_price":0.0,"order_id": f"paper-{uuid.uuid4().hex[:8]}", "fee_usd":0.0}
    ex = exchange.lower()
    if ex == "binance":
        binance_sync_time()
        payload = {"symbol":symbol.upper(),"side":side.upper(),"type":"MARKET","quantity":qty,"recvWindow":10000}
        if reduce_only:
            payload["reduceOnly"] = "true"
        if cl_oid: payload["newClientOrderId"] = cl_oid
        signed = binance_signed_post(payload)
        r = SESSION.post(f"{_private_base('binance')}/fapi/v1/order", headers=signed["headers"], data=signed["data"], timeout=REQUEST_TIMEOUT)
        j = r.json()
        if r.status_code==200:
            px = 0.0
            try: px = float((j.get("fills") or [{}])[0].get("price") or 0.0)
            except: pass
            # Среднюю цену и комиссии лучше добрать из userTrades позже, но попробуем из fills:
            fills = j.get("fills") or []
            prices = [float(f.get("price",0)) for f in fills if f]
            qtys   = [float(f.get("qty",0))   for f in fills if f]
            fees   = [float(f.get("commission",0)) for f in fills if f]  # USDT на фьючах
            px = _wavg(prices, qtys) if prices and qtys else float(j.get("avgPrice") or 0.0)
            fee_usd = _sum(fees)
            return {"status":"FILLED","avg_price":px,"order_id":str(j.get("orderId")), "client_order_id": j.get("clientOrderId") or cl_oid, "fee_usd": fee_usd}

        # Если market не FILLED, пробуем IOC LIMIT с защитной ценой
        if r.status_code != 200 or (j.get("status","") != "FILLED" and not j.get("fills")):
            # вычислим cap-цену от последнего BBO
            q = binance_quote(symbol, price_source="book") or {}
            bid, ask = float(q.get("bid") or 0), float(q.get("ask") or 0)
            px_ref = ask if side.upper()=="BUY" else bid
            if not px_ref or px_ref<=0:
                raise RuntimeError(f"Binance market failed and no BBO for IOC: resp={str(j)[:200]}")
            bps = float(getenv_float("PRICE_SAFETY_BPS", 15))
            mul = (1 + bps/10000.0) if side.upper()=="BUY" else (1 - bps/10000.0)
            px_cap = round(px_ref * mul, 6)

            payload_lim = {
                "symbol": symbol.upper(),
                "side": side.upper(),
                "type": "LIMIT",
                "quantity": qty,
                "price": _fmt_val(px_cap),
                "timeInForce": "IOC",
                "recvWindow": 10000
            }
            if reduce_only: payload_lim["reduceOnly"] = "true"
            if cl_oid: payload_lim["newClientOrderId"] = cl_oid
            signed2 = binance_signed_post(payload_lim)
            r2 = SESSION.post(f"{_private_base('binance')}/fapi/v1/order",
                            headers=signed2["headers"], data=signed2["data"], timeout=REQUEST_TIMEOUT)
            j2 = r2.json()
            if r2.status_code==200 and j2.get("status") in ("FILLED","PARTIALLY_FILLED"):
                fills = j2.get("fills") or []
                prices = [float(f.get("price",0)) for f in fills if f]
                qtys   = [float(f.get("qty",0)) for f in fills if f]
                avg = sum(p*q for p,q in zip(prices,qtys))/max(sum(qtys),1) if prices and qtys else float(j2.get("avgPrice") or 0.0)
                fee = sum(float(f.get("commission",0)) for f in fills if f)
                return {"status":"FILLED","avg_price": avg, "order_id": str(j2.get("orderId")),
                        "client_order_id": payload_lim.get("newClientOrderId"), "fee_usd": fee}
            raise RuntimeError(f"Binance IOC-limit fallback failed: http={r2.status_code} resp={str(j2)[:400]}")

        raise RuntimeError(f"Binance order failed: {r.status_code} {str(j)[:200]}")
    elif ex == "bybit":
        base = _private_base("bybit")  # корректный выбор demo/testnet/mainnet
        hedge_mode = getenv_bool("BYBIT_HEDGE_MODE", False)
        pos_idx = 0
        if hedge_mode:
            pos_idx = 1 if side.upper() == "BUY" else 2

        # Сначала пробуем MARKET как раньше
        payload_mkt = {
            "category": "linear",
            "symbol": symbol.upper(),
            "side": side.capitalize(),
            "orderType": "Market",
            "qty": _fmt_qty_str(qty),
            "timeInForce": "IOC",
            "reduceOnly": bool(reduce_only),
            "positionIdx": pos_idx
        }
        if _is_true("BYBIT_DEMO", False):
            payload_mkt["simulateTrading"] = True

        if cl_oid:
            payload_mkt["orderLinkId"] = cl_oid

        try:
            r = bybit_signed_post(payload_mkt, "/v5/order/create")
            j = r.json()
            if j.get("retCode") == 0:
                avg = 0.0
                try:
                    avg = float((j["result"].get("avgPrice") or 0.0))
                except:
                    pass
                return {"status": "FILLED", "avg_price": avg, "order_id": j["result"].get("orderId"), "client_order_id": payload_mkt.get("orderLinkId"), "fee_usd": 0.0}
            # если это MPP/границы цены — уйдём во fallback
            if j.get("retCode") not in (30209, 176001, 176002, 176003):
                # другие ошибки — фейлим сразу с диагностикой
                raise RuntimeError(f"Bybit order failed: retCode={j.get('retCode')} retMsg={j.get('retMsg')} resp={str(j)[:400]}")
        except Exception as e:
            # сеть/неожиданные — не прерываем, попробуем fallback
            logging.warning("Bybit market order error, will try IOC limit fallback: %s", e)

        # --- Fallback: IOC-LIMIT по лучшему биду/аску с небольшим запасом ---
        safety_bps = float(os.getenv("PRICE_SAFETY_BPS", "5")) / 10000.0  # 5 bps по умолчанию
        best_bid, best_ask = bybit_best_bid_ask(symbol)
        tick, pmin, pmax, qty_step = bybit_get_filters(symbol)

        if side.upper() == "BUY":
            raw_price = best_ask * (1 + safety_bps)
        else:
            raw_price = best_bid * (1 - safety_bps)

        price = _round_to_step(raw_price, tick, mode="down" if side.upper()=="BUY" else "up")
        price = max(min(price, pmax), pmin)

        payload_lim = {
            "category": "linear",
            "symbol": symbol.upper(),
            "side": side.capitalize(),
            "orderType": "Limit",
            "qty": _fmt_qty_str(qty),
            "price": _fmt_price_str(price),
            "timeInForce": "IOC",
            "reduceOnly": bool(reduce_only),
            "positionIdx": pos_idx
        }
        if _is_true("BYBIT_DEMO", False):
            payload_lim["simulateTrading"] = True

        if cl_oid:
            payload_lim["orderLinkId"] = cl_oid

        r2 = bybit_signed_post(payload_lim, "/v5/order/create")
        j2 = r2.json()

        if j2.get("retCode") == 0:
            avg = 0.0
            try:
                avg = float((j2["result"].get("avgPrice") or 0.0))
            except:
                pass
            return {"status": "FILLED", "avg_price": avg, "order_id": j2["result"].get("orderId"), "client_order_id": payload_lim.get("orderLinkId"), "fee_usd": 0.0}

        raise RuntimeError(f"Bybit IOC-limit fallback failed: retCode={j2.get('retCode')} retMsg={j2.get('retMsg')} req={payload_lim} resp={str(j2)[:400]}")
    elif ex == "okx":
        # --- OKX USDT-SWAP через REST v5 /api/v5/trade/order ---
        key = os.getenv("OKX_API_KEY", "")
        sec = os.getenv("OKX_API_SECRET", "")
        passphrase = os.getenv("OKX_PASSPHRASE", "")
        if not key or not sec or not passphrase:
            raise RuntimeError("OKX: API ключи не заданы (OKX_API_KEY/OKX_API_SECRET/OKX_PASSPHRASE)")

        base = okx_base()
        inst_id = okx_inst_from_symbol(symbol)
        td_mode = getenv_str("OKX_TD_MODE", "cross")  # cross | isolated

        body_dict = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side.lower(),        # buy / sell
            "ordType": "market",
            "sz": str(qty),
        }
        if reduce_only:
            # у OKX reduceOnly есть только для отдельных режимов, но в демо можно попробовать
            body_dict["reduceOnly"] = True
        if cl_oid:
            body_dict["clOrdId"] = cl_oid

        body = json.dumps(body_dict, separators=(",", ":"))

        # sign: ts + method + path + body
        path = "/api/v5/trade/order"
        method = "POST"
        ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        prehash = ts + method + path + body
        sign = base64.b64encode(
            hmac.new(sec.encode(), prehash.encode(), hashlib.sha256).digest()
        ).decode()

        headers = {
            "OK-ACCESS-KEY": key,
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": passphrase,
            "Content-Type": "application/json",
            # demo-режим: x-simulated-trading=1 для тестовых ключей
            "x-simulated-trading": "1" if os.getenv("OKX_TESTNET", "0").lower() in ("1", "true", "t", "yes") else "0",
        }

        url = f"{base}{path}"
        r = SESSION.post(url, headers=headers, data=body, timeout=REQUEST_TIMEOUT)
        j = r.json()

        if r.status_code != 200 or j.get("code") != "0":
            raise RuntimeError(
                f"OKX order failed: http={r.status_code} code={j.get('code')} msg={j.get('msg')} resp={str(j)[:400]}"
            )

        data = (j.get("data") or [{}])[0]
        avg_px = float(data.get("avgPx") or data.get("fillPx") or 0.0)
        try:
            fee = float(data.get("fee") or 0.0)
        except Exception:
            fee = 0.0

        return {
            "status": "FILLED",
            "avg_price": avg_px,
            "order_id": data.get("ordId") or "",
            "client_order_id": data.get("clOrdId") or cl_oid,
            "fee_usd": abs(fee),
        }
    elif ex == "gate":
        # --- Gate USDT futures /api/v4/futures/usdt/orders ---
        key = os.getenv("GATE_API_KEY", "")
        sec = os.getenv("GATE_API_SECRET", "")
        if not key or not sec:
            raise RuntimeError("Gate: API ключи не заданы (GATE_API_KEY/GATE_API_SECRET)")

        contract = gate_contract_from_symbol(symbol)
        base = gate_base()
        path = "/api/v4/futures/usdt/orders"
        url = f"{base}{path}"

        # В Gate size > 0 = long, size < 0 = short
        size = float(qty)
        if side.upper() == "SELL":
            size = -size

        body_dict = {
            "contract": contract,
            "size": size,
            "tif": "ioc",         # IOC как у нас везде
            "auto_size": "none",
        }
        if reduce_only:
            body_dict["reduce_only"] = True
        if cl_oid:
            body_dict["text"] = cl_oid

        body = json.dumps(body_dict, separators=(",", ":"))

        ts = str(int(time.time()))
        body_hash = hashlib.sha512(body.encode()).hexdigest()
        # sign_string = method + "\n" + path + "\n" + query + "\n" + body_hash + "\n" + timestamp
        method = "POST"
        query = ""
        msg = "\n".join([method, path, query, body_hash, ts])
        sign = _hmac_sha512_hex(sec, msg)

        headers = {
            "KEY": key,
            "Timestamp": ts,
            "SIGN": sign,
            "Content-Type": "application/json",
        }

        r = SESSION.post(url, headers=headers, data=body, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"Gate order HTTP {r.status_code}: {r.text[:400]}")

        j = r.json()
        # Ошибки Gate формата {"label":"INVALID_SIGNATURE","message":"..."}
        if isinstance(j, dict) and j.get("label"):
            raise RuntimeError(f"Gate order failed: label={j.get('label')} msg={j.get('message')} resp={str(j)[:400]}")

        data = j if isinstance(j, dict) else (j[0] if j else {})
        try:
            avg = float(data.get("fill_price") or data.get("price") or 0.0)
        except Exception:
            avg = 0.0
        try:
            fee = float(data.get("fee") or 0.0)
        except Exception:
            fee = 0.0
        oid = str(data.get("id") or data.get("order_id") or "")

        return {
            "status": "FILLED",
            "avg_price": avg,
            "order_id": oid,
            "client_order_id": cl_oid,
            "fee_usd": abs(fee),
        }
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
        contract = gate_contract_from_symbol(symbol)  # BTC_USDT
        return f"https://www.gate.io/futures_trade/USDT/{contract}"
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
    if ps == "book":
        # для закрытия возьмем mid на основе BBO (реалистичный центр спреда)
        b = to_float(row.get("bid")); a = to_float(row.get("ask"))
        return ((b + a) / 2.0) if (b is not None and a is not None) else None
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

        MIN_TOPBOOK_FACTOR = float(getenv_float("MIN_TOPBOOK_FACTOR", 1.25))
        need_usd = per_leg_notional_usd * MIN_TOPBOOK_FACTOR

        ask_px = float(to_float(row_low.get("ask")))
        bid_px = float(to_float(row_high.get("bid")))
        ask_sz = float(to_float(row_low.get("ask_qty") or row_low.get("askSize") or 0.0))
        bid_sz = float(to_float(row_high.get("bid_qty") or row_high.get("bidSize") or 0.0))

        ask_notional = (ask_px * ask_sz) if (ask_px and ask_sz) else 0.0
        bid_notional = (bid_px * bid_sz) if (bid_px and bid_sz) else 0.0

        if ask_notional < need_usd or bid_notional < need_usd:
            return None  # недостаточно ликвидности на лучших уровнях

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
STATS_COLS = [
    "symbol","ex_low","ex_high",
    "ema_mean","ema_var","count","updated_ms",
    "mean_bps","std_bps","required_spread_bps","z_req_profit",
    "Z_IN_suggested","entry_spread_bps_suggested"
]

def read_spread_stats() -> pd.DataFrame:
    return read_csv(SPREAD_STATS_PATH, STATS_COLS)
ALPHA = float(getenv_float("SPREAD_EMA_ALPHA", 0.05))
SAVE_EVERY_SEC = int(getenv_float("SAVE_EVERY_SEC", 30))

# ----------------- Z-score: reading spread stats -----------------
SPREAD_STATS_PATH = bucketize_path(getenv_str("SPREAD_STATS_PATH", "spread_stats.csv"))
STATS_COLS = [
    "symbol","ex_low","ex_high",
    "ema_mean","ema_var","count","updated_ms",
    "mean_bps","std_bps","required_spread_bps","z_req_profit",
    "Z_IN_suggested","entry_spread_bps_suggested"
]

def read_spread_stats() -> pd.DataFrame:
    return read_csv(SPREAD_STATS_PATH, STATS_COLS)

ALPHA = float(getenv_float("SPREAD_EMA_ALPHA", 0.05))
SAVE_EVERY_SEC = int(getenv_float("SAVE_EVERY_SEC", 30))
WINSOR_K = float(getenv_float("SPREAD_WINSOR_K", 5.0))  # насколько σ разрешаем отклоняться

class StatsStore:
    """
    Локальный стор статистики спредов: обновляет EMA и ПАРАЛЛЕЛЬНО
    рассчитывает рекомендации (Z_IN_suggested, entry_spread_bps_suggested) прямо в CSV.
    """
    def __init__(self, path: Optional[str], alpha: float):
        self.path = path
        self.alpha = alpha
        self.df = read_csv(self.path, STATS_COLS)
        self._last_save = time.time()

    def _mask(self, symbol: str, ex_low: str, ex_high: str):
        return (
            (self.df["symbol"] == symbol.upper()) &
            (self.df["ex_low"] == ex_low.lower()) &
            (self.df["ex_high"] == ex_high.lower())
        )

    def _ema_update(self, mean: Optional[float], var: Optional[float], x: float) -> Tuple[float,float]:
        # инициализация
        if mean is None or var is None or (mean != mean) or (var != var):
            return x, 1e-6  # стартуем с маленькой дисперсии

        std = math.sqrt(max(var, 1e-12))
        # вензоризация x вокруг mean на WINSOR_K*std
        lo = mean - WINSOR_K * std
        hi = mean + WINSOR_K * std
        xw = min(max(x, lo), hi)

        alpha = float(self.alpha)
        # стандартное EMA-обновление и дисперсия (экспоненциальная)
        new_mean = (1 - alpha) * mean + alpha * xw
        # вариант с экспоненциальной дисперсией Дж. Хантера
        new_var  = (1 - alpha) * (var + alpha * (xw - mean) * (xw - mean))
        new_var  = max(new_var, 1e-12)
        return new_mean, new_var

    def _recompute_reco(self, i: int):
        """
        Пересчитать рекомендации (z и входной спред) по строке i на основе текущих EMA.
        """
        z_default = float(getenv_float("Z_IN", 3.5))
        roundtrip_cost = float(getenv_float("ROUNDTRIP_FEES_BPS", 15.0))  # комиссии RT, bps
        safety = float(getenv_float("SAFETY_MARGIN_BPS", 10.0))           # подушка, bps

        mean = to_float(self.df.at[i,"ema_mean"]) or 0.0
        var  = to_float(self.df.at[i,"ema_var"])  or 0.0
        std  = math.sqrt(max(var, 0.0))

        mean_bps = mean * 10000.0
        std_bps  = std  * 10000.0
        required = roundtrip_cost + safety

        # z, при котором mean + z*std >= required
        if std_bps > 0:
            z_req = max(0.0, (required - mean_bps) / std_bps)
        else:
            z_req = float('nan')

        # финальная рекомендация: не ниже дефолта, округляем вверх к 0.1
        if z_req == z_req:  # not NaN
            z_sugg = max(z_default, math.ceil(z_req*10.0)/10.0)
        else:
            z_sugg = z_default

        entry_bps = mean_bps + z_sugg * std_bps

        # записываем в строки CSV
        self.df.at[i,"mean_bps"]  = mean_bps
        self.df.at[i,"std_bps"]   = std_bps
        self.df.at[i,"required_spread_bps"] = required
        self.df.at[i,"z_req_profit"] = z_req
        self.df.at[i,"Z_IN_suggested"] = z_sugg
        self.df.at[i,"entry_spread_bps_suggested"] = entry_bps

    def update_pair(self, symbol: str, ex_low: str, ex_high: str, x: float):
        m = self._mask(symbol, ex_low, ex_high)
        now_ms = int(time.time()*1000)

        if m.any():
            i = int(self.df[m].index[0])
            mean = to_float(self.df.at[i,"ema_mean"])
            var  = to_float(self.df.at[i,"ema_var"])
            new_mean, new_var = self._ema_update(mean, var, x)
            self.df.at[i,"ema_mean"] = new_mean
            self.df.at[i,"ema_var"]  = new_var
            self.df.at[i,"count"]    = int(to_float(self.df.at[i,"count"]) or 0) + 1
            self.df.at[i,"updated_ms"] = now_ms
            self._recompute_reco(i)
        else:
            # новая строка, сразу посчитаем рекомендации
            row = {
                "symbol":symbol.upper(),"ex_low":ex_low.lower(),"ex_high":ex_high.lower(),
                "ema_mean":x,"ema_var":1e-6,"count":1,"updated_ms":now_ms,
                "mean_bps":None,"std_bps":None,"required_spread_bps":None,"z_req_profit":None,
                "Z_IN_suggested":None,"entry_spread_bps_suggested":None
            }
            # --- вместо concat ---
            next_idx = len(self.df)
            self.df.loc[next_idx, STATS_COLS] = [row.get(c) for c in STATS_COLS]
            i = next_idx
            self._recompute_reco(i)

    def maybe_save(self, force: bool=False):
        if not self.path:
            return
        now = time.time()
        if force or (now - self._last_save) >= SAVE_EVERY_SEC:
            write_csv(self.path, self.df[STATS_COLS])
            self._last_save = now
            logging.info("Spread stats saved: rows=%d", len(self.df))

# --- ENV для фильтров z ---
SPREAD_MIN_COUNT = int(getenv_float("SPREAD_MIN_COUNT", 200))
SPREAD_STALE_SEC = int(getenv_float("SPREAD_STALE_SEC", 3*24*3600))  # по умолчанию 3 дня
SPREAD_STD_FLOOR = float(getenv_float("SPREAD_STD_FLOOR", 1e-6))     # защита от нулевой дисперсии



# ----------------- Atomic open with rollback -----------------
def new_attempt_id() -> str: return uuid.uuid4().hex[:12]

def atomic_cross_open(symbol: str, cheap_ex: str, rich_ex: str,
                      qty: float, price_low: float, price_high: float,
                      paper: bool) -> Tuple[bool,str,dict]:
    """
    Открываем кросс:
      cheap_ex -> BUY (LONG)
      rich_ex  -> SELL (SHORT)
    Без reduce_only. Возвращаем метаданные и клиентские id.
    """
    attempt_id = new_attempt_id()

    # проверяем исполнимость по шагам/минимумам
    if cheap_ex.lower() == "bybit":
        okA, msgA, qA = bybit_feasible(symbol, qty, price_low)
    else:
        okA, msgA, qA = binance_feasible(symbol, qty, price_low)

    if rich_ex.lower() == "bybit":
        okB, msgB, qB = bybit_feasible(symbol, qty, price_high)
    else:
        okB, msgB, qB = binance_feasible(symbol, qty, price_high)

    if not okA or not okB:
        return False, attempt_id, {"error": f"not feasible: {cheap_ex}({msgA}), {rich_ex}({msgB})"}

    qty_final = min(qA, qB)
    if qty_final <= 0:
        return False, attempt_id, {"error": "qty_final<=0 after rounding"}

    # client IDs для ног открытия
    cl_open_long  = _gen_cloid("OPENA", attempt_id, "A")
    cl_open_short = _gen_cloid("OPENB", attempt_id, "B")

    try:
        # LONG (BUY) на дешёвой бирже
        oa = _place_perp_market_order(
            cheap_ex, symbol, "BUY", qty_final,
            paper=paper, cl_oid=cl_open_long, reduce_only=False
        )
        if oa.get("status") != "FILLED":
            raise RuntimeError(f"legA not filled: {oa}")
    except Exception as e:
        return False, attempt_id, {"error": f"legA error: {e}"}

    try:
        # SHORT (SELL) на дорогой бирже
        ob = _place_perp_market_order(
            rich_ex, symbol, "SELL", qty_final,
            paper=paper, cl_oid=cl_open_short, reduce_only=False
        )
        if ob.get("status") != "FILLED":
            # откат первой ноги
            try:
                _ = _place_perp_market_order(cheap_ex, symbol, "SELL", qty_final,
                                            paper=paper, reduce_only=True)
            except Exception as e2:
                logging.error("Rollback failed: %s", e2)
            return False, attempt_id, {"error": f"legB not filled: {ob}"}
        if ob.get("status") != "FILLED":
            raise RuntimeError(f"legB not filled: {ob}")
    except Exception as e:
        # откат первой ноги (продаём то, что купили)
        try:
            _ = _place_perp_market_order(cheap_ex, symbol, "SELL", qty_final, paper=paper, reduce_only=True)
        except Exception as e2:
            logging.error("Rollback failed: %s", e2)
        return False, attempt_id, {"error": f"legB error: {e}"}

        # --- Контроль качества fill'а: проверяем, что реальный спред на входе достаточный ---
    long_px  = float(oa.get("avg_price") or 0.0)
    short_px = float(ob.get("avg_price") or 0.0)
    ENTRY_BPS_MIN = float(getenv_float("ENTRY_BPS_MIN", 0.0))

    # Если биржи не вернули avg_price (testnet/демо и т.п.),
    # не считаем это плохим fill'ом, а просто логируем и пропускаем проверку.
    if long_px <= 0 or short_px <= 0:
        logging.warning(
            "Skip ENTRY_BPS_MIN check for %s: long_px=%.6f short_px=%.6f (no avg_price in fills)",
            symbol, long_px, short_px
        )
        entry_bps_filled = None
    else:
        entry_bps_filled = (short_px - long_px) / long_px * 1e4
        if ENTRY_BPS_MIN > 0.0 and entry_bps_filled < ENTRY_BPS_MIN:
            logging.warning(
                "Bad fill for %s: entry_bps_filled=%.2f < %.2f bps, panic closing",
                symbol, entry_bps_filled, ENTRY_BPS_MIN
            )
            # Закрываем обе ноги reduce_only маркетами
            try:
                _ = _place_perp_market_order(
                    cheap_ex, symbol, "SELL", qty_final,
                    paper=paper, reduce_only=True
                )
            except Exception as e2:
                logging.error("Bad-fill close legA failed: %s", e2)
            try:
                _ = _place_perp_market_order(
                    rich_ex, symbol, "BUY", qty_final,
                    paper=paper, reduce_only=True
                )
            except Exception as e3:
                logging.error("Bad-fill close legB failed: %s", e3)

            return False, attempt_id, {
                "error": f"bad_fill: entry_bps_filled={entry_bps_filled:.2f} < {ENTRY_BPS_MIN:.2f}"
            }

    # --- Если всё ок, сохраняем метаданные обычным образом ---
    meta = {
        "qty": qty_final,
        "open_long_order_id":  oa.get("order_id"),
        "open_short_order_id": ob.get("order_id"),
        "open_long_px":  long_px,
        "open_short_px": short_px,
        "open_fees_usd": float(oa.get("fee_usd") or 0.0) + float(ob.get("fee_usd") or 0.0),
        "open_long_cloid":  oa.get("client_order_id"),
        "open_short_cloid": ob.get("client_order_id"),
        "entry_bps_filled": float(entry_bps_filled) if entry_bps_filled is not None else None,
    }

    return True, attempt_id, meta

########################
# Trades lookup helpers #
########################
def binance_user_trades(symbol: str, start_ms: int|None=None) -> list[dict]:
    key = getenv_str("BINANCE_API_KEY",""); sec = getenv_str("BINANCE_API_SECRET","")
    base = _private_base("binance")
    params = {"symbol": symbol.upper(), "limit": 1000}
    if start_ms: params["startTime"] = int(start_ms)
    # подпись
    now = int(time.time()*1000)
    params["timestamp"] = now + (_BINANCE_TIME_OFFSET_MS or 0)
    qs = "&".join([f"{k}={_fmt_val(v)}" for k,v in sorted(params.items())])
    sig = hmac.new(sec.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f"{base}/fapi/v1/userTrades?{qs}&signature={sig}"
    r = SESSION.get(url, headers={"X-MBX-APIKEY": key}, timeout=REQUEST_TIMEOUT)
    if r.status_code!=200: 
        logging.warning("binance_user_trades fail %s: %s", r.status_code, r.text[:200])
        return []
    return r.json() if isinstance(r.json(), list) else []

def bybit_exec_list(symbol: str, order_id: str|None=None, order_link_id: str|None=None, start_ms: int|None=None) -> list[dict]:
    """
    Correct v5 signing for GET /v5/execution/list:
    - recvWindow: only header + prehash prefix (NOT in query)
    - simulated trading: only header X-BAPI-SIMULATED-TRADING (NOT in query)
    - query is alphabetically sorted and signed exactly as sent
    """
    base = _private_base("bybit")
    key = getenv_str("BYBIT_API_KEY",""); sec = getenv_str("BYBIT_API_SECRET","")
    endpoint = "/v5/execution/list"

    # 1) Собираем ТОЛЬКО бизнес-параметры запроса (без recvWindow/simulateTrading)
    q: dict[str, str] = {
        "category": "linear",
        "symbol":   symbol.upper(),
    }
    # Если есть orderId — используем только его (без orderLinkId)
    if order_id:
        q["orderId"] = str(order_id)
    elif order_link_id:
        q["orderLinkId"] = str(order_link_id)
    if start_ms:
        q["startTime"] = str(int(start_ms))

    # 2) Формируем строку query ДЛЯ ПОДПИСИ (по алфавиту ключей)
    q_items = sorted(q.items(), key=lambda kv: kv[0])
    qstr = "&".join([f"{k}={v}" for k, v in q_items])

    # 3) Подпись по v5: prehash = ts + apiKey + recvWindow + queryString
    recv = "5000"
    ts = str(int(time.time() * 1000))
    prehash = ts + key + recv + qstr
    sign = hmac.new(sec.encode(), prehash.encode(), hashlib.sha256).hexdigest()

    # 4) Заголовки
    headers = {
        "X-BAPI-API-KEY":     key,
        "X-BAPI-TIMESTAMP":   ts,
        "X-BAPI-RECV-WINDOW": recv,
        "X-BAPI-SIGN":        sign,
        "X-BAPI-SIGN-TYPE":   "2",
    }
    # DEMO-режим — ТОЛЬКО через заголовок
    if _is_true("BYBIT_DEMO", False):
        headers["X-BAPI-SIMULATED-TRADING"] = "1"

    # 5) Выполняем запрос с ТОЧНО тем же q, который подписывали
    url = f"{base}{endpoint}?{qstr}"
    r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    try:
        j = r.json()
    except Exception:
        logging.warning("bybit exec list fail: %s %s", r.status_code, r.text[:200])
        return []

    if r.status_code != 200 or j.get("retCode") != 0:
        logging.warning("bybit exec list fail: %s %s", r.status_code, j)
        return []

    return (j.get("result") or {}).get("list") or []

# =======================
# Balances (post-trade)
# =======================

def binance_usdt_futures_balance() -> dict:  # NEW
    """
    Возвращает кошелёк и доступный баланс USDT на Binance Futures.
    """
    key = getenv_str("BINANCE_API_KEY","")
    sec = getenv_str("BINANCE_API_SECRET","")
    if not key or not sec:
        return {}
    base = _private_base("binance")
    ts = now_ms()
    qs = f"timestamp={ts}&recvWindow=5000"
    sig = _hmac_sha256_hex(sec, qs)
    headers = {"X-MBX-APIKEY": key}
    url = f"{base}/fapi/v2/balance?{qs}&signature={sig}"
    try:
        r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logging.debug("binance_usdt_futures_balance http=%s %s", r.status_code, r.text[:200])
            return {}
        arr = r.json() if isinstance(r.json(), list) else []
        row = next((x for x in arr if str(x.get("asset")) == "USDT"), {})
        wallet = float(row.get("balance") or row.get("walletBalance") or 0.0)
        avail  = float(row.get("availableBalance") or 0.0)
        upnl   = float(row.get("crossUnPnl") or 0.0)
        equity = wallet + upnl
        return {"asset":"USDT","wallet":wallet,"available":avail,"uPnL":upnl,"equity":equity}
    except Exception as e:
        logging.debug("binance_usdt_futures_balance err: %s", e)
        return {}

def bybit_unified_usdt_balance() -> dict:
    """
    Возвращает equity/кошелёк/доступный по USDT на Bybit Unified.
    Учитывает BYBIT_DEMO=1 (simulateTrading).
    """
    key = getenv_str("BYBIT_API_KEY", "")
    sec = getenv_str("BYBIT_API_SECRET", "")
    if not key or not sec:
        logging.warning("BYBIT_API_KEY или BYBIT_API_SECRET не заданы, возвращаю пустой dict")
        return {}

    base = _private_base("bybit")
    endpoint = "/v5/account/wallet-balance"
    params = "accountType=UNIFIED"
    if _is_true("BYBIT_DEMO", False):
        params += "&simulateTrading=true"

    recv = "5000"
    ts = str(now_ms())

    # ВАЖНО: формат подписи как в рабочем тесте
    prehash = ts + key + recv + params  # GET: ts+apiKey+recvWindow+query
    sign = _hmac_sha256_hex(sec, prehash)

    headers = {
        "X-BAPI-API-KEY": key,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv,
        "X-BAPI-SIGN": sign,
        "X-BAPI-SIGN-TYPE": "2",
    }
    if _is_true("BYBIT_DEMO", False):
        headers["X-BAPI-SIMULATED-TRADING"] = "1"

    url = f"{base}{endpoint}?{params}"
    try:
        r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        j = r.json()
        if r.status_code != 200 or j.get("retCode") != 0:
            logging.warning(
                "bybit_unified_usdt_balance ret=%s http=%s %s",
                j.get("retCode"), r.status_code, str(j)[:200]
            )
            return {}

        lst = ((j.get("result") or {}).get("list") or [])
        coins = (lst[0] or {}).get("coin", []) if lst else []
        row = next((c for c in coins if str(c.get("coin")) == "USDT"), {})

        equity   = float(row.get("equity") or 0.0)
        wallet   = float(row.get("walletBalance") or 0.0)
        avail    = float(
            row.get("availableToWithdraw")
            or row.get("availableBalance")
            or 0.0
        )
        unrealPnL = float(row.get("unrealisedPnl") or 0.0)

        return {
            "asset": "USDT",
            "equity": equity,
            "wallet": wallet,
            "available": avail,
            "uPnL": unrealPnL,
        }

    except Exception as e:
        logging.debug("bybit_unified_usdt_balance err: %s", e)
        return {}
    
def okx_usdt_balance() -> dict:  # NEW
    """
    Баланс USDT на OKX (унифицированный аккаунт).
    Учитывает OKX_TESTNET / OKX_PAPER через x-simulated-trading.
    """
    key = getenv_str("OKX_API_KEY","")
    sec = getenv_str("OKX_API_SECRET","")
    passphrase = getenv_str("OKX_PASSPHRASE","")
    if not key or not sec or not passphrase:
        return {}
    base = okx_base()
    endpoint = "/api/v5/account/balance"
    ts = time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime())
    method = "GET"
    body = ""
    prehash = f"{ts}{method}{endpoint}{body}"
    sign = base64.b64encode(hmac.new(sec.encode(), prehash.encode(), hashlib.sha256).digest()).decode()
    headers = {
        "OK-ACCESS-KEY": key,
        "OK-ACCESS-SIGN": sign,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "x-simulated-trading": "1" if _is_true("OKX_TESTNET", False) or _is_true("OKX_PAPER", False) else "0",
    }
    url = f"{base}{endpoint}"
    try:
        r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logging.debug("okx_usdt_balance http=%s %s", r.status_code, r.text[:200])
            return {}
        j = r.json()
        if str(j.get("code")) != "0":
            logging.debug("okx_usdt_balance code=%s %s", j.get("code"), str(j)[:200])
            return {}
        data = j.get("data") or []
        acc = data[0] if data else {}
        details = acc.get("details") or []
        row = next((d for d in details if str(d.get("ccy")) == "USDT"), {})
        # eq — equity по конкретной валюте, totalEq — общее equity по аккаунту
        equity = float(row.get("eq") or acc.get("totalEq") or 0.0)
        wallet = float(row.get("cashBal") or 0.0)
        avail  = float(row.get("availEq") or row.get("availBal") or wallet)
        return {"asset":"USDT","equity":equity,"wallet":wallet,"available":avail}
    except Exception as e:
        logging.debug("okx_usdt_balance err: %s", e)
        return {}

def gate_usdt_futures_balance() -> dict:  # NEW
    """
    Баланс USDT на Gate Futures USDT-счёте.
    Использует тот же sign, что и _check_gate_auth().
    """
    key = getenv_str("GATE_API_KEY","")
    sec = getenv_str("GATE_API_SECRET","")
    if not key or not sec:
        return {}
    method = "GET"
    path = "/api/v4/futures/usdt/accounts"
    query = ""
    body = ""
    ts = str(int(time.time()))
    body_hash = hashlib.sha512(body.encode()).hexdigest()
    msg = "\n".join([method, path, query, body_hash, ts])
    sign = _hmac_sha512_hex(sec, msg)
    headers = {
        "KEY": key,
        "Timestamp": ts,
        "SIGN": sign,
    }
    url = f"{gate_base()}{path}"
    try:
        r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logging.debug("gate_usdt_futures_balance http=%s %s", r.status_code, r.text[:200])
            return {}
        j = r.json()
        # Ответ может быть dict или list — подстрахуемся
        row = {}
        if isinstance(j, dict):
            row = j
        elif isinstance(j, list) and j:
            row = next((x for x in j if str(x.get("currency")) == "USDT"), j[0])
        if not row:
            return {}
        wallet = float(row.get("total") or row.get("equity") or row.get("balance") or 0.0)
        upnl   = float(row.get("unrealized_pnl") or row.get("unrealised_pnl") or 0.0)
        equity = float(row.get("equity") or (wallet + upnl))
        avail  = float(row.get("available") or row.get("available_margin") or (equity - float(row.get("position_margin") or 0.0) - float(row.get("order_margin") or 0.0)))
        return {"asset":"USDT","equity":equity,"wallet":wallet,"available":avail,"uPnL":upnl}
    except Exception as e:
        logging.debug("gate_usdt_futures_balance err: %s", e)
        return {}

def get_post_close_balances(exchanges: list[str]) -> dict[str, dict]:  # NEW
    """
    Возвращает словарь балансов по указанным биржам (ключ — EXCH in CAPS).
    Сейчас поддержаны: binance, bybit, okx, gate.
    """
    out: dict[str, dict] = {}
    for ex in exchanges:
        el = ex.lower().strip()
        if el == "binance":
            out["BINANCE"] = binance_usdt_futures_balance()
        elif el == "bybit":
            out["BYBIT"] = bybit_unified_usdt_balance()
        elif el == "okx":
            out["OKX"] = okx_usdt_balance()
        elif el == "gate":
            out["GATE"] = gate_usdt_futures_balance()
    return out

def summarize_fills(exchange: str, symbol: str, open_side: str, open_oid: str|None, open_cloid: str|None, close_side: str, close_oid: str|None, close_cloid: str|None, start_ms: int) -> dict:
    """
    Возвращает:
      avg_open_px, avg_close_px, fees_open_usd, fees_close_usd
    Берём трейды от момента открытия позиции (start_ms) и фильтруем по orderId/ClientId, где возможно.
    """
    ex = exchange.lower()
    avg_open = avg_close = 0.0
    fees_open = fees_close = 0.0
    if ex == "binance":
        trades = binance_user_trades(symbol, start_ms)
        # Binance futures отдает поля: orderId, buyer, maker, realizedPnl, commission, qty, price, side=BUY/SELL, clientOrderId (может быть пустой)
        open_tr = [t for t in trades if (str(t.get("orderId"))==str(open_oid) or (open_cloid and t.get("clientOrderId")==open_cloid))]
        close_tr= [t for t in trades if (str(t.get("orderId"))==str(close_oid) or (close_cloid and t.get("clientOrderId")==close_cloid))]
        def _avg(trs): 
            pxs=[float(t["price"]) for t in trs if "price" in t]; qs=[float(t["qty"]) for t in trs if "qty" in t]
            return _wavg(pxs, qs)
        avg_open  = _avg(open_tr)  or 0.0
        avg_close = _avg(close_tr) or 0.0
        fees_open = _sum([float(t.get("commission",0)) for t in open_tr])
        fees_close= _sum([float(t.get("commission",0)) for t in close_tr])
    elif ex == "bybit":
        # Bybit executions: execPrice, execQty, execFee, side, orderId, orderLinkId
        open_tr = bybit_exec_list(symbol, order_id=open_oid, order_link_id=open_cloid, start_ms=start_ms)
        close_tr= bybit_exec_list(symbol, order_id=close_oid, order_link_id=close_cloid, start_ms=start_ms)
        def _avg_bybit(trs):
            pxs=[float(t.get("execPrice",0)) for t in trs]; qs=[float(t.get("execQty",0)) for t in trs]
            return _wavg(pxs, qs)
        avg_open  = _avg_bybit(open_tr)  or 0.0
        avg_close = _avg_bybit(close_tr) or 0.0
        fees_open = _sum([float(t.get("execFee",0)) for t in open_tr])
        fees_close= _sum([float(t.get("execFee",0)) for t in close_tr])
    else:
        # Для OKX/Gate/MEXC можно расширить аналогично при необходимости
        pass
    return {"avg_open_px":avg_open, "avg_close_px":avg_close, "fees_open_usd":fees_open, "fees_close_usd":fees_close}

# ----------------- Positions -----------------
POS_COLS = ["id","attempt_id","symbol","long_ex","short_ex","opened_ms","last_ms","held_h",
            "size_usd","entry_spread_bps","entry_px_low","entry_px_high",
            "status","opened_at","closed_at","close_reason","validated_qty","note_net_usd",
            "open_long_order_id","open_short_order_id","open_long_px","open_short_px","open_fees_usd",
            "open_long_cloid","open_short_cloid",
            "close_long_order_id","close_short_order_id","close_long_px","close_short_px","close_fees_usd",
            "close_long_cloid","close_short_cloid",
            "realized_pnl_usd"]

def load_positions(path: str) -> pd.DataFrame:
    df = read_csv(path, POS_COLS)
    if df.empty:
        return df
    # Явно приводим «текстовые» поля к object/str, чтобы безопасно писать строки
    str_cols = [
        "opened_at","closed_at","close_reason",
        "open_long_order_id","open_short_order_id","close_long_order_id","close_short_order_id",
        "open_long_cloid","open_short_cloid","close_long_cloid","close_short_cloid",
        "status","symbol","long_ex","short_ex","attempt_id"
    ]
    for c in str_cols:
        if c in df.columns:
            df[c] = df[c].astype("string").fillna("")
    # Числовые поля — в float
    num_cols = [
        "id","opened_ms","last_ms","held_h","size_usd","entry_spread_bps",
        "entry_px_low","entry_px_high","validated_qty",
        "open_long_px","open_short_px","open_fees_usd",
        "close_long_px","close_short_px","close_fees_usd","realized_pnl_usd"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def save_positions(path: str, df: pd.DataFrame):
    write_csv(path, df)

def _estimate_net_now(px_low: float, px_high: float, qty: float,
                      per_leg_notional_usd: float, taker_fee: float) -> float:
    """Оценка PnL «сейчас» с учётом комиссий и базового слыппейджа."""
    spread = max(0.0, px_high - px_low)
    gross  = spread * max(0.0, qty)
    fees   = 4.0 * taker_fee * per_leg_notional_usd
    slip_bps = float(getenv_float("DRYRUN_SLIPPAGE_BPS", getenv_float("SLIPPAGE_BPS", 15.0)))
    slip   = (slip_bps / 1e4) * 2.0 * per_leg_notional_usd  # простая оценка
    return gross - fees - slip

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

    # --- Жёсткая фильтрация по качеству статистики (до расчёта Z) ---
    stats = stats_df if stats_df is not None else pd.DataFrame(columns=STATS_COLS)

    # Отсекаем редкие/шумные пары: требуем минимум наблюдений и разумную дисперсию лог-спреда
    MIN_SPREAD_COUNT = int(getenv_float("MIN_SPREAD_COUNT", 100))   # можно вынести в .env
    MAX_SPREAD_VAR   = float(getenv_float("MAX_SPREAD_VAR", 1.5e-5))

    if cands is not None and not cands.empty and stats is not None and not stats.empty:
        qual = (
            stats[["symbol", "ex_low", "ex_high", "ema_var", "count"]]
            .drop_duplicates()
            .rename(columns={"ex_low": "long_ex", "ex_high": "short_ex"})
        )
        cands = cands.merge(qual, on=["symbol", "long_ex", "short_ex"], how="left")

        # Применяем «маску качества»
        cands = cands[
            (cands["count"].fillna(0) >= MIN_SPREAD_COUNT) &
            (cands["ema_var"].fillna(9e9) <= MAX_SPREAD_VAR)
        ].copy()

    # --- читаем статистику спредов и считаем z для кандидатов ---
        Z_IN  = float(getenv_float("Z_IN", 2.5))
    Z_OUT = float(getenv_float("Z_OUT", 0.8))
    SLIPPAGE_BPS = float(getenv_float("SLIPPAGE_BPS", 1.0))  # консервативный запас
    ENTRY_MODE = getenv_str("ENTRY_MODE", "zscore").lower()
    if ENTRY_MODE not in ("zscore","price"):
        ENTRY_MODE = "zscore"
    STD_MIN_FOR_OPEN = float(getenv_float("STD_MIN_FOR_OPEN", 1e-4))

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
        # фильтр по входу — режимно
        if ENTRY_MODE == "zscore":
            cands = cands[(cands["net_usd_adj"]>0.0) & (cands["z"]>=Z_IN) & (cands["std"].fillna(0.0) >= STD_MIN_FOR_OPEN)].copy()
            cands = cands.sort_values(
                ["net_usd_adj", "spread_bps", "z"],
                ascending=[False, False, False]
            ).reset_index(drop=True)
        else:  # price
            cands = cands[(cands["net_usd_adj"]>0.0) & (cands["spread_bps"]>=entry_bps)].copy()
            cands = cands.sort_values(
                ["net_usd_adj", "spread_bps"],
                ascending=[False, False]
            ).reset_index(drop=True)

    best = None
    if cands is not None and not cands.empty:
        # отправим топ-N для наглядности
        topN = cands.head(top3_to_tg).copy()
        for _, r in topN.iterrows():
            maybe_send_telegram(format_signal_card(r, per_leg_notional_usd, price_source))
        # выбираем наиболее прибыльный сигнал (а не самый большой Z)
        best = cands.iloc[0]
    else:
        logging.info(f"No {ENTRY_MODE}-filtered candidates this cycle.")

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

        EXIT_HYST_BPS = float(getenv_float("EXIT_HYST_BPS", 3.0))
        exit_thr = max(0.0, exit_bps - EXIT_HYST_BPS)

        # --- Режим выхода: 'zscore' (по умолчанию) или 'price' ---
        EXIT_MODE = getenv_str("EXIT_MODE", "zscore").lower()
        if EXIT_MODE not in ("zscore","price"):
            EXIT_MODE = "zscore"

        EXIT_REQUIRE_POSITIVE = os.getenv("EXIT_REQUIRE_POSITIVE", "0").lower() in ("1","true","yes")

        if EXIT_MODE == "zscore":
            if (z_now is not None and z_now == z_now and z_now <= Z_OUT):
                if EXIT_REQUIRE_POSITIVE:
                    stored_qty = float(df_pos.at[i,"validated_qty"] or 0.0)
                    net_now = _estimate_net_now(px_low or 0.0, px_high or 0.0,
                                                stored_qty, per_leg_notional_usd, taker_fee)
                    if net_now >= 0:
                        do_close = True; reason = f"Z-out: z={z_now:.2f} ≤ {Z_OUT:.2f} (>=breakeven)"
                    else:
                        # ждём price-exit/стоп/время — Z-out проигнорирован из-за отриц. экономики
                        pass
                else:
                    do_close = True; reason = f"Z-out: z={z_now:.2f} ≤ {Z_OUT:.2f}"
            elif (spread_bps_now is not None) and (spread_bps_now <= exit_thr):
                do_close = True; reason = f"Spread converged: {spread_bps_now:.2f} bps ≤ {exit_thr:.2f} bps"
        else:  # price
            if (spread_bps_now is not None) and (spread_bps_now <= exit_thr):
                do_close = True; reason = f"Spread converged: {spread_bps_now:.2f} bps ≤ {exit_thr:.2f} bps (price-arb mode)"

                # --- страховки (необязательно, но очень рекомендуется) ---
        try:
            max_hold = int(getenv_float("MAX_HOLD_SEC", 0))
            if (not do_close) and max_hold > 0:
                opened_ms = int(df_pos.at[i,"opened_ms"] or 0)
                if opened_ms and (utc_ms_now() - opened_ms) >= max_hold*1000:
                    do_close = True; reason = f"Time stop: {max_hold}s"
            stop_bps = float(getenv_float("STOP_LOSS_BPS", 0.0))
            if (not do_close) and stop_bps < 0 and (spread_bps_now is not None):
                if spread_bps_now <= stop_bps:
                    do_close = True; reason = f"Hard stop: spread {spread_bps_now:.2f} bps ≤ {stop_bps:.2f} bps"
        except Exception:
            pass

        if rotate_on and (not do_close) and best is not None:
            try:
                # 1) Базовые числа (текущее ожидание по открытой позиции и новое по лучшему сигналу)
                current_expected = float(df_pos.at[i, "note_net_usd"] or 0.0)
                new_expected = float(best.get("net_usd") or 0.0)
                delta_abs = new_expected - current_expected

                # 2) Порог в USD, масштабируемый с капиталом/ногой/текущим net
                need_abs = _rotate_need_abs(current_expected, per_leg_notional_usd)

                # 3) Доп. фильтры против «дребезга» (включите/настройте в .env при желании)
                #    - требование улучшения Z на ROTATE_REQUIRE_Z_ADV
                #    - требование увеличения текущего спреда на ROTATE_HYSTERESIS_BPS
                z_adv_req = float(getenv_float("ROTATE_REQUIRE_Z_ADV", 0.0))
                hyst_bps  = float(getenv_float("ROTATE_HYSTERESIS_BPS", 0.0))

                z_best = float(best.get("z") or 0.0)
                ok_z_adv = True
                if z_adv_req > 0 and (z_now is not None and z_now == z_now):
                    ok_z_adv = (z_best - float(z_now)) >= z_adv_req

                spread_best_bps = float(best.get("spread_bps") or 0.0)
                ok_hyst = True
                if hyst_bps > 0 and (spread_bps_now is not None):
                    ok_hyst = (spread_best_bps - float(spread_bps_now)) >= hyst_bps

                # 4) Минимальное время удержания позиции (чтобы не «ёрзать»)
                min_hold = int(getenv_float("ROTATE_MIN_HOLD_SEC", 0))
                age_sec = max(0, int((now_ms - int(df_pos.at[i, "opened_ms"] or now_ms)) / 1000))
                ok_age = (age_sec >= min_hold)

                # 5) Общая проверка ротации
                if (delta_abs >= need_abs) and ok_z_adv and ok_hyst and ok_age:
                    do_close = True
                    reason = (f"Rotate: +${delta_abs:.2f} (need ≥ ${need_abs:.2f})"
                            + (f", ΔZ≥{z_adv_req:.2f}" if z_adv_req > 0 else "")
                            + (f", Δbps≥{hyst_bps:.2f}" if hyst_bps > 0 else "")
                            + (f", age≥{min_hold}s" if min_hold > 0 else ""))
            except Exception as _e:
                logging.debug("rotate eval skipped: %s", _e)

        if do_close:
            px_any = px_low or px_high or 1.0
            stored_qty = df_pos.at[i,"validated_qty"]
            qty = float(stored_qty) if (stored_qty is not None and not pd.isna(stored_qty) and float(stored_qty)>0) else per_leg_notional_usd/px_any
            # проставим client ids для закрытия
            cl_close_long  = _gen_cloid("CLOSEA", str(df_pos.at[i,"attempt_id"]), "A")
            cl_close_short = _gen_cloid("CLOSEB", str(df_pos.at[i,"attempt_id"]), "B")
            close_long_oid = close_short_oid = None
            close_long_px = close_short_px = 0.0
            fees_close_total = 0.0
            if not paper:
                try:
                    oa = _place_perp_market_order(long_ex,  sym, "SELL", qty, paper=False, cl_oid=cl_close_long)
                    ob = _place_perp_market_order(short_ex, sym, "BUY",  qty, paper=False, cl_oid=cl_close_short)
                    close_long_oid  = oa.get("order_id");  close_short_oid = ob.get("order_id")
                    close_long_px   = float(oa.get("avg_price") or 0.0); close_short_px = float(ob.get("avg_price") or 0.0)
                    fees_close_total= float(oa.get("fee_usd") or 0.0) + float(ob.get("fee_usd") or 0.0)
                except Exception as e:
                    logging.warning("Close error %s: %s", sym, e)
            # Попытка добрать фактические цены/комиссии по открытию и закрытию (если поддержано)
            start_ms_for_trades = int(df_pos.at[i,"opened_ms"] or (now_ms - 6*3600*1000))
            open_long_oid  = str(df_pos.at[i,"open_long_order_id"]  or "")
            open_short_oid = str(df_pos.at[i,"open_short_order_id"] or "")
            open_long_cl   = str(df_pos.at[i,"open_long_cloid"] or "")
            open_short_cl  = str(df_pos.at[i,"open_short_cloid"] or "")

            # Сводки по ногам
            sum_long  = summarize_fills(long_ex,  sym, "BUY",  open_long_oid,  open_long_cl,  "SELL", close_long_oid,  cl_close_long,  start_ms_for_trades)
            sum_short = summarize_fills(short_ex, sym, "SELL", open_short_oid, open_short_cl, "BUY",  close_short_oid, cl_close_short, start_ms_for_trades)
            
            #  Окончательные цены/комиссии: приоритет — из сводок, иначе — из мгновенных ответов
            open_long_px   = float(df_pos.at[i,"open_long_px"]  or 0.0) or sum_long["avg_open_px"]
            open_short_px  = float(df_pos.at[i,"open_short_px"] or 0.0) or sum_short["avg_open_px"]

            # если Bybit demo не вернуло execPrice — подставляем цены входа из меты
            if open_long_px == 0.0:
                open_long_px = float(df_pos.at[i, "entry_px_low"]  or 0.0)
            if open_short_px == 0.0:
                open_short_px = float(df_pos.at[i, "entry_px_high"] or 0.0)

            close_long_px  = close_long_px  or sum_long["avg_close_px"]
            close_short_px = close_short_px or sum_short["avg_close_px"]

            # если закрытие тоже нулевое — берём актуальные котировки из цикла (px_low/px_high)
            # (для long-SELL — реалистично близко к bid; у нас это выбранный PRICE_SOURCE,
            # поэтому безопасно подставить px_low/px_high соответствующих бирж)
            if (not close_long_px) and (px_low is not None):
                close_long_px = float(px_low)
            if (not close_short_px) and (px_high is not None):
                close_short_px = float(px_high)

            fees_open_total  = float(df_pos.at[i,"open_fees_usd"] or 0.0) + sum_long["fees_open_usd"] + sum_short["fees_open_usd"]
            fees_close_total = fees_close_total + sum_long["fees_close_usd"] + sum_short["fees_close_usd"]
            # Реализованный PnL: LONG (BUY->SELL) + SHORT (SELL->BUY)
            realized_long  = (close_long_px  - open_long_px ) * qty if (close_long_px and open_long_px) else 0.0
            realized_short = (open_short_px  - close_short_px) * qty if (close_short_px and open_short_px) else 0.0
            realized_total = realized_long + realized_short - (fees_open_total + fees_close_total)

            # --- защита от NaN перед сохранением
            if realized_total != realized_total:
                realized_total = 0.0

            # Запись в positions
            df_pos.at[i,"status"]="closed"
            df_pos.at[i,"closed_at"]=iso_utc(now_ms)
            df_pos.at[i,"close_reason"]=reason
            df_pos.at[i,"last_ms"]=now_ms
            df_pos.at[i,"close_long_order_id"]  = close_long_oid or ""
            df_pos.at[i,"close_short_order_id"] = close_short_oid or ""
            df_pos.at[i,"close_long_px"]  = close_long_px
            df_pos.at[i,"close_short_px"] = close_short_px
            df_pos.at[i,"close_fees_usd"] = fees_close_total
            df_pos.at[i,"close_long_cloid"]  = cl_close_long
            df_pos.at[i,"close_short_cloid"] = cl_close_short            
            df_pos.at[i,"realized_pnl_usd"] = realized_total

            globals().setdefault("_LAST_CLOSED", {})[sym] = time.time()

            maybe_send_telegram(f"✅ <b>Closed</b> {sym}\n{_anchor(long_ex,sym)} / {_anchor(short_ex,sym)}\nReason: {reason}")
            # Доп. карточка с фактом PnL
            try:
                pnl_lines = [
                    f"💰 <b>Realized PnL</b>: <b>${realized_total:.2f}</b>",
                    f"   Long: open {open_long_px:.6f} → close {close_long_px:.6f}",
                    f"   Short: open {open_short_px:.6f} → close {close_short_px:.6f}",
                    f"   Fees: open ${fees_open_total:.2f} + close ${fees_close_total:.2f}",
                ]

                # --- Балансы после закрытия (только если не PAPER)  # NEW
                if not paper:
                    try:
                        all_exchanges = getenv_list("EXCHANGES", DEFAULT_EXCHANGES)
                        balances = get_post_close_balances(all_exchanges)
                        if balances:
                            pnl_lines.append("🏦 <b>Балансы после закрытия</b>:")
                            bn = balances.get("BINANCE", {})
                            by = balances.get("BYBIT",   {})
                            ok = balances.get("OKX",     {})
                            gt = balances.get("GATE",    {})

                            if bn:
                                pnl_lines.append(
                                    f"   BINANCE (USDT): wallet ${bn.get('wallet',0):.2f}, "
                                    f"available ${bn.get('available',0):.2f}"
                                )
                            if by:
                                demo_tag = " (demo)" if _is_true("BYBIT_DEMO", False) else ""
                                pnl_lines.append(
                                    f"   BYBIT{demo_tag} (USDT): equity ${by.get('equity',0):.2f}, "
                                    f"wallet ${by.get('wallet',0):.2f}, available ${by.get('available',0):.2f}"
                                )
                            if ok:
                                sim_tag = " (paper)" if _is_true("OKX_PAPER", False) or _is_true("OKX_TESTNET", False) else ""
                                pnl_lines.append(
                                    f"   OKX{sim_tag} (USDT): equity ${ok.get('equity',0):.2f}, "
                                    f"wallet ${ok.get('wallet',0):.2f}, available ${ok.get('available',0):.2f}"
                                )
                            if gt:
                                sim_tag = " (testnet)" if _is_true("GATE_TESTNET", False) or _is_true("GATE_PAPER", False) else ""
                                pnl_lines.append(
                                    f"   GATE{sim_tag} (USDT): equity ${gt.get('equity',0):.2f}, "
                                    f"wallet ${gt.get('wallet',0):.2f}, available ${gt.get('available',0):.2f}"
                                )

                            # --- ИТОГО по всем биржам ---
                            total_equity = 0.0
                            for name, b in balances.items():
                                eq = b.get("equity")
                                if eq is None:
                                    eq = (b.get("wallet", 0.0) or 0.0) + float(b.get("uPnL", 0.0) or 0.0)
                                total_equity += float(eq or 0.0)
                            if total_equity > 0:
                                pnl_lines.append(f"   TOTAL ≈ ${total_equity:.2f}")
                    except Exception as e:
                        logging.debug("post-close balances fetch skipped: %s", e)


                maybe_send_telegram("\n".join(pnl_lines))
            except Exception:
                pass            
            save_positions(pos_path, df_pos)
            has_open = False

    if (not has_open) and best is not None:
        spread_bps = float(best["spread_bps"])
        if float(best.get("z", 0.0)) >= Z_IN and float(best.get("net_usd_adj", 0.0))>0.0:
            sym = str(best["symbol"]).upper()
            cheap_ex = str(best["long_ex"]); rich_ex=str(best["short_ex"])
            px_low = float(best["px_low"]); px_high=float(best["px_high"])
            # Базовая оценка из кандидата
            qty = float(best["qty_est"]) if best["qty_est"] and best["qty_est"]>0 else per_leg_notional_usd/max(px_low,1.0)
            # Жёсткий лимит от капитала и шага лота (для обеих бирж)
            try:
                # шаг на Bybit
                by_tick, _, _, by_qty_step = bybit_get_filters(sym)
            except Exception:
                by_qty_step = 0.0
            # для Binance шаг возьмём из exchangeInfo
            bn_info = _binance_symbol_info(sym) or {}
            lot_f = {f["filterType"]: f for f in bn_info.get("filters", [])}.get("LOT_SIZE") or {}
            bn_qty_step = float(lot_f.get("stepSize") or 0.0)
            qty_capA = cap_qty_by_capital(px_low,  by_qty_step or bn_qty_step or 0.0, float(getenv_float("CAPITAL",1000.0)), float(getenv_float("PERP_LEVERAGE",5.0)))
            qty_capB = cap_qty_by_capital(px_high, by_qty_step or bn_qty_step or 0.0, float(getenv_float("CAPITAL",1000.0)), float(getenv_float("PERP_LEVERAGE",5.0)))
            qty = max(0.0, min(qty, qty_capA, qty_capB))
            if qty <= 0:
                logging.debug("Qty capped to 0 by capital limits — skip %s", sym)
                return
            
            COOLDOWN_AFTER_CLOSE_SEC = int(getenv_float("COOLDOWN_AFTER_CLOSE_SEC", 30))
            _LAST_CLOSED: dict[str, float] = globals().setdefault("_LAST_CLOSED", {})

            # проверяем кулдаун до установки pair-lock
            if COOLDOWN_AFTER_CLOSE_SEC > 0 and sym in _LAST_CLOSED:
                if (time.time() - _LAST_CLOSED[sym]) < COOLDOWN_AFTER_CLOSE_SEC:
                    logging.debug("Skip open %s due to cooldown after close", sym)
                    return

            # Pair-lock: не даём начать открытие другого тикера
            if not open_lock_check_or_set(sym):
                return
                
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
                    "note_net_usd": float(best["net_usd"]),
                    "open_long_order_id":  meta.get("open_long_order_id",""),
                    "open_short_order_id": meta.get("open_short_order_id",""),
                    "open_long_px":  meta.get("open_long_px", 0.0),
                    "open_short_px": meta.get("open_short_px", 0.0),
                    "open_fees_usd": meta.get("open_fees_usd", 0.0),
                    "open_long_cloid": meta.get("open_long_cloid",""),
                    "open_short_cloid":meta.get("open_short_cloid","")
                }
                df_pos = pd.concat([df_pos, pd.DataFrame([new])], ignore_index=True)
                maybe_send_telegram("✅ <b>OPENED</b>\n" + format_signal_card(best, per_leg_notional_usd, price_source))
                open_lock_clear()
            else:
                logging.warning("Open failed: %s", meta.get("error"))
                open_lock_clear()
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

def matrix_per_exchange_symbols(matrix_path: str, exchanges: List[str]) -> Dict[str, set]:
    """
    Возвращает для каждой биржи множество символов, которые реально есть в матрице.
    Используется, чтобы *не* дергать котировки по символам, которых на бирже нет,
    даже если общий список symbols их содержит (например, при SYMBOLS_SOURCE=binance-top).
    """
    out: Dict[str, set] = {ex: set() for ex in exchanges}
    if not matrix_path:
        return out
    df = load_matrix_df(matrix_path)
    if df.empty:
        return out
    df.columns = [c.strip() for c in df.columns]
    if "symbol" not in df.columns:
        return out

    for ex in exchanges:
        if ex not in df.columns:
            continue
        # считаем, что "1"/True/непустое значение в колонке <ex> означает наличие контракта
        mask = df[ex].fillna(0) != 0
        syms = (
            df.loc[mask, "symbol"]
            .dropna()
            .astype(str)
            .str.upper()
            .unique()
            .tolist()
        )
        out[ex] = set(syms)
    return out

def binance_fapi_base_data() -> str:
    return binance_data_base()

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
# ===================== DRY-RUN PNL BLOCK (BEGIN) =====================
def _dryrun_enabled() -> bool:
    try:
        return _is_true("DRYRUN_PNL", False)
    except Exception:
        return False

def _dryrun_get(cfg_key: str, default=None):
    import os
    return os.getenv(cfg_key, default)

def _fetch_bbo(exchange: str, symbol: str) -> dict:
    """
    Возвращает {'bid':float,'ask':float} из ПУБЛИЧНОГО api соответствующей среды
    Учитывает demo/testnet за счёт ваших *data_base()* функций.
    """
    ex = exchange.lower()
    if ex == "binance":
        import requests
        base = binance_data_base()  # уже выбирает testnet/mainnet
        r = SESSION.get(f"{base}/fapi/v1/ticker/bookTicker", params={"symbol": symbol.upper()}, timeout=REQUEST_TIMEOUT)
        j = r.json()
        # ответ либо dict, либо список; страхуемся
        bid = float((j.get("bidPrice") if isinstance(j, dict) else j[0]["bidPrice"]))
        ask = float((j.get("askPrice") if isinstance(j, dict) else j[0]["askPrice"]))
        return {"bid": bid, "ask": ask}
    elif ex == "bybit":
        import requests
        base = bybit_data_base()    # уже выбирает demo/testnet/mainnet
        r = SESSION.get(f"{base}/v5/market/tickers", params={"category":"linear","symbol":symbol.upper()}, timeout=REQUEST_TIMEOUT)
        j = r.json()
        it = (j.get("result") or {}).get("list") or []
        if not it: raise RuntimeError(f"Empty BBO for {exchange}:{symbol}")
        bid = float(it[0]["bid1Price"])
        ask = float(it[0]["ask1Price"])
        return {"bid": bid, "ask": ask}
    else:
        raise ValueError(f"Unsupported exchange for BBO: {exchange}")

def _dry_price(price_source: str, bbo: dict, side: str) -> float:
    """
    Возвращает «базовую» цену до проскальзывания:
    - book: BUY=ask, SELL=bid
    - mid:  (bid+ask)/2
    """
    ps = (price_source or "book").lower()
    bid, ask = float(bbo["bid"]), float(bbo["ask"])
    if ps == "mid":
        return (bid + ask) / 2.0
    # default: book
    return ask if side.upper()=="BUY" else bid

def _with_slippage(price: float, side: str, slippage_bps: float) -> float:
    k = (slippage_bps or 0.0) / 10000.0
    if side.upper() == "BUY":
        return price * (1.0 + k)
    else:
        return price * (1.0 - k)

def _dryrun_pair_pnl(event: str,
                     ex_buy: str, sym_buy: str, qty_buy: float,
                     ex_sell: str, sym_sell: str, qty_sell: float,
                     taker_fee: float,
                     price_source: str,
                     slippage_bps: float,
                     logger=None):
    """
    Считает мгновенный PnL входа/выхода для пары (BUY leg + SELL leg)
    по текущему bid/ask с учётом проскальзывания и такер-комиссий.
    """
    import math, logging
    lg = logger or logging

    # --- 1) Получаем BBO на обеих биржах
    bbo_buy  = _fetch_bbo(ex_buy,  sym_buy)
    bbo_sell = _fetch_bbo(ex_sell, sym_sell)

    # --- 2) Базовые цены до проскальзывания
    px_buy_base  = _dry_price(price_source, bbo_buy,  "BUY")
    px_sell_base = _dry_price(price_source, bbo_sell, "SELL")

    # --- 3) Эффективные цены с проскальзыванием
    px_buy_eff  = _with_slippage(px_buy_base,  "BUY",  slippage_bps)
    px_sell_eff = _with_slippage(px_sell_base, "SELL", slippage_bps)

    # --- 4) Нотационал и комиссии (RT на входе/выходе: две стороны сразу)
    # На входе: платим 2*taker_fee*notional (за покупку и продажу)
    # Здесь считаем нотационалы по каждой ноге отдельно
    notional_buy  = qty_buy  * px_buy_eff
    notional_sell = abs(qty_sell) * px_sell_eff

    fees_usd = taker_fee * (notional_buy + notional_sell) * 2.0

    # --- 5) Валовой и чистый эффект сделки
    # Покупаем по px_buy_eff, продаём по px_sell_eff одинаковым qty в $
    # Для бесшовности используем min(qty_buy, qty_sell) по кол-ву контрактов
    q = min(qty_buy, abs(qty_sell))
    gross_usd = (px_sell_eff - px_buy_eff) * q
    net_usd   = gross_usd - fees_usd

    # --- 6) Оценка спрэда в бп от мида пары (для информативности)
    mid_buy  = (bbo_buy["bid"] + bbo_buy["ask"]) / 2.0
    mid_sell = (bbo_sell["bid"] + bbo_sell["ask"]) / 2.0
    # "кросс-мид" на входе как среднее двух мидов:
    cross_mid = (mid_buy + mid_sell) / 2.0
    spread_bps = 0.0
    if cross_mid > 0:
        spread_bps = 10000.0 * (px_sell_eff - px_buy_eff) / cross_mid

    # --- 7) Лог
    lg.info(
        f"🔎 DRY-RUN {event} • {sym_buy}@{ex_buy} BUY vs {sym_sell}@{ex_sell} SELL\n"
        f"    BBO BUY:  bid {bbo_buy['bid']:.8f} / ask {bbo_buy['ask']:.8f}  → base {px_buy_base:.8f} → eff {px_buy_eff:.8f}\n"
        f"    BBO SELL: bid {bbo_sell['bid']:.8f} / ask {bbo_sell['ask']:.8f} → base {px_sell_base:.8f} → eff {px_sell_eff:.8f}\n"
        f"    qty={q}  taker_fee={taker_fee:.5f}  slip={slippage_bps:.2f} bps  spread≈{spread_bps:.2f} bps\n"
        f"    notional_buy={notional_buy:.4f}  notional_sell={notional_sell:.4f}\n"
        f"    GROSS={gross_usd:.4f}  FEES≈{fees_usd:.4f}  NET≈{net_usd:.4f} USDT"
    )

def dryrun_log_open(ex_long: str, sym_long: str, qty_long: float,
                    ex_short: str, sym_short: str, qty_short: float):
    if not _dryrun_enabled(): return
    ps  = _dryrun_get("DRYRUN_PRICE_SOURCE", _dryrun_get("PRICE_SOURCE","book"))
    slp = float(_dryrun_get("DRYRUN_SLIPPAGE_BPS", _dryrun_get("SLIPPAGE_BPS", "5")))
    fee = float(_dryrun_get("TAKER_FEE","0.0005"))
    _dryrun_pair_pnl("OPEN", ex_long, sym_long, qty_long, ex_short, sym_short, qty_short, fee, ps, slp)

def dryrun_log_close(ex_long: str, sym_long: str, qty_long: float,
                     ex_short: str, sym_short: str, qty_short: float):
    if not _dryrun_enabled(): return
    ps  = _dryrun_get("DRYRUN_PRICE_SOURCE", _dryrun_get("PRICE_SOURCE","book"))
    slp = float(_dryrun_get("DRYRUN_SLIPPAGE_BPS", _dryrun_get("SLIPPAGE_BPS", "5")))
    fee = float(_dryrun_get("TAKER_FEE","0.0005"))
    _dryrun_pair_pnl("CLOSE", ex_long, sym_long, qty_long, ex_short, sym_short, qty_short, fee, ps, slp)
# ===================== DRY-RUN PNL BLOCK (END) =====================

# ----------------- Main loop -----------------
def main():
    exchanges = [x.lower() for x in getenv_list("EXCHANGES", DEFAULT_EXCHANGES)]
    # Проверка коннектов перед запуском цикла
    must_check = _is_true("CHECK_EXCHANGES_AT_START", True)
    must_quit  = _is_true("QUIT_ON_CONNECTIVITY_FAIL", True)
    probe_sym  = os.getenv("CONNECTIVITY_PROBE_SYMBOL", "BTCUSDT").upper()
    if must_check:
        # определяем окружение для каждой биржи (mainnet / testnet / demo)
        per_env = {}
        for ex in exchanges:
            base = _private_base(ex)
            env = "testnet" if "testnet" in base else ("demo" if "api-demo" in base else "mainnet")

            # Спец-логика для OKX: demo определяется не URL-ом, а заголовком x-simulated-trading
            if ex.lower() == "okx":
                if _is_true("OKX_TESTNET", False) or _is_true("OKX_PAPER", False):
                    env = "demo"

            per_env[ex] = env

        price_feed_env = "mainnet"
        logging.info(f"[ENV] price_feed_env={price_feed_env}  order_env_per_exchange={per_env}")

                # ===== ЖЁСТКАЯ ПРОВЕРКА BYBIT ПРИ СТАРТЕ =====
        try:
            logging.info("[DEBUG] Startup Bybit balance via bybit_unified_usdt_balance()")
            bal = bybit_unified_usdt_balance()
            logging.info("[DEBUG] bybit_unified_usdt_balance() -> %s", bal)
        except Exception as e:
            logging.exception("[DEBUG] bybit_unified_usdt_balance() startup error: %s", e)

        try:
            logging.info("[DEBUG] Startup Bybit auth via _check_bybit_auth()")
            ok = _check_bybit_auth()
            logging.info("[DEBUG] _check_bybit_auth() -> %s", ok)
        except Exception as e:
            logging.exception("[DEBUG] _check_bybit_auth() startup error: %s", e)
        # ==============================================

        # === проверка согласованности окружений ===
        #if len(set(per_env.values())) > 1:
        #    msg = f"❌ Mixed order environments detected: {per_env}. Use either all-mainnet or all-testnet/demo."
        #    logging.error(msg)
        #    maybe_send_telegram(msg)
        #    return
    # === проверка коннектов ===
        if not check_connectivity(exchanges, probe_symbol=probe_sym):
            msg = "Startup connectivity check failed — one or more exchanges unavailable."
            logging.error(msg)
            if must_quit:
                maybe_send_telegram(f"❌ {msg}")
                return
            else:
                logging.warning("Continuing despite failed connectivity (QUIT_ON_CONNECTIVITY_FAIL=0)")

        # Auth check (private)
        if _is_true("AUTH_CHECK_AT_START", True):
            logging.info("Running private API auth checks...")
            if not check_auth_connectivity(exchanges):
                msg = "Private API auth check failed — verify API keys/permissions/network."
                logging.error(msg)
                if must_quit:
                    maybe_send_telegram(f"❌ {msg}")
                    return
                else:
                    logging.warning("Continuing despite auth failure (QUIT_ON_CONNECTIVITY_FAIL=0)")

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

    per_ex_symbols = None
    if matrix_path and use_per_ex:
        per_ex_symbols = matrix_per_exchange_symbols(matrix_path, exchanges)
        logging.info(
            "Per-exchange symbol filter enabled (matrix): %s",
            {ex: len(s) for ex, s in per_ex_symbols.items()}
        )

    logging.info("Symbols selected (%d): %s", len(symbols), symbols[:20])
    pos_cross_path = bucketize_path(getenv_str("POS_CROSS_PATH", "positions_price_cross.csv"))

    entry_bps = float(getenv_float("ENTRY_SPREAD_BPS", getenv_float("ENTRY_APR", 10.0)))
    exit_bps  = float(getenv_float("EXIT_SPREAD_BPS",  getenv_float("EXIT_APR", 2.0)))
    taker_fee = float(getenv_float("TAKER_FEE", 0.0005))
    paper = getenv_bool("PAPER", True)
    sleep_s = int(getenv_float("SLEEP_SEC", 3))

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
    logging.info("ROTATE=%s | Δ%%=%.4f | min%%leg=%.4f | min%%cap=%.4f | hyst_bps=%.2f | z_adv=%.2f | hold=%ss",
             rotate_on,
             float(getenv_float("ROTATE_DELTA_PCT", 0.30)),
             float(getenv_float("ROTATE_MIN_NOTIONAL_PCT", 0.20)),
             float(getenv_float("ROTATE_MIN_CAP_PCT", 0.05)),
             float(getenv_float("ROTATE_HYSTERESIS_BPS", 0.0)),
             float(getenv_float("ROTATE_REQUIRE_Z_ADV", 0.0)),
             int(getenv_float("ROTATE_MIN_HOLD_SEC", 0)))


    price_source = getenv_str("PRICE_SOURCE", "book").lower()
    use_bulk = getenv_bool("USE_BULK_QUOTES", True)  # новый флаг

    while True:
        try:
            if use_bulk:
                # НОВЫЙ bulk-сканер
                quotes_df = scan_spreads_once(
                    exchanges=exchanges,
                    symbols=symbols,
                    per_leg_notional_usd=per_leg_notional,
                    taker_fee=taker_fee,
                    price_source=price_source,
                    alert_spread_pct=ALERT_SPREAD_PCT,
                    cooldown_sec=ALERT_COOLDOWN_SEC,
                    instant_open=True,
                    pos_path_for_instant=pos_cross_path,
                    paper=paper,
                )
            else:
                # СТАРЫЙ вариант — на всякий случай, чтобы можно было откатиться
                quotes_df = scan_all_with_instant_alerts(
                    exchanges=exchanges,
                    symbols=symbols,
                    per_leg_notional_usd=per_leg_notional,
                    taker_fee=taker_fee,
                    price_source=price_source,
                    alert_spread_pct=ALERT_SPREAD_PCT,
                    cooldown_sec=ALERT_COOLDOWN_SEC,
                    instant_open=True,
                    pos_path_for_instant=pos_cross_path,
                    paper=paper,
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
            err = str(e)[:1600].replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
            maybe_send_telegram(f"❌ PriceArb cycle error: <code>{err}</code>")

        time.sleep(max(3, sleep_s))

if __name__ == "__main__":
    main()
