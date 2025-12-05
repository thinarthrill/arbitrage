#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, json, math, hmac, hashlib, logging, uuid, base64
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd
import numpy as np
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
import sys

def ensure_gcs_credentials_from_env():
    """
    Использует именно твои env-переменные:
      - GCS_KEY_JSON (строка с service account json)
      - GOOGLE_APPLICATION_CREDENTIALS (если уже задан — не трогаем)
    Делает:
      1) парсит json
      2) пишет во временный файл
      3) устанавливает GOOGLE_APPLICATION_CREDENTIALS
    """
    import os, json, tempfile, logging

    # если путь уже задан (например, через Render Secret File) — ок
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        logging.info("[GCS] GOOGLE_APPLICATION_CREDENTIALS already set")
        return

    raw = os.getenv("GCS_KEY_JSON")
    if not raw:
        logging.warning("[GCS] GCS_KEY_JSON is empty -> GCS writes will be anonymous")
        return

    # в env у тебя значение обёрнуто в одинарные кавычки ' {...} '
    raw = raw.strip()
    if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
        raw = raw[1:-1].strip()

    try:
        sa = json.loads(raw)
    except Exception as e:
        logging.error("[GCS] Failed to parse GCS_KEY_JSON: %s", e)
        return

    try:
        fd, path = tempfile.mkstemp(prefix="gcs_sa_", suffix=".json")
        os.close(fd)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(sa, f)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
        logging.info("[GCS] Credentials loaded from GCS_KEY_JSON into %s", path)
    except Exception as e:
        logging.error("[GCS] Failed to write temp credentials file: %s", e)

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
            raw_bid = j.get("bid1Price")
            raw_ask = j.get("ask1Price")
            if not raw_bid or not raw_ask:
                continue
            try:
                bid = float(raw_bid)
                ask = float(raw_ask)
            except (TypeError, ValueError):
                continue
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

def _bulk_gate():
    base = gate_base()  # важно: используем ту же базу, что и для ордеров
    url = f"{base}/api/v4/futures/usdt/tickers"
    try:
        r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logging.warning("Gate bulk tickers HTTP %s: %s", r.status_code, r.text[:200])
            return {}

        data = r.json()
        out = {}
        skipped = 0

        for j in data:
            inst = j.get("contract")
            if not inst or not inst.endswith("_USDT"):
                continue

            # аккуратно достаём bid/ask, т.к. bid1/ask1 может не быть
            raw_bid = j.get("bid1") or j.get("highest_bid") or j.get("best_bid_price")
            raw_ask = j.get("ask1") or j.get("lowest_ask") or j.get("best_ask_price")

            if raw_bid is None or raw_ask is None:
                skipped += 1
                continue

            try:
                bid = float(raw_bid)
                ask = float(raw_ask)
            except (TypeError, ValueError):
                skipped += 1
                continue

            if bid <= 0 or ask <= 0:
                skipped += 1
                continue

            mark = None
            last = None
            if j.get("mark_price") not in (None, ""):
                try:
                    mark = float(j["mark_price"])
                except (TypeError, ValueError):
                    mark = None
            if j.get("last") not in (None, ""):
                try:
                    last = float(j["last"])
                except (TypeError, ValueError):
                    last = None

            sym = inst.replace("_", "").upper()
            out[sym] = {
                "bid": bid,
                "ask": ask,
                "mark": mark,
                "last": last,
            }

        logging.debug(
            "Gate bulk loaded %d contracts, skipped=%d without bid/ask",
            len(out),
            skipped,
        )
        return out

    except Exception:
        logging.exception("Gate bulk tickers failed")
        return {}
    
def _bulk_okx():
    url = "https://www.okx.com/api/v5/market/tickers?instType=SWAP"
    try:
        r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logging.warning("OKX bulk tickers HTTP %s: %s", r.status_code, r.text[:200])
            return {}
        data = r.json().get("data", [])
        out = {}
        for j in data:
            inst = j.get("instId", "")
            if not inst.endswith("-USDT-SWAP"):
                continue
            sym = inst.replace("-", "").replace("SWAP", "")
            bid = float(j.get("bidPx") or 0)
            ask = float(j.get("askPx") or 0)
            mark = float(j.get("markPx") or 0) if j.get("markPx") else None
            last = float(j.get("last", 0)) if j.get("last") else None
            if bid <= 0 or ask <= 0:
                continue
            out[sym.upper()] = {
                "bid": bid,
                "ask": ask,
                "mark": mark,
                "last": last,
            }
        logging.debug("OKX bulk loaded %d contracts", len(out))
        return out
    except Exception:
        logging.exception("OKX bulk tickers failed")
        return {}

def load_all_bulk_quotes(exchanges):
    """
    Bulk loader for top-of-book quotes.
    Returns dict: {ex: {symbol: {"bid","ask","mid","last","mark"}}}

    ВАЖНО:
    - Ничего не падает, даже если bulk-функция одной биржи умерла.
    - Вычищаем записи без bid/ask (иначе потом DF пустой или без колонок).
    - Пишем статистику в лог: сколько котировок реально пришло.
    """
    quotes = {}
    total = 0

    def _safe_bulk_call(ex: str, fn_name: str):
        fn = globals().get(fn_name)
        if not callable(fn):
            logging.warning("[BULK] %s: function %s not found", ex, fn_name)
            return {}

        try:
            data = fn() or {}
            if not isinstance(data, dict):
                logging.warning("[BULK] %s: %s returned non-dict (%s)", ex, fn_name, type(data))
                return {}
            return data
        except Exception as e:
            logging.exception("[BULK] %s: %s failed: %s", ex, fn_name, e)
            return {}

    for ex in (exchanges or []):
        ex_l = str(ex).lower()

        if ex_l == "binance":
            raw = _safe_bulk_call("binance", "_bulk_binance")
        elif ex_l == "bybit":
            raw = _safe_bulk_call("bybit", "_bulk_bybit")
        elif ex_l == "okx":
            raw = _safe_bulk_call("okx", "_bulk_okx")
        elif ex_l == "gate":
            raw = _safe_bulk_call("gate", "_bulk_gate")
        else:
            raw = {}

        # --- normalize & validate ---
        clean = {}
        bad = 0
        for sym_u, q in (raw or {}).items():
            if not isinstance(q, dict):
                bad += 1
                continue
            bid = q.get("bid")
            ask = q.get("ask")
            if bid is None or ask is None:
                bad += 1
                continue
            try:
                bid_f = float(bid)
                ask_f = float(ask)
            except Exception:
                bad += 1
                continue
            if bid_f <= 0 or ask_f <= 0:
                bad += 1
                continue

            clean[str(sym_u).upper()] = {
                "bid": bid_f,
                "ask": ask_f,
                "mid": float(q.get("mid") or (bid_f + ask_f) / 2.0),
                "last": q.get("last"),
                "mark": q.get("mark"),
            }

        quotes[ex_l] = clean
        total += len(clean)

        logging.info(
            "[BULK] %s loaded=%d bad=%d",
            ex_l.upper(), len(clean), bad
        )

    logging.info("[BULK] total quotes loaded=%d", total)
    return quotes

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

def get_bbo(symbol: str, ex_name: str):
    ex = (ex_name or "").lower()
    if ex == "bybit":
        return bybit_best_bid_ask(symbol)
    if ex == "okx":
        return okx_best_bid_ask(symbol)
    if ex == "gate":
        return gate_best_bid_ask(symbol)
    # default binance
    return binance_best_bid_ask(symbol)

def atomic_cross_close(symbol: str, cheap_ex: str, rich_ex: str,
                       qty: float, paper: bool) -> Tuple[bool, dict]:
    """
    Закрытие кросса.

    По умолчанию (REVERSE_SIDE=0), закрываем:
      cheap_ex -> SELL reduce_only (закрываем LONG)
      rich_ex  -> BUY  reduce_only (закрываем SHORT)

    Если REVERSE_SIDE=1 (на входе делали SHORT на cheap_ex и LONG на rich_ex),
    то для закрытия делаем наоборот:
      cheap_ex -> BUY  reduce_only (закрываем SHORT)
      rich_ex  -> SELL reduce_only (закрываем LONG)
    """

    attempt_id = new_attempt_id()
    cl_close_long  = _gen_cloid("CLOSEA", attempt_id, "A")
    cl_close_short = _gen_cloid("CLOSEB", attempt_id, "B")

    meta = {"attempt_id": attempt_id}

    # тот же флаг, что и при открытии
    reverse_side = getenv_bool("REVERSE_SIDE", False)

    side_a_close = "SELL"
    side_b_close = "BUY"
    if reverse_side:
        side_a_close, side_b_close = "BUY", "SELL"
    try:
        # закрываем позицию на дешёвой
        oa = _place_perp_market_order(
            cheap_ex, symbol, side_a_close, qty,
            paper=paper, cl_oid=cl_close_long, reduce_only=True
        )
        if oa.get("status") != "FILLED":
            raise RuntimeError(f"legA close not filled: {oa}")

        # закрываем позицию на дорогой
        ob = _place_perp_market_order(
            rich_ex, symbol, side_b_close, qty,
            paper=paper, cl_oid=cl_close_short, reduce_only=True
        )
        if ob.get("status") != "FILLED":
            raise RuntimeError(f"legB close not filled: {ob}")

        close_long_px  = float(oa.get("avgPrice") or oa.get("price") or 0.0)
        close_short_px = float(ob.get("avgPrice") or ob.get("price") or 0.0)

        meta.update({
            "close_long_order_id": oa.get("orderId") or oa.get("id") or "",
            "close_short_order_id": ob.get("orderId") or ob.get("id") or "",
            "close_long_px": close_long_px,
            "close_short_px": close_short_px,
        })

        return True, meta

    except Exception as e:
        meta["error"] = str(e)
        return False, meta

# ----------------- utils -----------------
def utc_ms_now() -> int: return int(datetime.now(timezone.utc).timestamp()*1000)

def now_utc_str() -> str:
    try:
        return iso_utc(utc_ms_now())
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

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

# ------------------------------------------------------------
# Generic BBO helpers for preflight refresh (fix undefined vars)
# ------------------------------------------------------------

def _best_bid_ask_generic(ex: str, symbol: str):
    """
    Returns (best_bid, best_ask) for given exchange.
    Uses existing low-level fetchers already present in the script.
    """
    ex = ex.lower()
    try:
        # if you already have a unified _fetch_bbo(ex, symbol) - use it
        if "_fetch_bbo" in globals():
            bid, ask = _fetch_bbo(ex, symbol)
            return float(bid or 0.0), float(ask or 0.0)
    except Exception:
        pass

    # fallback: try per-exchange quote fetcher if exists
    try:
        if ex == "binance" and "binance_book_ticker" in globals():
            q = binance_book_ticker(symbol)
            return float(q.get("bid", 0.0)), float(q.get("ask", 0.0))
        if ex == "okx" and "okx_best_bid_ask" in globals():
            return okx_best_bid_ask(symbol)
        if ex == "gate" and "_gate_best_bid_ask" in globals():
            return gate_best_bid_ask(symbol)
        if ex == "mexc" and "mexc_best_bid_ask" in globals():
            return mexc_best_bid_ask(symbol)
        if ex == "bybit" and "bybit_best_bid_ask" in globals():
            return bybit_best_bid_ask(symbol)        
    except Exception:
        pass

    return 0.0, 0.0


def binance_best_bid_ask(symbol: str):
    return _best_bid_ask_generic("binance", symbol)

def okx_best_bid_ask(symbol: str):
    return _best_bid_ask_generic("okx", symbol)

def gate_best_bid_ask(symbol: str):
    return _best_bid_ask_generic("gate", symbol)

def mexc_best_bid_ask(symbol: str):
    return _best_bid_ask_generic("mexc", symbol)

def bybit_best_bid_ask(symbol: str):
    return _best_bid_ask_generic("bybit", symbol)

def quote(ex: str, symbol: str, price_source: str = "mid"):
    """
    Unified quote() used by older parts of the code.
    Returns dict: {"bid","ask","mid","last","mark","ts"}
    """
    bid, ask = _best_bid_ask_generic(ex, symbol)
    mid = (bid + ask) / 2 if (bid and ask) else (bid or ask or 0.0)
    return {
        "exchange": ex,
        "symbol": symbol.upper(),
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "last": mid,
        "mark": mid,
        "ts": utc_ms_now() if "utc_ms_now" in globals() else int(time.time()*1000),
    }
# ----------------- Telegram -----------------
def format_signal_card(r: dict, per_leg_notional_usd: float, price_source: str) -> str:
    sym      = str(r.get("symbol", ""))
    long_ex  = str(r.get("long_ex", "")).upper()
    short_ex = str(r.get("short_ex", "")).upper()
    px_low   = float(to_float(r.get("px_low")) or 0.0)
    px_high  = float(to_float(r.get("px_high")) or 0.0)
    sp_pct   = float(to_float(r.get("spread_pct")) or 0.0)
    sp_bps   = float(to_float(r.get("spread_bps")) or 0.0)
    net_usd  = float(to_float(r.get("net_usd")) or 0.0)
    gross    = float(to_float(r.get("gross_usd")) or 0.0)
    fees_rt  = float(to_float(r.get("fees_roundtrip_usd")) or 0.0)
    ts       = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    price_lbl = {"mid":"MID","last":"LAST","mark":"MARK","bid":"BID","ask":"ASK","book":"BBO"}.get(price_source.lower(),"MID")
    z           = r.get("z", None)
    std         = r.get("std", None)
    net_usd_adj = r.get("net_usd_adj", None)  # НЕ пересчитываем тут, только отображаем

    lines = [
        "──────────────",
        f"<b>{sym}</b>",
        f"{_anchor(long_ex, sym)} BUY  ↔  {_anchor(short_ex, sym)} SELL",
    ]

    # Net after slippage (только если реально посчитан апстримом)
    if net_usd_adj is not None and net_usd_adj == net_usd_adj:
        lines.append(f"🧮 Net after slippage: ${float(net_usd_adj):.2f}")

    # Z / σ
    if z is not None and z == z:  # not NaN
        if std is not None and std == std:
            lines.append(f"\n📈 <code>Z-score: {float(z):.2f} (σ={float(std):.2f})</code>")
        else:
            lines.append(f"\n📈 <code>Z-score: {float(z):.2f}</code>")

    # Локальный entry-порог по спреду (если есть)
    entry_bps_sugg = r.get("entry_bps_sugg")
    if entry_bps_sugg is not None:
        lines.append(f"\n🎯 <code>Entry ≥ {float(entry_bps_sugg):.0f} bps</code>")

    # Основные метрики
    lines.extend([
        f"\n🧮 SPREAD: {sp_pct:.2f}% ({sp_bps:.0f} bps)",
        #f"💵 Gross: ${gross:.2f}",
        #f"💸 Fees RT: ${fees_rt:.2f}",
        f"✅ Net: ${net_usd:.2f}",
        f"📊 Prices [{price_lbl}]",
        f"   Low @ {long_ex}:  {px_low:.6f}",
        f"   High @ {short_ex}: {px_high:.6f}",
        f"🕒 {ts}",
    ])

    if getenv_bool("SHOW_ENTRY_FILTERS", False):
        # режим открытия
        entry_mode = getenv_str("ENTRY_MODE", "price").lower()
        if entry_mode not in ("zscore", "price"):
            entry_mode = "price"

        # пороги ТОЛЬКО из env (как в try_instant_open)
        z_in_loc         = float(getenv_float("Z_IN", 2.0))
        entry_bps        = float(getenv_float("ENTRY_SPREAD_BPS", 0.0))
        std_min_for_open = float(getenv_float("STD_MIN_FOR_OPEN", 1e-4))
        capital_env      = float(getenv_float("CAPITAL", 1000.0))
        entry_net_pct    = float(getenv_float("ENTRY_NET_PCT", 1.0))
        min_net_abs      = (entry_net_pct / 100.0) * capital_env

        # условия (1:1 с try_instant_open)
        eco_ok    = (net_usd_adj is not None) and (net_usd_adj == net_usd_adj) and (float(net_usd_adj) > min_net_abs)
        spread_ok = sp_bps >= entry_bps
        z_ok      = (z is not None) and (z == z) and (float(z) >= z_in_loc)
        std_ok    = (std is not None) and (std == std) and (float(std) >= std_min_for_open)

        def _flag(ok: bool) -> str:
            return "✅" if ok else "❌"

        #lines.append("\n\n⚙️ <b>ENTRY FILTERS</b>")
        # --- NEW: показываем пороги из env / локальные reco + фактические метрики ---
        capital_env      = float(getenv_float("CAPITAL", 1000.0))
        entry_net_pct    = float(getenv_float("ENTRY_NET_PCT", 1.0))
        entry_spread_env = float(getenv_float("ENTRY_SPREAD_BPS", 0.0))
        z_in_env         = float(getenv_float("Z_IN", 2.5))
        std_min_env      = float(getenv_float("STD_MIN_FOR_OPEN", 1e-4))
        slip_bps_env     = float(getenv_float("SLIPPAGE_BPS", 1.0))

        # min_net_abs уже посчитан выше, но на всякий явно покажем от чего он
        #lines.append(
        #    "\n🧷 <b>THRESHOLDS (env / reco)</b>\n"
        #    f"   ENTRY_MODE: <code>{entry_mode}</code>\n"
        #    f"   CAPITAL: <code>{capital_env:.2f}$</code>\n"
        #    f"   ENTRY_NET_PCT: <code>{entry_net_pct:.3f}%</code> → min_net_abs=<code>{min_net_abs:.4f}$</code>\n"
        #    f"   ENTRY_SPREAD_BPS: env=<code>{entry_spread_env:.0f}</code>, used=<code>{entry_bps:.0f}</code>\n"
        #    f"   Z_IN: env=<code>{z_in_env:.2f}</code>, used=<code>{z_in_loc:.2f}</code>\n"
        #    f"   STD_MIN_FOR_OPEN: <code>{std_min_env:.6f}</code>\n"
        #    f"   SLIPPAGE_BPS: <code>{slip_bps_env:.2f}</code>"
        #)

        # ---------- lazy-fix: если алёрт пришёл без метрик ----------
        if net_usd_adj is None:
            # считаем так же, как в positions_once
            sl_bps = float(getenv_float("SLIPPAGE_BPS", 1.0))
            net_usd_adj = net_usd - (4.0 * (sl_bps/1e4) * per_leg_notional_usd)

        if (z is None or std is None or (z != z) or (std != std)):
            try:
                stats_df = read_spread_stats()
                _, z_calc, std_calc = get_z_for_pair(
                    stats_df, symbol=sym,
                    ex_low=long_ex.lower(), ex_high=short_ex.lower(),
                    px_low=px_low, px_high=px_high
                )
                if (z is None) or (z != z):
                    z = z_calc
                if (std is None) or (std != std):
                    std = std_calc
            except Exception:
                pass
        # -----------------------------------------------------------
        # !!! ВАЖНО: факты и флаги считаем ПОСЛЕ lazy-fix,
        # иначе карточка показывает одно, а условия — другое.
        z_fact   = z if (z is not None and z == z) else None
        std_fact = std if (std is not None and std == std) else None
        net_fact = net_usd_adj if net_usd_adj is not None else None

        # пересчёт ok-флагов после lazy-fix (1:1 как в try_instant_open)
        std_min_for_open = float(getenv_float("STD_MIN_FOR_OPEN", 1e-4))
        capital_env      = float(getenv_float("CAPITAL", 1000.0))
        entry_net_pct    = float(getenv_float("ENTRY_NET_PCT", 1.0))
        min_net_abs      = (entry_net_pct / 100.0) * capital_env
        entry_bps        = float(getenv_float("ENTRY_SPREAD_BPS", 0.0))
        z_in_loc         = float(getenv_float("Z_IN", 2.0))

        eco_ok    = (net_usd_adj is not None) and (net_usd_adj == net_usd_adj) and (float(net_usd_adj) > min_net_abs)
        spread_ok = sp_bps >= entry_bps
        z_ok      = (z is not None) and (z == z) and (float(z) >= z_in_loc)
        std_ok    = (std is not None) and (std == std) and (float(std) >= std_min_for_open)

        lines.append(
            "\n📌 <b>FACT (current tick)</b>\n"
            f"   spread_bps=<code>{sp_bps:.2f}</code>\n"
            f"   net_usd_adj=<code>{'None' if net_fact is None else f'{float(net_fact):.4f}'}</code>\n"
            f"   z=<code>{'NaN' if z_fact is None else f'{float(z_fact):.4f}'}</code>\n"
            f"   std=<code>{'NaN' if std_fact is None else f'{float(std_fact):.6f}'}</code>"
        )

        # если в best пришли метаданные статистики — покажем их (помогает понять, почему NaN)
        if "count" in r or "ema_var" in r or "updated_ms" in r or "stats_ok" in r:
            try:
                cnt = r.get("count", None)
                ev  = r.get("ema_var", None)
                upd = r.get("updated_ms", None)
                okf = r.get("stats_ok", None)

                upd_age = None
                if upd is not None:
                    try:
                        upd_age = (time.time()*1000 - float(upd)) / 1000.0
                    except Exception:
                        upd_age = None

                lines.append(
                    "\n🧪 <b>STATS META</b>\n"
                    f"   stats_ok=<code>{okf}</code>\n"
                    f"   count=<code>{cnt}</code>\n"
                    f"   ema_var=<code>{ev}</code>\n"
                    f"   updated_ms_age=<code>{'None' if upd_age is None else f'{upd_age:.0f}s'}</code>"
                )
            except Exception as e:
                lines.append(f"\n🚫 <b>Error:</b> Ошибка в метаданных статистики %e")

        # eco_ok
        if net_usd_adj is not None:
            cmp = ">" if eco_ok else "≤"
            lines.append(
                f"{_flag(eco_ok)} eco_ok   · net_adj={float(net_usd_adj):.2f} {cmp} {min_net_abs:.2f}"
            )
        else:
            lines.append(
                f"{_flag(False)} eco_ok   · net_adj=None , min_net=${min_net_abs:.2f}"
            )

        # spread_ok
        lines.append(
            f"{_flag(spread_ok)} spread_ok · {sp_bps:.0f} bps ≥ {entry_bps:.0f} bps"
        )

        if entry_mode == "zscore":
            if z is not None and z == z:
                lines.append(
                    f"{_flag(z_ok)} z_ok      · z={float(z):.2f} ≥ {z_in_loc:.2f}"
                )
            else:
               lines.append(f"{_flag(False)} z_ok      · z={z} , need ≥ {z_in_loc:.2f}")

            if std is not None and std == std:
                lines.append(
                    f"{_flag(std_ok)} std_ok    · σ={float(std):.4f} ≥ {std_min_for_open:.4f}"
                )
            else:
                lines.append(f"{_flag(False)} std_ok    · σ={std} , need ≥ {std_min_for_open:g}")

        # маленький хвостик: режим
        lines.append(f"\n🔧 mode: {entry_mode}")
    lines.append(f"\n<b> ver: 2.37</b>")
    # --- NEW: show confirm snapshot from try_instant_open (if happened) ---
    try:
        if r.get("spread_bps_confirm") is not None:
            lines.append("\n🧷 <b>CONFIRM CHECK (actual open attempt)</b>")
            lines.append(
                "📌 FACT (confirm)\n"
                f"   spread_bps_confirm={to_float(r.get('spread_bps_confirm')):.2f}\n"
                f"   net_usd_adj_confirm={to_float(r.get('net_usd_adj_confirm')):.4f}\n"
                f"   px_low_confirm={to_float(r.get('px_low_confirm')):.6f}\n"
                f"   px_high_confirm={to_float(r.get('px_high_confirm')):.6f}"
            )
            lines.append(
                "⚙️ ENTRY FILTERS (confirm)\n"
                f"{'✅' if r.get('eco_ok_confirm') else '❌'} eco_ok_confirm   · net_adj_confirm "
                f"= {to_float(r.get('net_usd_adj_confirm')):.2f} "
                f"{'>' if r.get('eco_ok_confirm') else '≤'} {to_float(r.get('min_net_abs_used') or 0.0):.2f}\n"
                f"{'✅' if r.get('spread_ok_confirm') else '❌'} spread_ok_confirm · "
                f"{to_float(r.get('spread_bps_confirm')):.0f} bps ≥ {to_float(r.get('entry_bps_used') or 0.0):.0f} bps\n"
                f"{'✅' if r.get('z_ok_confirm') else '❌'} z_ok_confirm      · "
                f"z={to_float(r.get('z')):.2f} ≥ {to_float(r.get('z_in_used') or 0.0):.2f}\n"
                f"{'✅' if r.get('std_ok_confirm') else '❌'} std_ok_confirm    · "
                f"σ={to_float(r.get('std')):.6f} ≥ {to_float(r.get('std_min_for_open_used') or 0.0):.6f}"
            )
    except Exception as e:
        lines.append("\n🚫 <b>Ошибка</b>NEW: show confirm snapshot from try_instant_open. %e\n")

    # --- NEW: show exact open skip reasons (if any) ---
    try:
        rs = r.get("_open_skip_reasons") or []
        if isinstance(rs, (list, tuple)) and len(rs) > 0:
            lines.append(
                "\n🚫 <b>OPEN SKIPPED</b>\n"
                + "\n".join([f"   • {str(x)}" for x in rs])
            )
    except Exception as e:
        lines.append("\n🚫 <b>Ошибка</b>NEW: show exact open skip reasons. %e\n")
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

    # --- 0) sanity check ---
    if px_low <= 0 or px_high <= 0:
        return float("nan"), float("nan"), float("nan")

    # текущий лог-спред
    try:
        x = math.log(px_high / px_low)
    except Exception:
        return float("nan"), float("nan"), float("nan")

    s = symbol.upper()
    a = ex_low.lower()
    b = ex_high.lower()

    # --- 1) сначала ищем статистику в прямом направлении ---
    sub = stats[
        (stats["symbol"] == s) &
        (stats["ex_low"]  == a) &
        (stats["ex_high"] == b)
    ]

    flipped = False
    # --- 1b) если нет — пробуем зеркальную пару ---
    if sub.empty:
        sub2 = stats[
            (stats["symbol"] == s) &
            (stats["ex_low"]  == b) &
            (stats["ex_high"] == a)
        ]
        if not sub2.empty:
            sub = sub2
            flipped = True
            x = -x     # знак лог-спреда переворачивается

    if sub.empty:
        # нет статистики ни в прямом, ни в обратном порядке
        return x, float("nan"), float("nan")

    row  = sub.iloc[-1]   # всегда берём последнюю статистику
    mean = to_float(row.get("ema_mean"))
    var  = to_float(row.get("ema_var"))
    cnt  = int(row.get("count") or 0)
    upd  = float(row.get("updated_ms") or 0.0) / 1000.0
    now  = time.time()

    min_cnt = int(
        getenv_float("SPREAD_MIN_COUNT",
            getenv_float("MIN_SPREAD_COUNT", SPREAD_MIN_COUNT)
        )
    )

    # --- 2) проверка количества и свежести ---
    if cnt < min_cnt:
        logging.debug("[ZMISS] count too low for %s (%s->%s): cnt=%s < %s",
                      s, a, b, cnt, min_cnt)
        return x, float("nan"), float("nan")

    if upd > 0 and (now - upd) > SPREAD_STALE_SEC:
        logging.debug("[ZMISS] stats stale for %s (%s->%s): age=%.1fs > %ss",
                      s, a, b, now - upd, SPREAD_STALE_SEC)
        return x, float("nan"), float("nan")

    # --- 3) std ---
    try:
        std = math.sqrt(max(var or 0.0, 0.0))
    except Exception:
        std = float("nan")

    if std != std or std <= 0:
        return x, float("nan"), float("nan")

    std = max(std, SPREAD_STD_FLOOR)

    # при flipped — mean меняет знак, чтобы соответствовать перевёрнутому x
    if flipped and (mean is not None):
       mean = -mean

    # --- 4) z-score ---
    try:
        z = (x - (mean or 0.0)) / std
    except Exception:
        z = float("nan")

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
def try_instant_open(best, per_leg_notional_usd, taker_fee, paper, pos_path):
    """
    Единственный источник открытия позиции.
    Работает и в bulk, и в row_scan, не конфликтует со старым open-блоком.
    """

    if best is None:
        return False

    # --- 0) Базовые поля ---
    sym = str(best.get("symbol", "")).upper()
    if not sym:
        return False

    cheap_ex = str(best.get("long_ex") or best.get("cheap_ex") or "").lower()
    rich_ex  = str(best.get("short_ex") or best.get("rich_ex") or "").lower()

    # --- NEW: collect reject reasons for TG card ---
    skip_reasons: list[str] = []
    def _reject(msg: str) -> bool:
        try:
            skip_reasons.append(str(msg))
            best["_open_skip_reasons"] = skip_reasons
        except Exception:
            pass

        if getenv_bool("DEBUG_INSTANT_OPEN", False) and not best.get("_open_skip_logged"):
            logging.info("[OPEN_SKIP] %s %s", sym, msg)
            best["_open_skip_logged"] = True
            # отдельная карточка в TG с причинами, почему open не состоялся
            try:
                price_source = getenv_str("PRICE_SOURCE", "mid")
                card = format_signal_card(best, per_leg_notional_usd, price_source)
                reasons_text = "\n".join(f"• {r}" for r in skip_reasons)
                maybe_send_telegram(
                    "⚪ <b>OPEN SKIPPED</b>\n" + card + f"\nПричины:\n{reasons_text}"
                )
            except Exception:
                # не ломаем основной поток из-за ошибок в отправке дебажной карточки
                logging.exception("Failed to send OPEN SKIPPED card for %s", sym)

        return False

    if not cheap_ex or not rich_ex or cheap_ex == rich_ex:
        return _reject(f"bad exchanges: cheap_ex={cheap_ex}, rich_ex={rich_ex}")

    px_low  = float(best.get("px_low")  or 0.0)
    px_high = float(best.get("px_high") or 0.0)
    if px_low <= 0 or px_high <= 0:
        return _reject(f"bad prices: px_low={px_low}, px_high={px_high}")

    spread_bps = float(best.get("spread_bps") or 0.0)
    z_raw      = best.get("z")
    std_raw    = best.get("std")
    net_adj_raw = best.get("net_usd_adj")

    z   = float(z_raw)   if (z_raw is not None and z_raw == z_raw) else float("nan")
    std = float(std_raw) if (std_raw is not None and std_raw == std_raw) else float("nan")

    # --- FIX: если net_usd_adj не посчитан в кандиде — считаем здесь на тех же формулах ---
    if net_adj_raw is None or not (net_adj_raw == net_adj_raw):
        try:
            slippage_bps = float(getenv_float("SLIPPAGE_BPS", 1.0))
            spread_pct = (px_high - px_low) / max(px_low, 1e-12)
            gross_usd  = spread_pct * float(per_leg_notional_usd)
            fees_rt    = 4.0 * float(taker_fee) * float(per_leg_notional_usd)
            slip_usd   = 4.0 * (slippage_bps / 1e4) * float(per_leg_notional_usd)
            net_adj_raw = gross_usd - fees_rt - slip_usd
            best["net_usd_adj"] = net_adj_raw
        except Exception as e:
            net_adj_raw = float("nan")
            best["net_usd_adj"] = None
            # и заодно сохраним причину в skip-лог
            return _reject(f"net_usd_adj calc failed: {e}")

    net_adj = float(net_adj_raw) if net_adj_raw is not None else float("nan")

    ENTRY_MODE = getenv_str("ENTRY_MODE", "price").lower()
    Z_IN = float(getenv_float("Z_IN", 2.0))
    ENTRY_SPREAD_BPS = float(getenv_float("ENTRY_SPREAD_BPS", 0.0))

    std_min_for_open = float(getenv_float("STD_MIN_FOR_OPEN", 1e-4))
    capital = float(getenv_float("CAPITAL", 1000.0))
    min_net_abs = (float(getenv_float("ENTRY_NET_PCT", 1))/100.0) * capital
    # --- NEW: store "used" thresholds for TG/debug so card == real precheck ---
    best["entry_mode_used"] = ENTRY_MODE
    best["z_in_used"] = Z_IN
    best["entry_bps_used"] = ENTRY_SPREAD_BPS
    best["std_min_for_open_used"] = std_min_for_open
    best["min_net_abs_used"] = min_net_abs

    eco_ok = (net_adj == net_adj) and (net_adj > min_net_abs)
    spread_ok = spread_bps >= ENTRY_SPREAD_BPS
    z_ok = (z == z) and (z >= Z_IN)
    std_ok = (std == std) and (std >= std_min_for_open)

    best["eco_ok"] = eco_ok
    best["spread_ok"] = spread_ok
    best["z_ok"] = z_ok
    best["std_ok"] = std_ok

    if ENTRY_MODE == "zscore":
        if not (eco_ok and z_ok and std_ok):
            return _reject(
                f"precheck failed (zscore): eco_ok={eco_ok}, z_ok={z_ok}, std_ok={std_ok}"
            )
    else:
        if not (eco_ok and spread_ok):
            return _reject(
                f"precheck failed (price): eco_ok={eco_ok}, spread_ok={spread_ok}"
            )

    # --- 2) refresh-confirm (подтверждаем BBO на месте) ---
    REFRESH_CONFIRM = getenv_bool("REFRESH_CONFIRM", True)
    REFRESH_CONFIRM_SEC = float(getenv_float("REFRESH_CONFIRM_SEC", 0.25))
    MAX_LAG_BPS_CONFIRM = float(getenv_float("MAX_LAG_BPS_CONFIRM", 20.0))

    def _bbo(ex_name: str):
        ex = ex_name.lower()
        if ex == "bybit":
            return bybit_best_bid_ask(sym)
        if ex == "okx":
            return okx_best_bid_ask(sym)
        if ex == "gate":
            return gate_best_bid_ask(sym)
        # default binance
        return binance_best_bid_ask(sym)

    if REFRESH_CONFIRM:
        try:
            time.sleep(max(0.0, REFRESH_CONFIRM_SEC))

            low_bid, low_ask   = _bbo(cheap_ex)
            high_bid, high_ask = _bbo(rich_ex)

            if low_bid <= 0 or low_ask <= 0 or high_bid <= 0 or high_ask <= 0:
                logging.debug("REFRESH_CONFIRM bad bbo -> skip %s", sym)
                return _reject("refresh_confirm: bad bbo after sleep")

            # обновляем цены в best на подтверждённые
            px_low  = float(low_ask)   # BUY по ask на дешёвой
            px_high = float(high_bid)  # SELL по bid на дорогой

            spread_bps_confirm = (px_high - px_low) / max(px_low, 1e-12) * 1e4

            if spread_bps_confirm < spread_bps - MAX_LAG_BPS_CONFIRM:
                logging.debug(
                    "REFRESH_CONFIRM reject %s: old=%.2f bps new=%.2f bps",
                    sym, spread_bps, spread_bps_confirm
                )
                return _reject(
                    f"refresh_confirm: spread dropped old={spread_bps:.2f}bps "
                    f"new={spread_bps_confirm:.2f}bps lag>{MAX_LAG_BPS_CONFIRM:.2f}bps"
                )

            best["px_low"] = px_low
            best["px_high"] = px_high
            best["spread_bps"] = spread_bps_confirm

            # --- NEW: store confirm snapshot for TG card ---
            best["px_low_confirm"] = px_low
            best["px_high_confirm"] = px_high
            best["spread_bps_confirm"] = spread_bps_confirm

            # пересчёт net_usd_adj на подтверждённых ценах (по той же идее, что в build_price_arbitrage)
            slippage_bps = float(getenv_float("SLIPPAGE_BPS", 1.0))
            spread_pct_confirm = (px_high - px_low) / max(px_low, 1e-12)
            gross_usd_confirm = spread_pct_confirm * float(per_leg_notional_usd)
            fees_rt_confirm = 2.0 * float(taker_fee) * float(per_leg_notional_usd)
            slip_usd_confirm = 4.0 * (slippage_bps / 1e4) * float(per_leg_notional_usd)
            net_usd_adj_confirm = gross_usd_confirm - fees_rt_confirm - slip_usd_confirm
            best["net_usd_adj_confirm"] = net_usd_adj_confirm

            # confirm-флаги (z/std берём текущие, т.к. mean/var здесь нет)
            spread_ok_confirm = spread_bps_confirm >= ENTRY_SPREAD_BPS
            eco_ok_confirm = (net_usd_adj_confirm == net_usd_adj_confirm) and (net_usd_adj_confirm > min_net_abs)
            z_ok_confirm = (z == z) and (z >= Z_IN)
            std_ok_confirm = (std == std) and (std >= std_min_for_open)
            best["eco_ok_confirm"] = eco_ok_confirm
            best["spread_ok_confirm"] = spread_ok_confirm
            best["z_ok_confirm"] = z_ok_confirm
            best["std_ok_confirm"] = std_ok_confirm
        except Exception as e:
            logging.debug("REFRESH_CONFIRM exception: %s", e)
            return _reject(f"refresh_confirm exception: {e}")

    # --- 3) Qty расчёт как в positions_once ---
    try:
        qty = float(best.get("qty_est") or 0.0)
        if qty <= 0:
            qty = float(per_leg_notional_usd) / max(px_low, 1.0)

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
            return _reject("qty capped to 0 by capital limits")

    except Exception as e:
        logging.exception("Qty calc failed for %s: %s", sym, e)
        return _reject(f"qty calc failed: {e}")

    # --- 4) Кулдаун после закрытия ---
    COOLDOWN_AFTER_CLOSE_SEC = int(getenv_float("COOLDOWN_AFTER_CLOSE_SEC", 30))
    _LAST_CLOSED = globals().setdefault("_LAST_CLOSED", {})
    if COOLDOWN_AFTER_CLOSE_SEC > 0 and sym in _LAST_CLOSED:
        if (time.time() - _LAST_CLOSED[sym]) < COOLDOWN_AFTER_CLOSE_SEC:
            logging.debug("Skip open %s due to cooldown after close", sym)
            return _reject(
                f"cooldown after close: {COOLDOWN_AFTER_CLOSE_SEC}s not passed"
            )

    # --- 5) Проверка, что по этой паре нет уже открытой позиции в positions_cross.csv ---
    try:
        df_pos = load_positions(pos_path)
        if not df_pos.empty and all(c in df_pos.columns for c in ("symbol", "long_ex", "short_ex", "status")):
            mask = (
                df_pos["symbol"].astype(str).str.upper().eq(sym)
                & df_pos["long_ex"].astype(str).str.lower().eq(cheap_ex)
                & df_pos["short_ex"].astype(str).str.lower().eq(rich_ex)
                & df_pos["status"].astype(str).isin(["open", "closing"])
            )
            if mask.any():
                return _reject("position already open in positions_cross.csv")
    except Exception as e:
        # не блокируем open из-за проблем с CSV, просто логируем
        logging.debug("instant_open: positions check failed for %s: %s", sym, e)

    # --- 6) Открываем атомарно ---
    ok, attempt_id, meta = atomic_cross_open(
        symbol=sym, cheap_ex=cheap_ex, rich_ex=rich_ex,
        qty=qty, price_low=px_low, price_high=px_high, paper=paper
    )

    if not ok:
            # аккуратно достаём текст причины
            reason = None
            try:
                if isinstance(meta, dict):
                    reason = meta.get("error") or meta.get("reason") or str(meta)
                else:
                    reason = str(meta)
            except Exception:
                reason = str(meta)

            # добавим в причину контекст по сделке
            ctx = (
                f"{cheap_ex.upper()}→{rich_ex.upper()} "
                f"qty={qty:.4f} px_low={px_low:.6f} px_high={px_high:.6f}"
            )

            full_reason = f"{ctx}; error={reason}" if reason else ctx

            skip_reasons.append(f"atomic_open failed: {full_reason}")
            try:
                best["_open_skip_reasons"] = skip_reasons
            except Exception:
                pass

            # отдадим через _reject, чтобы ушла OPEN SKIPPED-карточка
            return _reject(f"atomic_open failed: {full_reason}")

    now_ms = utc_ms_now()

    if ok:
        df_pos = load_positions(pos_path)
        cur_max = pd.to_numeric(df_pos["id"], errors="coerce").max() if not df_pos.empty else None
        next_id = int(cur_max) + 1 if cur_max == cur_max and cur_max is not None else 1

        new = {
            "id": next_id, "attempt_id": attempt_id, "symbol": sym,
            "long_ex": cheap_ex, "short_ex": rich_ex,
            "opened_ms": now_ms, "last_ms": now_ms, "held_h": 0.0,
            "size_usd": per_leg_notional_usd,
            "entry_spread_bps": float(best["spread_bps"]),
            "entry_z": float(best.get("z") or 0.0),
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

        # карточка OPENED — только по fill-ценам
        price_source = getenv_str("PRICE_SOURCE", "mid")
        best_opened = dict(best)
        if meta.get("open_long_px"):
            best_opened["px_low"] = float(meta["open_long_px"])
        if meta.get("open_short_px"):
            best_opened["px_high"] = float(meta["open_short_px"])

        best_opened["spread_bps"] = (
            (best_opened["px_high"] - best_opened["px_low"]) / max(best_opened["px_low"], 1e-12) * 1e4
        )

        maybe_send_telegram(
            "✅ <b>OPENED</b>\n" + format_signal_card(best_opened, per_leg_notional_usd, price_source)
        )
        return True

    else:
        err = str((meta or {}).get("error") or "unknown error")

        # если откат первой ноги не удался — предупреждаем явно
        rb_warn = ""
        if isinstance(meta, dict) and meta.get("rollback_failed"):
            rb_ex = str(meta.get("rollback_ex") or cheap_ex).upper()
            rb_sym = sym
            rb_warn = f"\n⚠️ ROLLBACK WARNING: check {rb_sym} on {rb_ex}"

        logging.warning("Instant open failed for %s: %s", sym, err)

        if getenv_bool("DEBUG_INSTANT_OPEN", False):
            card = format_signal_card(best, per_leg_notional_usd, getenv_str("PRICE_SOURCE", "mid"))
            card = (
                "⚠️ <b>OPEN FAILED</b>\n"
                + card
                + f"\nПричина: <code>{err}</code>"
                + rb_warn
            )
            maybe_send_telegram(card)
        # даже если DEBUG_INSTANT_OPEN выключен — хотя бы короткое предупреждение
        elif rb_warn:
            maybe_send_telegram(rb_warn.strip())

        return False

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
    use_bulk = getenv_bool("USE_BULK_QUOTES", False)
    bulk_quotes = load_all_bulk_quotes(exchanges) if use_bulk else {}

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

            r = None

            # --- 1) Пытаемся взять котировку из bulk ---
            if use_bulk and ex in ("binance", "bybit", "okx", "gate"):
                q_ex = bulk_quotes.get(ex, {})
                q = q_ex.get(sym_u)
                if q:
                    bid = q.get("bid")
                    ask = q.get("ask")
                    mark = q.get("mark")
                    last = q.get("last")
                    mid = (bid + ask) / 2.0 if (bid is not None and ask is not None) else None

                    # Эмулируем ту же логику price_source, что и в *quote-функциях
                    ps = (price_source or "mid").lower()
                    if ps == "bid":
                        ask = last = mark = None
                    elif ps == "ask":
                        bid = last = mark = None
                    elif ps == "last":
                        bid = ask = mid = mark = None
                    elif ps == "mark":
                        bid = ask = mid = last = None
                    elif ps == "mid":
                        last = mark = None
                    elif ps == "book":
                        # чисто best bid/ask
                        last = mark = mid = None

                    r = {
                        "exchange": ex,
                        "symbol": sym_u,
                        "bid": bid,
                        "ask": ask,
                        "mid": mid,
                        "last": last,
                        "mark": mark,
                        "ts": utc_ms_now(),
                    }

            # --- 2) Если bulk выключен или для этой биржи/символа данных нет — старый путь ---
            # --- 2) Fallback отключён, если включён bulk ---
            if r is None:
                if use_bulk:
                    # bulk включён → НЕ делаем построчные запросы
                    continue
                # старый fallback, если bulk выключен
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

        # сохраняем локальный порог Z и спреда внутрь best, чтобы отрисовать в карточке
        best["z_in_loc"]       = Z_IN_LOC
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
        
        # --- ECONOMY fallback (bulk candidates иногда приходят без net_usd) ---
        # гарантируем qty_est если его не было
        if best.get("qty_est") is None:
            try:
                px_for_qty = float(best.get("px_low") or 0.0)
                if px_for_qty > 0:
                    best["qty_est"] = per_leg_notional_usd / px_for_qty
            except Exception:
                best["qty_est"] = None

        if best.get("net_usd") is None:
            try:
                px_low_f  = float(best.get("px_low") or 0.0)
                px_high_f = float(best.get("px_high") or 0.0)

                if px_low_f > 0 and px_high_f > 0:
                    qty_est = float(best.get("qty_est") or (per_leg_notional_usd / px_low_f))
                    gross_usd = (px_high_f - px_low_f) * qty_est
                    fees_roundtrip_usd = 4.0 * float(taker_fee) * float(per_leg_notional_usd)
                    net_usd = gross_usd - fees_roundtrip_usd

                    best["qty_est"] = qty_est
                    best["gross_usd"] = gross_usd
                    best["fees_roundtrip_usd"] = fees_roundtrip_usd
                    best["net_usd"] = net_usd
            except Exception as e:
                logging.debug("Economy fallback failed for %s: %s", best.get("symbol"), e)
                best["net_usd"] = None

        # --- net after slippage (safe) ---
        net_usd_adj = None
        try:
            if best.get("net_usd") is not None:
                net_usd_adj = float(best["net_usd"]) - (4.0 * (slip_bps / 1e4) * per_leg_notional_usd)
        except Exception as e:
            logging.debug("net_usd_adj calc failed for %s: %s", best.get("symbol"), e)
            net_usd_adj = None

        best["z"]   = z
        best["std"] = std
        best["net_usd_adj"] = net_usd_adj
        # если stats невалидна — помечаем отдельным флагом
        stats_ok = (std == std) and (std > 0)
        best["stats_ok"] = stats_ok

        if net_usd_adj is None:
            logging.debug(f"[NET_ADJ NONE] {best.get('symbol')} | "
                        f"px_low={best.get('px_low')} "
                        f"px_high={best.get('px_high')} "
                        f"qty_est={best.get('qty_est')} "
                        f"bbo_low={best.get('bbo_low')} "
                        f"bbo_high={best.get('bbo_high')}")
    
        # --- проверки на качество сигнала ---
        std_min_for_open = float(getenv_float("STD_MIN_FOR_OPEN", 1e-4))
        std_ok    = float(best.get("std") or 0.0) >= std_min_for_open
        spread_ok = float(best.get("spread_bps") or 0.0) >= entry_bps
        z_ok      = (z == z) and (z >= Z_IN_LOC)
        # минимальный ожидаемый net в долларах — 1% от CAPITAL
        capital = float(getenv_float("CAPITAL", 1000.0))
        min_net_abs =(float(getenv_float("ENTRY_NET_PCT", 1))/100) * capital  # 1% от капитала
        eco_ok    = (net_usd_adj is not None) and (net_usd_adj > min_net_abs)

        entry_mode_loc = getenv_str("ENTRY_MODE", "price").lower()
        # Открытие делаем только в positions_once (единый путь).
        # Внутри сканера открываемся ТОЛЬКО если явно включили OPEN_IN_SCANNER=1.
        open_in_scanner = getenv_bool("OPEN_IN_SCANNER", False)
        cond_open = (
            open_in_scanner
            and instant_open
            and spread_ok
            and eco_ok
            # в zscore-режиме открываемся по факту валидных z и std
            and (entry_mode_loc != "zscore" or (z_ok and std_ok))
            and (not getenv_bool("RECHECK_Z_AT_OPEN", False) or (z_ok and std_ok))
        )

        # Опциональный детальный лог, почему не открылись (если нужно дебажить)
        if not cond_open and getenv_bool("DEBUG_INSTANT_OPEN", False):
            logging.debug(
                "INSTANT_OPEN SKIP %s | mode=%s eco_ok=%s spread_ok=%s z_ok=%s std_ok=%s "
                "net_usd_adj=%.4f z=%.4f std=%.6f spread_bps=%.2f entry_bps=%.2f Z_IN_LOC=%.2f",
                best.get("symbol"),
                getenv_str("ENTRY_MODE", "price"),
                eco_ok,
                spread_ok,
                z_ok,
                std_ok,
                net_usd_adj,
                z,
                std,
                float(best.get("spread_bps") or 0.0),
                entry_bps,
                Z_IN_LOC,
            )

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
                if "net_usd_adj" not in best or best.get("net_usd_adj") is None:
                    try:
                        best["net_usd_adj"] = float(best.get("net_usd")) - (
                            4.0 * (SLIPPAGE_BPS / 1e4) * per_leg_notional_usd
                        )
                    except Exception:
                        best["net_usd_adj"] = None
                card = format_signal_card(best, per_leg_notional_usd, price_source)
                if INSTANT_ALERT:
                    maybe_send_telegram("🚨 <b>INSTANT ALERT</b>\n" + card)
                    _LAST_ALERT_TS[best["symbol"]] = now

    cols = ["exchange","symbol","bid","ask","mid","last","mark","ts"]
    return pd.DataFrame(rows_all) if rows_all else pd.DataFrame(columns=cols)

def scan_spreads_once(
    exchanges,
    symbols,
    spread_bps_min,
    spread_bps_max,
    per_leg_notional_usd,
    taker_fee,
    pos_path,
    price_stats_path,
    debug: bool = False,
    *,
    price_source: str = "mid",
    alert_spread_pct: float = 0.5,
    cooldown_sec: int = 60,
    instant_open: bool = False,
    pos_path_for_instant: Optional[str] = None,
    paper: bool = True,
    per_ex_symbols: Optional[dict] = None,   # <-- чтобы main не падал
):
    """
    Bulk-оптимизированный сканер:
    1) грузит bulk-котировки со всех бирж
    2) строит quotes_df
    3) считает price-arb кандидатов
    4) подмешивает z-score (если есть stats)
    5) шлёт instant-alert + может открыть сделку
    Возвращает: best_row_dict | None, quotes_df
    """

    # ---------- 1) грузим bulk quotes ----------
    use_bulk = getenv_bool("USE_BULK_QUOTES", True)
    bulk_quotes = load_all_bulk_quotes(exchanges) if use_bulk else {}

    rows_all = []
    now = time.time()

    # ---------- 2) flatten bulk -> quotes_df ----------
    for ex in exchanges:
        ex_l = ex.lower()
        ex_quotes = bulk_quotes.get(ex_l, {}) or {}

        allowed_syms = None
        if per_ex_symbols and ex_l in per_ex_symbols:
            # per_ex_symbols хранит UPPERCASE; приводим так же
            allowed_syms = set(s.upper() for s in per_ex_symbols[ex_l])

        for sym, q in ex_quotes.items():
            sym_u = str(sym).upper()
            if symbols and sym_u not in set(symbols):
                continue
            if allowed_syms is not None and sym_u not in allowed_syms:
                continue

            bid  = to_float(q.get("bid") or q.get("bestBid") or q.get("bidPx") or q.get("b")) or q.get("bestBid1")
            ask  = to_float(q.get("ask") or q.get("bestAsk") or q.get("askPx") or q.get("a")) or q.get("bestAsk1")
            last = to_float(q.get("last"))
            mark = to_float(q.get("mark"))

            mid = None
            if bid is not None and ask is not None and bid > 0 and ask > 0:
                mid = (bid + ask) / 2.0

            rows_all.append({
                "exchange": ex_l,
                "symbol": sym_u,
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "last": last,
                "mark": mark,
                "ts": q.get("ts", now),
            })

    cols = ["exchange", "symbol", "bid", "ask", "mid", "last", "mark", "ts"]
    quotes_df = pd.DataFrame(rows_all, columns=cols)
    if quotes_df.empty:
        return None, quotes_df

    # ---------- 3) price-arb кандидаты ----------
    price_source = (price_source or "mid").lower()
    cands = build_price_arbitrage(quotes_df, per_leg_notional_usd, taker_fee, price_source)

    if cands is None or cands.empty:
        return None, quotes_df

    # фильтр по спреду (на всякий, build_price_arbitrage не всегда режет)
    cands = cands[
        (cands["spread_bps"] >= float(spread_bps_min)) &
        (cands["spread_bps"] <= float(spread_bps_max))
    ].copy()

    if cands.empty:
        return None, quotes_df

    # ---------- 4) подмешиваем z-score из EMA stats ----------
    try:
        stats_df = read_spread_stats(price_stats_path)
    except Exception:
        stats_df = pd.DataFrame(columns=STATS_COLS)

    if not stats_df.empty:
        stats_df = stats_df.copy()
        stats_df["std"] = np.sqrt(stats_df["ema_var"].clip(lower=0.0))
        stats_key = stats_df.set_index(["symbol", "ex_low", "ex_high"])

        z_list, std_list, n_list = [], [], []
        for _, r in cands.iterrows():
            key = (str(r["symbol"]).upper(), str(r["ex_low"]).lower(), str(r["ex_high"]).lower())
            if key in stats_key.index:
                st = stats_key.loc[key]
                mu = float(st["ema_mean"])
                sd = float(st["std"])
                n  = float(st.get("n", 0))
                x  = float(r["log_spread"])
                z  = (x - mu) / sd if sd > 0 else np.nan
                z_list.append(z)
                std_list.append(sd)
                n_list.append(n)
            else:
                z_list.append(np.nan)
                std_list.append(np.nan)
                n_list.append(0)

        cands["z"] = z_list
        cands["std"] = std_list
        cands["n"] = n_list
    else:
        cands["z"] = np.nan
        cands["std"] = np.nan
        cands["n"] = 0

    # ---------- 5) правильный выбор best (как в positions_once) ----------
    entry_mode_loc = getenv_str("ENTRY_MODE", "price").lower()
    if entry_mode_loc == "zscore":
        # гарантируем колонки
        if "z" not in cands.columns:
            cands["z"] = np.nan
        if "std" not in cands.columns:
            cands["std"] = np.nan
        if "net_usd_adj" not in cands.columns:
            cands["net_usd_adj"] = pd.to_numeric(cands.get("net_usd"), errors="coerce")

        # фильтр валидных zscore-сигналов
        Z_IN_LOC = float(getenv_float("Z_IN", 2.0))
        std_min_for_open = float(getenv_float("STD_MIN_FOR_OPEN", 1e-4))
        cands["z"] = pd.to_numeric(cands["z"], errors="coerce")
        cands["std"] = pd.to_numeric(cands["std"], errors="coerce")
        cands["net_usd_adj"] = pd.to_numeric(cands["net_usd_adj"], errors="coerce")

        valid = cands[
            cands["z"].notna() &
            (cands["z"] >= Z_IN_LOC) &
            cands["std"].notna() &
            (cands["std"] >= std_min_for_open)
        ].copy()

        if not valid.empty:
            valid["__score__"] = valid["z"] * 10000 + valid["net_usd_adj"].fillna(0)
            cands = valid.sort_values("__score__", ascending=False).reset_index(drop=True)
        else:
            # fallback по прибыли
            sort_col = "net_usd_adj" if "net_usd_adj" in cands.columns else "net_usd"
            cands = cands.sort_values(sort_col, ascending=False).reset_index(drop=True)
    else:
        sort_col = "net_usd_adj" if "net_usd_adj" in cands.columns else "net_usd"
        cands = cands.sort_values(sort_col, ascending=False).reset_index(drop=True)

    best = None
    if cands is not None and not cands.empty:
        best = cands.iloc[0].to_dict()
    else:
        return None, quotes_df

    # ---- FIX: если z/std NaN в cands, подставляем реальные из get_z_for_pair ----
    if (best.get("z") is None or best.get("z") != best.get("z")) or \
    (best.get("std") is None or best.get("std") != best.get("std")):

        px_low  = float(best.get("px_low")  or 0)
        px_high = float(best.get("px_high") or 0)
        sym = best.get("symbol")
        ex_low = best.get("long_ex")
        ex_high = best.get("short_ex")

        x2, z2, std2 = get_z_for_pair(stats_df, sym, ex_low, ex_high, px_low, px_high)
        best["z"] = z2
        best["std"] = std2

    # ---- has_open: ГЛОБАЛЬНЫЙ флаг — есть ли хотя бы ОДНА открытая позиция ----
    has_open = False
    try:
        df_pos = load_positions(pos_path_for_instant or pos_path)
        if df_pos is not None and not df_pos.empty and "status" in df_pos.columns:
            # считаем, что status in ["open", "closing"] = позиция занята
            has_open = any(df_pos["status"].isin(["open", "closing"]))
    except Exception:
        has_open = False

    # ---- entry filters / cond_open ----
    entry_mode_loc = getenv_str("ENTRY_MODE", "price").lower()
    open_in_scanner = getenv_bool("OPEN_IN_SCANNER", False)
    Z_IN_LOC = float(getenv_float("Z_IN", 2.0))
    std_min_for_open = float(getenv_float("STD_MIN_FOR_OPEN", 1e-4))

    spread_bps = float(best.get("spread_bps") or 0.0)
    net_usd_adj = float(best.get("net_usd_adj") or best.get("net_usd") or 0.0)
    z = to_float(best.get("z"))
    std = to_float(best.get("std"))

    # --- FIX: nan -> None, чтобы фильтры z_ok/std_ok работали корректно ---
    try:
        if z is not None and isinstance(z, float) and np.isnan(z):
            z = None
        if std is not None and isinstance(std, float) and np.isnan(std):
            std = None
    except Exception:
        pass

    # --- LAZY-FIX: если z/std так и остались None, пересчитываем их как в карточке ---
    if (z is None) or (std is None):
        try:
            stats_df2 = read_spread_stats()
            px_low = float(best.get("px_low") or 0.0)
            px_high = float(best.get("px_high") or 0.0)
            sym = str(best.get("symbol") or "")
            ex_low = str(best.get("long_ex") or best.get("cheap_ex") or "").lower()
            ex_high = str(best.get("short_ex") or best.get("rich_ex") or "").lower()

            if sym and px_low > 0 and px_high > 0 and ex_low and ex_high:
                _, z2, std2 = get_z_for_pair(
                    stats_df2,
                    symbol=sym,
                    ex_low=ex_low,
                    ex_high=ex_high,
                    px_low=px_low,
                    px_high=px_high,
                )
                if z2 == z2:   # not NaN
                    z = float(z2)
                if std2 == std2:
                    std = float(std2)
        except Exception:
            pass

    # держим best синхронизированным с очищенными/пересчитанными значениями
    best["z"] = z
    best["std"] = std

    # --- LAZY-FIX: если z/std так и остались None, пересчитываем их как в карточке ---
    if (z is None) or (std is None):
        try:
            stats_df2 = read_spread_stats()
            px_low = float(best.get("px_low") or 0.0)
            px_high = float(best.get("px_high") or 0.0)
            sym = str(best.get("symbol") or "")
            ex_low = str(best.get("long_ex") or best.get("cheap_ex") or "").lower()
            ex_high = str(best.get("short_ex") or best.get("rich_ex") or "").lower()

            if sym and px_low > 0 and px_high > 0 and ex_low and ex_high:
                _, z2, std2 = get_z_for_pair(
                    stats_df2,
                    symbol=sym,
                    ex_low=ex_low,
                    ex_high=ex_high,
                    px_low=px_low,
                    px_high=px_high,
                )
                if z2 == z2:   # not NaN
                    z = float(z2)
                if std2 == std2:
                    std = float(std2)
        except Exception:
            pass

    # держим best синхронизированным с очищенными/пересчитанными значениями
    best["z"] = z
    best["std"] = std

    spread_ok = spread_bps >= float(spread_bps_min)
    eco_ok = net_usd_adj > 0
    z_ok = (z is not None) and (z >= Z_IN_LOC)
    std_ok = (std is not None) and (std >= std_min_for_open)

    cond_open = (
        open_in_scanner
        and instant_open
        and (not has_open)
        and spread_ok
        and eco_ok
        and (entry_mode_loc != "zscore" or (z_ok and std_ok))
        and (not getenv_bool("RECHECK_Z_AT_OPEN", False) or (z_ok and std_ok))
    )

    # ---- Telegram card with debug ----
    if best is not None:
        card = format_signal_card(best, per_leg_notional_usd, price_source)
        card += (
            f"\n🧪 *INSTANT-OPEN DEBUG*\n"
            f"• cond_open = `{cond_open}`\n"
            f"• has_open = `{has_open}`\n"
            f"• ENTRY_MODE = `{entry_mode_loc}`\n"
           f"• Z_IN = `{Z_IN_LOC}`\n"
            f"• std_min = `{std_min_for_open}`\n"
            f"• spread_ok = `{spread_ok}` ({spread_bps:.1f} ≥ {float(spread_bps_min):.1f})\n"
            f"• eco_ok = `{eco_ok}` (net_adj={net_usd_adj:.4f})\n"
            f"• z_ok = `{z_ok}` (z={z})\n"
            f"• std_ok = `{std_ok}` (std={std})\n"
        )
        maybe_send_telegram(card)

    # ---- instant open ----
    if cond_open:
        _pos_path = pos_path_for_instant or bucketize_path(getenv_str("POS_CROSS_PATH", "positions_price_cross.csv"))
        _paper    = bool(getenv_bool("PAPER", True)) if paper is None else bool(paper)
        try:
            _ = try_instant_open(best, per_leg_notional_usd, taker_fee, _paper, _pos_path)
        except Exception as e:
            # ошибку открытия try_instant_open сам покажет в TG при DEBUG_INSTANT_OPEN
            logging.exception("[OPEN] instant open failed: %s", e)

    return best, quotes_df

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
    except Exception:
        logging.exception("binance_sync_time failed")
        # не роняем бот, просто оставляем старый оффсет
        return

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

# ----------------- Client order ID helpers -----------------
def normalize_okx_clordid(cloid: str) -> str:
    """
    OKX: clOrdId должен быть 1-32 символа, только буквы/цифры/нижнее подчёркивание.
    Убираем запрещённые символы и обрезаем до 32.
    """
    import re
    if not cloid:
        return cloid
    s = re.sub(r'[^A-Za-z0-9_]', '', str(cloid))
    return s[:32]

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
    except Exception:
        logging.exception("_bybit_symbol_info failed for %s", symbol)
        # не роняем торговлю — просто вернём кеш/None
        return None
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
            try:
                px = float((j.get("fills") or [{}])[0].get("price") or 0.0)
            except Exception:
                logging.exception("Binance fills price parse failed (will continue)")
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
                except Exception:
                    logging.exception("Bybit avgPrice parse failed (will continue)")
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
            except Exception:
                logging.exception("Bybit IOC avgPrice parse failed (will continue)")
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
            body_dict["clOrdId"] = normalize_okx_clordid(cl_oid)

        body = json.dumps(body_dict, separators=(",", ":"))

        # sign: ts + method + path + body
        path = "/api/v5/trade/order"
        method = "POST"
        ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        prehash = ts + method + path + body
        sign = base64.b64encode(
            hmac.new(sec.encode(), prehash.encode(), hashlib.sha256).digest()
        ).decode()

        # demo/testnet для OKX определяется заголовком x-simulated-trading
        sim_flag = "1" if (_is_true("OKX_TESTNET", False) or _is_true("OKX_PAPER", False)) else "0"

        headers = {
            "OK-ACCESS-KEY": key,
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": passphrase,
            "Content-Type": "application/json",
            "x-simulated-trading": sim_flag,
        }

        url = f"{base}{path}"
        logging.info(
            "[OKX] NEW ORDER instId=%s side=%s qty=%s tdMode=%s sim=%s",
            inst_id, side.upper(), qty, td_mode, sim_flag,
        )
        logging.debug("[OKX] Request body: %s", body)

        r = SESSION.post(url, headers=headers, data=body, timeout=REQUEST_TIMEOUT)
        try:
            j = r.json()
        except Exception:
            logging.error("OKX order: non-JSON response http=%s text=%s", r.status_code, r.text[:400])
            raise RuntimeError(f"OKX order failed: http={r.status_code}, non-JSON response")

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

        # В Gate size — ЦЕЛОЕ число контрактов (int64), >0 = long, <0 = short
        raw_qty = float(qty)
        if raw_qty <= 0:
            raise RuntimeError(f"Gate: qty={raw_qty} <= 0")

        # Gate не принимает дробный size, только целые контракты
        size = int(raw_qty)
        if size == 0:
            size = 1
        if side.upper() == "SELL":
            size = -size

        body_dict = {
            "contract": contract,
            "size": size,
            "iceberg": 0,
            "price": "0",   # market через price=0 + tif=ioc
            "tif": "ioc",
        }

        if reduce_only:
            body_dict["reduce_only"] = True
        if cl_oid:
            # Gate требует, чтобы text начинался с 't-' и был коротким
            txt = str(cl_oid)
            if not txt.startswith("t-"):
                txt = "t-" + txt[:26]  # итого максимум ~28 символов
            body_dict["text"] = txt

        body = json.dumps(body_dict, separators=(",", ":"))

        ts = str(int(time.time()))
        body_hash = hashlib.sha512(body.encode()).hexdigest()
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

        # Gate при успешном создании ордера возвращает 201
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Gate order HTTP {r.status_code}: {r.text[:400]}")

        j = r.json()
        # Ошибки Gate формата {"label":"INVALID_SIGNATURE","message":"."}
        if isinstance(j, dict) and j.get("label"):
            raise RuntimeError(
                f"Gate order failed: label={j.get('label')} msg={j.get('message')} resp={str(j)[:400]}"
            )

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

        # --- Фильтр по минимальной цене монеты (из .env MIN_PRICE) ---
        # Отсекаем ультрадешёвые тикеры, чтобы не получать десятки миллионов контрактов.
        min_price = float(getenv_float("MIN_PRICE", 0.0))
        if min_price > 0.0 and (ask_px < min_price or bid_px < min_price):
           # цена монеты ниже допустимого порога — пару вообще не строим
            return None

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

    # --- Тот же фильтр MIN_PRICE для не-book режимов (mid/last/mark/bid/ask) ---
    min_price = float(getenv_float("MIN_PRICE", 0.0))
    if min_price > 0.0 and (px_low < min_price or px_high < min_price):
        # слишком дешёвая монета — сразу выходим, она нам не нужна
        return None

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

def _coalesce_cols(df: pd.DataFrame, targets: List[str], candidates: List[str]) -> pd.DataFrame:
     """
     Берёт первый существующий столбец из candidates и кладёт в targets[0].
     Если targets уже есть — не трогаем.
     """
     if df is None or df.empty:
         return df
     t = targets[0]
     if t in df.columns:
         return df
     for c in candidates:
         if c in df.columns:
             df[t] = df[c]
             return df
     return df

def build_price_arbitrage(
    df_raw: pd.DataFrame,
    per_leg_notional_usd: float,
    taker_fee: float,
    price_source: str
) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()

    use = df_raw.copy()
    ps = (price_source or "mid").lower()

    # --- FIX: гарантируем наличие bid/ask для book-режима ---
    # 1) нормализуем возможные названия из bulk
    use = _coalesce_cols(use, ["ask"], ["ask", "bestAsk", "askPrice", "a", "sell", "offer", "ask_px"])
    use = _coalesce_cols(use, ["bid"], ["bid", "bestBid", "bidPrice", "b", "buy", "bid_px"])

    # 2) если после коалесса всё равно нет — создаём
    for col in ("bid", "ask"):
        if col not in use.columns:
            use[col] = np.nan

    # 3) мягкое восстановление из mid/px если bulk вернул только их
    if use["ask"].isna().all() and "mid" in use.columns:
        use["ask"] = pd.to_numeric(use["mid"], errors="coerce")
    if use["bid"].isna().all() and "mid" in use.columns:
        use["bid"] = pd.to_numeric(use["mid"], errors="coerce")

    if use["ask"].isna().all() and "px" in use.columns:
        use["ask"] = pd.to_numeric(use["px"], errors="coerce")
    if use["bid"].isna().all() and "px" in use.columns:
        use["bid"] = pd.to_numeric(use["px"], errors="coerce")

    # 4) если bid/ask всё ещё пустые — логируем один раз на цикл
    if use["ask"].isna().all() or use["bid"].isna().all():
        logging.warning(
            "build_price_arbitrage(book): bid/ask still empty after fallbacks. cols=%s",
            list(use.columns)
        )

    # обычные режимы: строим px
    if ps != "book":
        use["px"] = use.apply(lambda r: select_px(r, price_source), axis=1)
        use = use.dropna(subset=["px"])

    out_rows = []
    for sym in sorted(use["symbol"].unique()):
        sub = use[use["symbol"] == sym]

        if ps == "book":
            # защита от отсутствующих колонок/NaN
            if "ask" not in sub.columns or "bid" not in sub.columns:
                continue

            # безопасно: если вдруг нет колонок — не падаем
            try:
                sub_ask = sub.dropna(subset=["ask"]).copy()
                sub_bid = sub.dropna(subset=["bid"]).copy()
            except KeyError:
                # на всякий случай, если pandas всё-таки не видит ask/bid
                logging.warning(
                    "build_price_arbitrage(book): missing ask/bid in sub for %s. sub_cols=%s",
                    sym, list(sub.columns)
                )
                continue
            if sub_ask.empty or sub_bid.empty:
                continue

            sub_ask["ask"] = pd.to_numeric(sub_ask["ask"], errors="coerce")
            sub_bid["bid"] = pd.to_numeric(sub_bid["bid"], errors="coerce")
            sub_ask = sub_ask.dropna(subset=["ask"])
            sub_bid = sub_bid.dropna(subset=["bid"])
            if sub_ask.empty or sub_bid.empty:
                continue

            cheapest = sub_ask.loc[sub_ask["ask"].idxmin()]
            priciest = sub_bid.loc[sub_bid["bid"].idxmax()]

            if str(cheapest["exchange"]) == str(priciest["exchange"]):
                continue

            px_low, ex_low   = float(cheapest["ask"]), str(cheapest["exchange"])
            px_high, ex_high = float(priciest["bid"]), str(priciest["exchange"])

        else:
            if len(sub) < 2:
                continue
            cheapest = sub.loc[sub["px"].idxmin()]
            priciest = sub.loc[sub["px"].idxmax()]
            px_low, ex_low   = float(cheapest["px"]), str(cheapest["exchange"])
            px_high, ex_high = float(priciest["px"]), str(priciest["exchange"])

        spread = px_high - px_low
        spread_pct = (spread / px_low) * 100.0 if px_low > 0 else 0.0
        spread_bps = spread_pct * 100.0

        qty = per_leg_notional_usd / px_low if px_low > 0 else 0.0
        gross = spread * qty
        fees_rt = 4.0 * taker_fee * per_leg_notional_usd
        net = gross - fees_rt

        out_rows.append({
            "symbol": sym,
            "long_ex": ex_low,
            "short_ex": ex_high,
            "px_low": px_low,
            "px_high": px_high,
            "spread": spread,
            "spread_pct": spread_pct,
            "spread_bps": spread_bps,
            "qty_est": qty,
            "gross_usd": gross,
            "fees_roundtrip_usd": fees_rt,
            "net_usd": net,
        })

    out = pd.DataFrame(out_rows)
    if not out.empty:
        out = out.sort_values(["net_usd", "spread_bps"], ascending=[False, False]).reset_index(drop=True)
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
    df = read_csv(SPREAD_STATS_PATH, STATS_COLS)
    if df is None or df.empty:
        return pd.DataFrame(columns=STATS_COLS)

    # нормализация регистров — ключевой фикс
    try:
        df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
        df["ex_low"]  = df["ex_low"].astype(str).str.lower().str.strip()
        df["ex_high"] = df["ex_high"].astype(str).str.lower().str.strip()
    except Exception as e:
        logging.warning(f"[STATS] normalization error: {e}")

    return df

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
        # --- CLEANUP: удаляем пустые/битые строки из spread_stats ---
        if self.df is not None and not self.df.empty:
            for c in ["symbol", "ex_low", "ex_high"]:
                if c in self.df.columns:
                    self.df[c] = self.df[c].astype("string").fillna("").str.strip()

            self.df = self.df[
                (self.df["symbol"] != "") &
                (self.df["ex_low"] != "") &
                (self.df["ex_high"] != "")
            ].reset_index(drop=True)
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

    def update_pair(self, symbol: str, ex_low: str, ex_high: str, x: float, now_ms: int | None = None):
        """
        Обновить (или создать) строку статистики для пары symbol/ex_low/ex_high.
        x = log-spread (в долях), например (high/low - 1).
        """
        if now_ms is None:
            now_ms = int(time.time() * 1000)

        symbol_u = (symbol or "").upper()
        ex_low_l = (ex_low or "").lower()
        ex_high_l = (ex_high or "").lower()

        if not math.isfinite(x):
            return

        # гарантируем колонки
        if self.df is None or self.df.empty:
            self.df = pd.DataFrame(columns=STATS_COLS)

        mask = (
            (self.df["symbol"].astype(str).str.upper() == symbol_u) &
            (self.df["ex_low"].astype(str).str.lower() == ex_low_l) &
            (self.df["ex_high"].astype(str).str.lower() == ex_high_l)
        )
        sub = self.df[mask]

        if not sub.empty:
            i = int(sub.index[0])

            old_mean = to_float(self.df.at[i, "ema_mean"]) or 0.0
            old_var  = to_float(self.df.at[i, "ema_var"])  or 0.0
            old_cnt  = to_float(self.df.at[i, "count"])    or 0.0

            # EMA mean
            new_mean = (1.0 - ALPHA) * old_mean + ALPHA * x

            # EMA variance (простая и стабильная формула)
            dx = x - old_mean
            new_var = (1.0 - ALPHA) * old_var + ALPHA * (dx * dx)

            self.df.at[i, "ema_mean"] = new_mean
            self.df.at[i, "ema_var"]  = max(new_var, 0.0)
            self.df.at[i, "count"]    = old_cnt + 1.0
            self.df.at[i, "updated_ms"] = now_ms

            # обновляем рекомендации
            self._recompute_reco(i)

        else:
            # ---- СОЗДАНИЕ НОВОЙ СТРОКИ ----
            row = {
                "symbol": symbol_u,
                "ex_low": ex_low_l,
                "ex_high": ex_high_l,
                "ema_mean": float(x),
                "ema_var": 0.0,
                "count": 1.0,
                "updated_ms": now_ms,

                # поля рекомендаций — заполним ниже
                "mean_bps": np.nan,
                "std_bps": np.nan,
                "required_spread_bps": np.nan,
                "z_req_profit": np.nan,
                "Z_IN_suggested": np.nan,
                "entry_spread_bps_suggested": np.nan,
            }

            self.df = pd.concat([self.df, pd.DataFrame([row], columns=STATS_COLS)], ignore_index=True)

            i = int(self.df.index[-1])
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
      по умолчанию:
        cheap_ex -> BUY (LONG)
        rich_ex  -> SELL (SHORT)

      если REVERSE_SIDE=1:
        cheap_ex -> SELL (SHORT)
        rich_ex  -> BUY  (LONG)

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

    # Реверс сторон по флагу REVERSE_SIDE (0/1)
    reverse_side = getenv_bool("REVERSE_SIDE", False)

    side_a_open = "BUY"
    side_b_open = "SELL"
    if reverse_side:
        # тупой реверс: где раньше покупали — теперь продаём, и наоборот
        side_a_open, side_b_open = "SELL", "BUY"

    # client IDs для ног открытия
    cl_open_long  = _gen_cloid("OPENA", attempt_id, "A")
    cl_open_short = _gen_cloid("OPENB", attempt_id, "B")

    try:
        # Нога A на дешёвой бирже
        oa = _place_perp_market_order(
            cheap_ex, symbol, side_a_open, qty_final,
            paper=paper, cl_oid=cl_open_long, reduce_only=False
        )
        if oa.get("status") != "FILLED":
            raise RuntimeError(f"legA not filled: {oa}")
        # --- NEW: фиксируем реальную цену открытия LONG ---
        open_long_px = float(oa.get("avg_price") or 0.0)
        if open_long_px <= 0:
            # avg_price не пришёл => берём актуальный BBO ПОСЛЕ фактического открытия
            bbo = get_bbo(symbol, cheap_ex)
            if isinstance(bbo, tuple):
                b, a = bbo
            elif isinstance(bbo, dict):
                b = bbo.get("bid")
                a = bbo.get("ask")
            else:
                b = a = None
            open_long_px = float(a or b or 0.0)

    except Exception as e:
        return False, attempt_id, {"error": f"legA error: {e}"}

    try:
        # Нога B на дорогой бирже
        ob = _place_perp_market_order(
            rich_ex, symbol, side_b_open, qty_final,
            paper=paper, cl_oid=cl_open_short, reduce_only=False
        )
        if ob.get("status") != "FILLED":
            # откат первой ноги
            rollback_failed = False
            rollback_err: Optional[Exception] = None
            try:
                _ = _place_perp_market_order(
                    cheap_ex, symbol, "SELL", qty_final,
                    paper=paper, reduce_only=True
                )
            except Exception as e2:
                rollback_failed = True
                rollback_err = e2
                logging.error("Rollback failed: %s", e2)

            meta_err: dict = {"error": f"legB not filled: {ob}"}
            if rollback_failed:
                meta_err.update({
                    "rollback_failed": True,
                    "rollback_ex": cheap_ex,
                    "rollback_symbol": symbol,
                    "rollback_qty": float(qty_final),
                    "rollback_error": str(rollback_err),
                })
            return False, attempt_id, meta_err
        
        if ob.get("status") != "FILLED":
            raise RuntimeError(f"legB not filled: {ob}")
        # --- NEW: фиксируем реальную цену открытия SHORT ---
        open_short_px = float(ob.get("avg_price") or 0.0)
        if open_short_px <= 0:
            bbo = get_bbo(symbol, rich_ex)
            if isinstance(bbo, tuple):
                b, a = bbo
            elif isinstance(bbo, dict):
                b = bbo.get("bid")
                a = bbo.get("ask")
            else:
                b = a = None
            open_short_px = float(b or a or 0.0)

    except Exception as e:
        # откат первой ноги (продаём то, что купили)
        rollback_failed = False
        rollback_err: Optional[Exception] = None
        try:
            _ = _place_perp_market_order(
                cheap_ex, symbol, "SELL", qty_final,
                paper=paper, reduce_only=True
            )
        except Exception as e2:
            rollback_failed = True
            rollback_err = e2
            logging.error("Rollback failed: %s", e2)

        meta_err: dict = {"error": f"legB error: {e}"}
        if rollback_failed:
            meta_err.update({
                "rollback_failed": True,
                "rollback_ex": cheap_ex,
                "rollback_symbol": symbol,
                "rollback_qty": float(qty_final),
                "rollback_error": str(rollback_err),
            })
        return False, attempt_id, meta_err

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
    # --- NEW: сразу подтягиваем реальные fills и переписываем open_*_px ---
    try:
        time.sleep(float(getenv_float("POST_OPEN_FILL_SLEEP", 0.25)))
        lookback_ms = int(getenv_float("FILL_LOOKBACK_MS", 180_000))
        start_ms_for_trades = utc_ms_now() - int(getenv_float("FILL_LOOKBACK_MS", 180_000))

        sum_long = summarize_fills(
            cheap_ex, symbol, "BUY",
            meta.get("open_long_order_id",""),
            meta.get("open_long_cloid",""),
            None, None, None,
            start_ms_for_trades
        )
        sum_short = summarize_fills(
            rich_ex, symbol, "SELL",
            meta.get("open_short_order_id",""),
            meta.get("open_short_cloid",""),
            None, None, None,
            start_ms_for_trades
        )

        if sum_long.get("avg_open_px"):
            meta["open_long_px"] = float(sum_long["avg_open_px"])
        if sum_short.get("avg_open_px"):
            meta["open_short_px"] = float(sum_short["avg_open_px"])

        meta["open_fees_usd"] = float(meta.get("open_fees_usd", 0.0)) \
                                + float(sum_long.get("fees_open_usd", 0.0)) \
                                + float(sum_short.get("fees_open_usd", 0.0))

    except Exception as e:
        logging.debug("post-open fill fetch skipped: %s", e)

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

def okx_exec_list(symbol: str,
                  order_id: str | None = None,
                  order_link_id: str | None = None,
                  start_ms: int | None = None) -> list[dict]:
    """
    OKX executions helper.
    Берём трейды из /api/v5/trade/fills-history по instId (SWAP),
    фильтруем по времени и ordId/clOrdId.
    """
    key = os.getenv("OKX_API_KEY") or ""
    sec = os.getenv("OKX_API_SECRET") or ""
    pph = os.getenv("OKX_PASSPHRASE") or ""
    if not key or not sec or not pph:
        logging.warning("okx_exec_list: missing OKX API keys")
        return []

    base = okx_base()
    inst_id = okx_inst_from_symbol(symbol)

    path = "/api/v5/trade/fills-history"
    params = {
        "instType": "SWAP",
        "instId": inst_id,
        "limit": "100",
    }
    query = "&".join(f"{k}={v}" for k, v in params.items())
    req_path = f"{path}?{query}"

    method = "GET"
    # OKX требует ISO-8601 в UTC
    ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    prehash = ts + method + req_path

    sign = base64.b64encode(
        hmac.new(sec.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
    ).decode()

    sim_flag = "1" if (_is_true("OKX_TESTNET", False) or _is_true("OKX_PAPER", False)) else "0"

    headers = {
        "OK-ACCESS-KEY": key,
        "OK-ACCESS-SIGN": sign,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": pph,
        "OK-ACCESS-PROJECT": os.getenv("OKX_PROJECT", ""),
        "x-simulated-trading": sim_flag,
    }

    url = f"{base}{req_path}"
    try:
        r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    except Exception:
        logging.exception("okx_exec_list: request failed")
        return []

    try:
        j = r.json()
    except Exception:
        logging.exception("okx_exec_list: bad JSON, status=%s body=%s", r.status_code, r.text[:500])
        return []

    if r.status_code != 200 or str(j.get("code")) not in ("0", "None", "null"):
        logging.warning("okx_exec_list: error code=%s msg=%s", j.get("code"), j.get("msg"))
        data = j.get("data") or []
    else:
        data = j.get("data") or []

    res: list[dict] = []
    for t in data:
        try:
            ts_ms = int(t.get("ts", 0))
        except Exception:
            ts_ms = 0

        if start_ms and ts_ms < start_ms:
            continue
        if order_id and str(t.get("ordId")) != str(order_id):
            continue
        if order_link_id and t.get("clOrdId") != str(order_link_id):
            continue
        res.append(t)

    return res

def gate_exec_list(symbol: str,
                   order_id: str | None = None,
                   client_order_id: str | None = None,
                   start_ms: int | None = None) -> list[dict]:
    """
    Gate futures user trades: /api/v4/futures/usdt/user_trades
    Фильтруем по contract, времени, order_id и text (clientOrderId).
    """
    key = os.getenv("GATE_API_KEY") or ""
    sec = os.getenv("GATE_API_SECRET") or ""
    if not key or not sec:
        logging.warning("gate_exec_list: missing Gate API keys")
        return []

    # Фьючи контракт: BTCUSDT -> BTC_USDT
    contract = symbol.upper()
    if contract.endswith("USDT") and "_" not in contract:
        contract = contract.replace("USDT", "_USDT")

    base = gate_base()
    path = "/api/v4/futures/usdt/user_trades"

    params = {
        "contract": contract,
        "limit": "100",
    }
    query = "&".join(f"{k}={v}" for k, v in params.items())

    method = "GET"
    body = ""
    body_hash = hashlib.sha512(body.encode("utf-8")).hexdigest()
    ts = str(int(time.time()))

    msg = "\n".join([method, path, query, body_hash, ts])
    sign = _hmac_sha512_hex(sec, msg)

    headers = {
        "KEY": key,
        "Timestamp": ts,
        "SIGN": sign,
    }

    url = f"{base}{path}?{query}"
    try:
        r = SESSION.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    except Exception:
        logging.exception("gate_exec_list: request failed")
        return []

    try:
        data = r.json()
    except Exception:
        logging.exception("gate_exec_list: bad JSON, status=%s body=%s", r.status_code, r.text[:500])
        return []

    # user_trades возвращает список, но на всякий случай нормализуем
    if isinstance(data, dict):
        # Ошибка от API
        if "label" in data or "message" in data:
            logging.warning("gate_exec_list: error %s", data)
            return []
        trades = data.get("trades") or []
    else:
        trades = data

    res: list[dict] = []
    for t in trades:
        try:
            ts_ms = int(float(t.get("create_time_ms", 0)))
        except Exception:
            ts_ms = 0

        if start_ms and ts_ms < start_ms:
            continue
        if order_id and str(t.get("order_id")) != str(order_id):
            continue
        if client_order_id and t.get("text") != str(client_order_id):
            continue
        res.append(t)

    return res

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

def summarize_fills(exchange: str, symbol: str,
                    open_side: str, open_oid: str | None, open_cloid: str | None,
                    close_side: str, close_oid: str | None, close_cloid: str | None,
                    start_ms: int) -> dict:
    """
    Возвращает:
      avg_open_px, avg_close_px, fees_open_usd, fees_close_usd
    Работает для: BINANCE, BYBIT, OKX, GATE.
    """

    ex = exchange.lower()
    avg_open = avg_close = 0.0
    fees_open = fees_close = 0.0

    # ------------------------------
    # BINANCE
    # ------------------------------
    if ex == "binance":
        trades = binance_user_trades(symbol, start_ms)

        open_tr  = [t for t in trades if (
            str(t.get("orderId")) == str(open_oid)
            or (open_cloid and t.get("clientOrderId") == open_cloid)
        )]

        close_tr = [t for t in trades if (
            str(t.get("orderId")) == str(close_oid)
            or (close_cloid and t.get("clientOrderId") == close_cloid)
        )]

        def _avg(trs):
            px = [float(t["price"]) for t in trs]
            q  = [float(t["qty"])   for t in trs]
            return _wavg(px, q)

        avg_open  = _avg(open_tr)  or 0.0
        avg_close = _avg(close_tr) or 0.0

        fees_open  = _sum([float(t.get("commission", 0)) for t in open_tr])
        fees_close = _sum([float(t.get("commission", 0)) for t in close_tr])

    # ------------------------------
    # BYBIT
    # ------------------------------
    elif ex == "bybit":
        open_tr  = bybit_exec_list(symbol, order_id=open_oid,  order_link_id=open_cloid,  start_ms=start_ms)
        close_tr = bybit_exec_list(symbol, order_id=close_oid, order_link_id=close_cloid, start_ms=start_ms)

        def _avg_bybit(trs):
            px = [float(t.get("execPrice", 0)) for t in trs]
            q  = [float(t.get("execQty", 0))   for t in trs]
            return _wavg(px, q)

        avg_open  = _avg_bybit(open_tr)  or 0.0
        avg_close = _avg_bybit(close_tr) or 0.0

        fees_open  = _sum([float(t.get("execFee", 0)) for t in open_tr])
        fees_close = _sum([float(t.get("execFee", 0)) for t in close_tr])

    # ------------------------------
    # OKX
    # ------------------------------
    elif ex == "okx":
        # OKX returns: fillPx, fillSz, fillFee, side, ordId, clOrdId
        # ВАЖНО: для открытия используем open_oid/open_cloid, для закрытия — close_oid/close_cloid
        open_tr  = okx_exec_list(symbol, order_id=open_oid,  order_link_id=open_cloid,  start_ms=start_ms)
        close_tr = okx_exec_list(symbol, order_id=close_oid, order_link_id=close_cloid, start_ms=start_ms)

        def _avg_okx(trs):
            px = [float(t.get("fillPx", 0)) for t in trs]
            q  = [abs(float(t.get("fillSz", 0))) for t in trs]
            return _wavg(px, q)

        avg_open  = _avg_okx(open_tr)  or 0.0
        avg_close = _avg_okx(close_tr) or 0.0

        fees_open  = _sum([abs(float(t.get("fillFee", 0))) for t in open_tr])
        fees_close = _sum([abs(float(t.get("fillFee", 0))) for t in close_tr])

    # ------------------------------
    # GATE
    # ------------------------------
    elif ex == "gate":
        # Gate futures trades: size, price, fee, order_id, text=clientOrderId
        open_tr  = gate_exec_list(symbol, order_id=open_oid,  order_link_id=open_cloid,  start_ms=start_ms)
        close_tr = gate_exec_list(symbol, order_id=close_oid, order_link_id=close_cloid, start_ms=start_ms)

        def _avg_gate(trs):
            px = [float(t.get("price", 0)) for t in trs]
            q  = [abs(float(t.get("size", 0))) for t in trs]
            return _wavg(px, q)

        avg_open  = _avg_gate(open_tr)  or 0.0
        avg_close = _avg_gate(close_tr) or 0.0

        fees_open  = _sum([abs(float(t.get("fee", 0))) for t in open_tr])
        fees_close = _sum([abs(float(t.get("fee", 0))) for t in close_tr])

    # ------------------------------
    # RESULT
    # ------------------------------
    return {
        "avg_open_px": avg_open,
        "avg_close_px": avg_close,
        "fees_open_usd": fees_open,
        "fees_close_usd": fees_close
    }

# ----------------- Positions -----------------
POS_COLS = ["id","attempt_id","symbol","long_ex","short_ex","opened_ms","last_ms","held_h",
            "size_usd","entry_spread_bps","entry_z","entry_px_low","entry_px_high",
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
        "id","opened_ms","last_ms","held_h","size_usd",
        "entry_spread_bps","entry_z",
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
    try:
        price_source = getenv_str("PRICE_SOURCE", "mid")
        cands = build_price_arbitrage(quotes_df, per_leg_notional_usd, taker_fee, price_source)
        logging.info(
            "[CYCLE] positions_once: quotes=%s cands=%s price_source=%s ENTRY_MODE=%s top3=%s",
            0 if quotes_df is None else len(quotes_df),
            0 if cands is None else len(cands),
            price_source, getenv_str("ENTRY_MODE", "zscore"), top3_to_tg
        )
    except Exception:
        logging.exception("[CYCLE] positions_once failed at build_price_arbitrage")
        return

    # ------------------------------
    # 1) квалификация (ema/std/z)
    # ------------------------------
    if stats_df is not None and not stats_df.empty:
        try:
            stats = stats_df.copy()
            # нормализуем названия колонок, если нужно
            for col in ["ex_low", "ex_high"]:
                if col not in stats.columns and col.replace("ex_", "") in stats.columns:
                    stats[col] = stats[col.replace("ex_", "")]

            # В spread_stats.csv реальные поля: ema_mean, ema_var, count, updated_ms.
            # Старых ema_spread_bps/std_spread_bps/zscore может не быть → делаем совместимо.
            base_cols = ["symbol", "ex_low", "ex_high"]
            opt_cols = []
            for oc in ["ema_spread_bps", "std_spread_bps", "zscore", "ema_mean", "ema_var", "count", "updated_ms"]:
                if oc in stats.columns:
                    opt_cols.append(oc)
            qual = (
                stats[base_cols + opt_cols]
                .drop_duplicates()
                .rename(columns={"ex_low": "long_ex", "ex_high": "short_ex"})
            )

            # если нет готовых bps/zscore — оценим из log-спреда
            if "ema_spread_bps" not in qual.columns and "ema_mean" in qual.columns:
                qual["ema_bps"] = pd.to_numeric(qual["ema_mean"], errors="coerce") * 1e4
            elif "ema_spread_bps" in qual.columns:
                qual["ema_bps"] = pd.to_numeric(qual["ema_spread_bps"], errors="coerce")

            if "std_spread_bps" not in qual.columns and "ema_var" in qual.columns:
                qual["std_bps"] = np.sqrt(pd.to_numeric(qual["ema_var"], errors="coerce").clip(lower=0.0)) * 1e4
            elif "std_spread_bps" in qual.columns:
                qual["std_bps"] = pd.to_numeric(qual["std_spread_bps"], errors="coerce")

            if "zscore" in qual.columns:
                qual["z"] = pd.to_numeric(qual["zscore"], errors="coerce")

            # мерджим только то, что реально посчитали
            keep = ["symbol", "long_ex", "short_ex"]
            for k in ["ema_bps", "std_bps", "z", "count", "ema_var", "updated_ms"]:
                if k in qual.columns:
                    keep.append(k)
            qual = qual[keep]

            cands = cands.merge(qual, on=["symbol", "long_ex", "short_ex"], how="left")
        except Exception as e:
            logging.debug("positions_once: merge stats failed: %s", e)

    # ------------------------------
    # 2) топ кандидатов + TG
    # ------------------------------
    if cands is None or cands.empty:
        cands = pd.DataFrame(columns=[
            "symbol","long_ex","short_ex","px_low","px_high",
            "spread_bps","net_usd","fees_usd","gross_usd",
            "ema_bps","std_bps","z"
        ])

    entry_mode_loc = getenv_str("ENTRY_MODE", "price").lower()

    if entry_mode_loc == "zscore":
        # --- FIX: гарантируем наличие z/std/net_usd_adj и алиасы из stats ---
        if "z" not in cands.columns:
           cands["z"] = np.nan
        # stats-мердж кладёт std_bps/ema_bps → делаем алиасы под try_instant_open
        if "std" not in cands.columns:
            if "std_bps" in cands.columns:
                cands["std"] = cands["std_bps"]
            else:
                cands["std"] = np.nan
        if "ema" not in cands.columns and "ema_bps" in cands.columns:
            cands["ema"] = cands["ema_bps"]
        if "net_usd_adj" not in cands.columns:
            cands["net_usd_adj"] = pd.to_numeric(cands.get("net_usd"), errors="coerce")

        # --- NEW: считаем те же флаги, что и try_instant_open, но для всех строк cands ---
        Z_IN_LOC = float(getenv_float("Z_IN", 2.0))
        std_min_for_open = float(getenv_float("STD_MIN_FOR_OPEN", 1e-4))
        capital = float(getenv_float("CAPITAL", 1000.0))
        min_net_abs = (float(getenv_float("ENTRY_NET_PCT", 1.0))/100.0) * capital

        cands["spread_bps"]  = pd.to_numeric(cands.get("spread_bps"), errors="coerce")
        cands["z"]           = pd.to_numeric(cands.get("z"), errors="coerce")
        cands["std"]         = pd.to_numeric(cands.get("std"), errors="coerce")
        cands["net_usd_adj"] = pd.to_numeric(cands.get("net_usd_adj"), errors="coerce")

        cands["spread_ok"] = cands["spread_bps"] >= float(entry_bps)
        cands["std_ok"]    = cands["std"].notna() & (cands["std"] >= std_min_for_open)
        cands["z_ok"]      = cands["z"].notna() & (cands["z"] >= Z_IN_LOC)
        cands["eco_ok"]    = cands["net_usd_adj"].notna() & (cands["net_usd_adj"] > min_net_abs)

        # 1) фильтруем только валидные сигналы
        cands = cands[
            cands["z_ok"] &
            cands["std_ok"] &
            cands["spread_ok"] &
            cands["eco_ok"]
        ].copy()

        # 2) если после фильтра пусто — fallback
        if cands.empty or cands["z"].isna().all():
            sort_col = "net_usd_adj" if "net_usd_adj" in cands.columns else "net_usd"
            cands = cands.sort_values(sort_col, ascending=False).reset_index(drop=True)
        else:
            # 3) сортировка с приоритетом zscore
            # сначала z (убывание), затем net_adj
            cands["__score__"] = (
                cands["z"] * 10000 +
                cands["net_usd_adj"].fillna(0)
            )
            cands = cands.sort_values("__score__", ascending=False).reset_index(drop=True)
    else:
        # price режим — как раньше
        sort_col = "net_usd_adj" if "net_usd_adj" in cands.columns else "net_usd"
        cands = cands.sort_values(sort_col, ascending=False).reset_index(drop=True)

    best = None
    if not cands.empty:
        best = dict(cands.iloc[0])


    if top3_to_tg and top3_to_tg > 0 and not cands.empty:
        topN = cands.head(int(top3_to_tg)).to_dict("records")
        for rec in topN:
            msg = format_signal_card(rec, per_leg_notional_usd, price_source)
            try:
                maybe_send_telegram(msg)
            except Exception:
                logging.exception(
                    "[TG] send failed for %s %s↔%s",
                    rec.get("symbol"), rec.get("long_ex"), rec.get("short_ex")
                )

    # ------------------------------
    # 3) загрузка/обновление позиций
    # ------------------------------
    df_pos = load_positions(pos_path)
    if df_pos is None or df_pos.empty:
        df_pos = pd.DataFrame(columns=[
            "symbol","long_ex","short_ex",
            "open_price_long","open_price_short",
            "qty","status","opened_at",
            "open_long_order_id","open_short_order_id",
            "close_long_order_id","close_short_order_id",
            "reason","pnl_usd"
        ])

    # ------------------------------------------------------------------
    # FIX: совместимость схемы positions_cross.csv между try_instant_open и positions_once
    # try_instant_open пишет validated_qty / entry_px_low / entry_px_high
    # ------------------------------------------------------------------
    try:
        if "qty" not in df_pos.columns and "validated_qty" in df_pos.columns:
            df_pos["qty"] = pd.to_numeric(df_pos["validated_qty"], errors="coerce")
        if "open_price_long" not in df_pos.columns and "entry_px_low" in df_pos.columns:
            df_pos["open_price_long"] = pd.to_numeric(df_pos["entry_px_low"], errors="coerce")
        if "open_price_short" not in df_pos.columns and "entry_px_high" in df_pos.columns:
            df_pos["open_price_short"] = pd.to_numeric(df_pos["entry_px_high"], errors="coerce")
    except Exception as e:
        logging.debug("positions_once: schema normalize failed: %s", e)

    has_open = False
    try:
        if not df_pos.empty and "status" in df_pos.columns:
            # глобальный признак: есть ли хотя бы одна активная позиция
            has_open = any(df_pos["status"].isin(["open", "closing"]))
    except Exception:
        has_open = False

    # ------------------------------
    # 4) проверяем открытые позиции на выход
    # ------------------------------
    if not df_pos.empty:
        for i, row in df_pos.iterrows():
            if str(row.get("status","")) != "open":
                continue

            sym = str(row.get("symbol","")).upper()
            ex_l = str(row.get("long_ex",""))
            ex_h = str(row.get("short_ex",""))
            qty  = float(row.get("qty", 0.0) or 0.0)

            if not sym or not ex_l or not ex_h or qty <= 0:
                continue

            # --- возраст сделки через MAX_HOLD_SEC ---
            max_hold_sec = float(getenv_float("MAX_HOLD_SEC", 0.0))
            max_hold_reached = False
            age_sec = None

            try:
                opened_ms = float(row.get("opened_ms") or 0.0)
                now_ms    = utc_ms_now()
                if opened_ms > 0:
                    age_sec = max(0.0, (now_ms - opened_ms) / 1000.0)
                    # обновляем last_ms в CSV
                    df_pos.at[i, "last_ms"] = now_ms
                    if max_hold_sec > 0 and age_sec >= max_hold_sec:
                        max_hold_reached = True
            except Exception:
                pass

            # --- ЖЁСТКИЙ ВЫХОД ПО ТАЙМЕРУ (MAX_HOLD_SEC) ---
            if max_hold_sec > 0 and max_hold_reached:
                try:
                    logging.info(
                        "positions_once: FORCE TIME EXIT %s %s↔%s age=%.0fs >= MAX_HOLD_SEC=%s",
                        sym,
                        ex_l,
                        ex_h,
                        age_sec if age_sec is not None else -1.0,
                        max_hold_sec,
                    )
                except Exception:
                    logging.info(
                        "positions_once: FORCE TIME EXIT %s %s↔%s (age or MAX_HOLD_SEC unknown)",
                        sym,
                        ex_l,
                        ex_h,
                    )

                try:
                    ok, meta = atomic_cross_close(
                        symbol=sym,
                        cheap_ex=ex_l,
                        rich_ex=ex_h,
                        qty=qty,
                        paper=paper,
                    )
                except Exception as e:
                    ok, meta = False, {"error": str(e)}

                if ok:
                    df_pos.at[i, "status"] = "closed"
                    df_pos.at[i, "close_reason"] = "time"
                    try:
                        df_pos.at[i, "closed_at"] = now_utc_str()
                    except Exception:
                        pass

                    # PnL из meta в CSV (аналогично обычному exit-блоку)
                    pnl = 0.0
                    try:
                        if isinstance(meta, dict):
                            if "pnl_usd" in meta:
                                pnl = float(meta.get("pnl_usd") or 0.0)
                            elif "pnl" in meta:
                                pnl = float(meta.get("pnl") or 0.0)
                        df_pos.at[i, "realized_pnl_usd"] = pnl
                    except Exception:
                        pass

                    # --- NEW: попытка подтянуть балансы и total equity для CLOSED BY TIME ---
                    balances_text = ""
                    total_eq = None

                    try:
                        per_ex: dict[str, float] = {}

                        # Binance Futures USDT
                        try:
                            b = binance_usdt_futures_balance()
                            if b:
                                per_ex["BINANCE"] = float(
                                    b.get("equity") or b.get("wallet") or 0.0
                                )
                        except Exception as e_b:
                            logging.debug(
                                "positions_once: binance_usdt_futures_balance failed: %s",
                                e_b,
                            )

                        # Bybit Unified USDT
                        try:
                            bb = bybit_unified_usdt_balance()
                            if bb:
                                per_ex["BYBIT"] = float(
                                    bb.get("equity") or bb.get("wallet") or 0.0
                                )
                        except Exception as e_bb:
                            logging.debug(
                                "positions_once: bybit_unified_usdt_balance failed: %s",
                                e_bb,
                            )

                        # OKX unified USDT
                        try:
                            ok = okx_usdt_balance()
                            if ok:
                                per_ex["OKX"] = float(
                                    ok.get("equity") or ok.get("wallet") or 0.0
                                )
                        except Exception as e_ok:
                            logging.debug(
                                "positions_once: okx_usdt_balance failed: %s",
                                e_ok,
                            )

                        # Gate futures USDT
                        try:
                            gt = gate_usdt_futures_balance()
                            if gt:
                                per_ex["GATE"] = float(
                                    gt.get("equity") or gt.get("wallet") or 0.0
                                )
                        except Exception as e_gt:
                            logging.debug(
                                "positions_once: gate_usdt_futures_balance failed: %s",
                                e_gt,
                            )

                        if per_ex:
                            lines: list[str] = []
                            total_val = 0.0
                            for ex_name, val in per_ex.items():
                                try:
                                    v = float(val)
                                except Exception:
                                    continue
                                total_val += v
                                lines.append(f"   • {ex_name}: ${v:,.2f}")
                            if lines:
                                balances_text = "\n".join(lines)
                                total_eq = total_val
                        else:
                            logging.warning(
                                "positions_once: balances are empty after time-close for %s %s↔%s",
                                sym, ex_l, ex_h,
                            )
                    except Exception as e_imp:
                        logging.warning(
                            "positions_once: balances fetch failed (time-close): %s",
                            e_imp,
                        )

                    # Формируем карточку как у обычного CLOSED, но с пометкой BY TIME
                    try:
                        msg_lines = ["⏰ <b>CLOSED BY TIME</b>"]
                    
                        # вставляем equity сразу после заголовка, если он рассчитан
                        if total_eq is not None:
                            msg_lines.append(f"🏦 Total equity: ${total_eq:,.2f}")
                    
                        # потом строка с парами бирж
                        msg_lines.append(f"{sym} {ex_l.upper()} ↔ {ex_h.upper()}")

                        if age_sec is not None:
                            msg_lines.append(
                                f"age ≈ {age_sec:.0f} s (MAX_HOLD_SEC={int(max_hold_sec)})"
                            )

                        msg_lines.append(f"💰 PnL: {pnl:+.2f} USD")

                        if balances_text:
                            msg_lines.append("")
                            msg_lines.append("📊 Balances after close:")
                            msg_lines.append(balances_text)

                        maybe_send_telegram("\n".join(msg_lines))
                    except Exception:
                        pass

                else:
                    # Ошибка при тайм-аут закрытии — лог + TG
                    err_msg = ""
                    try:
                        if isinstance(meta, dict) and "error" in meta:
                            err_msg = str(meta.get("error"))
                        else:
                            err_msg = str(meta)
                    except Exception:
                        err_msg = "unknown"

                    logging.error(
                        "[CLOSE] time-based close failed for %s %s↔%s qty=%s: %s",
                        sym,
                        ex_l,
                        ex_h,
                        qty,
                        err_msg,
                    )
                    try:
                        maybe_send_telegram(
                            "❌ <b>CLOSE ERROR (TIME)</b>\n"
                            f"{sym} {ex_l.upper()} ↔ {ex_h.upper()}\n"
                            f"MAX_HOLD_SEC={max_hold_sec}, age≈{age_sec} s\n"
                           f"{err_msg}"
                        )
                    except Exception:
                        pass

                # По этой строке дальше ничего не делаем
                continue

            # ------- локальная функция котировок -------
            def _single_quote(ex: str, symbol: str):
                ps = getenv_str("PRICE_SOURCE", "mid") or "mid"
                try:
                    ex_lc = str(ex).lower()
                    if ex_lc == "binance":
                        return binance_quote(symbol, ps)
                    if ex_lc == "bybit":
                        return bybit_quote(symbol, ps)
                    if ex_lc == "okx":
                        return okx_quote(symbol, ps)
                    if ex_lc == "gate":
                        return gate_quote(symbol, ps)
                    if ex_lc == "mexc":
                        return mexc_quote(symbol, ps)
                except Exception as e:
                    logging.debug(
                        "positions_once: direct quote failed for %s@%s: %s",
                        symbol, ex, e
                    )
                return None

            row_low  = _single_quote(ex_l, sym)
            row_high = _single_quote(ex_h, sym)
            if row_low is None or row_high is None:
                continue

            bid_low  = float(row_low.get("bid") or 0.0)
            ask_low  = float(row_low.get("ask") or 0.0)
            bid_high = float(row_high.get("bid") or 0.0)
            ask_high = float(row_high.get("ask") or 0.0)

            if ask_low <= 0 or bid_high <= 0:
                continue

            # текущий спред выхода в тех же bps, что и entry_spread_bps:
            # long продаём по bid_low, short откупаем по ask_high
            # → спред = (ask_high - bid_low) / bid_low * 1e4  (всегда >= 0 при нормальном арбитраже)
            exit_bps_now = (ask_high - bid_low) / bid_low * 1e4

            EXIT_HYST_BPS = float(getenv_float("EXIT_HYST_BPS", 3.0))
            exit_req = float(exit_bps)

            # ENTRY_MODE=zscore → выходим по z / Δz / времени, а не по exit_bps_now
            entry_mode_loc = getenv_str("ENTRY_MODE", "price").lower()
            exit_mode_loc  = getenv_str("EXIT_MODE", "zscore").lower()
            use_z_exit = (entry_mode_loc == "zscore") or (exit_mode_loc == "zscore")

            exit_mode_loc   = getenv_str("EXIT_MODE", "zscore").lower()
            entry_mode_glob = getenv_str("ENTRY_MODE", "price").lower()

            # В режиме ENTRY_MODE=zscore полностью игнорируем триггер по exit_bps_now:
            # выходим только по z / времени / PnL-логике.
            if exit_mode_loc == "price" and entry_mode_glob != "zscore":
                # price-режим: выходим по уровню спреда (спред вернулся к "норме")
                exit_ok = exit_bps_now <= (exit_req + EXIT_HYST_BPS)
            else:
                # zscore-режим: по самому уровню спреда не выходим,
                # он используется только для оценки PnL/стоп-лосса
                exit_ok = True

            # --- ожидаемый PnL при закрытии ---
            pnl_est_ok = True
            try:
                size_usd = float(row.get("size_usd") or per_leg_notional_usd)
                entry_spread_bps = float(row.get("entry_spread_bps") or 0.0)

                # насколько спред схлопнулся относительно входа
                # (entry_spread_bps и exit_bps_now теперь в одной системе координат)
                delta_bps = entry_spread_bps - exit_bps_now
                gross_est = (delta_bps / 1e4) * size_usd

                # комиссии
                taker_fee_env = float(getenv_float("TAKER_FEE", taker_fee))
                close_fee_est = 2.0 * taker_fee_env * size_usd
                open_fees_usd = float(row.get("open_fees_usd") or 0.0)

                pnl_est = gross_est - open_fees_usd - close_fee_est

                # политика по PnL:
                #   EXIT_REQUIRE_POSITIVE=1 → до истечения MAX_HOLD_SEC
                #   не закрываем позицию с нулевым/отрицательным ожидаемым PnL
                #   STOP_LOSS_BPS < 0 → жёсткий стоп-лосс по спреду против нас
                require_pos = getenv_bool("EXIT_REQUIRE_POSITIVE", False)
                STOP_LOSS_BPS = float(getenv_float("STOP_LOSS_BPS", 0.0))

                if use_z_exit:
                    # В zscore-режиме PnL-логика НЕ блокирует выход:
                    # закрываемся по z / Δz / времени, а не по exit_bps_now / pnl_est
                    pnl_est_ok = True
                else:
                    if require_pos and (not max_hold_reached):
                        pnl_est_ok = pnl_est > 0.0
                    else:
                        pnl_est_ok = True
                    # дополнительно: грубый стоп-лосс по bps, если задали
                    if STOP_LOSS_BPS < 0:
                        # считаем delta_bps как разницу между entry и текущим спредом
                        entry_spread_bps = float(row.get("entry_spread_bps") or 0.0)
                        cur_spread_bps   = 1e4 * max(0.0, bid_high - ask_low) / max(ask_low, 1e-12)
                        delta_bps = cur_spread_bps - entry_spread_bps
                        if delta_bps <= STOP_LOSS_BPS:
                            pnl_est_ok = True

                # после MAX_HOLD_SEC даём позиции закрыться даже с минусом — просто логируем
                if max_hold_reached:
                    logging.info(
                        "positions_once: MAX_HOLD_SEC reached for %s %s↔%s (pnl_est=%.4f)",
                        sym, ex_l, ex_h, pnl_est
                    )

            except Exception:
                # если оценка PnL сломалась — не блокируем выход
                pnl_est_ok = True

            # z / Δz / время — условия выхода в zscore-режиме
            z_ok = True
            if use_z_exit:
                try:
                    z_out = float(getenv_float("Z_OUT", 0.0))

                    # если Z_OUT не задан (0.0) — не используем z как триггер
                    if z_out <= 0.0:
                        z_ok = bool(max_hold_reached)
                    else:
                        z_cur = float("nan")

                        # Берём stats: либо переданный в positions_once, либо читаем с диска
                        stats_df2 = None
                        try:
                            if stats_df is not None and not stats_df.empty:
                                stats_df2 = stats_df
                            else:
                                stats_df2 = read_spread_stats()
                        except Exception:
                            stats_df2 = None

                        if stats_df2 is not None and not stats_df2.empty:
                            try:
                                # z для текущего спреда по текущим ценам
                                _, z_val, _ = get_z_for_pair(
                                    stats_df2,
                                    symbol=sym,
                                    ex_low=str(ex_l).lower(),
                                    ex_high=str(ex_h).lower(),
                                    px_low=ask_low,      # где мы long (дешёвый)
                                    px_high=bid_high,    # где мы short (дорогой)
                                )
                                if z_val == z_val:  # not NaN
                                    z_cur = float(z_val)
                            except Exception:
                                z_cur = float("nan")

                        # Основной триггер: |z_cur| <= Z_OUT
                        cond_z = (z_cur == z_cur) and (abs(z_cur) <= abs(z_out))
                        # Резервный триггер: истёк MAX_HOLD_SEC
                        cond_time = bool(max_hold_reached)

                        z_ok = cond_z or cond_time

                except Exception:
                    # если z посчитать не смогли — разрешаем выход только по времени
                    z_ok = bool(max_hold_reached)

            # Если достигли MAX_HOLD_SEC — разрешаем выход строго по таймингу,
            # даже если условия по z / pnl_est формально не выполняются.
            if max_hold_reached:
                exit_ok = True
                z_ok = True
                pnl_est_ok = True

            if exit_ok and z_ok and pnl_est_ok:
                # закрываем позицию
                try:
                    ok, meta = atomic_cross_close(
                        symbol=sym,
                        cheap_ex=ex_l,
                        rich_ex=ex_h,
                        qty=qty,
                        paper=paper,
                    )
                except Exception as e:
                    ok, meta = False, {"error": str(e)}

                if ok:
                    df_pos.at[i, "status"] = "closed"
                    # пишем в корректное поле схемы — close_reason
                    df_pos.at[i, "close_reason"] = "exit"
                    try:
                        df_pos.at[i, "closed_at"] = now_utc_str()
                    except Exception:
                        pass

                    # --- NEW: PnL из meta в CSV + для карточки ---
                    pnl = 0.0
                    try:
                        # базовый вариант – ожидаем pnl_usd
                        if "pnl_usd" in meta:
                            pnl = float(meta.get("pnl_usd") or 0.0)
                        # fallback – вдруг atomic_cross_close вернёт просто "pnl"
                        elif "pnl" in meta:
                            pnl = float(meta.get("pnl") or 0.0)
                        # сохраняем в колонку схемы POS_COLS
                        df_pos.at[i, "realized_pnl_usd"] = pnl
                    except Exception:
                        pass

                    # --- NEW: попытка подтянуть балансы и total equity ---
                    balances_text = ""
                    total_eq = None

                    try:
                        per_ex: dict[str, float] = {}

                        # Binance Futures USDT
                        try:
                            b = binance_usdt_futures_balance()
                            if b:
                                per_ex["BINANCE"] = float(
                                    b.get("equity") or b.get("wallet") or 0.0
                                )
                        except Exception as e_b:
                            logging.debug(
                                "positions_once: binance_usdt_futures_balance failed: %s",
                                e_b,
                            )

                        # Bybit Unified USDT
                        try:
                            bb = bybit_unified_usdt_balance()
                            if bb:
                                per_ex["BYBIT"] = float(
                                    bb.get("equity") or bb.get("wallet") or 0.0
                                )
                        except Exception as e_bb:
                            logging.debug(
                                "positions_once: bybit_unified_usdt_balance failed: %s",
                                e_bb,
                            )

                        # OKX unified USDT
                        try:
                            ok = okx_usdt_balance()
                            if ok:
                                per_ex["OKX"] = float(
                                    ok.get("equity") or ok.get("wallet") or 0.0
                                )
                        except Exception as e_ok:
                            logging.debug(
                                "positions_once: okx_usdt_balance failed: %s",
                                e_ok,
                            )

                        # Gate futures USDT
                        try:
                            gt = gate_usdt_futures_balance()
                            if gt:
                                per_ex["GATE"] = float(
                                    gt.get("equity") or gt.get("wallet") or 0.0
                                )
                        except Exception as e_gt:
                            logging.debug(
                                "positions_once: gate_usdt_futures_balance failed: %s",
                                e_gt,
                            )

                        if per_ex:
                            lines: list[str] = []
                            total_val = 0.0
                            for ex_name, val in per_ex.items():
                                try:
                                    v = float(val)
                                except Exception:
                                    continue
                                total_val += v
                                lines.append(f"   • {ex_name}: ${v:,.2f}")
                            if lines:
                                balances_text = "\n".join(lines)
                                total_eq = total_val
                        else:
                            # Явно логируем, что баланс не удалось получить
                            logging.warning(
                                "positions_once: balances are empty after close for %s %s↔%s",
                                sym, ex_l, ex_h,
                            )
                    except Exception as e_imp:
                        logging.warning(
                            "positions_once: balances fetch failed: %s",
                            e_imp,
                        )

                    msg_lines = ["✅ <b>CLOSED</b>"]
                    
                    # вставляем equity сразу после заголовка, если он есть
                    if total_eq is not None:
                        msg_lines.append(f"🏦 Total equity: ${total_eq:,.2f}")
                    
                    # затем строка с парами бирж
                    msg_lines.append(f"{sym} {ex_l.upper()} ↔ {ex_h.upper()}")

                    if balances_text:
                        msg_lines.append("")  # пустая строка
                        msg_lines.append("📊 Balances after close:")
                        msg_lines.append(balances_text)

                    # важно: отправляем ОДНУ строку, а не список
                    maybe_send_telegram("\n".join(msg_lines))

                else:
                    # Отладка: закрытие не удалось — лог + карточка в TG
                    err_msg = ""
                    try:
                        if isinstance(meta, dict) and "error" in meta:
                            err_msg = str(meta.get("error"))
                        else:
                            err_msg = str(meta)
                    except Exception:
                        err_msg = "unknown"

                    logging.error(
                        "[CLOSE] exit close failed for %s %s↔%s qty=%s: %s",
                        sym,
                        ex_l,
                        ex_h,
                        qty,
                        err_msg,
                    )

                    try:
                        maybe_send_telegram(
                            "❌ <b>CLOSE ERROR</b>\n"
                            f"{sym} {ex_l.upper()} ↔ {ex_h.upper()}\n"
                            f"qty={qty:.4f}\n"
                            f"error={err_msg}"
                        )
                    except Exception as e_tg:
                        logging.debug(
                            "positions_once: telegram close-error notify failed: %s",
                            e_tg,
                        )

    # ------------------------------
    # 5) ротация (optional)
    # ------------------------------
    if rotate_on and best is not None and not df_pos.empty:
        try:
            # ищем открытую и сравниваем её net с новым best
            open_rows = df_pos[df_pos["status"] == "open"]
            if not open_rows.empty:
                open_row = open_rows.iloc[0]
                open_net = float(open_row.get("net_usd_adj") or open_row.get("net_usd") or 0.0)
                best_net = float(best.get("net_usd_adj") or best.get("net_usd") or 0.0)

                if best_net - open_net >= float(rotate_delta_usd):
                    # закрыть текущую (по причине rotate), открыть best через try_instant_open
                    sym = str(open_row["symbol"]).upper()
                    ex_l = str(open_row["long_ex"])
                    ex_h = str(open_row["short_ex"])
                    qty = float(open_row.get("qty", 0.0) or 0.0)
                    if qty > 0:
                        ok, meta = atomic_cross_close(
                            symbol=sym,
                            cheap_ex=ex_l,
                            rich_ex=ex_h,
                            qty=qty,
                            paper=paper,
                        )
                        if ok:
                            idx = open_rows.index[0]
                            df_pos.at[idx, "status"] = "closed"
                            df_pos.at[idx, "reason"] = "rotate"
                            maybe_send_telegram(
                                "🔁 <b>ROTATED OUT</b>\n"
                                f"{sym} {ex_l.upper()} ↔ {ex_h.upper()}\n"
                                f"open_net={open_net:.2f} → best_net={best_net:.2f}"
                            )
                        else:
                            # Отладка: не смогли закрыть перед ротацией
                            err_msg = ""
                            try:
                                if isinstance(meta, dict) and "error" in meta:
                                    err_msg = str(meta.get("error"))
                                else:
                                    err_msg = str(meta)
                            except Exception:
                                err_msg = "unknown"

                            logging.error(
                                "[ROTATE] close before rotate failed for %s %s↔%s qty=%s: %s",
                                sym,
                                ex_l,
                                ex_h,
                                qty,
                                err_msg,
                            )

                            try:
                                maybe_send_telegram(
                                    "❌ <b>ROTATE CLOSE ERROR</b>\n"
                                    f"{sym} {ex_l.upper()} ↔ {ex_h.upper()}\n"
                                    f"qty={qty:.4f}\n"
                                    f"error={err_msg}"
                                )
                            except Exception as e_tg:
                                logging.debug(
                                    "positions_once: telegram rotate-close notify failed: %s",
                                    e_tg,
                                )

                            # Позицию намеренно оставляем статусом open,
                            # чтобы в positions_cross.csv было видно, что она "зависла".
                            # открыть новый best тем же механизмом, что в блоке 6 (atomic_cross_open)
                            entry_mode_loc = getenv_str("ENTRY_MODE", "price").lower()

                            spread_bps = float(best.get("spread_bps") or 0.0)
                            net_adj    = float(best.get("net_usd_adj") or 0.0)
                            z          = float(best.get("z") or float("nan"))
                            std        = float(best.get("std") or float("nan"))

                            Z_IN_LOC = float(getenv_float("Z_IN", 2.0))
                            std_min  = float(getenv_float("STD_MIN_FOR_OPEN", 1e-4))
                            capital  = float(getenv_float("CAPITAL", 1000.0))
                            min_net_abs = (float(getenv_float("ENTRY_NET_PCT", 1.0))/100.0) * capital

                            spread_ok = spread_bps >= float(entry_bps)
                            eco_ok    = net_adj > min_net_abs

                            if entry_mode_loc == "zscore":
                                z_ok   = (z == z) and (z >= Z_IN_LOC)
                                std_ok = (std == std) and (std >= std_min)
                            else:
                                z_ok, std_ok = True, True

                            if spread_ok and eco_ok and z_ok and std_ok:
                                _paper = bool(getenv_bool("PAPER", True)) if paper is None else bool(paper)
                                ok2, attempt_id2, meta2 = atomic_cross_open(
                                    symbol=str(best["symbol"]).upper(),
                                    cheap_ex=str(best["long_ex"]).lower(),
                                    rich_ex=str(best["short_ex"]).lower(),
                                    qty=float(best["qty_est"]),
                                   price_low=float(best["px_low"]),
                                    price_high=float(best["px_high"]),
                                    paper=_paper
                                )
                                if not ok2:
                                    logging.warning("[ROTATE_OPEN] failed %s: %s", best.get("symbol"), meta2)
                                else:
                                    df_pos = load_positions(pos_path)
        except Exception as e:
            logging.debug("positions_once: rotate failed: %s", e)

    # 6) ЕДИНЫЙ путь ОТКРЫТИЯ (через atomic_cross_open)
    # ------------------------------
    if (not has_open) and best is not None:
        entry_mode_loc = getenv_str("ENTRY_MODE", "price").lower()

        spread_bps = float(best.get("spread_bps") or 0.0)
        net_adj    = float(best.get("net_usd_adj") or 0.0)
        z          = float(best.get("z") or float("nan"))
        std        = float(best.get("std") or float("nan"))

        Z_IN_LOC = float(getenv_float("Z_IN", 2.0))
        std_min  = float(getenv_float("STD_MIN_FOR_OPEN", 1e-4))
        capital  = float(getenv_float("CAPITAL", 1000.0))
        min_net_abs = (float(getenv_float("ENTRY_NET_PCT", 1.0))/100.0) * capital

        spread_ok = spread_bps >= float(entry_bps)
        eco_ok    = net_adj > min_net_abs

        if entry_mode_loc == "zscore":
            z_ok   = (z == z) and (z >= Z_IN_LOC)
            std_ok = (std == std) and (std >= std_min)
        else:
            z_ok, std_ok = True, True

        if spread_ok and eco_ok and z_ok and std_ok:
            _paper = bool(getenv_bool("PAPER", True)) if paper is None else bool(paper)
            ok, attempt_id, meta = atomic_cross_open(
                symbol=str(best["symbol"]).upper(),
                cheap_ex=str(best["long_ex"]).lower(),
                rich_ex=str(best["short_ex"]).lower(),
                qty=float(best["qty_est"]),
                price_low=float(best["px_low"]),
                price_high=float(best["px_high"]),
                paper=_paper
            )
            if not ok:
                logging.warning("[OPEN] failed %s: %s", best.get("symbol"), meta)
            else:
                # обновим позиции после успеха
                df_pos = load_positions(pos_path)

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
    # ----- глобальный traceback в лог -----
    def _global_excepthook(exc_type, exc, tb):
        try:
            logging.critical("FATAL EXCEPTION", exc_info=(exc_type, exc, tb))
        finally:
            traceback.print_exception(exc_type, exc, tb)
    sys.excepthook = _global_excepthook

    exchanges = [x.lower() for x in getenv_list("EXCHANGES", DEFAULT_EXCHANGES)]

    must_check = _is_true("CHECK_EXCHANGES_AT_START", True)
    must_quit  = _is_true("QUIT_ON_CONNECTIVITY_FAIL", True)
    probe_sym  = os.getenv("CONNECTIVITY_PROBE_SYMBOL", "BTCUSDT").upper()

    # ----- GCS creds -----
    try:
        ensure_gcs_credentials_from_env()
    except Exception as e:
        logging.exception("[GCS] ensure_gcs_credentials_from_env failed: %s", e)

    if must_check:
        per_env = {}
        for ex in exchanges:
            base = _private_base(ex)
            env = "testnet" if "testnet" in base else ("demo" if "api-demo" in base else "mainnet")
            if ex.lower() == "okx":
                if _is_true("OKX_TESTNET", False) or _is_true("OKX_PAPER", False):
                    env = "demo"
            per_env[ex] = env

        logging.info(f"[ENV] price_feed_env=mainnet  order_env_per_exchange={per_env}")

        if not check_connectivity(exchanges, probe_symbol=probe_sym):
            msg = "Startup connectivity check failed — one or more exchanges unavailable."
            logging.error(msg)
            if must_quit:
                maybe_send_telegram(f"❌ {msg}")
                return

        if _is_true("AUTH_CHECK_AT_START", True):
            logging.info("Running private API auth checks...")
            if not check_auth_connectivity(exchanges):
                msg = "Private API auth check failed — verify API keys/permissions/network."
                logging.error(msg)
                if must_quit:
                    maybe_send_telegram(f"❌ {msg}")
                    return

    # ---------------- Symbols ----------------
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
        symbols = symbols_from_matrix(matrix_path, exchanges, mode="union") if matrix_path else COMMON_SYMBOLS
    elif src == "matrix":
        symbols = symbols_from_matrix(matrix_path, exchanges, mode=matrix_mode) if matrix_path else COMMON_SYMBOLS
    else:
        symbols = COMMON_SYMBOLS

    per_ex_symbols = None
    if matrix_path and use_per_ex:
        per_ex_symbols = matrix_per_exchange_symbols(matrix_path, exchanges)
        logging.info("Per-exchange symbol filter enabled (matrix): %s",
                     {ex: len(s) for ex, s in per_ex_symbols.items()})

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

    price_source = getenv_str("PRICE_SOURCE", "book").lower()

    logging.info("PriceArb started | PAPER=%s | per-leg=$%.2f | ENTRY=%.2f bps | EXIT=%.2f bps | taker=%.4f",
                 paper, per_leg_notional, entry_bps, exit_bps, taker_fee)

    # ==================== MAIN LOOP ====================
    while True:
        try:
            # ============================================================
            # A) ТОРГОВЫЙ СКАНЕР — ищет кандидаты и обрабатывает entry
            # ============================================================
            best, quotes_df = scan_spreads_once(
                exchanges=exchanges,
                symbols=symbols,
                spread_bps_min=entry_bps,
                spread_bps_max=float(getenv_float("SPREAD_BPS_MAX", 1e9)),
                per_leg_notional_usd=per_leg_notional,
                taker_fee=taker_fee,
                pos_path=pos_cross_path,
                price_stats_path=SPREAD_STATS_PATH,
                debug=False,
                price_source=price_source,
                alert_spread_pct=ALERT_SPREAD_PCT,
                cooldown_sec=ALERT_COOLDOWN_SEC,
                instant_open=True,
                pos_path_for_instant=pos_cross_path,
                paper=paper,
                per_ex_symbols=per_ex_symbols,
            )

            # ============================================================
            # C) inline-обновление stats_store (как у тебя)
            # ============================================================
            try:
                if quotes_df is not None and not quotes_df.empty:
                    df = quotes_df.copy()
                    if price_source == "book":
                        if "ask" in df.columns and "bid" in df.columns:
                            for sym in sorted(df["symbol"].unique()):
                                sub = df[df["symbol"] == sym]
                                sub_ask = sub.dropna(subset=["ask"])
                                sub_bid = sub.dropna(subset=["bid"])
                                if sub_ask.empty or sub_bid.empty:
                                    continue
                                sub_ask["ask"] = pd.to_numeric(sub_ask["ask"], errors="coerce")
                                sub_bid["bid"] = pd.to_numeric(sub_bid["bid"], errors="coerce")
                                sub_ask = sub_ask.dropna(subset=["ask"])
                                sub_bid = sub_bid.dropna(subset=["bid"])
                                if sub_ask.empty or sub_bid.empty:
                                    continue
                                row_low  = sub_ask.loc[sub_ask["ask"].idxmin()]
                                row_high = sub_bid.loc[sub_bid["bid"].idxmax()]
                                if str(row_low["exchange"]) == str(row_high["exchange"]):
                                    continue
                                px_low  = float(row_low["ask"])
                                px_high = float(row_high["bid"])
                                if px_low > 0 and px_high > 0:
                                    x = math.log(px_high / px_low)

                                    ex_low  = str(row_low["exchange"]).strip().lower()
                                    ex_high = str(row_high["exchange"]).strip().lower()
                                    sym_u   = str(sym).strip().upper()

                                    # x может быть 0.0 — это нормально. Проверяем только None/NaN.
                                    if (not sym_u) or (not ex_low) or (not ex_high) or (x != x):
                                        logging.warning(
                                            "Stats inline update error. Mode: book | sym=%r ex_low=%r ex_high=%r x=%r "
                                            "| px_low=%r px_high=%r",
                                            sym_u, ex_low, ex_high, x, px_low, px_high
                                        )
                                        continue  # НЕ return, чтобы не ронять цикл

                                    stats_store.update_pair(sym_u, ex_low, ex_high, x)

                    else:
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
                            for i in range(len(pxs)):
                                for j in range(i+1, len(pxs)):
                                    px_low, px_high = float(pxs[i]), float(pxs[j])
                                    if px_low<=0 or px_high<=0: 
                                        continue
                                    ex_low, ex_high = str(exs[i]), str(exs[j])
                                    x = math.log(px_high/px_low)
                                    if not sym or not ex_low or not ex_high or not x:
                                        logging.warning("Stats inline update error")
                                        return
                                    stats_store.update_pair(sym, ex_low, ex_high, x)

                stats_store.maybe_save(force=False)
            except Exception as e:
                logging.warning("Stats inline update error: %s", e)

            # ============================================================
            # D) позиции + top-3 в TG
            # ============================================================
            try:
                positions_once(
                    quotes_df=quotes_df,
                    per_leg_notional_usd=per_leg_notional,
                    entry_bps=entry_bps,
                    exit_bps=exit_bps,
                    taker_fee=taker_fee,
                    pos_path=pos_cross_path,
                    paper=paper,
                    top3_to_tg=int(getenv_float("TOP_N_TELEGRAM", 3)),
                    rotate_on=rotate_on,
                    rotate_delta_usd=rotate_delta_usd,
                    stats_df=stats_store.df,
                )
            except Exception as e:
                logging.exception("positions_once error: %s", e)

        except Exception as e:
            logging.exception("FATAL ERROR IN MAIN LOOP")
            err = str(e)[:1600].replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
            maybe_send_telegram(f"❌ PriceArb cycle error: <code>{err}</code>")
            raise

        time.sleep(max(3, sleep_s))

if __name__ == "__main__":
    main()