#!/usr/bin/env python3
"""
TREA & FWEA – Cloud API
Versão: trea_fwea_cloud_api - 20260213_42
Status: Premium SSE (base Pasta 51)

Endpoints (v1):
  • POST /api/v1/events/publish        - recebe eventos do TREA (JSON)
  • GET  /api/v1/events/stream_ndjson  - entrega stream NDJSON para o FWEA
  • GET  /api/v1/health                - heartbeat (público)

Segurança:
  • Authorization: Bearer <token> (OBRIGATÓRIO em PROD)
  • ?token=<...> aceito APENAS em DEV/TESTE (controlado por TFA_ALLOW_QUERY_TOKEN)
  • Tokens via env (TFA_VALID_TOKENS) separados por vírgula; fallback automático APENAS se a env não estiver definida (DEV).

Notas:
  • Buffer em memória com lock (thread-safe) para esta fase.
  • Campo incremental "id" (since) para consumo incremental do FWEA.
  • Logging reduzido (werkzeug WARNING).
"""

from __future__ import annotations
import os, json, time, threading, logging
import urllib.request
import urllib.error
import requests
from urllib.parse import parse_qs, unquote_plus
from typing import Any, Dict, Iterable, List
from functools import wraps

from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS

# ========================= Config =========================
DEFAULT_TOKENS = ["TREA_MT5_DEV_TOKEN_001", "FWEA_MT5_DEV_TOKEN_001"]
VALID_TOKENS = [
    t.strip()
    for t in os.getenv("TFA_VALID_TOKENS", ",".join(DEFAULT_TOKENS)).split(",")
    if t.strip()
]
HOST = os.getenv("TFA_HOST", "0.0.0.0")
PORT = int(os.getenv("TFA_PORT", "8080"))

TFA_REDIS_SHADOW = (
    os.getenv("REDIS_SHADOW")
    or os.getenv("TFA_REDIS_SHADOW", "0")
).strip().lower() in ("1", "true", "yes", "on")

UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL", "").strip()
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "").strip()

# ===== Upstash HTTP client (Session / keep-alive) =====
UPSTASH_CONNECT_TIMEOUT = float(os.getenv("TFA_UPSTASH_CONNECT_TIMEOUT", "2.0"))
UPSTASH_READ_TIMEOUT    = float(os.getenv("TFA_UPSTASH_READ_TIMEOUT", "5.0"))
TFA_LOG_UPSTASH_ERRORS  = os.getenv("TFA_LOG_UPSTASH_ERRORS", "1").strip().lower() in ("1","true","yes","on")

_UPSTASH_SESSION = requests.Session()
_UPSTASH_SESSION.headers.update({
    "Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}",
    "Content-Type": "application/json",
})

# ===== Redis Streams (MVP confiável) =====
TFA_REDIS_STREAMS = (
    os.getenv("TFA_REDIS_STREAMS", "1").strip().lower() in ("1", "true", "yes", "on")
)

TFA_STREAM_PREFIX = os.getenv("TFA_STREAM_PREFIX", "tfa:events:").strip()  # + trader_key
TFA_DEDUPE_PREFIX = os.getenv("TFA_DEDUPE_PREFIX", "tfa:dedupe:").strip()  # + trader_key + event_id
TFA_DEDUPE_TTL_SEC = int(os.getenv("TFA_DEDUPE_TTL_SEC", "604800"))  # 7 dias
TFA_CONSUME_COUNT = int(os.getenv("TFA_CONSUME_COUNT", "20"))
TFA_CONSUME_WAIT_DEFAULT = int(os.getenv("TFA_CONSUME_WAIT_DEFAULT", "15"))
TFA_CONSUME_WAIT_MAX     = int(os.getenv("TFA_CONSUME_WAIT_MAX", "25"))
TFA_LONGPOLL_SLEEP_MS    = float(os.getenv("TFA_LONGPOLL_SLEEP_MS", "0.10"))
TFA_CONSUME_WAIT_DEFAULT_MS = int(os.getenv("TFA_CONSUME_WAIT_DEFAULT_MS", "800"))   # Premium v1
TFA_CONSUME_WAIT_MAX_MS     = int(os.getenv("TFA_CONSUME_WAIT_MAX_MS", "2000"))

# DEV ONLY: aceitar token na querystring (?token=...) só em dev/teste
TFA_ALLOW_QUERY_TOKEN = os.getenv("TFA_ALLOW_QUERY_TOKEN", "0").strip().lower() in ("1", "true", "yes", "on")

logging.info(
    "BOOT: TFA_REDIS_SHADOW=%s UPSTASH_URL=%s TOKEN_SET=%s",
    TFA_REDIS_SHADOW,
    (UPSTASH_REDIS_REST_URL[:40] + "...") if UPSTASH_REDIS_REST_URL else "",
    bool(UPSTASH_REDIS_REST_TOKEN),
)

logging.getLogger("werkzeug").setLevel(logging.WARNING)

app = Flask(__name__)
CORS(app, supports_credentials=False)

# ========================= Storage ========================
class EventStore:
    """
    Thread-safe store com id incremental + persistência simples (JSONL).
    Nesta fase, persistimos todos os eventos em arquivo para evitar reset de id/seq em restart.
    """
    def __init__(self, persist_path: str = "") -> None:
        self._lock = threading.RLock()
        self._events: List[Dict[str, Any]] = []
        self._last_id = 0
        self._created_at = time.time()
        self._persist_path = persist_path.strip()
        if self._persist_path:
            self._load_from_disk()

    def _load_from_disk(self) -> None:
        try:
            if not os.path.exists(self._persist_path):
                return
            with open(self._persist_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        evt = json.loads(line)
                        if isinstance(evt, dict):
                            self._events.append(evt)
                            self._last_id = max(self._last_id, int(evt.get("id", 0) or 0))
                    except Exception:
                        continue
            # garante ordenação por id após load
            self._events.sort(key=lambda e: int(e.get("id", 0) or 0))
        except Exception as e:
            logging.warning("EventStore: falha ao carregar persistência: %s", e)

    def _append_to_disk(self, evt: Dict[str, Any]) -> None:
        if not self._persist_path:
            return
        try:
            os.makedirs(os.path.dirname(self._persist_path) or ".", exist_ok=True)
            with open(self._persist_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(evt, ensure_ascii=False) + "\n")
        except Exception as e:
            logging.warning("EventStore: falha ao persistir evento: %s", e)

    def add(self, evt: Dict[str, Any]) -> Dict[str, Any]:
        """Adiciona evento com id incremental, preservando 'ts' e 'seq' se vierem do TREA."""
        with self._lock:
            self._last_id += 1
            evt_id = self._last_id
            evt_copy = dict(evt)
            evt_copy.setdefault("ts", int(time.time()))
            evt_copy["id"] = evt_id
            evt_copy["server_ts"] = int(time.time())
            self._events.append(evt_copy)
            self._append_to_disk(evt_copy)
            return evt_copy

    def since(self, since_id: int) -> List[Dict[str, Any]]:
        """
        Retorna eventos com id > since_id, sempre ordenados por id crescente.
        Mantido por compatibilidade (API legada).
        """
        with self._lock:
            if since_id <= 0:
                events = list(self._events)
            else:
                events = [e for e in self._events if int(e.get("id", 0) or 0) > since_id]
            events.sort(key=lambda e: int(e.get("id", 0) or 0))
            return events

    def since_seq(self, trader_key: str, since_seq: int) -> List[Dict[str, Any]]:
        """
        Retorna eventos com seq > since_seq para um trader_key específico,
        ordenados por (seq, id). Este é o cursor definitivo (Opção B).
        """
        tk = (trader_key or "").strip()
        if not tk:
            return []
        with self._lock:
            out = []
            for e in self._events:
                if e.get("trader_key") != tk:
                    continue
                s = int(e.get("seq", 0) or 0)
                if s > since_seq:
                    out.append(e)
            out.sort(key=lambda e: (int(e.get("seq", 0) or 0), int(e.get("id", 0) or 0)))
            return out

    def last_seq_by_trader(self, limit: int = 50) -> Dict[str, int]:
        with self._lock:
            last: Dict[str, int] = {}
            # percorre do fim para o começo para ser rápido
            for e in reversed(self._events):
                tk = e.get("trader_key")
                if not tk or tk in last:
                    continue
                last[tk] = int(e.get("seq", 0) or 0)
                if len(last) >= limit:
                    break
            return last

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "count": len(self._events),
                "last_id": self._last_id,
                "uptime_s": int(time.time() - self._created_at),
                "persist_path": self._persist_path if self._persist_path else "",
            }


PERSIST_PATH = os.getenv("TFA_PERSIST_PATH", "/tmp/trea_fwea_events.jsonl")
STORE = EventStore(persist_path=PERSIST_PATH)

# ======================== Metrics =========================
_METRICS_LOCK = threading.RLock()
_METRICS = {
    # OK
    "publish_ok_total": 0,
    "publish_duplicate_total": 0,
    "consume_ok_total": 0,
    "consume_events_total": 0,

    # AUTH errors (geral)
    "auth_401_total": 0,
    "auth_403_total": 0,

    # AUTH errors por endpoint
    "auth_401_publish_total": 0,
    "auth_403_publish_total": 0,
    "auth_401_consume_total": 0,
    "auth_403_consume_total": 0,
    "auth_401_metrics_total": 0,
    "auth_403_metrics_total": 0,
}

# ===================== Auth (flexível) ====================
def require_token_flexible(fn):
    """Aceita Authorization: Bearer <token> OU X-Api-Token; em /publish aceita token no body; ?token= apenas DEV."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        token = ""

        # 1) Padrão: Authorization: Bearer <token>
        if auth.startswith("Bearer "):
            token = auth.split(" ", 1)[1].strip()

        # 2) Fallback MT5-friendly: X-Api-Token
        if not token:
            token = (request.headers.get("X-Api-Token", "") or "").strip()

        # 2.5) Fallback FINAL (MT5-safe): token no JSON BODY (apenas no /publish e /consume)
        if not token and request.method == "POST":
            p = (request.path or "")
            if p.endswith("/api/v1/events/publish") or p.endswith("/api/v1/events/consume") or p.endswith("/api/v1/events/consume_wait"):
                try:
                    body_evt = parse_json_body()
                    token = (body_evt.get("token", "") or "").strip()
                except Exception:
                    token = ""

        # 3) DEV ONLY: query token (?token=)
        if not token:
            if TFA_ALLOW_QUERY_TOKEN:
                token = request.args.get("token", "").strip()
            else:
                token = ""


        if not token:
            with _METRICS_LOCK:
                _METRICS["auth_401_total"] += 1
                p = request.path or ""
                if p.endswith("/api/v1/events/publish"):
                    _METRICS["auth_401_publish_total"] += 1
                elif p.endswith("/api/v1/events/consume"):
                    _METRICS["auth_401_consume_total"] += 1
                elif p.endswith("/api/v1/metrics"):
                    _METRICS["auth_401_metrics_total"] += 1
                elif p.endswith("/api/v1/metrics/reset"):
                    _METRICS["auth_401_metrics_total"] += 1   # (no bloco 401)

            return jsonify({"ok": False, "error": "missing_token"}), 401

        if token not in VALID_TOKENS:
            with _METRICS_LOCK:
                _METRICS["auth_403_total"] += 1
                p = request.path or ""
                if p.endswith("/api/v1/events/publish"):
                    _METRICS["auth_403_publish_total"] += 1
                elif p.endswith("/api/v1/events/consume"):
                    _METRICS["auth_403_consume_total"] += 1
                elif p.endswith("/api/v1/metrics"):
                    _METRICS["auth_403_metrics_total"] += 1
                elif p.endswith("/api/v1/metrics/reset"):
                    _METRICS["auth_403_metrics_total"] += 1

            return jsonify({"ok": False, "error": "unauthorized"}), 403

        return fn(*args, **kwargs)
    return wrapper

# ======================= Validators =======================
REQUIRED_FIELDS = [
    "ts",
    "trader_id",
    "action",
    "symbol",
    "volume",
    "sl",
    "tp",
    "position_id",
    "deal_ticket",
    "order_ticket",
    "magic",
    "comment",
]
ACTIONS = {
    "OPEN_BUY",
    "OPEN_SELL",
    "MODIFY",
    "CLOSE",
    "BUY",
    "SELL",
    "BUY_MARKET",
    "SELL_MARKET",
}


def parse_json_body() -> Dict[str, Any]:
    obj: Any = None

    # A) Primeiro: lê corpo bruto SEM depender do Content-Type
    raw = request.get_data(cache=True, as_text=True)
    raw = (raw or "").replace("\x00", "")

    if raw:
        # tenta JSON direto
        try:
            obj = json.loads(raw)
        except Exception:
            obj = None

        # se veio como string JSON (dupla serialização), tenta de novo
        if isinstance(obj, str):
            s = obj.strip()
            if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
                s = s[1:-1]
            try:
                obj = json.loads(s)
            except Exception:
                pass

        if isinstance(obj, dict):
            return obj

        # JSON puro vindo como form-urlencoded (MT5 faz isso)
        sraw = raw.strip()
        if sraw.startswith("{") or sraw.startswith("["):
            try:
                obj = json.loads(sraw)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                pass

        # fallback: body como querystring (ex: data=%7B...%7D)
        try:
            qs = parse_qs(raw, keep_blank_values=True)
            for key in ("json", "data", "body"):
                if key in qs and qs[key]:
                    v = unquote_plus(qs[key][0]).strip()
                    obj = json.loads(v)
                    if isinstance(obj, dict):
                        return obj
        except Exception:
            pass

    # B) Segundo: tentativa padrão do Flask
    data = request.get_json(silent=True)
    if isinstance(data, str):
        try:
            data2 = json.loads(data)
            if isinstance(data2, dict):
                return data2
        except Exception:
            pass
    if isinstance(data, dict):
        return data

    # C) Terceiro: fallback form
    try:
        if request.form:
            for key in ("json", "data", "body"):
                if key in request.form:
                    obj = json.loads(request.form[key])
                    if isinstance(obj, dict):
                        return obj
    except Exception:
        pass

    raise ValueError("Body must be a JSON object")

def validate_event(evt: Dict[str, Any]) -> None:
    missing = [k for k in REQUIRED_FIELDS if k not in evt]
    if missing:
        raise ValueError(f"Missing fields: {', '.join(missing)}")
    try:
        evt["ts"] = int(evt["ts"]) if str(evt["ts"]).isdigit() else int(time.time())
        evt["volume"] = float(evt["volume"])
        evt["sl"] = float(evt.get("sl", 0.0))
        evt["tp"] = float(evt.get("tp", 0.0))
        evt["position_id"] = int(evt.get("position_id", 0))
        evt["deal_ticket"] = int(evt.get("deal_ticket", 0))
        evt["order_ticket"] = int(evt.get("order_ticket", 0))
        evt["magic"] = int(evt.get("magic", 0))

        # Opcional: SEQ do TREA (monotônico por origem)
        if "seq" in evt:
            evt["seq"] = int(evt.get("seq", 0))
        # NOVO: campos opcionais de proporcionalidade vindos do TREA
        if "acc_balance" in evt:
            evt["acc_balance"] = float(evt["acc_balance"])
        if "acc_equity" in evt:
            evt["acc_equity"] = float(evt["acc_equity"])

    except Exception as e:
        raise ValueError(f"Invalid types: {e}")

    evt["action"] = str(evt["action"]).upper()
    if evt["action"] not in ACTIONS and evt["action"] not in {"OPEN", "CLOSE_ALL"}:
        raise ValueError(f"Unsupported action: {evt['action']}")

def _upstash_cmd(args: List[str]) -> Dict[str, Any]:
    if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
        return {"ok": False, "error": "missing_upstash_env"}

    try:
        resp = _UPSTASH_SESSION.post(
            UPSTASH_REDIS_REST_URL,
            data=json.dumps(args).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}",
                "Content-Type": "application/json",
            },
            timeout=(UPSTASH_CONNECT_TIMEOUT, UPSTASH_READ_TIMEOUT),
        )

        raw = resp.text or ""
        if resp.status_code >= 400:
            if TFA_LOG_UPSTASH_ERRORS:
                return {"ok": False, "error": f"http_{resp.status_code}", "body": raw[:200]}
            return {"ok": False, "error": f"http_{resp.status_code}"}
        return json.loads(raw) if raw else {"result": None}
    except Exception as e:
        if TFA_LOG_UPSTASH_ERRORS:
            return {"ok": False, "error": f"upstash_exc:{e}"}
        return {"ok": False, "error": "upstash_exc"}

# ===================== Redis Streams (core) =====================
def _stream_name(trader_key: str) -> str:
    tk = (trader_key or "").strip()
    return f"{TFA_STREAM_PREFIX}{tk}"

def _dedupe_key(trader_key: str, event_id: str) -> str:
    tk = (trader_key or "").strip()
    eid = (event_id or "").strip()
    return f"{TFA_DEDUPE_PREFIX}{tk}:{eid}"

def _redis_set_dedupe_if_new(trader_key: str, event_id: str, stream_id: str = "") -> bool:
    """
    Retorna True se conseguiu registrar como novo (NX), False se já existia (duplicado).
    """
    if not event_id:
        # se não tiver event_id, não dá pra dedupar; trata como novo
        return True

    key = _dedupe_key(trader_key, event_id)
    val = stream_id or "1"
    # SET key val NX EX <ttl>
    r = _upstash_cmd(["SET", key, val, "NX", "EX", str(TFA_DEDUPE_TTL_SEC)])
    # Upstash REST normalmente retorna {"result": "OK"} quando seta, ou {"result": None} se não setou (já existia)
    return isinstance(r, dict) and (r.get("result") == "OK")

def _redis_xadd_event(evt: Dict[str, Any]) -> Dict[str, Any]:
    """
    Grava no Redis Stream do trader_key. Retorna dict:
      {ok:bool, stream:str, redis_id:str, error?:str}
    """
    if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
        return {"ok": False, "error": "missing_upstash_env"}

    trader_key = (evt.get("trader_key") or "").strip()
    if not trader_key:
        trader_key = f"trader_{evt.get('trader_id','')}".strip()

    stream = _stream_name(trader_key)
    evt_json = json.dumps(evt, ensure_ascii=False, separators=(",", ":"))

    # Campos indexáveis + payload completo em "json"
    args = [
        "XADD", stream, "*",
        "event_id", str(evt.get("event_id", "")),
        "seq", str(evt.get("seq", 0)),
        "ts", str(evt.get("ts", 0)),
        "server_ts", str(int(evt.get("server_ts", int(time.time())))),
        "action", str(evt.get("action", "")),
        "symbol", str(evt.get("symbol", "")),
        "position_id", str(evt.get("position_id", 0)),
        "json", evt_json,
    ]

    r = _upstash_cmd(args)
    redis_id = r.get("result") if isinstance(r, dict) else None
    if not redis_id:
        return {"ok": False, "stream": stream, "error": f"xadd_failed:{r}"}
    return {"ok": True, "stream": stream, "redis_id": str(redis_id), "trader_key": trader_key}

def _redis_xread_events(trader_key: str, cursor: str, count: int) -> Dict[str, Any]:
    """
    Lê do stream a partir do cursor (Redis Stream ID).
    cursor:
      - "$" (não recomendado p/ MVP) -> só novos
      - "0-0" -> desde o início
      - "<last_id>" -> a partir do último consumido (retorna > last_id)
    Retorna:
      {ok, events:[dict], next_cursor, stream, error?}
    """
    if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
        return {"ok": False, "error": "missing_upstash_env"}

    tk = (trader_key or "").strip()
    stream = _stream_name(tk)
    cur = (cursor or "0-0").strip()
    # proteção anti-replay: se cur não tiver "-", força formato Redis ID
    if "-" not in cur:
        cur = f"{cur}-0"

    # XREAD COUNT <n> STREAMS <stream> <cursor>
    r = _upstash_cmd(["XREAD", "COUNT", str(max(1, count)), "STREAMS", stream, cur])
    if not isinstance(r, dict) or r.get("result") is None:
        # sem eventos
        return {"ok": True, "events": [], "next_cursor": cur, "stream": stream}

    try:
        # formato típico: [[stream, [[id, [k1,v1,k2,v2...]], [id2, [...]]]]]
        outer = r["result"]
        items = outer[0][1] if outer and outer[0] and len(outer[0]) > 1 else []
        out_events: List[Dict[str, Any]] = []
        next_cursor = cur

        for it in items:
            rid = it[0]
            kv = it[1]  # lista [k,v,k,v...]
            d = {kv[i]: kv[i+1] for i in range(0, len(kv), 2)}
            payload = d.get("json", "")
            if payload:
                try:
                    evt = json.loads(payload)
                    if isinstance(evt, dict):
                        evt["_redis_id"] = rid
                        out_events.append(evt)
                except Exception:
                    pass
            next_cursor = rid  # último id lido

        return {"ok": True, "events": out_events, "next_cursor": next_cursor, "stream": stream}
    except Exception as e:
        return {"ok": False, "error": f"xread_parse_failed:{e}", "raw": str(r)[:200], "stream": stream}

def _redis_xread_events_block(trader_key: str, cursor: str, count: int, block_ms: int) -> Dict[str, Any]:
    """
    XREAD com BLOCK (long-poll real no Redis Streams).
    block_ms: 1..TFA_CONSUME_WAIT_MAX_MS
    """
    if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
        return {"ok": False, "error": "missing_upstash_env"}

    tk = (trader_key or "").strip()
    stream = _stream_name(tk)
    cur = (cursor or "0-0").strip()
    # proteção anti-replay: se cur não tiver "-", força formato Redis ID
    if "-" not in cur:
        cur = f"{cur}-0"

    ms = int(block_ms)
    ms = max(1, min(ms, int(TFA_CONSUME_WAIT_MAX_MS)))

    # XREAD BLOCK <ms> COUNT <n> STREAMS <stream> <cursor>
    r = _upstash_cmd(["XREAD", "BLOCK", str(ms), "COUNT", str(max(1, count)), "STREAMS", stream, cur])

    if not isinstance(r, dict) or r.get("result") is None:
        # timeout sem eventos
        return {"ok": True, "events": [], "next_cursor": cur, "stream": stream, "blocked_ms": ms}

    try:
        outer = r["result"]
        items = outer[0][1] if outer and outer[0] and len(outer[0]) > 1 else []
        out_events: List[Dict[str, Any]] = []
        next_cursor = cur

        for it in items:
            rid = it[0]
            kv = it[1]
            d = {kv[i]: kv[i+1] for i in range(0, len(kv), 2)}
            payload = d.get("json", "")
            if payload:
                try:
                    evt = json.loads(payload)
                    if isinstance(evt, dict):
                        evt["_redis_id"] = rid
                        out_events.append(evt)
                except Exception:
                    pass
            next_cursor = rid

        return {"ok": True, "events": out_events, "next_cursor": next_cursor, "stream": stream, "blocked_ms": ms}
    except Exception as e:
        return {"ok": False, "error": f"xread_block_parse_failed:{e}", "raw": str(r)[:200], "stream": stream}

def _shadow_xadd(evt: Dict[str, Any]) -> None:
    if not TFA_REDIS_SHADOW:
        return
    if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
        return

    trader_key = (evt.get("trader_key") or "").strip()
    if not trader_key:
        trader_key = f"trader_{evt.get('trader_id','')}".strip()

    stream = f"tfa:events:{trader_key}"
    evt_json = json.dumps(evt, ensure_ascii=False, separators=(",", ":"))

    args = [
        "XADD", stream, "*",
        "id", str(evt.get("id", 0)),
        "seq", str(evt.get("seq", 0)),
        "ts", str(evt.get("ts", 0)),
        "server_ts", str(evt.get("server_ts", 0)),
        "action", str(evt.get("action", "")),
        "symbol", str(evt.get("symbol", "")),
        "position_id", str(evt.get("position_id", 0)),
        "json", evt_json,
    ]

    logging.info(
        "REDIS_SHADOW: about to XADD stream=%s id=%s seq=%s",
        stream, evt.get("id", 0), evt.get("seq", 0)
    )

    r = _upstash_cmd(args)

    logging.info(
        "REDIS_SHADOW: XADD result stream=%s id=%s resp=%s",
        stream, evt.get("id", 0), r
    )

    if not isinstance(r, dict) or not r.get("result"):
        logging.info("REDIS_SHADOW: XADD failed stream=%s id=%s err=%s", stream, evt.get("id"), r)

# ======================== Routes ==========================
@app.get("/api/v1/health")
def health():
    s = STORE.stats()

    s["redis_streams_enabled"] = bool(TFA_REDIS_STREAMS)
    s["upstash_configured"] = bool(UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN)
    s["allow_query_token"] = bool(TFA_ALLOW_QUERY_TOKEN)
    s["consume_wait_default"] = int(TFA_CONSUME_WAIT_DEFAULT)
    s["consume_wait_max"] = int(TFA_CONSUME_WAIT_MAX)
    s["consume_count_default"] = int(TFA_CONSUME_COUNT)

    return jsonify({"status": "ok", "ts": int(time.time()), **s})


@app.post("/api/v1/metrics/reset")
@require_token_flexible
def metrics_reset():
    reset_token = (os.getenv("TFA_METRICS_RESET_TOKEN") or "").strip()
    req_token = (request.args.get("reset_token") or "").strip()

    if not reset_token or req_token != reset_token:
        return jsonify({"ok": False, "error": "reset_forbidden"}), 403

    with _METRICS_LOCK:
        for k in _METRICS.keys():
            _METRICS[k] = 0

    return jsonify({"ok": True, "ts": int(time.time()), "metrics": dict(_METRICS)}), 200


@app.get("/api/v1/metrics")
@require_token_flexible
def metrics():
    with _METRICS_LOCK:
        snap = dict(_METRICS)
    return jsonify({"ok": True, "ts": int(time.time()), "metrics": snap}), 200


@app.post("/api/v1/events/publish")
@require_token_flexible
def publish_event():
    try:
        evt = parse_json_body()
        api_recv_ms = int(time.time() * 1000)
        evt["api_recv_ms"] = api_recv_ms
    except ValueError as ve:
        raw_dbg = request.get_data(cache=True, as_text=True)
        return jsonify({
            "ok": False,
            "error": str(ve),
            "ct": (request.content_type or ""),
            "raw_head": (raw_dbg[:200] if raw_dbg else ""),
        }), 400
    except Exception as e:
        return jsonify({"ok": False, "error": f"internal_parse:{e}"}), 500

    trader_key = (evt.get("trader_key") or "").strip()
    if not trader_key:
        return jsonify({"ok": False, "error": "missing_trader_key"}), 400

    event_id = (evt.get("event_id") or "").strip()
    if not event_id:
        event_id = (
            f"{trader_key}|"
            f"{evt.get('seq',0)}|"
            f"{evt.get('action','')}|"
            f"{evt.get('position_id',0)}|"
            f"{evt.get('ts',0)}"
        )
        evt["event_id"] = event_id

    evt["server_ts"] = int(api_recv_ms / 1000)
    evt["cloud_pub_ms"] = int(time.time() * 1000)

    if TFA_REDIS_STREAMS:
        is_new = _redis_set_dedupe_if_new(trader_key, event_id)
        if not is_new:
            with _METRICS_LOCK:
                _METRICS["publish_ok_total"] += 1
                _METRICS["publish_duplicate_total"] += 1

            return jsonify({
                "ok": True,
                "duplicate": True,
                "event_id": event_id,
                "trader_key": trader_key,
                "api_recv_ms": evt.get("api_recv_ms", 0),
                "cloud_pub_ms": evt.get("cloud_pub_ms", 0),
            }), 200

        r = _redis_xadd_event(evt)
        if not r.get("ok"):
            return jsonify({
                "ok": False,
                "error": "redis_xadd_failed",
                "detail": r,
                "cloud_pub_ms": evt["cloud_pub_ms"],
            }), 500

        try:
            print(f"API_PUBLISH_OK trader_key={trader_key} stream={r.get('stream')} redis_id={r.get('redis_id')}", flush=True)
        except Exception:
            pass

        with _METRICS_LOCK:
            _METRICS["publish_ok_total"] += 1

        return jsonify({
            "ok": True,
            "event_id": event_id,
            "trader_key": trader_key,
            "redis_stream": r.get("stream"),
            "redis_id": r.get("redis_id"),
            "server_ts": evt["server_ts"],
            "api_recv_ms": evt.get("api_recv_ms", 0),
            "cloud_pub_ms": evt["cloud_pub_ms"],
        }), 200

    STORE.add(evt)
    return jsonify({
        "ok": True,
        "legacy": True,
        "event_id": event_id,
        "trader_key": trader_key,
        "api_recv_ms": evt.get("api_recv_ms", 0),
        "cloud_pub_ms": evt.get("cloud_pub_ms", 0),
        "server_ts": evt.get("server_ts", 0),
    }), 200


@app.post("/api/v1/events/ack")
@require_token_flexible
def ack_event():
    try:
        b = parse_json_body()
    except Exception:
        b = {}

    trader_key = (b.get("trader_key") or "").strip()
    event_id   = (b.get("event_id") or "").strip()
    redis_id   = (b.get("redis_id") or b.get("_redis_id") or "").strip()

    if not trader_key:
        return jsonify({"ok": False, "error": "missing_trader_key"}), 400
    if not event_id and not redis_id:
        return jsonify({"ok": False, "error": "missing_event_id_or_redis_id"}), 400

    api_ack_ms = int(time.time() * 1000)

    key = f"tfa:ack:{trader_key}:{event_id or redis_id}"
    r = _upstash_cmd(["SET", key, str(api_ack_ms), "PX", "86400000"])  # 24h

    if not isinstance(r, dict) or r.get("result") not in ("OK", True):
        return jsonify({"ok": False, "error": "ack_set_failed", "detail": r}), 500

    return jsonify({
        "ok": True,
        "trader_key": trader_key,
        "event_id": event_id,
        "redis_id": redis_id,
        "api_ack_ms": api_ack_ms
    }), 200


@app.route("/api/v1/events/consume", methods=["GET", "POST"])
@require_token_flexible
def consume_events():
    if request.method == "POST":
        try:
            b = parse_json_body()
        except Exception:
            b = {}
        trader_key = str(b.get("trader_key", "") or "").strip()
        cursor = str(b.get("cursor", "0-0") or "0-0").strip()
        try:
            count = int(b.get("count", TFA_CONSUME_COUNT))
            count = max(1, min(count, 1000))
        except Exception:
            count = TFA_CONSUME_COUNT
    else:
        trader_key = (request.args.get("trader_key") or "").strip()
        cursor = (request.args.get("cursor") or "0-0").strip()
        try:
            count = int(request.args.get("count") or TFA_CONSUME_COUNT)
            count = max(1, min(count, 1000))
        except Exception:
            count = TFA_CONSUME_COUNT

    if not trader_key:
        return jsonify({"ok": False, "error": "missing_trader_key"}), 400

    if TFA_REDIS_STREAMS:
        r = _redis_xread_events(trader_key, cursor, count)
        if not r.get("ok"):
            return jsonify({"ok": False, "error": "redis_xread_failed", "detail": r}), 500

        events_out = r.get("events", []) or []

        with _METRICS_LOCK:
            _METRICS["consume_ok_total"] += 1
            _METRICS["consume_events_total"] += len(events_out)

        return jsonify({
            "ok": True,
            "trader_key": trader_key,
            "stream": r.get("stream"),
            "cursor": cursor,
            "next_cursor": r.get("next_cursor"),
            "events": events_out
        }), 200

    try:
        since_id = int(cursor.split("-")[0]) if cursor else 0
    except Exception:
        since_id = 0

    events = STORE.since(since_id)
    next_cursor = cursor
    if events:
        next_cursor = str(events[-1].get("id", since_id))

    return jsonify({
        "ok": True,
        "legacy": True,
        "trader_key": trader_key,
        "cursor": cursor,
        "next_cursor": next_cursor,
        "events": events
    }), 200


@app.route("/api/v1/events/consume_wait", methods=["GET", "POST"])
@require_token_flexible
def consume_events_wait():
    try:
        data = parse_json_body()
    except Exception:
        data = {}

    trader_key = (request.args.get("trader_key") or data.get("trader_key") or data.get("feed_id") or "").strip()
    cursor     = (request.args.get("cursor")     or data.get("cursor")     or "0-0").strip()

    if not trader_key:
        return jsonify({"ok": False, "error": "missing_trader_key"}), 400

    if isinstance(cursor, str) and cursor.strip().lower() in ("latest", "$"):
        stream = _stream_name(trader_key)
        last_id = "0-0"
        try:
            rr = _upstash_cmd(["XREVRANGE", stream, "+", "-", "COUNT", "1"])
            if isinstance(rr, dict) and rr.get("result"):
                last_id = str(rr["result"][0][0])
        except Exception:
            last_id = "0-0"

        return jsonify({
            "ok": True,
            "trader_key": trader_key,
            "stream": stream,
            "cursor": cursor,
            "next_cursor": last_id,
            "events": [],
            "waited_s": 0.0
        }), 200

    try:
        count = int(request.args.get("count") or data.get("count") or TFA_CONSUME_COUNT)
        count = max(1, min(count, 1000))
    except Exception:
        count = TFA_CONSUME_COUNT

    try:
        raw_wait_ms = request.args.get("wait_ms", None)
        if raw_wait_ms is None:
            raw_wait_ms = data.get("wait_ms", None)

        if raw_wait_ms is not None and str(raw_wait_ms).strip() != "":
            wait_ms = int(float(str(raw_wait_ms).strip()))
        else:
            raw_wait_s = request.args.get("wait", None)
            if raw_wait_s is None:
                raw_wait_s = data.get("wait", None)

            if raw_wait_s is None or str(raw_wait_s).strip() == "":
                wait_ms = int(TFA_CONSUME_WAIT_DEFAULT_MS)
            else:
                wait_ms = int(float(str(raw_wait_s).strip()) * 1000.0)

        wait_ms = max(0, min(wait_ms, int(TFA_CONSUME_WAIT_MAX_MS)))
    except Exception:
        wait_ms = int(TFA_CONSUME_WAIT_DEFAULT_MS)

    if wait_ms <= 0:
        r = _redis_xread_events(trader_key, cursor, count)
        if not r.get("ok"):
            return jsonify({"ok": False, "error": "redis_xread_failed", "detail": r}), 500

        events_out = r.get("events", []) or []

        if events_out:
            for e in events_out:
                try:
                    api_recv_ms = int(e.get("api_recv_ms", 0) or 0)
                except Exception:
                    api_recv_ms = 0

                eid = (e.get("event_id") or "").strip()
                rid = (e.get("_redis_id") or "").strip()

                ack_key = ""
                if eid:
                    ack_key = f"tfa:ack:{trader_key}:{eid}"
                elif rid:
                    ack_key = f"tfa:ack:{trader_key}:{rid}"

                api_ack_ms = 0
                if ack_key:
                    rr = _upstash_cmd(["GET", ack_key])
                    if isinstance(rr, dict) and rr.get("result"):
                        try:
                            api_ack_ms = int(rr["result"])
                        except Exception:
                            api_ack_ms = 0

                if api_ack_ms > 0:
                    e["api_ack_ms"] = api_ack_ms
                    if api_recv_ms > 0 and api_ack_ms >= api_recv_ms:
                        e["end_to_end_official_ms"] = int(api_ack_ms - api_recv_ms)

        now_ms = int(time.time() * 1000)
        if events_out:
            for e in events_out:
                try:
                    pub_ms = int(e.get("cloud_pub_ms", 0) or 0)
                except Exception:
                    pub_ms = 0
                if pub_ms > 0:
                    e["cloud_queue_dt_ms"] = max(0, now_ms - pub_ms)
                else:
                    e["cloud_queue_dt_ms"] = -1
                e["cloud_consume_ms"] = now_ms

        with _METRICS_LOCK:
            _METRICS["consume_ok_total"] += 1
            _METRICS["consume_events_total"] += len(events_out)

        return jsonify({
            "ok": True,
            "trader_key": trader_key,
            "stream": r.get("stream"),
            "cursor": cursor,
            "next_cursor": r.get("next_cursor"),
            "events": events_out,
            "waited_s": 0.0
        }), 200

    t0 = time.time()
    r = _redis_xread_events_block(trader_key, cursor, count, wait_ms)
    if not r.get("ok"):
        return jsonify({"ok": False, "error": "redis_xread_failed", "detail": r}), 500

    events_out = r.get("events", []) or []

    if events_out:
        for e in events_out:
            try:
                api_recv_ms = int(e.get("api_recv_ms", 0) or 0)
            except Exception:
                api_recv_ms = 0

            eid = (e.get("event_id") or "").strip()
            rid = (e.get("_redis_id") or "").strip()

            ack_key = ""
            if eid:
                ack_key = f"tfa:ack:{trader_key}:{eid}"
            elif rid:
                ack_key = f"tfa:ack:{trader_key}:{rid}"

            api_ack_ms = 0
            if ack_key:
                rr = _upstash_cmd(["GET", ack_key])
                if isinstance(rr, dict) and rr.get("result"):
                    try:
                        api_ack_ms = int(rr["result"])
                    except Exception:
                        api_ack_ms = 0

            if api_ack_ms > 0:
                e["api_ack_ms"] = api_ack_ms
                if api_recv_ms > 0 and api_ack_ms >= api_recv_ms:
                    e["end_to_end_official_ms"] = int(api_ack_ms - api_recv_ms)

    with _METRICS_LOCK:
        _METRICS["consume_ok_total"] += 1
        _METRICS["consume_events_total"] += len(events_out)

    waited_s = round(time.time() - t0, 3)

    return jsonify({
        "ok": True,
        "trader_key": trader_key,
        "stream": r.get("stream"),
        "cursor": cursor,
        "next_cursor": r.get("next_cursor"),
        "events": events_out,
        "waited_s": waited_s
    }), 200


def _iter_ndjson(objs: Iterable[Dict[str, Any]]):
    for obj in objs:
        yield json.dumps(obj, separators=(",", ":")) + "\n"


@app.get("/api/v1/events/stream_ndjson")
@require_token_flexible
def stream_ndjson():
    try:
        trader_key = (request.args.get("trader_key", "") or "").strip()
        since_seq_raw = (request.args.get("since_seq", "") or "").strip()

        if trader_key and since_seq_raw != "":
            try:
                since_seq = int(since_seq_raw)
            except Exception:
                since_seq = 0
            events = STORE.since_seq(trader_key, since_seq)
            return Response(_iter_ndjson(events), mimetype="application/x-ndjson")

        since_raw = (request.args.get("since", "0") or "0").strip()
        try:
            since_id = int(since_raw)
        except Exception:
            since_id = 0

        events = STORE.since(since_id)
        return Response(_iter_ndjson(events), mimetype="application/x-ndjson")
    except Exception as e:
        return jsonify({"error": f"internal: {e}"}), 500


@app.get("/api/v1/events/stream_sse")
@require_token_flexible
def stream_sse():
    trader_key = (request.args.get("trader_key") or "").strip()
    if not trader_key:
        return jsonify({"ok": False, "error": "missing_trader_key"}), 400

    cursor = (request.args.get("cursor") or "latest").strip()
    try:
        block_ms = int(request.args.get("block_ms") or "25000")
    except Exception:
        block_ms = 25000
    block_ms = max(1, min(block_ms, int(TFA_CONSUME_WAIT_MAX_MS)))

    try:
        keepalive_s = int(request.args.get("keepalive_s") or "15")
    except Exception:
        keepalive_s = 15
    keepalive_s = max(5, min(keepalive_s, 60))

    if cursor.lower() in ("latest", "$"):
        stream = _stream_name(trader_key)
        last_id = "0-0"
        try:
            rr = _upstash_cmd(["XREVRANGE", stream, "+", "-", "COUNT", "1"])
            if isinstance(rr, dict) and rr.get("result"):
                last_id = str(rr["result"][0][0])
        except Exception:
            last_id = "0-0"
        cursor = last_id

    @stream_with_context
    def gen():
        nonlocal cursor
        last_keepalive = time.time()
        yield ": connected\n\n"

        while True:
            r = _redis_xread_events_block(trader_key, cursor, 50, block_ms)
            if not r.get("ok"):
                err = {"ok": False, "error": "redis_xread_failed", "detail": r}
                yield f"event: error\ndata: {json.dumps(err, separators=(',',':'))}\n\n"
                return

            events_out = r.get("events", []) or []
            if events_out:
                for evt in events_out:
                    payload = json.dumps(evt, ensure_ascii=False, separators=(",", ":"))
                    yield f"data: {payload}\n\n"
                cursor = str(r.get("next_cursor") or cursor)

            now = time.time()
            if now - last_keepalive >= keepalive_s:
                yield f": keepalive {int(now)}\n\n"
                last_keepalive = now

    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return Response(gen(), headers=headers)


# ======================== Main ============================
if __name__ == "__main__":
    app.run(host=HOST, port=PORT, threaded=True)

