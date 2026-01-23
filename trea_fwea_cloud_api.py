#!/usr/bin/env python3
"""
TREA & FWEA – Cloud API (Etapa 14 – Fase Cloud Real / Migração)

Endpoints (v1):
  • POST /api/v1/events/publish        - recebe eventos do TREA (JSON)
  • GET  /api/v1/events/stream_ndjson  - entrega stream NDJSON para o FWEA
  • GET  /api/v1/health                - heartbeat (público)

Segurança:
  • Authorization: Bearer <token>
  • Também aceita ?token=<...> na query string (fallback)
  • Tokens via env (TFA_VALID_TOKENS) separados por vírgula; fallback DEV.

Notas:
  • Buffer em memória com lock (thread-safe) para esta fase.
  • Campo incremental "id" (since) para consumo incremental do FWEA.
  • Logging reduzido (werkzeug WARNING).
"""

from __future__ import annotations
import os, json, time, threading, logging
import urllib.request
import urllib.error
from urllib.parse import parse_qs, unquote_plus
from typing import Any, Dict, Iterable, List
from functools import wraps

from flask import Flask, request, jsonify, Response
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

# ===== Redis Streams (MVP confiável) =====
TFA_REDIS_STREAMS = (
    os.getenv("TFA_REDIS_STREAMS", "1").strip().lower() in ("1", "true", "yes", "on")
)

TFA_STREAM_PREFIX = os.getenv("TFA_STREAM_PREFIX", "tfa:events:").strip()  # + trader_key
TFA_DEDUPE_PREFIX = os.getenv("TFA_DEDUPE_PREFIX", "tfa:dedupe:").strip()  # + trader_key + event_id
TFA_DEDUPE_TTL_SEC = int(os.getenv("TFA_DEDUPE_TTL_SEC", "604800"))  # 7 dias
TFA_CONSUME_COUNT = int(os.getenv("TFA_CONSUME_COUNT", "200"))

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

# ===================== Auth (flexível) ====================
def require_token_flexible(fn):
    """Aceita Authorization: Bearer <token> OU ?token=<token>."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        token = ""
        if auth.startswith("Bearer "):
            token = auth.split(" ", 1)[1].strip()
        if not token:
            token = request.args.get("token", "").strip()
        if not token:
            return jsonify({"error": "Missing or invalid token"}), 401
        if token not in VALID_TOKENS:
            return jsonify({"error": "Unauthorized"}), 403
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

    payload = json.dumps(args).encode("utf-8")
    req = urllib.request.Request(
        UPSTASH_REDIS_REST_URL,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="ignore")
        except Exception:
            pass
        return {"ok": False, "error": f"http_{e.code}", "body": body[:200]}
    except Exception as e:
        return {"ok": False, "error": f"upstash_exc:{e}"}

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
        "server_ts", str(int(time.time())),
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
    # last_seq_by_trader ajuda diagnóstico / recovery
    s["last_seq_by_trader"] = STORE.last_seq_by_trader(limit=50)
    return jsonify({"status": "ok", "ts": int(time.time()), **s})


@app.post("/api/v1/events/publish")
def publish_event():
    try:
        evt = parse_json_body()
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

    # --- validações mínimas ---
    trader_key = (evt.get("trader_key") or "").strip()
    if not trader_key:
        return jsonify({"ok": False, "error": "missing_trader_key"}), 400

    event_id = (evt.get("event_id") or "").strip()
    if not event_id:
        # gera event_id determinístico a partir do payload do TREA
        event_id = (
            f"{trader_key}|"
            f"{evt.get('seq',0)}|"
            f"{evt.get('action','')}|"
            f"{evt.get('position_id',0)}|"
            f"{evt.get('ts',0)}"
        )
        evt["event_id"] = event_id

    evt["server_ts"] = int(time.time())

    # --- Redis Streams (Fase 1) ---
    if TFA_REDIS_STREAMS:
        is_new = _redis_set_dedupe_if_new(trader_key, event_id)
        if not is_new:
            return jsonify({
                "ok": True,
                "duplicate": True,
                "event_id": event_id,
                "trader_key": trader_key
            }), 200

        r = _redis_xadd_event(evt)
        if not r.get("ok"):
            return jsonify({
                "ok": False,
                "error": "redis_xadd_failed",
                "detail": r
            }), 500

        return jsonify({
            "ok": True,
            "event_id": event_id,
            "trader_key": trader_key,
            "redis_stream": r.get("stream"),
            "redis_id": r.get("redis_id"),
            "server_ts": evt["server_ts"]
        }), 200

    # --- fallback legado ---
    STORE.add(evt)
    return jsonify({
        "ok": True,
        "legacy": True,
        "event_id": event_id,
        "trader_key": trader_key
    }), 200

@app.get("/api/v1/events/consume")
def consume_events():
    trader_key = (request.args.get("trader_key") or "").strip()
    if not trader_key:
        return jsonify({"ok": False, "error": "missing_trader_key"}), 400

    cursor = (request.args.get("cursor") or "0-0").strip()
    try:
        count = int(request.args.get("count") or TFA_CONSUME_COUNT)
        count = max(1, min(count, 1000))
    except Exception:
        count = TFA_CONSUME_COUNT

    # --- Redis Streams (Fase 1) ---
    if TFA_REDIS_STREAMS:
        r = _redis_xread_events(trader_key, cursor, count)
        if not r.get("ok"):
            return jsonify({
                "ok": False,
                "error": "redis_xread_failed",
                "detail": r
            }), 500

        return jsonify({
            "ok": True,
            "trader_key": trader_key,
            "stream": r.get("stream"),
            "cursor": cursor,
            "next_cursor": r.get("next_cursor"),
            "events": r.get("events", [])
        }), 200

    # --- fallback legado (NDJSON) ---
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

def _iter_ndjson(objs: Iterable[Dict[str, Any]]):
    for obj in objs:
        yield json.dumps(obj, separators=(",", ":")) + "\n"

@app.get("/api/v1/events/stream_ndjson")
@require_token_flexible
def stream_ndjson():
    """
    NDJSON stream para o FWEA.

    Compatibilidade:
      - Legado: ?since=<id>  -> retorna eventos com id > since
      - Novo (Opção B): ?trader_key=XXX&since_seq=YYY -> retorna eventos com seq > since_seq (ordenado por seq)
    """
    try:
        trader_key = (request.args.get("trader_key", "") or "").strip()
        since_seq_raw = (request.args.get("since_seq", "") or "").strip()

        # --- Novo cursor por SEQ ---
        if trader_key and since_seq_raw != "":
            try:
                since_seq = int(since_seq_raw)
            except Exception:
                since_seq = 0
            events = STORE.since_seq(trader_key, since_seq)
            return Response(_iter_ndjson(events), mimetype="application/x-ndjson")

        # --- Legado por id ---
        since_raw = (request.args.get("since", "0") or "0").strip()
        try:
            since_id = int(since_raw)
        except Exception:
            since_id = 0
        events = STORE.since(since_id)
        return Response(_iter_ndjson(events), mimetype="application/x-ndjson")
    except Exception as e:
        return jsonify({"error": f"internal: {e}"}), 500


# ======================== Main ============================
if __name__ == "__main__":
    app.run(host=HOST, port=PORT, threaded=True)

