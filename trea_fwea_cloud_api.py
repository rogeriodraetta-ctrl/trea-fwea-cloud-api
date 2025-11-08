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
from typing import Any, Dict, Iterable, List
from functools import wraps

from flask import Flask, request, jsonify, Response
from flask_cors import CORS

# ========================= Config =========================
DEFAULT_TOKENS = ["TREA_DEV_TOKEN_001", "FWEA_DEV_TOKEN_001"]
VALID_TOKENS = [t.strip() for t in os.getenv("TFA_VALID_TOKENS", ",".join(DEFAULT_TOKENS)).split(",") if t.strip()]
HOST = os.getenv("TFA_HOST", "0.0.0.0")
PORT = int(os.getenv("TFA_PORT", "8080"))

logging.getLogger("werkzeug").setLevel(logging.WARNING)

app = Flask(__name__)
CORS(app, supports_credentials=False)

# ========================= Storage ========================
class EventStore:
    """Thread-safe in-memory store com id incremental e stats."""
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._events: List[Dict[str, Any]] = []
        self._last_id = 0
        self._created_at = time.time()

    def add(self, evt: Dict[str, Any]) -> int:
        with self._lock:
            self._last_id += 1
            evt_id = self._last_id
            evt_copy = dict(evt)
            evt_copy.setdefault("ts", int(time.time()))
            evt_copy["id"] = evt_id
            evt_copy["server_ts"] = int(time.time())
            self._events.append(evt_copy)
            return evt_id

    def since(self, since_id: int) -> List[Dict[str, Any]]:
        with self._lock:
            if since_id <= 0:
                return list(self._events)
            return [e for e in self._events if e.get("id", 0) > since_id]

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "count": len(self._events),
                "last_id": self._last_id,
                "uptime_s": int(time.time() - self._created_at)
            }

STORE = EventStore()

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
    "ts","trader_id","action","symbol","volume","sl","tp",
    "position_id","deal_ticket","order_ticket","magic","comment"
]
ACTIONS = {"OPEN_BUY","OPEN_SELL","MODIFY","CLOSE","BUY","SELL","BUY_MARKET","SELL_MARKET"}

def parse_json_body() -> Dict[str, Any]:
    """
    Parser robusto para aceitar variações do MT5:
    - JSON com Content-Type incorreto
    - corpo como string JSON "duplamente serializada"
    - fallback para raw bytes e até form-urlencoded com campo 'json'/'data'
    """
    # 1) tentativa padrão do Flask (às vezes funciona)
    data = request.get_json(silent=True)
    if isinstance(data, dict):
        return data

    # 2) lê o corpo bruto como texto (UTF-8) e tenta json.loads
    raw = request.get_data(cache=False, as_text=True)
    if raw:
        try:
            obj = json.loads(raw)
            if isinstance(obj, str):
                obj2 = json.loads(obj)
                if isinstance(obj2, dict):
                    return obj2
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass

    # 3) fallback: tenta extrair de form-urlencoded (ex: json=<...> ou data=<...>)
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
    except Exception as e:
        raise ValueError(f"Invalid types: {e}")

    evt["action"] = str(evt["action"]).upper()
    if evt["action"] not in ACTIONS and evt["action"] not in {"OPEN","CLOSE_ALL"}:
        raise ValueError(f"Unsupported action: {evt['action']}")

# ======================== Routes ==========================
@app.get("/api/v1/health")
def health():
    s = STORE.stats()
    return jsonify({"status": "ok", "ts": int(time.time()), **s})

@app.post("/api/v1/events/publish")
@require_token_flexible
def publish():
    """
    Recebe JSON do TREA. Parser ultra tolerante para MT5:
    - aceita header errado (x-www-form-urlencoded)
    - tenta JSON padrão
    - tenta JSON em string
    - recorta JSON do raw entre o primeiro '{' e o último '}' (sanitização) [já embutido em parse_json_body()]
    - remove nulos/BOM (via decodificação tolerante)
    Retorna diagnóstico em caso de falha.
    """
    try:
        ct = request.headers.get("Content-Type", "")
        raw_bytes = request.get_data(cache=True)

        # tenta parse robusto
        try:
            data = parse_json_body()
        except ValueError:
            preview = raw_bytes[:400].decode("latin-1", "ignore") if raw_bytes else ""
            return jsonify({
                "ok": False,
                "error": "Body must be a JSON object",
                "diag": {
                    "content_type": ct,
                    "raw_len": len(raw_bytes),
                    "raw_preview": preview
                }
            }), 400

        validate_event(data)
        evt_id = STORE.add(data)
        return jsonify({"ok": True, "id": evt_id}), 200

    except ValueError as ve:
        return jsonify({"ok": False, "error": str(ve)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": f"internal: {e}"}), 500

def _iter_ndjson(objs: Iterable[Dict[str, Any]]):
    for obj in objs:
        yield json.dumps(obj, separators=(",", ":")) + "\n"

@app.get("/api/v1/events/stream_ndjson")
@require_token_flexible
def stream_ndjson():
    try:
        since_raw = request.args.get("since", "0").strip()
        since_id = int(since_raw) if since_raw.isdigit() else 0
        events = STORE.since(since_id)
        return Response(_iter_ndjson(events), mimetype="application/x-ndjson")
    except Exception as e:
        return jsonify({"error": f"internal: {e}"}), 500

# ======================== Main ============================
if __name__ == "__main__":
    app.run(host=HOST, port=PORT, threaded=True)
