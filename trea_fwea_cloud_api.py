#!/usr/bin/env python3
"""
TREA & FWEA – Cloud API (Etapa 14 – Fase Cloud Real / Migração)

Endpoints (v1):
  • POST /api/v1/events/publish        - recebe eventos do TREA (JSON)
  • GET  /api/v1/events/stream_ndjson  - entrega stream NDJSON para o FWEA
  • GET  /api/v1/health                - heartbeat (público)

Segurança:
  • Authorization: Bearer <token>
  • Tokens via env (TFA_VALID_TOKENS) separados por vírgula
    - Fallback para tokens de desenvolvimento se env não for definido.

Notas:
  • Buffer em memória com lock (thread-safe) para primeira fase.
  • Campo incremental "id" (since) para o FWEA consumir incrementalmente.
  • Aceita JSON mesmo com Content-Type genérico (force=True).
  • Logging enxuto (Werkzeug reduzido para WARNING).

Execução local:
  export TFA_VALID_TOKENS="TREA_DEV_TOKEN_001,FWEA_DEV_TOKEN_001"
  export TFA_HOST=0.0.0.0
  export TFA_PORT=8080
  python trea_fwea_cloud_api.py

Requisitos (requirements.txt):
  flask>=3.0.0
  flask-cors>=4.0.0

"""
from __future__ import annotations
import os
import json
import time
import threading
from typing import Any, Dict, Iterable, List
from flask import Flask, request, jsonify, Response
from flask_cors import CORS

# ========================= Config =========================
DEFAULT_TOKENS = ["TREA_DEV_TOKEN_001", "FWEA_DEV_TOKEN_001"]
VALID_TOKENS = [t.strip() for t in os.getenv("TFA_VALID_TOKENS", ",".join(DEFAULT_TOKENS)).split(",") if t.strip()]
HOST = os.getenv("TFA_HOST", "0.0.0.0")
PORT = int(os.getenv("TFA_PORT", "8080"))

# Reduce werkzeug noise
import logging
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
            evt_copy.setdefault("ts", int(time.time()))  # se TREA não enviar ts
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
                "uptime_s": int(time.time() - self._created_at),
            }

STORE = EventStore()

# ===================== Auth Decorator =====================
from functools import wraps

def require_token(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return jsonify({"error": "Missing or invalid token"}), 401
        token = auth.split(" ", 1)[1].strip()
        if token not in VALID_TOKENS:
            return jsonify({"error": "Unauthorized"}), 403
        return fn(*args, **kwargs)
    return wrapper

# ======================= Validators =======================
REQUIRED_FIELDS = [
    "ts", "trader_id", "action", "symbol", "volume",
    "sl", "tp", "position_id", "deal_ticket", "order_ticket", "magic", "comment"
]

ACTIONS = {"OPEN_BUY", "OPEN_SELL", "MODIFY", "CLOSE", "BUY", "SELL", "BUY_MARKET", "SELL_MARKET"}


def parse_json_body() -> Dict[str, Any]:
    # tolerante a Content-Type incorreto
    data = request.get_json(silent=True, force=True)
    if not isinstance(data, dict):
        raise ValueError("Body must be a JSON object")
    return data


def validate_event(evt: Dict[str, Any]) -> None:
    missing = [k for k in REQUIRED_FIELDS if k not in evt]
    if missing:
        raise ValueError(f"Missing fields: {', '.join(missing)}")
    # tipos básicos / coerções leves
    try:
        evt["ts"] = int(evt["ts"]) if str(evt["ts"]).isdigit() else int(time.time())
        evt["volume"] = float(evt["volume"])
        evt["sl"] = float(evt["sl"]) if evt.get("sl") is not None else 0.0
        evt["tp"] = float(evt["tp"]) if evt.get("tp") is not None else 0.0
        evt["position_id"] = int(evt["position_id"]) if evt.get("position_id") is not None else 0
        evt["deal_ticket"] = int(evt["deal_ticket"]) if evt.get("deal_ticket") is not None else 0
        evt["order_ticket"] = int(evt["order_ticket"]) if evt.get("order_ticket") is not None else 0
        evt["magic"] = int(evt["magic"]) if evt.get("magic") is not None else 0
    except Exception as e:
        raise ValueError(f"Invalid types: {e}")

    evt["action"] = str(evt["action"]).upper()
    if evt["action"] not in ACTIONS:
        # permitir variantes do TREA
        if evt["action"] in {"OPEN", "CLOSE_ALL"}:
            pass
        else:
            raise ValueError(f"Unsupported action: {evt['action']}")

# ======================== Routes ==========================
@app.get("/api/v1/health")
def health():
    s = STORE.stats()
    return jsonify({"status": "ok", "ts": int(time.time()), **s})


@app.post("/api/v1/events/publish")
@require_token
def publish():
    try:
        evt = parse_json_body()
        validate_event(evt)
        evt_id = STORE.add(evt)
        return jsonify({"ok": True, "id": evt_id}), 200
    except ValueError as ve:
        return jsonify({"ok": False, "error": str(ve)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": f"internal: {e}"}), 500


def _iter_ndjson(objs: Iterable[Dict[str, Any]]):
    for obj in objs:
        yield json.dumps(obj, separators=(",", ":")) + "\n"


@app.get("/api/v1/events/stream_ndjson")
@require_token
def require_token(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        # 1) primeiro tenta header Authorization: Bearer <token>
        auth = request.headers.get("Authorization", "")
        token = ""
        if auth.startswith("Bearer "):
            token = auth.split(" ", 1)[1].strip()
        # 2) fallback: aceita ?token=<...> na query string
        if not token:
            token = request.args.get("token", "").strip()
        if not token:
            return jsonify({"error": "Missing or invalid token"}), 401
        if token not in VALID_TOKENS:
            return jsonify({"error": "Unauthorized"}), 403
        return fn(*args, **kwargs)
    return wrapper

# ======================== Main ============================
if __name__ == "__main__":
    app.run(host=HOST, port=PORT, threaded=True)

