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
VALID_TOKENS = [
    t.strip()
    for t in os.getenv("TFA_VALID_TOKENS", ",".join(DEFAULT_TOKENS)).split(",")
    if t.strip()
]
HOST = os.getenv("TFA_HOST", "0.0.0.0")
PORT = int(os.getenv("TFA_PORT", "8080"))

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

    def add(self, evt: Dict[str, Any]) -> int:
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
            return evt_id

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

# ======================== Routes ==========================
@app.get("/api/v1/health")
def health():
    s = STORE.stats()
    # last_seq_by_trader ajuda diagnóstico / recovery
    s["last_seq_by_trader"] = STORE.last_seq_by_trader(limit=50)
    return jsonify({"status": "ok", "ts": int(time.time()), **s})


@app.post("/api/v1/events/publish")
def publish():
    """
    Recebe JSON do TREA. Parser ultra tolerante para MT5:
    - aceita header errado (x-www-form-urlencoded)
    - remove BOM e byte nulo (\x00)
    - tenta JSON direto; se falhar, recorta entre { e } e tenta de novo
    - fallback para campos form ('json'/'data'/'body')
    Retorna diagnóstico em caso de falha.
    """
    try:
        ct = request.headers.get("Content-Type", "")
        raw_bytes = request.get_data(cache=False)  # sem cache para pegar o corpo exato

        def _clean_text(b: bytes) -> str:
            if not b:
                return ""
            s = b.decode("utf-8", errors="ignore")
            # remove BOM e byte nulo do MT5
            s = s.replace("\ufeff", "").replace("\x00", "")
            return s.strip()

        text = _clean_text(raw_bytes)
        data = None

        # 1) tenta via get_json forçado (independe do Content-Type)
        try:
            gj = request.get_json(force=True, silent=True)
            if isinstance(gj, dict):
                data = gj
        except Exception:
            pass

        # 2) tenta carregar o texto inteiro como JSON
        if data is None and text:
            try:
                obj = json.loads(text)
                # alguns clientes mandam string contendo um JSON
                if isinstance(obj, str):
                    obj2 = json.loads(obj)
                    if isinstance(obj2, dict):
                        data = obj2
                elif isinstance(obj, dict):
                    data = obj
            except Exception:
                pass

        # 3) recorta entre o 1º '{' e o último '}' e tenta novamente
        if data is None and text:
            i, j = text.find("{"), text.rfind("}")
            if i != -1 and j != -1 and j > i:
                slice_text = text[i : j + 1]
                try:
                    obj = json.loads(slice_text)
                    if isinstance(obj, dict):
                        data = obj
                except Exception:
                    pass

        # 4) fallback: form-urlencoded com campos 'json'/'data'/'body'
        if data is None and request.form:
            for k in ("json", "data", "body"):
                v = request.form.get(k, "")
                v = v.replace("\ufeff", "").replace("\x00", "").strip()
                if not v:
                    continue
                try:
                    obj = json.loads(v)
                    if isinstance(obj, dict):
                        data = obj
                        break
                except Exception:
                    continue

        if not isinstance(data, dict):
            preview = (
                raw_bytes[:400].decode("latin-1", "ignore") if raw_bytes else ""
            )
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "Body must be a JSON object",
                        "diag": {
                            "content_type": ct,
                            "raw_len": len(raw_bytes),
                            "raw_preview": preview,
                        },
                    }
                ),
                400,
            )

        # valida e persiste
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

