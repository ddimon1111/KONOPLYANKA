import json
import os
import threading
import time
from flask import Flask, jsonify, request

DATA_FILE = "game_data.json"
LOCK = threading.Lock()

app = Flask(__name__)


def ensure_shape(data):
    data.setdefault("players", {})
    data.setdefault("farms", {})
    data.setdefault("cartels", {})
    data.setdefault("tournaments", {})
    data.setdefault("meta", {})
    data.setdefault("auctions", {})
    data["meta"].setdefault("active_raid", None)
    data["meta"].setdefault("next_auction_id", 1)
    data["meta"].setdefault("next_tournament_id", 1)
    data["meta"].setdefault("next_duel_id", 1)
    data["meta"].setdefault("duels", {})
    return data


def load_file():
    if not os.path.exists(DATA_FILE):
        save_file(ensure_shape({}))
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            data = {}
    return ensure_shape(data)


def save_file(data):
    data = ensure_shape(data)
    tmp = DATA_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DATA_FILE)


@app.get("/get")
def get_data():
    with LOCK:
        return jsonify(load_file())


@app.post("/save")
def save_data():
    payload = request.get_json(silent=True) or {}
    with LOCK:
        save_file(payload)
    return jsonify({"ok": True})


@app.post("/update_player")
def update_player():
    payload = request.get_json(silent=True) or {}
    user_id = str(payload.get("user_id", ""))
    updates = payload.get("updates", {})
    if not user_id or not isinstance(updates, dict):
        return jsonify({"ok": False, "error": "bad payload"}), 400
    with LOCK:
        data = load_file()
        players = data.setdefault("players", {})
        players.setdefault(user_id, {})
        players[user_id].update(updates)
        save_file(data)
    return jsonify({"ok": True})


def backup_loop():
    while True:
        time.sleep(300)
        with LOCK:
            data = load_file()
            ts = int(time.time())
            path = f"data_backup_{ts}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    threading.Thread(target=backup_loop, daemon=True).start()
    app.run(host="127.0.0.1", port=5001, debug=False)
