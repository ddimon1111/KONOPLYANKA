import json
import os
import threading
import time
import sys
from flask import Flask, jsonify, request
from PyQt5 import QtCore, QtGui, QtWidgets

DATA_FILE = "game_data.json"
LOG_FILE = "bot_events.log"
SERVER_HOST = os.getenv("WEEDSIM_SERVER_HOST", "127.0.0.1")
SERVER_PORT = int(os.getenv("WEEDSIM_SERVER_PORT", "5001"))
LOCK = threading.Lock()

app = Flask(__name__)


def server_log(message: str) -> None:
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] SERVER | system | {message}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


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
    try:
        with LOCK:
            return jsonify(load_file())
    except Exception as e:
        server_log(f"GET /get failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/save")
def save_data():
    payload = request.get_json(silent=True) or {}
    try:
        with LOCK:
            save_file(payload)
        return jsonify({"ok": True})
    except Exception as e:
        server_log(f"POST /save failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/update_player")
def update_player():
    payload = request.get_json(silent=True) or {}
    user_id = str(payload.get("user_id", ""))
    updates = payload.get("updates", {})
    if not user_id or not isinstance(updates, dict):
        return jsonify({"ok": False, "error": "bad payload"}), 400
    try:
        with LOCK:
            data = load_file()
            players = data.setdefault("players", {})
            players.setdefault(user_id, {})
            players[user_id].update(updates)
            save_file(data)
        return jsonify({"ok": True})
    except Exception as e:
        server_log(f"POST /update_player failed for {user_id}: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/health")
def health():
    return jsonify({"ok": True, "status": "up", "port": SERVER_PORT})


@app.get("/logs")
def get_logs():
    kind = (request.args.get("type") or "").upper().strip()
    if not os.path.exists(LOG_FILE):
        return jsonify({"ok": True, "logs": []})
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()[-500:]
    if kind:
        lines = [line for line in lines if kind in line.upper()]
    return jsonify({"ok": True, "logs": [line.rstrip() for line in lines]})


@app.post("/admin/save_player")
def admin_save_player():
    payload = request.get_json(silent=True) or {}
    user_id = str(payload.get("user_id", "")).strip()
    updates = payload.get("updates", {})
    if not user_id or not isinstance(updates, dict):
        return jsonify({"ok": False, "error": "bad payload"}), 400
    try:
        with LOCK:
            data = load_file()
            players = data.setdefault("players", {})
            player = players.setdefault(user_id, {})
            for key, value in updates.items():
                player[key] = value
            save_file(data)
        return jsonify({"ok": True})
    except Exception as e:
        server_log(f"POST /admin/save_player failed for {user_id}: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/admin")
def admin_page():
    return """
<!doctype html>
<html><head><meta charset="utf-8"><title>WeedSimulator Admin</title>
<style>
body{font-family:Segoe UI,Arial,sans-serif;background:#111;color:#f2f2f2;margin:16px}
table{width:100%;border-collapse:collapse;margin-top:12px}th,td{border:1px solid #333;padding:8px}
input,select,button{background:#1e1e1e;color:#fff;border:1px solid #444;padding:6px;border-radius:6px}
button{cursor:pointer} .row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
pre{background:#1b1b1b;border:1px solid #333;padding:10px;max-height:260px;overflow:auto}
</style></head>
<body>
<h2>WeedSimulator Local Admin</h2>
<div class="row">
<button onclick="loadPlayers()">Обновить игроков</button>
<select id="logType"><option value="">ALL LOGS</option><option>RAID</option><option>BUY</option><option>USE</option><option>ERROR</option><option>WARN</option></select>
<button onclick="loadLogs()">Обновить логи</button></div>
<table id="players"><thead><tr><th>ID</th><th>Имя</th><th>Деньги</th><th>Уровень</th><th>Inventory(JSON)</th><th>Действие</th></tr></thead><tbody></tbody></table>
<h3>Логи</h3><pre id="logs"></pre>
<script>
async function loadPlayers(){
 const res=await fetch('/get'); const data=await res.json(); const players=(data.players||{});
 const body=document.querySelector('#players tbody'); body.innerHTML='';
 Object.entries(players).forEach(([id,p])=>{
  const tr=document.createElement('tr');
  tr.innerHTML=`<td>${id}</td><td><input value="${(p.username||'').replaceAll('"','&quot;')}"></td>
  <td><input type="number" value="${Number(p.money||0)}"></td>
  <td><input type="number" value="${Number(p.level||1)}"></td>
  <td><input value='${JSON.stringify(p.inventory||{}).replaceAll(\"'\",\"&#39;\")}'></td>
  <td><button>Сохранить</button></td>`;
  tr.querySelector('button').onclick=async()=>{
   let inv={}; try{inv=JSON.parse(tr.children[4].querySelector('input').value||'{}')}catch(e){alert('Inventory JSON invalid');return;}
   const updates={username:tr.children[1].querySelector('input').value,money:Number(tr.children[2].querySelector('input').value||0),level:Number(tr.children[3].querySelector('input').value||1),inventory:inv};
   const r=await fetch('/admin/save_player',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({user_id:id,updates})});
   const ans=await r.json(); if(!ans.ok){alert(ans.error||'save failed');}
  };
  body.appendChild(tr);
 });
}
async function loadLogs(){
 const type=document.getElementById('logType').value;
 const res=await fetch('/logs?type='+encodeURIComponent(type)); const data=await res.json();
 document.getElementById('logs').textContent=(data.logs||[]).join('\\n');
}
loadPlayers(); loadLogs();
</script></body></html>
"""


def backup_loop():
    while True:
        time.sleep(300)
        try:
            with LOCK:
                data = load_file()
                ts = int(time.time())
                path = f"data_backup_{ts}.json"
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            server_log(f"backup_loop failed: {e}")


if __name__ == "__main__":
    def run_server():
        threading.Thread(target=backup_loop, daemon=True).start()
        server_log(f"local server started on {SERVER_HOST}:{SERVER_PORT}")
        app.run(host=SERVER_HOST, port=SERVER_PORT, debug=False, use_reloader=False)

    if "--nogui" in sys.argv:
        run_server()
        raise SystemExit(0)

    class ServerThread(QtCore.QThread):
        status = QtCore.pyqtSignal(str)

        def run(self):
            try:
                self.status.emit("Server starting...")
                run_server()
            except Exception as e:
                self.status.emit(f"Error: {e}")

    class ServerWindow(QtWidgets.QWidget):
        def __init__(self):
            super().__init__()
            self.thread = None
            self.tray_icon = None
            self.setup_ui()
            self.setup_tray()

        def setup_ui(self):
            self.setWindowTitle("Local Data Server")
            self.setFixedSize(640, 380)
            self.setWindowFlags(QtCore.Qt.FramelessWindowHint)
            self.setAttribute(QtCore.Qt.WA_TranslucentBackground)

            frame = QtWidgets.QFrame(self)
            frame.setGeometry(10, 10, 620, 360)
            frame.setStyleSheet(
                "QFrame{background:rgba(35,35,35,230);border-radius:14px;border:1px solid rgba(255,255,255,35);} "
                "QPushButton{background:#3f8f4e;color:white;border-radius:10px;padding:8px;font-weight:600;} "
                "QPushButton:hover{background:#4fa95f;} QLabel{color:white;font-size:13px;}"
            )
            lay = QtWidgets.QVBoxLayout(frame)
            title = QtWidgets.QLabel("🌐 Local Flask Server (127.0.0.1:5001)")
            title.setAlignment(QtCore.Qt.AlignCenter)
            self.status_lbl = QtWidgets.QLabel("Stopped")
            self.log = QtWidgets.QPlainTextEdit()
            self.log.setReadOnly(True)
            self.start_btn = QtWidgets.QPushButton("▶ Start server")
            self.stop_btn = QtWidgets.QPushButton("⏹ Stop (close)")
            self.min_btn = QtWidgets.QPushButton("🗕 Minimize")
            self.tray_btn = QtWidgets.QPushButton("📌 To tray")
            self.start_btn.clicked.connect(self.start_server)
            self.stop_btn.clicked.connect(self.close)
            self.min_btn.clicked.connect(self.showMinimized)
            self.tray_btn.clicked.connect(self.minimize_to_tray)
            lay.addWidget(title)
            lay.addWidget(self.status_lbl)
            lay.addWidget(self.log)
            btns = QtWidgets.QGridLayout()
            btns.addWidget(self.start_btn, 0, 0)
            btns.addWidget(self.stop_btn, 0, 1)
            btns.addWidget(self.min_btn, 1, 0)
            btns.addWidget(self.tray_btn, 1, 1)
            lay.addLayout(btns)

        def start_server(self):
            if self.thread and self.thread.isRunning():
                self.status_lbl.setText("Already running")
                return
            self.thread = ServerThread()
            self.thread.status.connect(self.on_status)
            self.thread.start()

        def on_status(self, text: str):
            self.status_lbl.setText(text)
            self.log.appendPlainText(text)

        def setup_tray(self):
            if not QtWidgets.QSystemTrayIcon.isSystemTrayAvailable():
                return
            icon = self.style().standardIcon(QtWidgets.QStyle.SP_ComputerIcon)
            self.tray_icon = QtWidgets.QSystemTrayIcon(icon, self)
            menu = QtWidgets.QMenu()
            open_action = menu.addAction("Open")
            exit_action = menu.addAction("Exit")
            open_action.triggered.connect(self.restore_from_tray)
            exit_action.triggered.connect(QtWidgets.QApplication.instance().quit)
            self.tray_icon.setContextMenu(menu)
            self.tray_icon.activated.connect(self.on_tray_activated)
            self.tray_icon.show()

        def minimize_to_tray(self):
            if self.tray_icon:
                self.hide()
                self.tray_icon.showMessage("Local Data Server", "Свернуто в трей", QtWidgets.QSystemTrayIcon.Information, 2000)

        def restore_from_tray(self):
            self.show()
            self.raise_()
            self.activateWindow()

        def on_tray_activated(self, reason):
            if reason == QtWidgets.QSystemTrayIcon.DoubleClick:
                self.restore_from_tray()

    app_qt = QtWidgets.QApplication(sys.argv)
    w = ServerWindow()
    w.show()
    app_qt.exec()
