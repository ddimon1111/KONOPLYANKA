import json
import os
import threading
import time
import sys
from flask import Flask, jsonify, request
from PyQt5 import QtCore, QtGui, QtWidgets

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
    def run_server():
        threading.Thread(target=backup_loop, daemon=True).start()
        app.run(host="127.0.0.1", port=5001, debug=False, use_reloader=False)

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
