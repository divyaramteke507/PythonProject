"""Simple multi-user LAN chat and file-sharing client with a Tkinter GUI.

Run with: python client.py
Connect to 127.0.0.1 for local testing, or enter the server computer's LAN IP.
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import re
import socket
import struct
import threading
import uuid
from pathlib import Path
from tkinter import END, DISABLED, NORMAL, Tk, StringVar, filedialog, messagebox
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText
from typing import Any

PORT = 5000
CHUNK_SIZE = 64 * 1024
MAX_FILE_SIZE = 25 * 1024 * 1024
MAX_CHAT_LENGTH = 2000
MAX_PACKET_SIZE = CHUNK_SIZE + 256 * 1024
APP_DIR = Path(__file__).resolve().parent
RECEIVED_DIR = APP_DIR / "received_files"


def _send_packet_unlocked(sock: socket.socket, header: dict[str, Any], payload: bytes) -> None:
    header_bytes = json.dumps(header, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    body_size = 4 + len(header_bytes) + len(payload)
    if body_size > MAX_PACKET_SIZE:
        raise ValueError("Packet is too large")
    sock.sendall(struct.pack("!I", body_size))
    sock.sendall(struct.pack("!I", len(header_bytes)))
    sock.sendall(header_bytes)
    if payload:
        sock.sendall(payload)


def send_packet(
    sock: socket.socket,
    header: dict[str, Any],
    payload: bytes = b"",
    lock: threading.Lock | None = None,
) -> None:
    if lock is None:
        _send_packet_unlocked(sock, header, payload)
    else:
        with lock:
            _send_packet_unlocked(sock, header, payload)


def recv_exact(sock: socket.socket, count: int) -> bytes:
    parts: list[bytes] = []
    remaining = count
    while remaining:
        try:
            part = sock.recv(remaining)
        except socket.timeout:
            continue
        if not part:
            raise ConnectionError("The server closed the connection")
        parts.append(part)
        remaining -= len(part)
    return b"".join(parts)


def recv_packet(sock: socket.socket) -> tuple[dict[str, Any], bytes]:
    body_size = struct.unpack("!I", recv_exact(sock, 4))[0]
    if body_size < 4 or body_size > MAX_PACKET_SIZE:
        raise ValueError("Invalid or oversized packet")
    header_size = struct.unpack("!I", recv_exact(sock, 4))[0]
    if header_size < 2 or header_size > body_size - 4 or header_size > 256 * 1024:
        raise ValueError("Invalid packet header")
    header = json.loads(recv_exact(sock, header_size).decode("utf-8"))
    if not isinstance(header, dict):
        raise ValueError("Packet header must be an object")
    payload_size = body_size - 4 - header_size
    payload = recv_exact(sock, payload_size) if payload_size else b""
    return header, payload


def safe_filename(value: str) -> str:
    value = value.replace("\\", "/").split("/")[-1]
    value = re.sub(r"[\x00-\x1f\x7f]", "", value).strip()
    return value[:180] or "received_file"


def unique_destination(folder: Path, filename: str) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    original = folder / safe_filename(filename)
    if not original.exists():
        return original
    stem, suffix = original.stem, original.suffix
    counter = 1
    while True:
        candidate = folder / f"{stem} ({counter}){suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


class ChatClientApp:
    def __init__(self, root: Tk) -> None:
        self.root = root
        self.root.title("Simple LAN Chat and File Sharing")
        self.root.geometry("800x590")
        self.root.minsize(640, 450)

        self.sock: socket.socket | None = None
        self.send_lock = threading.Lock()
        self.events: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.closing = False
        self.username = ""
        self.incoming_files: dict[str, dict[str, Any]] = {}

        self.host_var = StringVar(value="127.0.0.1")
        self.username_var = StringVar()
        self.status_var = StringVar(value="Not connected")

        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self.close_app)
        self.root.after(100, self._poll_events)

    def _build_ui(self) -> None:
        connection = ttk.LabelFrame(self.root, text="Connection")
        connection.pack(fill="x", padx=10, pady=(10, 5))

        ttk.Label(connection, text="Server IP:").grid(row=0, column=0, padx=(8, 4), pady=8, sticky="w")
        self.host_entry = ttk.Entry(connection, textvariable=self.host_var, width=18)
        self.host_entry.grid(row=0, column=1, padx=4, pady=8, sticky="w")

        ttk.Label(connection, text="Username:").grid(row=0, column=2, padx=(12, 4), pady=8, sticky="w")
        self.username_entry = ttk.Entry(connection, textvariable=self.username_var, width=18)
        self.username_entry.grid(row=0, column=3, padx=4, pady=8, sticky="w")

        self.connect_button = ttk.Button(connection, text="Connect", command=self.connect)
        self.connect_button.grid(row=0, column=4, padx=8, pady=8)
        connection.columnconfigure(5, weight=1)

        self.chat_log = ScrolledText(self.root, wrap="word", state=DISABLED, font=("Consolas", 10))
        self.chat_log.pack(fill="both", expand=True, padx=10, pady=5)

        compose = ttk.Frame(self.root)
        compose.pack(fill="x", padx=10, pady=(5, 5))
        self.message_var = StringVar()
        self.message_entry = ttk.Entry(compose, textvariable=self.message_var, state=DISABLED)
        self.message_entry.pack(side="left", fill="x", expand=True, padx=(0, 6))
        self.message_entry.bind("<Return>", lambda _event: self.send_message())
        self.send_button = ttk.Button(compose, text="Send", command=self.send_message, state=DISABLED)
        self.send_button.pack(side="left", padx=(0, 6))
        self.file_button = ttk.Button(compose, text="Share File", command=self.choose_file, state=DISABLED)
        self.file_button.pack(side="left")

        ttk.Label(self.root, textvariable=self.status_var, anchor="w", relief="sunken").pack(fill="x", side="bottom")

    def _log(self, message: str) -> None:
        self.chat_log.configure(state=NORMAL)
        self.chat_log.insert(END, message + "\n")
        self.chat_log.see(END)
        self.chat_log.configure(state=DISABLED)

    def connect(self) -> None:
        host = self.host_var.get().strip()
        username = self.username_var.get().strip()
        if not host or not username:
            messagebox.showwarning("Missing details", "Enter the server IP address and a username.", parent=self.root)
            return
        if len(username) > 24:
            messagebox.showwarning("Username too long", "Use a username of 24 characters or fewer.", parent=self.root)
            return
        self.connect_button.configure(state=DISABLED)
        self.status_var.set("Connecting...")
        threading.Thread(target=self._connect_worker, args=(host, username), daemon=True).start()

    def _connect_worker(self, host: str, username: str) -> None:
        sock: socket.socket | None = None
        try:
            sock = socket.create_connection((host, PORT), timeout=10)
            sock.settimeout(15.0)
            self.send_lock = threading.Lock()
            send_packet(sock, {"type": "hello", "username": username}, lock=self.send_lock)
            header, payload = recv_packet(sock)
            if payload:
                raise ValueError("Unexpected data received during connection")
            if header.get("type") != "welcome":
                raise ConnectionError(header.get("message", "The server rejected the connection."))
            self.sock = sock
            self.username = str(header.get("username", username))
            self.events.put(("connected", self.username))
            threading.Thread(target=self._receive_loop, args=(sock,), daemon=True).start()
        except Exception as exc:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
            self.events.put(("connection_failed", str(exc)))

    def _receive_loop(self, sock: socket.socket) -> None:
        reason = "Disconnected from server."
        try:
            while not self.closing:
                header, payload = recv_packet(sock)
                packet_type = header.get("type")
                if packet_type == "history":
                    self.events.put(("history", header.get("messages", [])))
                elif packet_type == "chat":
                    self.events.put(("chat", header))
                elif packet_type == "system":
                    self.events.put(("system", header.get("message", "System message")))
                elif packet_type == "error":
                    self.events.put(("error", header.get("message", "Server error")))
                elif packet_type == "file_start":
                    self._receive_file_start(header)
                elif packet_type == "file_chunk":
                    self._receive_file_chunk(header, payload)
                elif packet_type == "file_end":
                    self._receive_file_end(header)
                elif packet_type == "file_abort":
                    self._receive_file_abort(header)
        except (ConnectionError, OSError, ValueError, json.JSONDecodeError) as exc:
            reason = str(exc) or reason
        finally:
            try:
                sock.close()
            except OSError:
                pass
            self.events.put(("disconnected", reason))

    def _receive_file_start(self, header: dict[str, Any]) -> None:
        file_id = header.get("file_id")
        filename = safe_filename(str(header.get("filename", "received_file")))
        size = header.get("size", -1)
        digest = str(header.get("sha256", "")).lower()
        sender = str(header.get("sender", "Another user"))
        if not isinstance(file_id, str) or not isinstance(size, int) or size < 0 or size > MAX_FILE_SIZE:
            self.events.put(("error", "Received invalid file-transfer details."))
            return
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            self.events.put(("error", f"Invalid checksum for {filename}; transfer ignored."))
            return
        if file_id in self.incoming_files:
            self.events.put(("error", f"Duplicate file transfer ignored: {filename}"))
            return
        try:
            destination = unique_destination(RECEIVED_DIR, filename)
            handle = destination.open("wb")
            self.incoming_files[file_id] = {
                "handle": handle,
                "path": destination,
                "filename": filename,
                "expected_size": size,
                "received": 0,
                "expected_hash": digest,
                "hasher": hashlib.sha256(),
                "sender": sender,
            }
            self.events.put(("status", f"Receiving {filename} from {sender}..."))
        except OSError as exc:
            self.events.put(("error", f"Could not save {filename}: {exc}"))

    def _receive_file_chunk(self, header: dict[str, Any], payload: bytes) -> None:
        file_id = header.get("file_id")
        info = self.incoming_files.get(file_id) if isinstance(file_id, str) else None
        if info is None:
            return
        if info["received"] + len(payload) > info["expected_size"]:
            self._discard_incoming_file(file_id, "Received file was larger than expected.")
            return
        try:
            info["handle"].write(payload)
            info["hasher"].update(payload)
            info["received"] += len(payload)
        except OSError as exc:
            self._discard_incoming_file(file_id, f"Could not write received file: {exc}")

    def _receive_file_end(self, header: dict[str, Any]) -> None:
        file_id = header.get("file_id")
        info = self.incoming_files.pop(file_id, None) if isinstance(file_id, str) else None
        if info is None:
            return
        try:
            info["handle"].close()
            size_ok = info["received"] == info["expected_size"]
            hash_ok = info["hasher"].hexdigest() == info["expected_hash"] == str(header.get("sha256", "")).lower()
            if size_ok and hash_ok:
                self.events.put(("file_received", str(info["path"])))
            else:
                try:
                    info["path"].unlink(missing_ok=True)
                except OSError:
                    pass
                self.events.put(("error", f"Integrity check failed for {info['filename']}; incomplete file deleted."))
        except OSError as exc:
            self.events.put(("error", f"Could not finish receiving {info['filename']}: {exc}"))

    def _receive_file_abort(self, header: dict[str, Any]) -> None:
        file_id = header.get("file_id")
        message = str(header.get("message", "File transfer was cancelled."))
        self._discard_incoming_file(file_id, message)

    def _discard_incoming_file(self, file_id: Any, message: str) -> None:
        info = self.incoming_files.pop(file_id, None) if isinstance(file_id, str) else None
        if info is not None:
            try:
                info["handle"].close()
            except OSError:
                pass
            try:
                info["path"].unlink(missing_ok=True)
            except OSError:
                pass
            self.events.put(("status", f"Transfer stopped: {message}"))

    def send_message(self) -> None:
        sock = self.sock
        message = self.message_var.get().strip()
        if sock is None or not message:
            return
        if len(message) > MAX_CHAT_LENGTH:
            messagebox.showwarning("Message too long", f"Keep messages under {MAX_CHAT_LENGTH} characters.", parent=self.root)
            return
        try:
            send_packet(sock, {"type": "chat", "message": message}, lock=self.send_lock)
            self.message_var.set("")
        except (OSError, ValueError) as exc:
            self.events.put(("error", f"Could not send message: {exc}"))

    def choose_file(self) -> None:
        if self.sock is None:
            return
        filename = filedialog.askopenfilename(parent=self.root, title="Choose a file to share")
        if filename:
            threading.Thread(target=self._send_file_worker, args=(Path(filename), self.sock), daemon=True).start()

    def _send_file_worker(self, path: Path, sock: socket.socket) -> None:
        try:
            size = path.stat().st_size
            if size > MAX_FILE_SIZE:
                self.events.put(("error", "Files must be 25 MB or smaller."))
                return
            digest = hashlib.sha256()
            with path.open("rb") as file_handle:
                for block in iter(lambda: file_handle.read(1024 * 1024), b""):
                    digest.update(block)
            checksum = digest.hexdigest()
            transfer_id = uuid.uuid4().hex
            send_packet(
                sock,
                {
                    "type": "file_start",
                    "file_id": transfer_id,
                    "filename": path.name,
                    "size": size,
                    "sha256": checksum,
                },
                lock=self.send_lock,
            )
            sent = 0
            last_percent = -1
            with path.open("rb") as file_handle:
                while True:
                    chunk = file_handle.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    send_packet(sock, {"type": "file_chunk", "file_id": transfer_id}, chunk, self.send_lock)
                    sent += len(chunk)
                    percent = 100 if size == 0 else int((sent / size) * 100)
                    if percent != last_percent and (percent == 100 or percent // 10 != last_percent // 10):
                        self.events.put(("status", f"Sending {path.name}: {percent}%"))
                        last_percent = percent
            send_packet(sock, {"type": "file_end", "file_id": transfer_id}, lock=self.send_lock)
            self.events.put(("status", f"File sent; waiting for server verification: {path.name}"))
        except (OSError, ValueError) as exc:
            self.events.put(("error", f"Could not share {path.name}: {exc}"))

    def _poll_events(self) -> None:
        try:
            while True:
                event, value = self.events.get_nowait()
                if event == "connected":
                    self.username = str(value)
                    self.host_entry.configure(state=DISABLED)
                    self.username_entry.configure(state=DISABLED)
                    self.connect_button.configure(state=DISABLED)
                    self.message_entry.configure(state=NORMAL)
                    self.send_button.configure(state=NORMAL)
                    self.file_button.configure(state=NORMAL)
                    self.status_var.set(f"Connected as {self.username} - share files up to 25 MB")
                    self._log(f"[SYSTEM] Connected as {self.username}.")
                    self.message_entry.focus_set()
                elif event == "connection_failed":
                    self.status_var.set("Connection failed")
                    self.connect_button.configure(state=NORMAL)
                    self._log(f"[ERROR] Connection failed: {value}")
                    messagebox.showerror("Connection failed", str(value), parent=self.root)
                elif event == "disconnected":
                    if self.sock is not None:
                        self.sock = None
                    self.host_entry.configure(state=NORMAL)
                    self.username_entry.configure(state=NORMAL)
                    self.connect_button.configure(state=NORMAL)
                    self.message_entry.configure(state=DISABLED)
                    self.send_button.configure(state=DISABLED)
                    self.file_button.configure(state=DISABLED)
                    self.status_var.set("Not connected")
                    if not self.closing:
                        self._log(f"[SYSTEM] {value}")
                elif event == "history":
                    for item in value if isinstance(value, list) else []:
                        if isinstance(item, dict):
                            self._display_chat(item)
                elif event == "chat":
                    self._display_chat(value)
                elif event == "system":
                    self._log(f"[SYSTEM] {value}")
                elif event == "error":
                    self._log(f"[ERROR] {value}")
                    self.status_var.set(str(value))
                elif event == "status":
                    self.status_var.set(str(value))
                elif event == "file_received":
                    self.status_var.set(f"File received and verified: {value}")
                    self._log(f"[FILE OK] Saved and verified: {value}")
                elif event == "shutdown":
                    pass
        except queue.Empty:
            pass
        if not self.closing:
            self.root.after(100, self._poll_events)

    def _display_chat(self, item: dict[str, Any]) -> None:
        time_text = str(item.get("timestamp", ""))
        username = str(item.get("username", "Unknown"))
        message = str(item.get("message", ""))
        self._log(f"[{time_text}] {username}: {message}")

    def close_app(self) -> None:
        self.closing = True
        sock = self.sock
        self.sock = None
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass
        for info in list(self.incoming_files.values()):
            try:
                info["handle"].close()
            except OSError:
                pass
            try:
                info["path"].unlink(missing_ok=True)
            except OSError:
                pass
        self.incoming_files.clear()
        self.root.destroy()


def main() -> None:
    root = Tk()
    ChatClientApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
