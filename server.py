"""Simple multi-user LAN chat and file-sharing server.

Run with: python server.py
The server listens on port 5000 and stores chat history in chat_history.db.
This prototype is intended for trusted local networks, not the public internet.
"""

from __future__ import annotations

import hashlib
import json
import re
import socket
import sqlite3
import struct
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

HOST = "0.0.0.0"
PORT = 5000
MAX_CLIENTS = 100
CHUNK_SIZE = 64 * 1024
MAX_FILE_SIZE = 25 * 1024 * 1024  # 25 MB per file
MAX_CHAT_LENGTH = 2000
MAX_PACKET_SIZE = CHUNK_SIZE + 256 * 1024
DB_PATH = Path(__file__).resolve().parent / "chat_history.db"

clients: dict[socket.socket, "ClientConnection"] = {}
clients_lock = threading.RLock()
transfers: dict[str, dict[str, Any]] = {}
transfers_lock = threading.RLock()
db_lock = threading.Lock()
database = sqlite3.connect(DB_PATH, check_same_thread=False)


def timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def initialize_database() -> None:
    with db_lock:
        database.execute(
            """CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                username TEXT NOT NULL,
                message TEXT NOT NULL,
                message_type TEXT NOT NULL
            )"""
        )
        database.commit()


def save_message(username: str, message: str, message_type: str = "chat") -> None:
    with db_lock:
        database.execute(
            "INSERT INTO messages (timestamp, username, message, message_type) VALUES (?, ?, ?, ?)",
            (timestamp(), username, message, message_type),
        )
        database.commit()


def load_history(limit: int = 100) -> list[dict[str, str]]:
    with db_lock:
        rows = database.execute(
            "SELECT timestamp, username, message, message_type FROM messages ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    rows.reverse()
    return [
        {"timestamp": row[0], "username": row[1], "message": row[2], "kind": row[3]}
        for row in rows
    ]


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
    lock: threading.Lock | threading.RLock | None = None,
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
            # Keep waiting through idle timeouts without losing partial packet data.
            continue
        if not part:
            raise ConnectionError("The connection was closed")
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


def clean_username(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    value = re.sub(r"[\x00-\x1f\x7f]", "", value).strip()
    return value[:24]


def clean_filename(value: Any) -> str:
    if not isinstance(value, str):
        return "shared_file"
    value = value.replace("\\", "/").split("/")[-1]
    value = re.sub(r"[\x00-\x1f\x7f]", "", value).strip()
    return value[:180] or "shared_file"


@dataclass
class ClientConnection:
    sock: socket.socket
    address: tuple[Any, ...]
    username: str
    send_lock: threading.Lock = field(default_factory=threading.Lock)


def send_to(client: ClientConnection, header: dict[str, Any], payload: bytes = b"") -> None:
    send_packet(client.sock, header, payload, client.send_lock)


def broadcast_packet(
    header: dict[str, Any],
    payload: bytes = b"",
    exclude: ClientConnection | None = None,
) -> None:
    with clients_lock:
        recipients = list(clients.values())
    failed: list[ClientConnection] = []
    for client in recipients:
        if client is exclude:
            continue
        try:
            send_to(client, header, payload)
        except (OSError, ValueError):
            failed.append(client)
    for client in failed:
        drop_connection(client, announce=False)


def send_error(client: ClientConnection | None, message: str) -> None:
    if client is None:
        return
    try:
        send_to(client, {"type": "error", "message": message})
    except OSError:
        drop_connection(client, announce=False)


def drop_connection(client: ClientConnection, announce: bool = True) -> None:
    with clients_lock:
        was_registered = clients.get(client.sock) is client
        if was_registered:
            del clients[client.sock]

    try:
        client.sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        client.sock.close()
    except OSError:
        pass

    # Also clean up if another broadcast already removed this client.
    with transfers_lock:
        abandoned = [
            (transfer_id, info)
            for transfer_id, info in transfers.items()
            if info["sender"] is client
        ]
        for transfer_id, _ in abandoned:
            transfers.pop(transfer_id, None)
    for transfer_id, _info in abandoned:
        broadcast_packet(
            {"type": "file_abort", "file_id": transfer_id, "message": "Sender disconnected before transfer completed."},
            exclude=client,
        )

    if announce and was_registered:
        broadcast_packet({"type": "system", "message": f"{client.username} disconnected.", "timestamp": timestamp()})
        print(f"[-] {client.username} disconnected from {client.address}")


def register_client(sock: socket.socket, address: tuple[Any, ...], requested_name: str) -> ClientConnection:
    username = clean_username(requested_name)
    if not username:
        raise ValueError("Enter a username containing at least one visible character.")

    # Acquire the connection's send lock before registration so its welcome and
    # history packets always arrive before any concurrent broadcast.
    client = ClientConnection(sock=sock, address=address, username=username)
    client.send_lock.acquire()
    try:
        with clients_lock:
            if len(clients) >= MAX_CLIENTS:
                raise RuntimeError(f"The server is full (limit: {MAX_CLIENTS} users).")
            if any(c.username.casefold() == username.casefold() for c in clients.values()):
                raise ValueError("That username is already in use. Please choose another.")
            clients[sock] = client

        _send_packet_unlocked(sock, {"type": "welcome", "username": username}, b"")
        _send_packet_unlocked(sock, {"type": "history", "messages": load_history()}, b"")
    except Exception:
        with clients_lock:
            if clients.get(sock) is client:
                clients.pop(sock, None)
        raise
    finally:
        client.send_lock.release()

    broadcast_packet({"type": "system", "message": f"{username} joined the chat.", "timestamp": timestamp()})
    print(f"[+] {username} connected from {address}. Users: {len(clients)}/{MAX_CLIENTS}")
    return client


def handle_chat(client: ClientConnection, header: dict[str, Any]) -> None:
    message = header.get("message", "")
    if not isinstance(message, str):
        send_error(client, "Message must be text.")
        return
    message = message.strip()
    if not message:
        return
    if len(message) > MAX_CHAT_LENGTH:
        send_error(client, f"Messages must be {MAX_CHAT_LENGTH} characters or fewer.")
        return
    time_text = timestamp()
    save_message(client.username, message, "chat")
    broadcast_packet(
        {"type": "chat", "username": client.username, "message": message, "timestamp": time_text, "kind": "chat"}
    )


def handle_file_start(client: ClientConnection, header: dict[str, Any]) -> None:
    file_id = header.get("file_id")
    filename = clean_filename(header.get("filename"))
    size = header.get("size")
    digest = header.get("sha256")
    if not isinstance(file_id, str) or not (1 <= len(file_id) <= 64):
        send_error(client, "Invalid file-transfer identifier.")
        return
    if not isinstance(size, int) or isinstance(size, bool) or size < 0 or size > MAX_FILE_SIZE:
        send_error(client, "Files must be 25 MB or smaller.")
        return
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", digest):
        send_error(client, "Invalid SHA-256 checksum.")
        return
    with transfers_lock:
        if file_id in transfers:
            send_error(client, "That file-transfer identifier is already active.")
            return
        transfers[file_id] = {
            "sender": client,
            "filename": filename,
            "size": size,
            "sha256": digest.lower(),
            "received": 0,
            "hasher": hashlib.sha256(),
        }
    broadcast_packet(
        {
            "type": "file_start",
            "file_id": file_id,
            "filename": filename,
            "size": size,
            "sha256": digest.lower(),
            "sender": client.username,
        },
        exclude=client,
    )


def handle_file_chunk(client: ClientConnection, header: dict[str, Any], payload: bytes) -> None:
    file_id = header.get("file_id")
    if not isinstance(file_id, str):
        send_error(client, "Invalid file-transfer identifier.")
        return
    with transfers_lock:
        info = transfers.get(file_id)
        if info is None or info["sender"] is not client:
            send_error(client, "Unknown file transfer. Please try sharing the file again.")
            return
        if not payload or len(payload) > CHUNK_SIZE or info["received"] + len(payload) > info["size"]:
            transfers.pop(file_id, None)
            invalid = True
        else:
            info["received"] += len(payload)
            info["hasher"].update(payload)
            invalid = False
    if invalid:
        broadcast_packet({"type": "file_abort", "file_id": file_id, "message": "Invalid file data received."}, exclude=client)
        send_error(client, "File data did not match the declared size.")
        return
    broadcast_packet({"type": "file_chunk", "file_id": file_id}, payload, exclude=client)


def handle_file_end(client: ClientConnection, header: dict[str, Any]) -> None:
    file_id = header.get("file_id")
    if not isinstance(file_id, str):
        send_error(client, "Invalid file-transfer identifier.")
        return
    with transfers_lock:
        info = transfers.get(file_id)
        if info is None or info["sender"] is not client:
            send_error(client, "Unknown file transfer.")
            return
        transfers.pop(file_id, None)
        size_ok = info["received"] == info["size"]
        hash_ok = info["hasher"].hexdigest() == info["sha256"]
    if not size_ok or not hash_ok:
        message = "File transfer failed its size or SHA-256 integrity check."
        broadcast_packet({"type": "file_abort", "file_id": file_id, "message": message}, exclude=client)
        send_error(client, message)
        return

    broadcast_packet({"type": "file_end", "file_id": file_id, "sha256": info["sha256"]}, exclude=client)
    display_message = f"Shared file: {info['filename']} ({info['size']} bytes)"
    time_text = timestamp()
    save_message(client.username, display_message, "file")
    broadcast_packet(
        {
            "type": "chat",
            "username": client.username,
            "message": display_message,
            "timestamp": time_text,
            "kind": "file",
        }
    )
    print(f"[FILE] {client.username} shared {info['filename']} ({info['size']} bytes)")


def handle_client(sock: socket.socket, address: tuple[Any, ...]) -> None:
    client: ClientConnection | None = None
    try:
        first_header, first_payload = recv_packet(sock)
        if first_payload or first_header.get("type") != "hello":
            send_packet(sock, {"type": "error", "message": "Send a valid username to connect."})
            return
        try:
            client = register_client(sock, address, first_header.get("username", ""))
        except (ValueError, RuntimeError) as exc:
            try:
                send_packet(sock, {"type": "error", "message": str(exc)})
            except OSError:
                pass
            return

        while True:
            header, payload = recv_packet(sock)
            packet_type = header.get("type")
            if packet_type == "chat":
                if payload:
                    send_error(client, "Chat packets cannot contain file data.")
                else:
                    handle_chat(client, header)
            elif packet_type == "file_start":
                if payload:
                    send_error(client, "File metadata packet cannot contain file data.")
                else:
                    handle_file_start(client, header)
            elif packet_type == "file_chunk":
                handle_file_chunk(client, header, payload)
            elif packet_type == "file_end":
                if payload:
                    send_error(client, "File-end packet cannot contain file data.")
                else:
                    handle_file_end(client, header)
            elif packet_type == "file_abort":
                file_id = header.get("file_id")
                with transfers_lock:
                    info = transfers.get(file_id) if isinstance(file_id, str) else None
                    if info is not None and info["sender"] is client:
                        transfers.pop(file_id, None)
                    else:
                        info = None
                if info is not None:
                    broadcast_packet(
                        {"type": "file_abort", "file_id": file_id, "message": "Sender cancelled the transfer."},
                        exclude=client,
                    )
            else:
                send_error(client, "Unknown request type.")
    except (ConnectionError, OSError, ValueError, json.JSONDecodeError) as exc:
        if client is not None:
            print(f"[!] Connection issue with {client.username}: {exc}")
    finally:
        if client is not None:
            drop_connection(client, announce=True)
        else:
            try:
                sock.close()
            except OSError:
                pass


def main() -> None:
    initialize_database()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.bind((HOST, PORT))
        server.listen(MAX_CLIENTS)
        print("=" * 56)
        print(" Simple LAN Chat and File-Sharing Server")
        print(f" Listening on {HOST}:{PORT} | maximum clients: {MAX_CLIENTS}")
        print(f" Chat history database: {DB_PATH.name}")
        print(" Press Ctrl+C to stop the server.")
        print("=" * 56)
        while True:
            client_sock, address = server.accept()
            client_sock.settimeout(15.0)
            thread = threading.Thread(target=handle_client, args=(client_sock, address), daemon=True)
            thread.start()
    except KeyboardInterrupt:
        print("\nStopping server...")
    except OSError as exc:
        print(f"Could not start server: {exc}")
        print("Check whether port 5000 is already in use.")
    finally:
        server.close()
        with clients_lock:
            active = list(clients.values())
        for client in active:
            drop_connection(client, announce=False)
        with db_lock:
            database.close()
        print("Server stopped.")


if __name__ == "__main__":
    main()
