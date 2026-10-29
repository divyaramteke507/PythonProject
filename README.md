# LAN Chat and File-Sharing Application

A beginner-oriented Python client-server project for ICS 214 - IT Workshop III. It provides group text chat, file sharing, a Tkinter client interface, SQLite chat history, SHA-256 integrity verification, and basic error handling.

## Files

- `server.py` - accepts up to 100 registered clients, broadcasts chat messages and file-transfer packets, verifies incoming file bytes, and stores history in SQLite.
- `client.py` - Tkinter chat window for connecting, messaging, and sharing files.
- `LAN_Chat_Project.ipynb` - project explanation and implementation cells. Run the `%%writefile` cells from top to bottom to regenerate the two Python files.
- `Project_Report.pdf` - report for the course submission.

No third-party Python packages are required. Python 3.10+ is recommended. Tkinter should be included in a standard Windows Python installation.

## Run on one computer first

1. Open a terminal in this folder.
2. Start the server:

   ```bash
   python server.py
   ```

3. Open a second terminal in the same folder and run:

   ```bash
   python client.py
   ```

4. In the client window, keep Server IP as `127.0.0.1`, enter a username, and click **Connect**.
5. Run `python client.py` again in another terminal to open a second client with a different username.
6. Send a message or use **Share File** to choose a file. The other connected clients receive files in the `received_files` folder.

## Run over the same Wi-Fi/LAN

1. Connect the server computer and client computers to the same Wi-Fi/LAN.
2. Run `python server.py` on the computer acting as the server.
3. On the server computer, find its local IPv4 address. On Windows, run `ipconfig` and find the active Wi-Fi adapter's IPv4 Address.
4. On each client computer, run `python client.py`, enter the server computer's IPv4 address in **Server IP**, enter a unique username, and click **Connect**.
5. If Windows Firewall prompts you, allow Python on your **private network**. Never expose this unauthenticated prototype directly to the public internet.

Some guest or campus Wi-Fi networks block communication between connected devices (client isolation). In that case, use a permitted local network or test on one computer first.

## Basic behaviour and limits

- TCP port: `5000`.
- Configured registered-client limit: `100`.
- Maximum file size: `25 MB`.
- Text messages: up to `2,000` characters.
- File sharing broadcasts to every other connected user; the initial version does not select one recipient.
- Received files are saved under `received_files/` beside `client.py`.
- The server creates `chat_history.db` beside `server.py` and retains the most recent 100 chat/file records for new clients to view.
- The server checks file size and SHA-256 before declaring a transfer complete; each recipient also verifies the received file.
- Chat traffic is not encrypted and usernames are not authenticated. Use only on a trusted test network. Do not send sensitive information.

## Testing

The implementation was checked with Python syntax compilation and local integration tests covering two-client messaging, binary file transfer, SHA-256 verification, disconnect handling, and a Tkinter GUI flow with two windows. A capacity check connected 100 simulated local clients; the 101st connection was rejected at the configured limit. This is a local protocol/capacity check, not a benchmark of 100 people using a real Wi-Fi router at once; real-network performance should be tested in the intended environment.
