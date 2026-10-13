#!/usr/bin/env python3
# telegateway example bot (Python, stdlib only).
# Listens on 127.0.0.1:8081 and echoes every message back via sendMessage.
# Without TELEGRAM_BOT_TOKEN it logs what it would send and answers 200.
"""Echo bot: replies to every message with the same text."""
import json
import os
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")


def send_message(chat_id: int, text: str) -> None:
    if not TOKEN:
        print(f"[dry-run] would send to {chat_id}: {text}", flush=True)
        return
    body = json.dumps({"chat_id": chat_id, "text": text}).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=10).read()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802 - http.server naming
        if self.path != "/webhook":
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", 0))
        update = json.loads(self.rfile.read(length) or b"{}")
        message = update.get("message") or {}
        chat = message.get("chat") or {}
        text = message.get("text")
        if text and chat.get("id") is not None:
            print(
                f"echo-python: update {update.get('update_id')} from chat {chat['id']}",
                flush=True,
            )
            try:
                send_message(chat["id"], text)
            except Exception as e:  # example code: log and keep serving
                print(f"sendMessage failed: {e}", flush=True)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):  # keep output to our own prints
        pass


if __name__ == "__main__":
    print("echo-python listening on 127.0.0.1:8081", flush=True)
    HTTPServer(("127.0.0.1", 8081), Handler).serve_forever()
