#!/usr/bin/env python3
"""Minimal DIVA inbound worker for Server B.

Receives LINE group events directly from the Go bridge over Docker's internal
network and prints the four fields required for validation.

Smoke-test rule only:
- exact inbound text "123456654" -> ask Go bridge to reply "789" to the same chat.

No Supabase access and no driver/order logic yet.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOST = "0.0.0.0"
PORT = 8080
TEST_TRIGGER = "123456654"
TEST_REPLY = "789"


class Handler(BaseHTTPRequestHandler):
    server_version = "DIVAWorker/0.2"

    def log_message(self, fmt: str, *args: object) -> None:
        # Keep output clean; accepted events are printed explicitly below.
        return

    def _send_json(self, status: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send_json(200, {"ok": True})
            return
        self._send_json(404, {"ok": False, "error": "not_found"})

    def do_POST(self) -> None:
        if self.path != "/line/inbound":
            self._send_json(404, {"ok": False, "error": "not_found"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 1024 * 1024:
                raise ValueError("invalid content length")
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except (ValueError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            self._send_json(400, {"ok": False, "error": str(exc)})
            return

        required = ("text", "group_id", "sender_id", "message_id")
        if not isinstance(payload, dict) or any(not isinstance(payload.get(k), str) for k in required):
            self._send_json(400, {"ok": False, "error": "invalid_payload"})
            return

        print("\n[DIVA_PYTHON]", flush=True)
        print(f"TEXT: {payload['text']}", flush=True)
        print(f"GROUP_ID: {payload['group_id']}", flush=True)
        print(f"SENDER_ID: {payload['sender_id']}", flush=True)
        print(f"MESSAGE_ID: {payload['message_id']}", flush=True)

        response: dict[str, object] = {"ok": True}
        if payload["text"] == TEST_TRIGGER:
            response["reply_text"] = TEST_REPLY
            print(f"AUTO_REPLY: {TEST_REPLY}", flush=True)

        self._send_json(200, response)


def main() -> None:
    print(f"DIVA worker listening on {HOST}:{PORT}", flush=True)
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
