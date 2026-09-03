#!/usr/bin/env python3
"""DIVA minimum inbound experiment.

Tails the matrix-line container's structured [DIVA_RX] events and prints only
what the Python worker needs for the first Server B validation:
text, LINE group ID, sender ID, and LINE message ID.

This intentionally does not touch Supabase or driver/order logic.
"""

from __future__ import annotations

import shlex
import subprocess
import sys
from pathlib import Path


def parse_diva_line(line: str) -> dict[str, str] | None:
    if "[DIVA_RX]" not in line:
        return None

    try:
        tokens = shlex.split(line)
    except ValueError:
        return None

    fields: dict[str, str] = {}
    for token in tokens:
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        if key in {
            "diva_event",
            "text",
            "group_id",
            "sender_id",
            "message_id",
            "decryption_failed",
        }:
            fields[key] = value

    if fields.get("diva_event") != "DIVA_RX":
        return None
    if fields.get("decryption_failed") == "true":
        return None
    if not all(fields.get(key) for key in ("group_id", "sender_id", "message_id")):
        return None
    return fields


def main() -> int:
    repo_dir = Path(__file__).resolve().parent
    command = [
        "docker",
        "compose",
        "logs",
        "--since=1s",
        "-f",
        "matrix-line",
    ]

    print("DIVA Python listener started. Waiting for LINE group messages...", flush=True)

    process = subprocess.Popen(
        command,
        cwd=repo_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    try:
        assert process.stdout is not None
        for raw_line in process.stdout:
            event = parse_diva_line(raw_line)
            if event is None:
                continue

            print("\n[DIVA_PYTHON]", flush=True)
            print(f"TEXT: {event.get('text', '')}", flush=True)
            print(f"GROUP_ID: {event['group_id']}", flush=True)
            print(f"SENDER_ID: {event['sender_id']}", flush=True)
            print(f"MESSAGE_ID: {event['message_id']}", flush=True)
    except KeyboardInterrupt:
        print("\nDIVA Python listener stopped.", flush=True)
    finally:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()

    return process.returncode or 0


if __name__ == "__main__":
    sys.exit(main())
