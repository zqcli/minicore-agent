#!/usr/bin/env python3
"""Deterministic loopback Responses API used by the Stage 7 PTY harness.

This is intentionally a separate process. It accepts only loopback traffic,
records request metadata without prompt/tool payloads, and returns one real
Tool call followed by one final assistant response.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


class LoopbackModelHandler(BaseHTTPRequestHandler):
    server_version = "minicore-stage7-loopback/1"

    def log_message(self, _format: str, *_args: Any) -> None:
        # HTTP access logs would make the terminal artifact nondeterministic.
        return

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")
            return
        self.send_error(404)

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if self.path != "/v1/responses":
            self.send_error(404)
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        try:
            request = json.loads(body)
        except json.JSONDecodeError:
            self.send_error(400, "invalid JSON")
            return

        server = self.server  # type: ignore[assignment]
        server.request_count += 1
        input_items = request.get("input")
        if not isinstance(input_items, list):
            input_items = []
        item_types = [
            item.get("type")
            for item in input_items
            if isinstance(item, dict) and isinstance(item.get("type"), str)
        ]
        record = {
            "count": server.request_count,
            "path": self.path,
            "model": request.get("model"),
            "input_item_types": item_types,
            "has_tools": isinstance(request.get("tools"), list),
        }
        with server.request_log.open("a", encoding="utf-8") as output:
            output.write(json.dumps(record, sort_keys=True) + "\n")

        if server.request_count == 1:
            # Delay the first response so the PTY harness captures the real
            # in-progress working/tool-request surface before completion.
            time.sleep(server.first_delay_ms / 1000.0)
            reasoning_id = "rs_stage7_1"
            reasoning_text = "".join(
                [
                    "Step one: locate the fixture file.\n",
                    "Step two: confirm it exists in the workspace.\n",
                    "Step three: read its contents and report.\n",
                    "Step four: no further tool use is required.\n",
                ]
            )
            reasoning_deltas = [
                # A real multi-line reasoning item: the Agent renders this as
                # a Thinking block, and the collapsed card is what the Stage 7
                # harness expands with a real mouse click.
                {
                    "type": "response.output_item.added",
                    "output_index": 0,
                    "item": {"type": "reasoning", "id": reasoning_id, "summary": []},
                }
            ] + [
                {
                    "type": "response.reasoning_summary_text.delta",
                    "item_id": reasoning_id,
                    "output_index": 0,
                    "delta": delta,
                }
                for delta in [
                    "Step one: locate the fixture file.\n",
                    "Step two: confirm it exists in the workspace.\n",
                    "Step three: read its contents and report.\n",
                    "Step four: no further tool use is required.\n",
                ]
            ] + [
                {
                    "type": "response.output_item.done",
                    "output_index": 0,
                    "item": {
                        "type": "reasoning",
                        "id": reasoning_id,
                        "summary": [
                            {"type": "summary_text", "text": reasoning_text.rstrip()}
                        ],
                    },
                },
                {
                    "type": "response.output_item.added",
                    "output_index": 1,
                    "item": {
                        "type": "function_call",
                        "call_id": "stage7-read-1",
                        "name": "read",
                        "arguments": "",
                    },
                },
                {
                    "type": "response.function_call_arguments.delta",
                    "item_id": "fc_stage7_1",
                    "output_index": 1,
                    "delta": '{"path":"fixture.txt"}',
                },
                {
                    "type": "response.function_call_arguments.done",
                    "item_id": "fc_stage7_1",
                    "output_index": 1,
                    "arguments": '{"path":"fixture.txt"}',
                },
                {
                    "type": "response.output_item.done",
                    "output_index": 1,
                    "item": {
                        "type": "function_call",
                        "call_id": "stage7-read-1",
                        "name": "read",
                        "arguments": '{"path":"fixture.txt"}',
                    },
                },
                completed_event(
                    reasoning_tokens=17,
                    output_items=[
                        {
                            "type": "reasoning",
                            "id": reasoning_id,
                            "summary": [
                                {"type": "summary_text", "text": reasoning_text.rstrip()}
                            ],
                        },
                        {
                            "type": "function_call",
                            "call_id": "stage7-read-1",
                            "name": "read",
                            "arguments": '{"path":"fixture.txt"}',
                        },
                    ],
                ),
            ]
            events = reasoning_deltas
        else:
            # Keep the real Agent in its post-Tool model request long enough
            # for the PTY harness to capture the Tool result surface.
            time.sleep(server.second_delay_ms / 1000.0)
            events = [
                {
                    "type": "response.output_text.delta",
                    "delta": "The loopback fixture is present and readable.\n",
                },
                {
                    "type": "response.output_text.delta",
                    "delta": "It contains two fixture lines as expected.\n",
                },
                {
                    "type": "response.output_text.delta",
                    "delta": "This completes the same-loop verification.\n",
                },
                completed_event(reasoning_tokens=0),
            ]

        payload = "".join(
            f"data: {json.dumps(event, separators=(',', ':'))}\n\n" for event in events
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
        self.wfile.flush()


def completed_event(reasoning_tokens: int = 0, output_items: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    # Keep usage internally consistent (the Agent validates that
    # reasoning_tokens <= output_tokens and total = input + output).
    input_tokens = 10
    output_tokens = reasoning_tokens + 12
    response: dict[str, Any] = {
        "status": "completed",
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
            "input_tokens_details": {
                "cached_tokens": 0,
                "cache_write_tokens": 0,
            },
            "output_tokens_details": {"reasoning_tokens": reasoning_tokens},
        },
    }
    if output_items is not None:
        response["output"] = output_items
    return {"type": "response.completed", "response": response}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port-file", required=True, type=Path)
    parser.add_argument("--request-log", required=True, type=Path)
    parser.add_argument("--first-delay-ms", type=int, default=900)
    parser.add_argument("--second-delay-ms", type=int, default=1600)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.port_file.parent.mkdir(parents=True, exist_ok=True)
    args.request_log.parent.mkdir(parents=True, exist_ok=True)
    args.request_log.write_text("", encoding="utf-8")

    server = ThreadingHTTPServer(("127.0.0.1", 0), LoopbackModelHandler)
    server.request_count = 0
    server.request_log = args.request_log
    server.first_delay_ms = max(0, args.first_delay_ms)
    server.second_delay_ms = max(0, args.second_delay_ms)
    args.port_file.write_text(str(server.server_port), encoding="ascii")

    def stop(_signum: int, _frame: Any) -> None:
        # BaseServer.shutdown must run away from serve_forever's thread.
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        server.serve_forever(poll_interval=0.05)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
