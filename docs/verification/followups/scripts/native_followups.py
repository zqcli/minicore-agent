#!/usr/bin/env python3
"""Bounded native iTerm2 acceptance for the accepted MiniCore binaries.

The mock Responses server runs in this Python process without a Pi/Node
worker. The TUI runs only in one owned /bin/sh iTerm2 window, and all model
inputs/results are synthetic fixtures.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
from stage7_loopback_model import completed_event

BASE = Path(__file__).resolve().parents[1]
PROFILE = os.environ.get("NATIVE_PROFILE", "debug")
TUI = BASE / "artifacts" / f"minicore-tui-{PROFILE}"
AGENT = BASE / "artifacts" / f"minicore-agent-{PROFILE}"

ROOT = Path(tempfile.mkdtemp(prefix="minicore-native-followups-")).resolve()
WORK = ROOT / "workspace"
DATA = ROOT / "data"
CONF = ROOT / "config"
PROMPTS = CONF / "prompts"
OUT = ROOT / "evidence"
for path in (WORK, DATA, CONF, PROMPTS, OUT):
    path.mkdir(parents=True)

KEY = "MINICORE_NATIVE_FOLLOWUPS_KEY"
MARK_CANCEL = "NATIVE_ESC_CANCEL"
MARK_READ = "NATIVE_READ_MISSING"
MARK_PATCH = "NATIVE_PATCH_UPDATE"
MARK_SINGLE = "NATIVE_SUBAGENT_SINGLE"
MARK_PARALLEL = "NATIVE_SUBAGENT_PARALLEL"
MARK_CHAIN = "NATIVE_SUBAGENT_CHAIN"
MARK_RELOAD = "NATIVE_RELOAD_AFTER"
MARK_ALIAS = "NATIVE_MODEL_ALIAS"
ALL_MARKERS = [
    MARK_CANCEL,
    MARK_READ,
    MARK_PATCH,
    MARK_SINGLE,
    MARK_PARALLEL,
    MARK_CHAIN,
    MARK_RELOAD,
    MARK_ALIAS,
    "NATIVE_CHILD_SINGLE",
    "NATIVE_PARALLEL_CHILD_ONE",
    "NATIVE_PARALLEL_CHILD_TWO",
    "NATIVE_CHAIN_CHILD_ONE",
    "NATIVE_CHAIN_CHILD_TWO",
    "NATIVE_CHAIN_CHILD_THREE",
]

class MockState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.records: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self.read_waiting = threading.Event()
        self.read_release = threading.Event()
        self.cancel_waiting = threading.Event()
        self.cancel_release = threading.Event()

    def add(self, record: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            record["count"] = len(self.records) + 1
            self.records.append(record)
            return record

    def update(self, record: dict[str, Any], **values: Any) -> None:
        with self.lock:
            record.update(values)

    def error(self, message: str) -> None:
        with self.lock:
            self.errors.append(message)

    def snapshot(self) -> list[dict[str, Any]]:
        with self.lock:
            return list(self.records)


def strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            result.extend(strings(item))
        return result
    if isinstance(value, dict):
        result = []
        for item in value.values():
            result.extend(strings(item))
        return result
    return []


def body_marker(body: dict[str, Any]) -> str | None:
    users = [item for item in body.get("input", [])
             if isinstance(item, dict) and item.get("role") == "user"]
    text = "\n".join(strings(users[-1].get("content", []))) if users else ""
    found = [(text.rfind(marker), marker) for marker in ALL_MARKERS if marker in text]
    return max(found, default=(-1, None))[1]


def current_user_index(items: list[Any], marker: str | None) -> int | None:
    if not marker:
        return None
    found: int | None = None
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        if item.get("role") == "user" and marker in "\n".join(strings(item)):
            found = index
    return found


def current_function_outputs(body: dict[str, Any], marker: str | None) -> list[str]:
    items = body.get("input")
    if not isinstance(items, list):
        return []
    start = current_user_index(items, marker)
    if start is None:
        return []
    return [
        item.get("output", "")
        for item in items[start + 1 :]
        if isinstance(item, dict)
        and item.get("type") == "function_call_output"
        and isinstance(item.get("output", ""), str)
    ]


def function_tools(body: dict[str, Any]) -> list[dict[str, Any]]:
    tools = body.get("tools")
    if not isinstance(tools, list):
        return []
    return [tool for tool in tools if isinstance(tool, dict) and tool.get("type") == "function"]


def tool_names(body: dict[str, Any]) -> list[str]:
    return [tool["name"] for tool in function_tools(body) if isinstance(tool.get("name"), str)]


def sse(tool: str, call_id: str, arguments: dict[str, Any]) -> bytes:
    encoded = json.dumps(arguments, separators=(",", ":"))
    item = {"type": "function_call", "id": f"fc_{call_id}", "call_id": call_id,
            "name": tool, "arguments": encoded, "status": "completed"}
    events = [
        {"type": "response.output_item.added", "output_index": 0,
         "item": {**item, "arguments": "", "status": "in_progress"}},
        {"type": "response.function_call_arguments.delta", "item_id": f"fc_{call_id}", "output_index": 0, "delta": encoded},
        {"type": "response.function_call_arguments.done", "item_id": f"fc_{call_id}", "output_index": 0, "arguments": encoded},
        {"type": "response.output_item.done", "output_index": 0, "item": item},
        completed_event(reasoning_tokens=0, output_items=[item]),
    ]
    return "".join(f"data: {json.dumps(event, separators=(',', ':'))}\n\n" for event in events).encode()


def text_sse(text: str) -> bytes:
    item = {"type": "message", "id": f"msg_{time.monotonic_ns()}", "role": "assistant",
            "status": "completed", "content": [{"type": "output_text", "text": text, "annotations": []}]}
    events = [
        {"type": "response.output_item.added", "output_index": 0,
         "item": {**item, "status": "in_progress", "content": []}},
        {"type": "response.output_text.delta", "output_index": 0, "content_index": 0,
         "item_id": item["id"], "delta": text},
        {"type": "response.output_item.done", "output_index": 0, "item": item},
        completed_event(reasoning_tokens=0, output_items=[item]),
    ]
    return "".join(f"data: {json.dumps(event, separators=(',', ':'))}\n\n" for event in events).encode()


class Provider(BaseHTTPRequestHandler):
    server_version = "minicore-native-followups/1"

    def log_message(self, _format: str, *_args: Any) -> None:
        return

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if self.path != "/v1/responses":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length))
        except (ValueError, json.JSONDecodeError) as error:
            self.send_error(400, str(error))
            return
        if not isinstance(body, dict):
            self.send_error(400, "request is not an object")
            return

        state: MockState = self.server.mock_state  # type: ignore[attr-defined]
        marker = body_marker(body)
        names = tool_names(body)
        parent = "subagent" in names
        outputs = current_function_outputs(body, marker)
        record = state.add({
            "model": body.get("model"),
            "marker": marker,
            "parent": parent,
            "tools": names,
            "reasoning": body.get("reasoning"),
            "input_types": [
                item.get("type")
                for item in body.get("input", [])
                if isinstance(item, dict) and isinstance(item.get("type"), str)
            ] if isinstance(body.get("input"), list) else [],
            "has_function_call_output_in_current_user_turn": bool(outputs),
            "function_outputs": outputs,
            "body": body,
        })

        response: bytes | None = None
        if marker == MARK_CANCEL:
            state.update(record, response_kind="cancel-held")
            state.cancel_waiting.set()
            if not state.cancel_release.wait(40):
                state.error("cancel request was not released")
                self.send_error(504, "cancel release timeout")
                return
            response = text_sse("NATIVE_CANCEL_LATE_RESPONSE")
        elif marker == MARK_READ:
            if outputs:
                state.update(record, response_kind="read-final")
                state.read_waiting.set()
                if not state.read_release.wait(40):
                    state.error("read follow-up was not released")
                    self.send_error(504, "read follow-up release timeout")
                    return
                response = text_sse("NATIVE_READ_DONE")
            else:
                state.update(record, response_kind="read-tool", tool="read", arguments={"path": "missing-native.txt"})
                response = sse("read", "native-read-missing", {"path": "missing-native.txt"})
        elif marker == MARK_PATCH:
            if outputs:
                state.update(record, response_kind="patch-final")
                response = text_sse("NATIVE_PATCH_DONE")
            else:
                patch = "*** Begin Patch\n*** Update File: fixture.txt\n@@\n-before\n+after\n*** End Patch\n"
                arguments = {"path": "fixture.txt", "patch": patch}
                state.update(record, response_kind="patch-tool", tool="apply_patch", arguments=arguments)
                response = sse("apply_patch", "native-patch-update", arguments)
        elif marker in {MARK_SINGLE, MARK_PARALLEL, MARK_CHAIN} and parent:
            if outputs:
                state.update(record, response_kind="subagent-final")
                response = text_sse({
                    MARK_SINGLE: "NATIVE_SUBAGENT_SINGLE_DONE",
                    MARK_PARALLEL: "NATIVE_SUBAGENT_PARALLEL_DONE",
                    MARK_CHAIN: "NATIVE_SUBAGENT_CHAIN_DONE",
                }[marker])
            else:
                if marker == MARK_SINGLE:
                    arguments = {
                        "model": None, "reasoning": None, "task": "NATIVE_CHILD_SINGLE",
                        "tasks": None, "chain": None, "cwd": None,
                    }
                elif marker == MARK_PARALLEL:
                    arguments = {
                        "model": None, "reasoning": None, "task": None,
                        "tasks": [
                            {"model": None, "reasoning": None, "task": "NATIVE_PARALLEL_CHILD_ONE", "cwd": None},
                            {"model": None, "reasoning": None, "task": "NATIVE_PARALLEL_CHILD_TWO", "cwd": None},
                        ],
                        "chain": None, "cwd": None,
                    }
                else:
                    arguments = {
                        "model": None, "reasoning": None, "task": None,
                        "tasks": None,
                        "chain": [
                            {"model": None, "reasoning": None, "task": "NATIVE_CHAIN_CHILD_ONE", "cwd": None},
                            {"model": None, "reasoning": None, "task": "NATIVE_CHAIN_CHILD_TWO previous={previous}", "cwd": None},
                            {"model": None, "reasoning": None, "task": "NATIVE_CHAIN_CHILD_THREE previous={previous}", "cwd": None},
                        ],
                        "cwd": None,
                    }
                state.update(record, response_kind="subagent-tool", tool="subagent", arguments=arguments)
                response = sse("subagent", f"native-{marker.lower()}-call", arguments)
        elif marker == "NATIVE_CHILD_SINGLE":
            state.update(record, response_kind="child-final")
            response = text_sse("child-single")
        elif marker == "NATIVE_PARALLEL_CHILD_ONE":
            state.update(record, response_kind="child-final")
            response = text_sse("child-one")
        elif marker == "NATIVE_PARALLEL_CHILD_TWO":
            state.update(record, response_kind="child-final")
            response = text_sse("child-two")
        elif marker == "NATIVE_CHAIN_CHILD_ONE":
            state.update(record, response_kind="child-final")
            response = text_sse("child-one")
        elif marker == "NATIVE_CHAIN_CHILD_TWO":
            state.update(record, response_kind="child-final")
            response = text_sse("child-two")
        elif marker == "NATIVE_CHAIN_CHILD_THREE":
            state.update(record, response_kind="child-final")
            response = text_sse("child-three")
        elif marker == MARK_RELOAD:
            state.update(record, response_kind="reload-final")
            response = text_sse("NATIVE_RELOAD_DONE")
        elif marker == MARK_ALIAS:
            state.update(record, response_kind="alias-final")
            response = text_sse("NATIVE_MODEL_ALIAS_DONE")
        else:
            state.error(f"unexpected provider route marker={marker!r} tools={names!r}")
            self.send_error(500, "unexpected synthetic route")
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        try:
            self.wfile.write(response)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            if marker != MARK_CANCEL:
                raise


def osa(script: str) -> str:
    return subprocess.check_output(["osascript", "-e", script], text=True).rstrip("\n")


def create_window() -> int:
    return int(osa(
        'tell application "iTerm2"\n'
        'activate\n'
        'set w to (create window with default profile command "/bin/sh")\n'
        'return id of w\n'
        'end tell'
    ))


def send(window: int, text: str) -> None:
    expression = " & ".join(f"(character id {ord(char)})" for char in text) or '""'
    osa(
        f'tell application "iTerm2" to tell current session of window id {window} '
        f"to write text ({expression}) newline NO"
    )
    if text == "\x1b":
        time.sleep(0.2)


def screen(window: int) -> str:
    rows = int(osa(f'tell application "iTerm2" to get rows of current session of window id {window}'))
    contents = osa(f'tell application "iTerm2" to get contents of current session of window id {window}')
    return "\n".join(contents.splitlines()[-rows:])


def capture(window: int, path: Path) -> str:
    text = screen(window)
    path.write_text(text, encoding="utf-8")
    return text


def wait_screen(window: int, predicate: Any, label: str, out: Path, seconds: float = 15) -> str:
    deadline = time.monotonic() + seconds
    latest = ""
    while time.monotonic() < deadline:
        latest = screen(window)
        if predicate(latest):
            return latest
        time.sleep(0.1)
    (out / "failure-screen.txt").write_text(latest, encoding="utf-8")
    raise AssertionError(label)


def wait_finished(window: int, marker: str, label: str, out: Path) -> str:
    def finished(text: str) -> bool:
        if marker in text and "ready" in text.lower():
            return True
        if "↓ new output" in text:
            send(window, "\x1b[1;5F")
        return False
    return wait_screen(window, finished, label, out)


def wait_event(event: threading.Event, label: str, window: int, out: Path, seconds: float = 15) -> None:
    if event.wait(seconds):
        return
    capture(window, out / "failure-screen.txt")
    raise AssertionError(label)


def seek_text(window: int, needle: str, out: Path, pages: int = 8) -> tuple[str, int]:
    for page in range(pages + 1):
        text = screen(window)
        if needle in text:
            return text, page
        if page < pages:
            send(window, "\x1b[5~")
            time.sleep(0.1)
    capture(window, out / "failure-screen.txt")
    raise AssertionError(f"history text {needle} was not found")


def click_text(window: int, needle: str, out: Path, times: int = 1) -> None:
    text = wait_screen(window, lambda value: needle in value, f"clickable text {needle}", out)
    row, line = next((index + 1, value) for index, value in enumerate(text.splitlines()) if needle in value)
    column = line.index(needle) + 2
    for _ in range(times):
        send(window, f"\x1b[<0;{column};{row}M\x1b[<0;{column};{row}m")
        time.sleep(0.15)


def write_config(path: Path, port: int, raw_model: str, include_verify_b: bool) -> None:
    text = f'''data_dir = {json.dumps(str(DATA))}
default_profile = "coding"

[profiles.coding]
model = "verify-a"
reasoning = "high"
system_prompt = {{ file = "prompts/system.md" }}
tools = ["read", "apply_patch", "subagent"]
max_tool_rounds = 8
approval = "auto"

[models.verify-a]
provider = "open_ai_responses"
model = {json.dumps(raw_model)}
base_url = "http://127.0.0.1:{port}/v1"
api_key_env = "{KEY}"
physical_context_window = 32000
output_budget_tokens = 4096
safety_margin_tokens = 1000
supported_reasoning = ["high", "max"]
supports_tools = true
request_timeout_seconds = 60
'''
    if include_verify_b:
        text += f'''
[models.verify-b]
provider = "open_ai_responses"
model = "native-verify-b"
base_url = "http://127.0.0.1:{port}/v1"
api_key_env = "{KEY}"
physical_context_window = 32000
output_budget_tokens = 4096
safety_margin_tokens = 1000
supported_reasoning = ["high", "max"]
supports_tools = true
request_timeout_seconds = 60
'''
    path.write_text(text, encoding="utf-8")


def session_dirs() -> list[Path]:
    root = DATA / "sessions"
    if not root.is_dir():
        return []
    return sorted(path for path in root.iterdir() if path.is_dir())


def user_count(history: bytes) -> int:
    count = 0
    for line in history.splitlines():
        value = json.loads(line)
        count += sum(1 for item in value["items"] if item.get("type") == "user")
    return count


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def workspace_snapshot() -> dict[str, tuple[str, str | None]]:
    entries = {}
    for path in sorted(WORK.rglob("*")):
        name = str(path.relative_to(WORK))
        if path.is_symlink():
            entries[name] = ("symlink", os.readlink(path))
        elif path.is_dir():
            entries[name] = ("directory", None)
        else:
            entries[name] = ("file", sha256(path))
    return entries


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def assert_subagent_schema(record: dict[str, Any]) -> None:
    tools = function_tools(record["body"])
    subagent = next((tool for tool in tools if tool.get("name") == "subagent"), None)
    require(subagent is not None, "parent request did not advertise subagent")
    require(subagent.get("strict") is True, "subagent function tool is not strict")
    require(all(tool.get("strict") is None for tool in tools if tool.get("name") != "subagent"), "legacy tool unexpectedly received strict mode")
    parameters = subagent.get("parameters", {})
    require(parameters.get("additionalProperties") is False, "subagent schema is not closed")
    require(parameters.get("required") == ["model", "reasoning", "task", "tasks", "chain", "cwd"], "top-level subagent fields are not all required")
    nested = parameters.get("properties", {}).get("tasks", {}).get("items", {})
    require(nested.get("additionalProperties") is False, "nested task schema is not closed")
    require(nested.get("required") == ["model", "reasoning", "task", "cwd"], "nested task fields are not all required")


def latest_result(records: list[dict[str, Any]], marker: str) -> dict[str, Any]:
    parents = [record for record in records if record.get("marker") == marker and record.get("parent")]
    require(len(parents) >= 2, f"{marker}: missing parent tool/follow-up requests")
    followup = next((record for record in reversed(parents) if record["function_outputs"]), None)
    require(followup is not None, f"{marker}: missing actual function_call_output")
    require(followup["has_function_call_output_in_current_user_turn"], f"{marker}: function output was not local to current user turn")
    value = json.loads(followup["function_outputs"][-1])
    require(isinstance(value, dict), f"{marker}: subagent result is not an object")
    return value


def assert_subagent_result(value: dict[str, Any], mode: str, stage_count: int) -> None:
    require(value.get("status") == "completed", f"{mode}: parent result did not complete")
    require(value.get("mode") == mode, f"{mode}: wrong serialized mode")
    stages = value.get("stages")
    require(isinstance(stages, list) and len(stages) == stage_count, f"{mode}: wrong stage count")
    require(value.get("requests") == stage_count, f"{mode}: aggregate requests are not truthful")
    require(value.get("tool_rounds") == 0, f"{mode}: aggregate tool rounds are not truthful")
    expected_outputs = {
        "single": ["child-single"],
        "parallel": ["child-one", "child-two"],
        "chain": ["child-one", "child-two", "child-three"],
    }[mode]
    usage = value.get("usage")
    require(isinstance(usage, dict), f"{mode}: aggregate usage is missing")
    total_input = 0
    total_output = 0
    for stage, expected_output in zip(stages, expected_outputs):
        require(stage.get("status") == "completed", f"{mode}: child stage failed")
        require(stage.get("model") == "verify-a", f"{mode}: child model alias was not inherited")
        require(stage.get("reasoning") == "high", f"{mode}: child reasoning was not inherited")
        require(stage.get("cwd") == str(WORK), f"{mode}: child cwd escaped the parent workspace")
        require(stage.get("output") == expected_output, f"{mode}: child final output is not truthful")
        require(stage.get("requests") == 1, f"{mode}: child request count is wrong")
        require(stage.get("tool_rounds") == 0, f"{mode}: child tool rounds are wrong")
        stage_usage = stage.get("usage")
        require(isinstance(stage_usage, dict), f"{mode}: child usage is missing")
        require(stage_usage.get("input_tokens") == 10, f"{mode}: child input usage is not from mock")
        require(stage_usage.get("output_tokens") == 12, f"{mode}: child output usage is not from mock")
        total_input += stage_usage["input_tokens"]
        total_output += stage_usage["output_tokens"]
    require(usage.get("input_tokens") == total_input, f"{mode}: aggregate input usage is not a sum")
    require(usage.get("output_tokens") == total_output, f"{mode}: aggregate output usage is not a sum")


state = MockState()
server = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
server.mock_state = state  # type: ignore[attr-defined]
threading.Thread(target=server.serve_forever, daemon=True).start()
(PROMPTS / "system.md").write_text("NATIVE_PARENT_SYSTEM\nSynthetic fixture only.\n", encoding="utf-8")
write_config(CONF / "agent.toml", server.server_port, "native-before", False)

window: int | None = None
checks: list[str] = []
result: dict[str, Any] = {
    "status": "RUNNING",
    "profile": PROFILE,
    "tui": str(TUI),
    "agent": str(AGENT),
    "evidence_dir": str(OUT),
    "request_log": str(OUT / "requests.jsonl"),
    "result_path": str(OUT / "result.json"),
    "pixel_screenshots": "not used; iTerm2 screen text and mouse input only",
}

try:
    result["tui_sha256"] = sha256(TUI)
    result["agent_sha256"] = sha256(AGENT)
    window = create_window()
    result["window_id"] = window
    osa(
        f'tell application "iTerm2" to tell current session of window id {window}\n'
        "set columns to 100\nset rows to 32\nend tell"
    )
    time.sleep(0.25)
    command = (
        f"cd {shlex.quote(str(WORK))} && "
        f"env {KEY}=dummy "
        f"{shlex.quote(str(TUI))} --agent-bin {shlex.quote(str(AGENT))} "
        f"--agent-config {shlex.quote(str(CONF / 'agent.toml'))} "
        f"--workspace {shlex.quote(str(WORK))} --theme dark; "
        'rc=$?; printf "\\nNATIVE_FOLLOWUPS_EXIT=%s\\n" "$rc"'
    )
    send(window, "/bin/sh -c " + shlex.quote(command) + "\r")
    wait_screen(window, lambda text: "Create session" in text, "new session form", OUT)
    send(window, "\t\t\t\tNative Followups\t\r")
    wait_screen(window, lambda text: "ready" in text.lower(), "created native session", OUT)
    require(len(session_dirs()) == 1, "initial create did not produce exactly one owned session")
    session_history = session_dirs()[0] / "history.jsonl"
    wait_screen(window, lambda text: "MINICORE" in text and "v0.2.8" in text
                and "Coding agent TUI" in text, "confirmed empty startup guidance", OUT)
    capture(window, OUT / "00-startup-guidance.txt")
    checks.append("fresh workspace/store, one real created session, and confirmed empty startup guidance")

    before_draft = len(session_dirs())
    send(window, "/new\r")
    wait_screen(window, lambda text: "New session" in text and "MINICORE" in text
                and "v0.2.8" in text, "/new draft startup header", OUT)
    capture(window, OUT / "01-new-draft.txt")
    send(window, "\x1b")
    wait_screen(window, lambda text: "New session" not in text and "ready" in text.lower(), "escape new draft", OUT)
    require(len(session_dirs()) == before_draft, "Esc from /new created an extra session record")
    require(not state.snapshot(), "new-session startup flow unexpectedly called the provider")
    checks.append("/new draft is visible and Esc creates no extra record")

    send(window, f"{MARK_CANCEL}\r")
    wait_event(state.cancel_waiting, "Esc cancellation request was not held", window, OUT)
    send(window, "\x1b")
    wait_screen(window, lambda text: MARK_CANCEL in text and "cancelled" in text.lower(), "Esc cancels an active turn", OUT)
    capture(window, OUT / "02-esc-cancel.txt")
    state.cancel_release.set()
    wait_screen(window, lambda text: "ready" in text.lower(), "cancelled turn settled", OUT)
    cancel_records = [record for record in state.snapshot() if record.get("marker") == MARK_CANCEL]
    require(len(cancel_records) == 1, "Esc cancellation caused a retry or duplicate provider request")
    checks.append("real Esc cancels an active model request without retry")

    send(window, f"{MARK_READ}\r")
    wait_event(state.read_waiting, "read follow-up was not held", window, OUT)
    read_screen = wait_screen(
        window,
        lambda text: "missing-native.txt" in text and "tool execution failed" in text,
        "read failure static presentation",
        OUT,
    )
    capture(window, OUT / "02-read-failure-before-toggle.txt")
    click_text(window, "missing-native.txt", OUT)
    read_toggle_one = capture(window, OUT / "02-read-toggle-one.txt")
    click_text(window, "missing-native.txt", OUT)
    read_toggle_two = capture(window, OUT / "02-read-toggle-two.txt")
    require("tool execution failed" in read_screen, "read failure showed only a failed label")
    require("tool execution failed" in read_toggle_one and "tool execution failed" in read_toggle_two, "read static error body was lost during fold toggle")
    folded = next((text for text in (read_toggle_one, read_toggle_two) if "ctrl+o" in text), None)
    expanded = next((text for text in (read_toggle_one, read_toggle_two) if "ctrl+o" not in text), None)
    require(folded is not None and expanded is not None, "real mouse toggle did not produce folded and expanded screens")
    (OUT / "02-read-folded.txt").write_text(folded, encoding="utf-8")
    (OUT / "02-read-expanded.txt").write_text(expanded, encoding="utf-8")
    require(not (WORK / "missing-native.txt").exists(), "missing read unexpectedly mutated the workspace")
    state.read_release.set()
    wait_finished(window, "NATIVE_READ_DONE", "read final response", OUT)
    read_records = [record for record in state.snapshot() if record.get("marker") == MARK_READ]
    require(len(read_records) == 2, "read flow did not make exactly two provider requests")
    require(not read_records[0]["has_function_call_output_in_current_user_turn"], "read first request inherited a stale tool output")
    require(read_records[1]["has_function_call_output_in_current_user_turn"], "read follow-up did not contain its own function output")
    checks.append("missing read uses static Agent error body and real mouse fold/expand")

    (WORK / "fixture.txt").write_text("before\n", encoding="utf-8")
    workspace_before = workspace_snapshot()
    (OUT / "patch-workspace-before.json").write_text(json.dumps(workspace_before, indent=2) + "\n")
    send(window, f"{MARK_PATCH}\r")
    wait_finished(window, "NATIVE_PATCH_DONE", "Codex apply_patch final response", OUT)
    require((WORK / "fixture.txt").read_text(encoding="utf-8") == "after\n", "Codex Update File did not produce the expected file")
    workspace_after = workspace_snapshot()
    expected_workspace = dict(workspace_before)
    expected_workspace["fixture.txt"] = ("file", hashlib.sha256(b"after\n").hexdigest())
    (OUT / "patch-workspace-after.json").write_text(json.dumps(workspace_after, indent=2) + "\n")
    require(workspace_after == expected_workspace, "patch changed other workspace content or entries")
    patch_records = [record for record in state.snapshot() if record.get("marker") == MARK_PATCH]
    require(len(patch_records) == 2, "patch flow did not make exactly two provider requests")
    require(patch_records[1]["has_function_call_output_in_current_user_turn"], "patch follow-up lacks local function output")
    require(any("patched" in output and "fixture.txt" in output for output in patch_records[1]["function_outputs"]), "provider did not receive actual apply_patch success output")
    capture(window, OUT / "03-apply-patch.txt")
    checks.append("Codex Update File target and complete workspace content/entry set verified")

    for marker, mode, stages, final_marker in [
        (MARK_SINGLE, "single", 1, "NATIVE_SUBAGENT_SINGLE_DONE"),
        (MARK_PARALLEL, "parallel", 2, "NATIVE_SUBAGENT_PARALLEL_DONE"),
        (MARK_CHAIN, "chain", 3, "NATIVE_SUBAGENT_CHAIN_DONE"),
    ]:
        send(window, f"{marker}\r")
        wait_finished(window, final_marker, f"{mode} subagent terminal marker", OUT)
        records = state.snapshot()
        parent_records = [record for record in records if record.get("marker") == marker and record.get("parent")]
        require(len(parent_records) == 2, f"{mode}: expected exactly one tool request and one follow-up")
        assert_subagent_schema(parent_records[0])
        require(parent_records[0].get("model") == "native-before", f"{mode}: parent used the wrong configured model")
        require(isinstance(parent_records[0].get("reasoning"), dict) and parent_records[0]["reasoning"].get("effort") == "high", f"{mode}: parent did not use high reasoning")
        arguments = next((record.get("arguments") for record in parent_records if record.get("response_kind") == "subagent-tool"), None)
        require(isinstance(arguments, dict), f"{mode}: mock did not retain actual subagent arguments")
        require(set(arguments) >= {"model", "reasoning", "task", "tasks", "chain", "cwd"}, f"{mode}: top-level optional keys were not explicit")
        if mode == "single":
            require(arguments.get("task") == "NATIVE_CHILD_SINGLE" and arguments.get("tasks") is None and arguments.get("chain") is None, "single: wrong mutually exclusive task shape")
        elif mode == "parallel":
            require(isinstance(arguments.get("tasks"), list) and len(arguments["tasks"]) == 2 and arguments.get("task") is None and arguments.get("chain") is None, "parallel: wrong mutually exclusive task shape")
        else:
            require(isinstance(arguments.get("chain"), list) and len(arguments["chain"]) == 3 and arguments.get("task") is None and arguments.get("tasks") is None, "chain: wrong mutually exclusive task shape")
        child_markers = {
            "single": {"NATIVE_CHILD_SINGLE"},
            "parallel": {"NATIVE_PARALLEL_CHILD_ONE", "NATIVE_PARALLEL_CHILD_TWO"},
            "chain": {"NATIVE_CHAIN_CHILD_ONE", "NATIVE_CHAIN_CHILD_TWO", "NATIVE_CHAIN_CHILD_THREE"},
        }[mode]
        child_records = [record for record in records if record.get("marker") in child_markers]
        require(len(child_records) == stages, f"{mode}: wrong child model request count")
        require(all("subagent" not in record["tools"] for record in child_records), f"{mode}: child advertised recursive subagent")
        require(all(isinstance(record.get("reasoning"), dict) and record["reasoning"].get("effort") == "high" for record in child_records), f"{mode}: child did not inherit high reasoning")
        require(all("NATIVE_PARENT_SYSTEM" in json.dumps(record["body"]) for record in child_records), f"{mode}: child did not receive the inherited system prompt")
        require(all(MARK_READ not in json.dumps(record["body"]) and MARK_PATCH not in json.dumps(record["body"]) for record in child_records), f"{mode}: child inherited unrelated parent history")
        value = latest_result(records, marker)
        assert_subagent_result(value, mode, stages)
        if mode == "chain":
            chain = sorted((record for record in child_records if record["marker"].startswith("NATIVE_CHAIN_CHILD")), key=lambda record: record["count"])
            require(len(chain) == 3, "chain: wrong child request count")
            request_texts = ["\n".join(strings(record["body"].get("input", []))) for record in chain]
            require(any("NATIVE_CHAIN_CHILD_TWO previous=child-one" in text for text in request_texts), "chain did not substitute first actual output")
            require(any("NATIVE_CHAIN_CHILD_THREE previous=child-two" in text for text in request_texts), "chain did not substitute second actual output")
        checks.append(f"native stateless subagent {mode}: actual child requests, schema, tools, output, and usage")

    require(len(session_dirs()) == 1, "stateless subagent execution created child Store sessions")
    history_before_reload = session_history.read_bytes()
    users_before_reload = user_count(history_before_reload)
    require(users_before_reload == 6, "six explicit parent turns must be present before reload")
    write_config(CONF / "agent.toml", server.server_port, "native-after", True)
    send(window, "/reload\r")
    wait_screen(window, lambda text: "configuration and read-only state reloaded" in text.lower(), "idle configuration reload", OUT)
    history_at_reload = session_history.read_bytes()
    require(history_at_reload == history_before_reload, "reload appended or rewrote owned history")
    require(user_count(history_at_reload) == users_before_reload, "reload created a fake User history item")
    _, pages_up = seek_text(window, "tool failed", OUT)
    capture(window, OUT / "04-reload-retained-failure.txt")
    for _ in range(pages_up):
        send(window, "\x1b[6~")
        time.sleep(0.1)
    send(window, "/model\r")
    wait_screen(window, lambda text: "verify-b" in text, "reloaded model selector", OUT)
    capture(window, OUT / "04-reload-model-selector.txt")
    send(window, "\x1b")
    wait_screen(window, lambda text: "Select model" not in text and "ready" in text.lower(), "escape model selector", OUT)
    send(window, f"{MARK_RELOAD}\r")
    wait_finished(window, "NATIVE_RELOAD_DONE", "post-reload provider turn", OUT)
    reload_records = [record for record in state.snapshot() if record.get("marker") == MARK_RELOAD]
    require(reload_records and reload_records[0].get("model") == "native-after", "post-reload provider request used the old raw model")
    history_after_reload_turn = session_history.read_bytes()
    require(history_after_reload_turn.startswith(history_before_reload), "post-reload history is not append-only")
    require(user_count(history_after_reload_turn) == users_before_reload + 1, "post-reload history user count is incorrect")
    send(window, "/model\r")
    wait_screen(window, lambda text: "verify-b" in text, "reloaded model selector", OUT)
    capture(window, OUT / "05-reload-model-selector.txt")
    click_text(window, "verify-b", OUT, times=2)
    wait_screen(window, lambda text: "ready" in text.lower() and "verify b" in text.lower(), "select reloaded model alias", OUT)
    send(window, f"{MARK_ALIAS}\r")
    wait_finished(window, "NATIVE_MODEL_ALIAS_DONE", "selected model alias provider turn", OUT)
    alias_records = [record for record in state.snapshot() if record.get("marker") == MARK_ALIAS]
    require(alias_records and alias_records[0].get("model") == "native-verify-b", "selected model alias did not reach the provider")
    history_after_alias_turn = session_history.read_bytes()
    require(history_after_alias_turn.startswith(history_before_reload), "model alias history is not append-only")
    require(user_count(history_after_alias_turn) == users_before_reload + 2, "model alias history user count is incorrect")
    checks.append("idle reload is read-only, retains failed Tool history, exposes verify-b, and changes both raw model and alias selection")

    send(window, "\x03")
    time.sleep(0.15)
    send(window, "\x03")
    wait_screen(window, lambda text: "NATIVE_FOLLOWUPS_EXIT=0" in text, "clean two-Ctrl-C exit", OUT)
    require(not state.errors, f"loopback server errors: {state.errors}")
    request_text = "\n".join(json.dumps(record["body"], sort_keys=True) for record in state.snapshot())
    require(KEY not in request_text and "dummy" not in request_text, "credential material leaked into a recorded provider body")
    result["status"] = "PASS"
    result["exit_code"] = 0
    result["clean_exit_confirmed"] = True
    require(len(session_dirs()) == 1, "clean exit changed the owned session count")
except BaseException as error:
    result["status"] = "FAIL"
    result["error"] = str(error)
    if window is not None:
        try:
            capture(window, OUT / "failure-screen.txt")
        except Exception as screen_error:
            result["screen_error"] = str(screen_error)
    raise
finally:
    state.read_release.set()
    state.cancel_release.set()
    if window is not None and not result.get("clean_exit_confirmed"):
        try:
            for _ in range(3):
                send(window, "\x03")
                time.sleep(0.2)
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                if "NATIVE_FOLLOWUPS_EXIT=0" in screen(window):
                    result["owned_cleanup_exit_confirmed"] = True
                    break
                time.sleep(0.1)
        except Exception as cleanup_error:
            result["owned_cleanup_error"] = str(cleanup_error)
    server.shutdown()
    server.server_close()
    records = state.snapshot()
    (OUT / "requests.jsonl").write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )
    result["checks"] = checks
    result["request_count"] = len(records)
    result["server_errors"] = list(state.errors)
    result["owned_root"] = str(ROOT)
    (OUT / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    if window is not None and (result.get("clean_exit_confirmed") or result.get("owned_cleanup_exit_confirmed")):
        osa(f'tell application "iTerm2" to close window id {window}')
