#!/usr/bin/env python3
"""Local dashboard and process manager for multiple Alpha Harness profiles."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import RLock, Thread
from urllib.parse import unquote, urlparse

MANAGER_HOST = "127.0.0.1"
MANAGER_PORT = 5100
APP_ROOT = (
    Path(
        os.environ.get(
            "ALPHA_HARNESS_REPO",
            str(Path(__file__).resolve().parent.parent / "alpha-harness"),
        )
    )
    .expanduser()
    .resolve()
)
BACKEND_DIR = APP_ROOT / "backend"
FRONTEND_DIR = APP_ROOT / "frontend"
STATE_DIR = Path.home() / ".alpha-harness-manager"
DB_PATH = STATE_DIR / "accounts.sqlite3"
LOG_DIR = STATE_DIR / "logs"
DASHBOARD_URL = f"http://{MANAGER_HOST}:{MANAGER_PORT}"
PROFILE_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?")
PORT_RE = re.compile(r"^acc([1-9][0-9]*)$")
MAX_NUMBERED_PROFILE = 57535
PROCESS_GRACE_SECONDS = 8.0
_operation_lock = RLock()
_dashboard_open = True


def normalise_profile(value: str) -> str:
    profile = value.strip().lower()
    if not PROFILE_RE.fullmatch(profile):
        raise ValueError(
            "Use 1-32 lowercase letters, numbers, and internal hyphens for an account name."
        )
    match = PORT_RE.fullmatch(profile)
    if match and int(match.group(1)) > MAX_NUMBERED_PROFILE:
        raise ValueError(f"Numbered profiles must be between acc1 and acc{MAX_NUMBERED_PROFILE}.")
    return profile


def ports_for(profile: str) -> tuple[int, int]:
    numbered = PORT_RE.fullmatch(profile)
    if numbered:
        number = int(numbered.group(1))
        return 8000 + number, 5173 + number
    slot = int.from_bytes(hashlib.sha256(profile.encode()).digest()[:4], "big") % 20_000
    return 10_000 + slot, 30_000 + slot


def _port_pair_is_available(
    connection: sqlite3.Connection, profile: str, ports: tuple[int, int]
) -> bool:
    conflict = connection.execute(
        """
        SELECT profile FROM accounts
        WHERE profile != ? AND (backend_port IN (?, ?) OR frontend_port IN (?, ?))
        """,
        (profile, *ports, *ports),
    ).fetchone()
    if conflict:
        return False
    try:
        for port in ports:
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", port))
    except OSError:
        return False
    return True


def _find_free_port_pair(
    connection: sqlite3.Connection, profile: str, preferred: tuple[int, int]
) -> tuple[int, int]:
    first_slot = int.from_bytes(hashlib.sha256(profile.encode()).digest()[:4], "big") % 20_000
    for offset in range(20_000):
        if offset == 0:
            candidate = preferred
        else:
            slot = (first_slot + offset) % 20_000
            candidate = (10_000 + slot, 30_000 + slot)
        if _port_pair_is_available(connection, profile, candidate):
            return candidate
    raise RuntimeError("No free backend and frontend port pair is available.")


def initialise() -> None:
    if not (BACKEND_DIR / "pyproject.toml").is_file():
        raise RuntimeError(f"Alpha Harness backend was not found at {BACKEND_DIR}")
    if not (FRONTEND_DIR / "package.json").is_file():
        raise RuntimeError(f"Alpha Harness frontend was not found at {FRONTEND_DIR}")
    STATE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    LOG_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                profile TEXT PRIMARY KEY,
                backend_port INTEGER NOT NULL,
                frontend_port INTEGER NOT NULL,
                backend_pid INTEGER,
                frontend_pid INTEGER,
                state TEXT NOT NULL,
                error TEXT,
                updated_at TEXT NOT NULL,
                ports_locked INTEGER NOT NULL DEFAULT 1,
                last_started_at TEXT
            )
            """
        )
        columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(accounts)").fetchall()
        }
        if "ports_locked" not in columns:
            connection.execute(
                "ALTER TABLE accounts ADD COLUMN ports_locked INTEGER NOT NULL DEFAULT 1"
            )
            default_ports = ports_for("default")
            connection.execute(
                """
                UPDATE accounts SET ports_locked = 0
                WHERE profile = 'default' AND state = 'stopped'
                    AND backend_port = ? AND frontend_port = ?
                """,
                default_ports,
            )
        if "last_started_at" not in columns:
            connection.execute("ALTER TABLE accounts ADD COLUMN last_started_at TEXT")
        default = connection.execute(
            "SELECT 1 FROM accounts WHERE profile = 'default'"
        ).fetchone()
        if default is None:
            backend_port, frontend_port = _find_free_port_pair(
                connection, "default", ports_for("default")
            )
            connection.execute(
                """
                INSERT INTO accounts (
                    profile, backend_port, frontend_port, state, error, updated_at, ports_locked
                ) VALUES ('default', ?, ?, 'stopped', NULL, ?, 0)
                """,
                (backend_port, frontend_port, _now()),
            )


def connect() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, timeout=10)
    connection.row_factory = sqlite3.Row
    return connection


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _account(row: sqlite3.Row) -> dict[str, object]:
    profile = str(row["profile"])
    state = str(row["state"])
    error = row["error"]
    backend_pid = row["backend_pid"]
    frontend_pid = row["frontend_pid"]
    if state in {"starting", "running"}:
        backend_alive = _pid_alive(backend_pid)
        frontend_alive = _pid_alive(frontend_pid)
        if not backend_alive or not frontend_alive:
            if backend_alive and backend_pid:
                _stop_process(int(backend_pid))
            if frontend_alive and frontend_pid:
                _stop_process(int(frontend_pid))
            failed_processes = []
            if not backend_alive:
                failed_processes.append("backend")
            if not frontend_alive:
                failed_processes.append("frontend")
            error = (
                f"{' and '.join(failed_processes).capitalize()} process exited unexpectedly. "
                f"Check the logs in {LOG_DIR}."
            )
            _set_state(profile, "error", error=error, clear_pids=True)
            state = "error"
        elif _url_ready(int(row["backend_port"]), "/docs") and _url_ready(
            int(row["frontend_port"]), "/"
        ):
            state = "running"
            _set_state(profile, state)
        else:
            state = "starting"
    return {
        "profile": profile,
        "state": state,
        "backend_url": f"http://127.0.0.1:{row['backend_port']}",
        "frontend_url": f"http://localhost:{row['frontend_port']}",
        "backend_port": int(row["backend_port"]),
        "frontend_port": int(row["frontend_port"]),
        "last_started_at": row["last_started_at"],
        "error": error,
    }


def _set_state(
    profile: str,
    state: str,
    *,
    backend_pid: int | None = None,
    frontend_pid: int | None = None,
    error: str | None = None,
    clear_pids: bool = False,
) -> None:
    if clear_pids:
        query = """
            UPDATE accounts
            SET state = ?, backend_pid = NULL, frontend_pid = NULL, error = ?, updated_at = ?
            WHERE profile = ?
        """
        values: tuple[object, ...] = (state, error, _now(), profile)
    elif backend_pid is not None and frontend_pid is not None:
        query = """
            UPDATE accounts
            SET state = ?, backend_pid = ?, frontend_pid = ?, error = ?, updated_at = ?,
                last_started_at = ?
            WHERE profile = ?
        """
        started_at = _now()
        values = (state, backend_pid, frontend_pid, error, started_at, started_at, profile)
    else:
        query = """
            UPDATE accounts
            SET state = ?, error = ?, updated_at = ?
            WHERE profile = ?
        """
        values = (state, error, _now(), profile)
    with connect() as connection:
        connection.execute(query, values)


def _pid_alive(pid: object) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _url_ready(port: int, path: str) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=0.5) as response:
            return response.status < 500
    except (OSError, urllib.error.URLError):
        return False


def accounts() -> list[dict[str, object]]:
    with connect() as connection:
        rows = connection.execute("SELECT * FROM accounts ORDER BY profile").fetchall()
    return [_account(row) for row in rows]


def _reserve_profile(profile: str, backend_port: int, frontend_port: int) -> tuple[int, int]:
    with connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            """
            SELECT state, backend_pid, frontend_pid, backend_port, frontend_port, ports_locked
            FROM accounts WHERE profile = ?
            """,
            (profile,),
        ).fetchone()
        if (
            row
            and row["state"] in {"starting", "running"}
            and (_pid_alive(row["backend_pid"]) or _pid_alive(row["frontend_pid"]))
        ):
            raise RuntimeError(f"Account {profile} is already running or starting.")

        if row and row["ports_locked"]:
            selected_ports = (int(row["backend_port"]), int(row["frontend_port"]))
            if not _port_pair_is_available(connection, profile, selected_ports):
                raise RuntimeError(
                    f"The URL for profile {profile} is fixed to ports "
                    f"{selected_ports[0]} and {selected_ports[1]}, but one is already in use. "
                    "Free the port and try again; the profile URL will not change."
                )
        else:
            preferred_ports = (
                (int(row["backend_port"]), int(row["frontend_port"]))
                if row
                else (backend_port, frontend_port)
            )
            selected_ports = _find_free_port_pair(
                connection, profile, preferred_ports
            )
        backend_port, frontend_port = selected_ports
        connection.execute(
            """
            INSERT INTO accounts (
                profile, backend_port, frontend_port, state, error, updated_at, ports_locked
            ) VALUES (?, ?, ?, 'starting', NULL, ?, 1)
            ON CONFLICT(profile) DO UPDATE SET
                backend_port = excluded.backend_port,
                frontend_port = excluded.frontend_port,
                state = 'starting',
                backend_pid = NULL,
                frontend_pid = NULL,
                error = NULL,
                updated_at = excluded.updated_at,
                ports_locked = 1
            """,
            (profile, backend_port, frontend_port, _now()),
        )
    return backend_port, frontend_port


def _spawn(
    command: list[str], cwd: Path, env: dict[str, str], log_path: Path
) -> subprocess.Popen[bytes]:
    handle = log_path.open("ab")
    try:
        return subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=os.name != "nt",
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        )
    finally:
        handle.close()


def start_account(value: str) -> dict[str, object]:
    profile = normalise_profile(value)
    backend_port, frontend_port = ports_for(profile)
    with _operation_lock:
        backend_port, frontend_port = _reserve_profile(profile, backend_port, frontend_port)
        env = os.environ.copy()
        env["ALPHA_HARNESS_PROFILE"] = profile
        env.pop("ALPHA_HARNESS_DATA_DIR", None)
        env["ALPHA_HARNESS_PORT"] = str(backend_port)
        env["ALPHA_HARNESS_FRONTEND_PORT"] = str(frontend_port)
        backend_command = (
            ["uv", "run", "alpha-harness"]
            if profile == "default"
            else [
                "uv",
                "run",
                "python",
                "-m",
                "uvicorn",
                "alpha_harness.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(backend_port),
            ]
        )
        frontend_command = (
            ["npm", "run", "dev"]
            if profile == "default"
            else ["npm", "run", "dev", "--", "--host", "127.0.0.1"]
        )
        backend: subprocess.Popen[bytes] | None = None
        frontend: subprocess.Popen[bytes] | None = None
        try:
            backend = _spawn(
                backend_command,
                BACKEND_DIR,
                env,
                LOG_DIR / f"{profile}-backend.log",
            )
            frontend = _spawn(
                frontend_command,
                FRONTEND_DIR,
                env,
                LOG_DIR / f"{profile}-frontend.log",
            )
            if backend.poll() is not None:
                raise RuntimeError(
                    f"Backend exited during startup; see {LOG_DIR / f'{profile}-backend.log'}"
                )
            if frontend.poll() is not None:
                raise RuntimeError(
                    f"Frontend exited during startup; see {LOG_DIR / f'{profile}-frontend.log'}"
                )
            _set_state(
                profile,
                "starting",
                backend_pid=backend.pid,
                frontend_pid=frontend.pid,
            )
        except (OSError, RuntimeError) as exc:
            if frontend is not None:
                _stop_process(frontend.pid)
            if backend is not None:
                _stop_process(backend.pid)
            _set_state(profile, "error", error=str(exc))
            raise RuntimeError(str(exc)) from exc
    return get_account(profile)


def get_account(value: str) -> dict[str, object]:
    profile = normalise_profile(value)
    with connect() as connection:
        row = connection.execute("SELECT * FROM accounts WHERE profile = ?", (profile,)).fetchone()
    if row is None:
        raise ValueError(f"Account {profile} has not been added.")
    return _account(row)


def _stop_process(pid: int) -> None:
    if not _pid_alive(pid):
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            check=False,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        return
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + PROCESS_GRACE_SECONDS
    while time.monotonic() < deadline:
        try:
            os.killpg(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.1)
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def stop_account(value: str) -> dict[str, object]:
    profile = normalise_profile(value)
    with _operation_lock:
        with connect() as connection:
            row = connection.execute(
                "SELECT backend_pid, frontend_pid FROM accounts WHERE profile = ?",
                (profile,),
            ).fetchone()
        if row is None:
            raise ValueError(f"Account {profile} has not been added.")
        _set_state(profile, "stopping")
        for key in ("frontend_pid", "backend_pid"):
            pid = row[key]
            if isinstance(pid, int):
                _stop_process(pid)
        _set_state(profile, "stopped", clear_pids=True)
    return get_account(profile)


def stop_all_accounts() -> None:
    failures = []
    for account in accounts():
        if account["state"] in {"running", "starting"}:
            profile = str(account["profile"])
            try:
                stop_account(profile)
            except (RuntimeError, ValueError, OSError, sqlite3.Error) as exc:
                failures.append(f"{profile}: {exc}")
    if failures:
        raise RuntimeError("Could not stop every profile: " + "; ".join(failures))


def remove_account(value: str) -> None:
    profile = normalise_profile(value)
    if profile == "default":
        raise RuntimeError("The built-in default profile cannot be removed.")
    account = get_account(profile)
    if account["state"] in {"running", "starting"}:
        raise RuntimeError(f"Stop account {profile} before removing it.")
    with connect() as connection:
        connection.execute("DELETE FROM accounts WHERE profile = ?", (profile,))


def _print_accounts(items: list[dict[str, object]]) -> None:
    if not items:
        print(f"No accounts added. Dashboard: {DASHBOARD_URL}")
        return
    print(f"{'ACCOUNT':<20} {'STATUS':<10} {'FRONTEND':<34} BACKEND")
    for item in items:
        frontend = str(item["frontend_url"]) if item["state"] in {"running", "starting"} else "—"
        backend = str(item["backend_url"]) if item["state"] in {"running", "starting"} else "—"
        print(f"{item['profile']:<20} {item['state']:<10} {frontend:<34} {backend}")
    print(f"\nDashboard: {DASHBOARD_URL}")


def _ensure_dashboard() -> None:
    if _url_ready(MANAGER_PORT, "/api/ping"):
        return
    initialise()
    with (STATE_DIR / "manager.log").open("ab") as log:
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "serve"],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=os.name != "nt",
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        )
    del process
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if _url_ready(MANAGER_PORT, "/api/ping"):
            return
        time.sleep(0.1)
    raise RuntimeError(f"Could not start account dashboard. See {STATE_DIR / 'manager.log'}.")


def _set_dashboard_open() -> None:
    request = urllib.request.Request(
        f"{DASHBOARD_URL}/api/manager/start",
        data=b"{}",
        headers={"Content-Type": "application/json", "Origin": DASHBOARD_URL},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=3) as response:
        response.read()


def _json_response(handler: BaseHTTPRequestHandler, status: int, body: object) -> None:
    encoded = json.dumps(body).encode()
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(encoded)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(encoded)


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "AlphaHarnessAccounts/1"

    def log_message(self, _format: str, *args: object) -> None:
        return

    def _allowed_host(self) -> bool:
        return self.headers.get("Host", "").lower() in {
            f"127.0.0.1:{MANAGER_PORT}",
            f"localhost:{MANAGER_PORT}",
        }

    def _allowed_origin(self) -> bool:
        return self.headers.get("Origin") in {
            f"http://127.0.0.1:{MANAGER_PORT}",
            f"http://localhost:{MANAGER_PORT}",
        }

    def _body(self) -> dict[str, object]:
        if self.headers.get("Content-Type", "").split(";", 1)[0] != "application/json":
            raise ValueError("Requests must use application/json.")
        length = int(self.headers.get("Content-Length", "0"))
        if length > 4096:
            raise ValueError("Request body is too large.")
        body = json.loads(self.rfile.read(length))
        if not isinstance(body, dict):
            raise ValueError("Expected a JSON object.")
        return body

    def do_GET(self) -> None:
        if not self._allowed_host():
            _json_response(self, 403, {"error": "Invalid Host header."})
            return
        if self.path == "/api/ping":
            _json_response(self, 200, {"ok": True})
        elif self.path == "/api/accounts":
            _json_response(self, 200, {"accounts": accounts()})
        elif self.path == "/":
            page = DASHBOARD_HTML if _dashboard_open else DASHBOARD_CLOSED_HTML
            encoded = page.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(encoded)
        else:
            _json_response(self, 404, {"error": "Not found."})

    def do_POST(self) -> None:
        if not self._allowed_host() or not self._allowed_origin():
            _json_response(self, 403, {"error": "Request must come from this local dashboard."})
            return
        if self.path == "/api/shutdown":
            _json_response(self, 200, {"ok": True})
            Thread(target=self.server.shutdown, daemon=True).start()
            return
        try:
            body = self._body()
            if self.path == "/api/manager/close":
                global _dashboard_open
                _dashboard_open = False
                _json_response(self, 200, {"ok": True})
                return
            if self.path == "/api/manager/start":
                _dashboard_open = True
                _json_response(self, 200, {"ok": True})
                return
            if self.path == "/api/accounts/shutdown":
                stop_all_accounts()
                _json_response(self, 200, {"ok": True})
                return
            if self.path == "/api/accounts":
                profile = body.get("profile")
                if not isinstance(profile, str):
                    raise ValueError("Enter an account name.")
                account = start_account(profile)
                _json_response(self, 201, {"account": account})
                return
            path = urlparse(self.path).path.strip("/").split("/")
            if len(path) == 4 and path[:2] == ["api", "accounts"] and path[3] in {"start", "stop"}:
                profile = unquote(path[2])
                account = start_account(profile) if path[3] == "start" else stop_account(profile)
                _json_response(self, 200, {"account": account})
                return
            _json_response(self, 404, {"error": "Not found."})
        except (ValueError, RuntimeError, OSError, sqlite3.Error) as exc:
            _json_response(self, 400, {"error": str(exc)})

    def do_DELETE(self) -> None:
        if not self._allowed_host() or not self._allowed_origin():
            _json_response(self, 403, {"error": "Request must come from this local dashboard."})
            return
        path = urlparse(self.path).path.strip("/").split("/")
        if len(path) != 3 or path[:2] != ["api", "accounts"]:
            _json_response(self, 404, {"error": "Not found."})
            return
        try:
            remove_account(unquote(path[2]))
            _json_response(self, 200, {"ok": True})
        except (ValueError, RuntimeError, OSError, sqlite3.Error) as exc:
            _json_response(self, 400, {"error": str(exc)})


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Alpha Harness activation</title>
  <style>
    :root {
      color-scheme: light; font: 15px/1.5 system-ui, sans-serif; background: #f4f6fa; color: #202938;
      --accent: #3478f6; --accent-dark: #2464d6; --accent-soft: #2768dc;
      --accent-border: #d6e2f8; --muted: #758196; --surface: #fff;
      --line: #e4e9f0;
    }
    *, *::before, *::after { box-sizing: border-box; }
    body { max-width: 1120px; margin: 42px auto; padding: 0 28px; }
    .titlebar { display: flex; align-items: center; justify-content: space-between; gap: 24px; }
    .heading-group { min-width: 0; }
    h1 { margin: 0; font-size: 1.7rem; letter-spacing: -.035em; color: #182335; }
    p, small { color: var(--muted); }
    .intro { margin: 7px 0 0; color: #68758a; }
    form, article { display: flex; align-items: center; gap: 16px; }
    form {
      margin: 0 30px; padding: 5px; border: 1px solid var(--line); border-radius: 10px;
      background: var(--surface); box-shadow: 0 3px 12px rgba(25, 42, 70, .045);
    }
    #accounts { display: grid; gap: 10px; }
    .profiles-section { margin-top: 34px; }
    .list-heading { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; }
    h2 { margin: 0; color: #28364a; font-size: 1.3rem; font-weight: 650; letter-spacing: -.015em; }
    article {
      width: 100%; min-width: 0; justify-content: space-between; gap: 28px;
      padding: 16px 18px; margin: 0;
      border: 1px solid var(--line); border-radius: 12px;
      background: var(--surface); box-shadow: 0 3px 12px rgba(25, 42, 70, .04);
      transition: border-color .18s ease, transform .18s ease, box-shadow .18s ease;
    }
    article:hover { border-color: #cbd8ec; transform: translateY(-1px); box-shadow: 0 7px 20px rgba(25, 42, 70, .08); }
    input, button { border: 1px solid #dce3ed; border-radius: 8px; background: var(--surface); color: inherit; padding: 9px 12px; }
    input { width: 230px; } button { cursor: pointer; transition: background .18s ease, border-color .18s ease, box-shadow .18s ease; }
    form input {
      box-sizing: border-box; flex: 0 1 220px; width: 420px; min-width: 280px; height: 38px; padding: 0 14px;
      border-color: #dce3ed; border-radius: 4px; background: #fbfcfe; font-size: .98rem;
      transition: border-color .18s ease, box-shadow .18s ease, background .18s ease;
    }
    form input::placeholder { color: #98a2b2; }
    form input:focus { outline: none; border-color: var(--accent); background: #fff; box-shadow: 0 0 0 3px rgba(52, 120, 246, .13); }
    form .primary { min-height: 38px; padding: 0 20px; border-radius: 4px; font-size: .98rem; font-weight: 600; letter-spacing: .005em; white-space: nowrap; }
    button:hover { background: #f5f8fd; border-color: #b9c9df; }
    button.primary { background: #edf7f0; border-color: #c5e4cf; color: #287344; box-shadow: 0 2px 5px rgba(40, 115, 68, .08); }
    button.primary:hover { background: #e3f2e8; border-color: #a9d5b7; color: #205d37; }
    a { color: var(--accent-soft); text-decoration: none; }
    a:hover { color: #174fae; }
    .links { display: flex; align-items: center; flex-wrap: wrap; gap: 7px; margin-top: 0; }
    .links a, .icon-button {
      display: inline-flex; align-items: center; justify-content: center; min-height: 38px;
      padding: 7px 11px; border: 1px solid var(--line); border-radius: 8px;
      background: #f8fafd; color: #46546a;
      transition: background .18s ease, border-color .18s ease, transform .18s ease, box-shadow .18s ease;
    }
    .links a:hover, .icon-button:hover { border-color: #bfd2f3; background: #eef4ff; color: #245fc5; transform: translateY(-1px); }
    .app-url { overflow-wrap: anywhere; }
    .icon-button { padding: 7px; color: #536178; }
    .icon-button.copy-url { border-color: #c5e4cf; color: #287344; background: #edf7f0; }
    .icon-button.copy-url:hover { border-color: #a9d5b7; color: #205d37; background: #e3f2e8; }
    .icon-button svg { width: 16px; height: 16px; }
    .actions .icon-button, .title-actions .icon-button { width: 40px; height: 40px; padding: 10px; }
    .icon-button.stop-warning { border-color: #e8b9b5; color: #b84038; background: #fff0ee; }
    .icon-button.stop-warning:hover { border-color: #d98780; color: #9f2f29; background: #ffe3e0; }
    .icon-button.danger { border-color: #f0d9d7; color: #c65b54; background: #fffafa; }
    .icon-button.danger:hover { border-color: #e9b6b2; background: #fff0ef; color: #a93d36; }
    .state { display: inline-flex; align-items: center; min-height: 23px; padding: 1px 9px; border-radius: 999px; font-size: .78rem; font-weight: 600; }
    .state-running { background: #e3f6e9; color: #238044; }
    .state-starting { background: #fff2d7; color: #946315; }
    .state-stopping { background: #edf0f5; color: #657188; }
    .state-stopped { background: #eef1f5; color: #667286; }
    .state-error { background: #ffebe9; color: #b9433b; }
    .error { color: #b9433b; min-height: 1.3em; }
    .started-at { margin-top: 4px; color: #8490a1; font-size: .82rem; }
    .profile-update { margin: -4px 8px 12px; padding: 9px 12px; border-left: 2px solid var(--accent); color: #536178; font-size: .9rem; }
    .profile-update.error { border-left-color: #d45a52; color: #b9433b; }
    .actions { display: flex; align-items: center; justify-content: flex-end; flex-wrap: wrap; gap: 18px; }
    .actions .links { gap: 8px; }
    .profile-controls { display: flex; align-items: center; gap: 8px; flex: 0 0 auto; }
    .empty { padding: 24px 0; color: var(--muted); }
    .header-controls { display: flex; align-items: center; justify-content: flex-end; gap: 26px; flex: 0 0 auto; }
    .title-actions { display: flex; align-items: center; gap: 12px; flex: 0 0 auto; }
    @media (max-width: 760px) {
      body { margin: 28px auto; padding: 0 16px; }
      .titlebar { align-items: flex-start; flex-direction: column; gap: 16px; }
      .header-controls { width: 100%; justify-content: space-between; }
      h1 { font-size: 1.4rem; }
      article { align-items: stretch; flex-direction: column; gap: 16px; padding: 16px; }
      .actions { width: 100%; justify-content: space-between; gap: 12px; }
      .actions .links { min-width: 0; }
      .profiles-section { margin-top: 28px; }
    }
    @media (max-width: 430px) {
      .header-controls { align-items: flex-start; flex-wrap: wrap; }
      #add { flex: 1 1 100%; }
      #add input { flex: 1 1 auto; width: auto; }
    }
    @media (prefers-reduced-motion: reduce) { *, *::before, *::after { transition-duration: .01ms !important; } }
  </style>
</head>
<body>
  <header class="titlebar">
    <div class="heading-group">
      <h1>Alpha Harness activation</h1>
      <p class="intro">Manage your local Alpha Harness profiles.</p>
    </div>
    <div class="header-controls">
      <form id="add">
        <input id="profile" required maxlength="64" pattern="[A-Za-z0-9]+([A-Za-z0-9-]*[A-Za-z0-9])?"
          placeholder="Profile name" aria-label="Profile name">
        <button class="primary">Add &amp; start</button>
      </form>
      <div class="title-actions">
        <button id="shutdown-all" class="icon-button danger" type="button" title="Shut down all profiles" aria-label="Shut down all profiles">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 3v8"></path><path d="M6.2 6.2a9 9 0 1 0 11.6 0"></path></svg>
        </button>
        <button id="close-manager" class="icon-button" type="button" title="Close manager (profiles keep running)" aria-label="Close manager, leaving profiles running">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M18 6 6 18M6 6l12 12"></path></svg>
        </button>
      </div>
    </div>
  </header>
  <div id="form-message" class="error" role="status"></div>
  <section class="profiles-section" aria-labelledby="profiles-heading">
    <div class="list-heading"><h2 id="profiles-heading">Profiles</h2></div>
    <main id="accounts"><p class="empty">Loading profiles…</p></main>
  </section>
  <script>
    const main = document.querySelector('#accounts');
    const formMessage = document.querySelector('#form-message');
    const profileMessages = new Map();
    async function api(url, options = {}) {
      const response = await fetch(url, { ...options, headers: { 'Content-Type': 'application/json', ...options.headers } });
      const body = await response.json();
      if (!response.ok) throw new Error(body.error || `Request failed (${response.status})`);
      return body;
    }
    function render(items) {
      main.replaceChildren();
      items.sort((a, b) =>
        Number(b.profile === 'default') - Number(a.profile === 'default') ||
        a.profile.localeCompare(b.profile),
      );
      if (!items.length) {
        const empty = document.createElement('p'); empty.className = 'empty';
        empty.textContent = 'No accounts yet. Add one above.'; main.append(empty); return;
      }
      for (const item of items) {
        const article = document.createElement('article');
        const info = document.createElement('div');
        const name = document.createElement('strong');
        name.textContent = item.profile === 'default' ? 'WorldQuant Brain' : item.profile;
        const state = document.createElement('span'); state.className = `state state-${item.state}`; state.textContent = item.state;
        info.append(name, document.createTextNode('  '), state);
        const started = document.createElement('div'); started.className = 'started-at';
        started.textContent = item.last_started_at
          ? `Last started: ${new Date(item.last_started_at).toLocaleString()}`
          : 'Not started yet';
        info.append(started);
        if (item.error) profileMessages.set(item.profile, item.error);
        let links = null;
        if (item.state === 'running' || item.state === 'starting') {
          links = document.createElement('div'); links.className = 'links';
          const appLink = document.createElement('a'); appLink.className = 'app-url';
          appLink.textContent = item.frontend_url; appLink.href = item.frontend_url;
          appLink.target = '_blank'; appLink.rel = 'noopener';
          const copy = document.createElement('button'); copy.className = 'icon-button copy-url';
          copy.type = 'button'; copy.title = 'Copy app URL'; copy.setAttribute('aria-label', 'Copy app URL');
          copy.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="8" y="8" width="13" height="13" rx="2"></rect><path d="M16 8V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h3"></path></svg>';
          copy.onclick = async () => {
            try {
              await navigator.clipboard.writeText(item.frontend_url);
              profileMessages.set(item.profile, 'App URL copied.');
            } catch (error) {
              profileMessages.set(item.profile, `Could not copy app URL: ${error.message}`);
            }
            refresh();
          };
          const backendLink = document.createElement('a'); backendLink.textContent = 'Backend';
          backendLink.href = item.backend_url; backendLink.target = '_blank'; backendLink.rel = 'noopener';
          links.append(appLink, copy, backendLink);
        }
        const actions = document.createElement('div'); actions.className = 'actions';
        const toggle = document.createElement('button');
        const isActive = item.state === 'running' || item.state === 'starting';
        toggle.className = `icon-button${isActive ? ' stop-warning' : ''}`;
        toggle.type = 'button';
        toggle.title = isActive ? 'Stop profile' : 'Start profile';
        toggle.setAttribute('aria-label', `${isActive ? 'Stop' : 'Start'} ${item.profile}`);
        toggle.innerHTML = isActive
          ? '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="5" y="5" width="14" height="14" rx="2"></rect></svg>'
          : '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m8 5 12 7-12 7z"></path></svg>';
        toggle.onclick = async () => {
          toggle.disabled = true; profileMessages.delete(item.profile);
          try { await api(`/api/accounts/${encodeURIComponent(item.profile)}/${isActive ? 'stop' : 'start'}`, { method: 'POST', body: '{}' }); await refresh(); }
          catch (error) { profileMessages.set(item.profile, error.message); await refresh(); }
          finally { toggle.disabled = false; }
        };
        const remove = document.createElement('button');
        remove.className = 'icon-button danger';
        remove.type = 'button';
        remove.title = 'Remove profile';
        remove.setAttribute('aria-label', `Remove ${item.profile}`);
        remove.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M3 6h18"></path><path d="M8 6V4h8v2"></path><path d="m19 6-1 14H6L5 6"></path><path d="M10 11v5M14 11v5"></path></svg>';
        remove.disabled = item.profile === 'default' || item.state === 'running' || item.state === 'starting';
        if (item.profile === 'default') remove.title = 'The built-in default profile cannot be removed.';
        remove.onclick = async () => {
          try { await api(`/api/accounts/${encodeURIComponent(item.profile)}`, { method: 'DELETE' }); profileMessages.delete(item.profile); await refresh(); }
          catch (error) { profileMessages.set(item.profile, error.message); await refresh(); }
        };
        const controls = document.createElement('div'); controls.className = 'profile-controls';
        controls.append(toggle, remove);
        if (links) actions.append(links);
        actions.append(controls); article.append(info, actions); main.append(article);
        const update = profileMessages.get(item.profile);
        if (update) {
          const notice = document.createElement('p');
          notice.className = `profile-update${item.error || update.startsWith('Could not') ? ' error' : ''}`;
          notice.setAttribute('role', 'status'); notice.textContent = update; main.append(notice);
        }
      }
    }
    async function refresh() {
      try { render((await api('/api/accounts')).accounts); }
      catch (error) { formMessage.textContent = error.message; }
    }
    document.querySelector('#add').onsubmit = async event => {
      event.preventDefault(); formMessage.textContent = '';
      const input = document.querySelector('#profile');
      try { await api('/api/accounts', { method: 'POST', body: JSON.stringify({ profile: input.value }) }); input.value = ''; await refresh(); }
      catch (error) { formMessage.textContent = error.message; }
    };
    document.querySelector('#shutdown-all').onclick = async event => {
      if (!confirm('Shut down all running profiles? The manager dashboard will stay open.')) return;
      const button = event.currentTarget;
      button.disabled = true; formMessage.textContent = '';
      try {
        await api('/api/accounts/shutdown', { method: 'POST', body: '{}' });
        profileMessages.clear();
        formMessage.textContent = 'All profiles shut down.';
        await refresh();
      } catch (error) {
        formMessage.textContent = error.message;
        await refresh();
      } finally {
        button.disabled = false;
      }
    };
    document.querySelector('#close-manager').onclick = async event => {
      if (!confirm('Close the manager? Running profiles will keep running.')) return;
      const button = event.currentTarget;
      button.disabled = true; formMessage.textContent = '';
      try {
        await api('/api/manager/close', { method: 'POST', body: '{}' });
        window.location.reload();
      } catch (error) {
        formMessage.textContent = error.message;
        button.disabled = false;
      }
    };
    refresh(); setInterval(refresh, 3000);
  </script>
</body>
</html>"""


DASHBOARD_CLOSED_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Alpha Harness manager paused</title>
  <style>
    :root { color-scheme: light; font: 15px/1.5 system-ui, sans-serif; color: #202938; background: #f4f6fa; }
    * { box-sizing: border-box; }
    body { min-height: 100vh; display: grid; place-items: center; margin: 0; padding: 24px; }
    main { width: min(100%, 440px); padding: 36px; text-align: center; background: #fff; border: 1px solid #e4e9f0; border-radius: 16px; box-shadow: 0 12px 36px rgba(25, 42, 70, .09); }
    .mark { display: grid; place-items: center; width: 52px; height: 52px; margin: 0 auto 20px; border-radius: 14px; background: #edf3ff; color: #3478f6; }
    .mark svg { width: 26px; height: 26px; }
    h1 { margin: 0; color: #182335; font-size: 1.45rem; letter-spacing: -.025em; }
    p { margin: 10px 0 24px; color: #758196; }
    button { min-height: 42px; padding: 0 18px; border: 1px solid #3478f6; border-radius: 9px; color: #fff; background: #3478f6; font: inherit; font-weight: 600; cursor: pointer; transition: background .18s ease, transform .18s ease; }
    button:hover { background: #2464d6; transform: translateY(-1px); }
    button:disabled { cursor: wait; opacity: .65; transform: none; }
    #message { min-height: 1.4em; margin-top: 14px; color: #b9433b; font-size: .9rem; }
  </style>
</head>
<body>
  <main>
    <div class="mark" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7" rx="1.5"></rect><rect x="14" y="3" width="7" height="7" rx="1.5"></rect><rect x="3" y="14" width="7" height="7" rx="1.5"></rect><rect x="14" y="14" width="7" height="7" rx="1.5"></rect></svg></div>
    <h1>Manager closed</h1>
    <p>Running profiles are still available at their existing URLs.</p>
    <button id="start-manager" type="button">Start manager</button>
    <div id="message" role="status" aria-live="polite"></div>
  </main>
  <script>
    document.querySelector('#start-manager').onclick = async event => {
      const button = event.currentTarget;
      const message = document.querySelector('#message');
      button.disabled = true; message.textContent = '';
      try {
        const response = await fetch('/api/manager/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: '{}',
        });
        const result = await response.json();
        if (!response.ok) throw new Error(result.error || `Request failed (${response.status})`);
        window.location.reload();
      } catch (error) {
        message.textContent = error.message;
        button.disabled = false;
      }
    };
  </script>
</body>
</html>"""


def serve() -> None:
    initialise()
    server = ThreadingHTTPServer((MANAGER_HOST, MANAGER_PORT), DashboardHandler)
    server.daemon_threads = True
    print(f"Alpha Harness Accounts dashboard: {DASHBOARD_URL}", flush=True)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage local Alpha Harness account profiles.")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("dashboard", help="Open the local accounts page.")
    for command in ("add", "start", "stop", "remove"):
        child = subparsers.add_parser(command, help=f"{command.title()} an account profile.")
        child.add_argument("profile")
    subparsers.add_parser("list", help="List account statuses and URLs.")
    subparsers.add_parser("serve", help=argparse.SUPPRESS)
    subparsers.add_parser("shutdown", help="Close the dashboard and leave profiles running.")
    args = parser.parse_args()

    try:
        initialise()
        if args.command == "serve":
            serve()
            return 0
        _ensure_dashboard()
        if args.command in {None, "dashboard"}:
            _set_dashboard_open()
            webbrowser.open(DASHBOARD_URL)
            print(f"Dashboard: {DASHBOARD_URL}")
        elif args.command in {"add", "start"}:
            account = start_account(args.profile)
            _print_accounts([account])
        elif args.command == "stop":
            _print_accounts([stop_account(args.profile)])
        elif args.command == "remove":
            remove_account(args.profile)
            print(f"Removed {normalise_profile(args.profile)}.")
        elif args.command == "list":
            _print_accounts(accounts())
        elif args.command == "shutdown":
            request = urllib.request.Request(
                f"{DASHBOARD_URL}/api/shutdown",
                data=b"{}",
                headers={
                    "Content-Type": "application/json",
                    "Origin": DASHBOARD_URL,
                },
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=3) as response:
                response.read()
            print("Dashboard closed. Managed profiles continue running.")
        return 0
    except (RuntimeError, ValueError, OSError, sqlite3.Error) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
