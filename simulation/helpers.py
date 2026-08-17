#!/usr/bin/env python3
"""
helper functions for the other scripts

needs python >= 3.11 (tomllib)

"""
from __future__ import annotations

import hashlib
import json
import os
import signal
import re
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

CLK_TCK = os.sysconf("SC_CLK_TCK")
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")

SYSTEMS = ("decentralized", "central")

# microseconds
def now_us() -> int:
    return int(time.time() * 1_000_000)


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# hash of a json serializable object for caches and run ids
def stable_key(obj: Any, length: int = 12) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()[:length]


def read_jsonl(path: str | Path) -> Iterator[dict]:
    with open(path) as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


@dataclass
class NodeInfo:
    node_id: int
    path: str
    mode: str                    # decentralized | central
    role: str                    # sink | constraint_node | central | forwarder
    tcp: str                     # conformance listen address
    overlay: str | None
    unix_socket: str
    event_ids: list[str]         # event ids accepted on the unix socket
    constraints: list[dict]
    db_path: str | None
    log_file: str

    @property
    def is_sink(self) -> bool:
        # true only for the dedicated decentralized result sink (node 0) since in the central system the central node also hosts the sink
        return self.role == "sink"


def load_system_configs(config_dir: str | Path) -> list[NodeInfo]:
    nodes: list[NodeInfo] = []
    for path in sorted(Path(config_dir).glob("*-config.toml")):
        with open(path, "rb") as config_file:
            config = tomllib.load(config_file)
        mode = config.get("mode", "decentralized")
        constraints = config.get("constraints", [])
        sink = config.get("result_sink")
        if sink and not constraints and "forwarder" not in config:
            role = "sink"
        elif mode == "central":
            role = "forwarder" if "forwarder" in config else "central"
        else:
            role = "constraint_node"
        if role == "forwarder":
            events = list(config.get("forwarder", {}).get("include_event_ids") or [])
        elif constraints:
            events = sorted({c["event_id"] for c in constraints})
        else:
            events = []
        nodes.append(NodeInfo(
            node_id=int(config.get("node_id", -1)),
            path=str(path),
            mode=mode,
            role=role,
            tcp=str(config.get("tcp_listen_address", "")),
            overlay=(config.get("overlay") or {}).get("listen_address"),
            unix_socket=str(config.get("unix_socket", "")),
            event_ids=events,
            constraints=constraints,
            db_path=(sink or {}).get("db_path"),
            log_file=str(config.get("log_file", "app.log")),
        ))
    return nodes


_HEADER = re.compile(r"^\s*\[\[?([^]]+)\]\]?\s*$")


class Raw(str):
    """a value that is already valid toml (arrays, inline tables)"""


def _toml_value(value: Any) -> str:
    if isinstance(value, Raw):
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(str(value))


# write a partial node config for `gen-config --baseline` from the baseline parametrisation
def write_toml(path: Path, table: dict[str, Any]) -> None:
    def render(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, (list, tuple)):
            return "[" + ", ".join(render(item) for item in value) + "]"
        return json.dumps(str(value))

    scalars = {key: value for key, value in table.items() if not isinstance(value, dict)}
    sections = {key: value for key, value in table.items() if isinstance(value, dict)}
    lines = [f"{key} = {render(value)}" for key, value in scalars.items()]
    for section, values in sections.items():
        lines.append("")
        lines.append(f"[{section}]")
        lines += [f"{key} = {render(value)}" for key, value in values.items()]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def binary_cmd(repo_root: Path, bin_dir: str | None, name: str, use_cargo: bool = False) -> list[str]:
    if not use_cargo and bin_dir:
        candidate = repo_root / bin_dir / name
        if candidate.exists() and os.access(candidate, os.X_OK):
            return [str(candidate)]
    return ["cargo", "run", "--release", "--quiet", "--bin", name, "--"]


# run a command with logging stdout and stderr to a file
def run_logged(cmd: list[str], cwd: Path, out_path: Path | None = None,
               env: dict | None = None, check: bool = True, timeout: float | None = None) -> subprocess.CompletedProcess:
    log(f"  $ {' '.join(str(c) for c in cmd)}")
    full_env = {**os.environ, **(env or {})}
    proc = subprocess.run([str(c) for c in cmd], cwd=str(cwd), env=full_env,
                          capture_output=True, text=True, timeout=timeout)
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(f"$ {' '.join(str(c) for c in cmd)}\n"
                            f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}\n")
    if check and proc.returncode != 0:
        sys.stderr.write(proc.stdout[-4000:] + "\n" + proc.stderr[-4000:] + "\n")
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(str(c) for c in cmd)}")
    return proc


def tcp_ready(address: str, timeout: float = 0.25) -> bool:
    host, _, port = address.rpartition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


# refuse to launch into addresses that are already taken
def assert_addresses_free(nodes: list[NodeInfo]) -> None:
    taken = []
    for node in nodes:
        for address in (node.tcp, node.overlay):
            if address and tcp_ready(address):
                taken.append(f"node {node.node_id} {address}")
    if taken:
        raise RuntimeError(
            f"these addresses are already in use, most likely by node processes of an earlier run that were never stopped: {taken}. "
            "check with `pgrep -a target/release/node` and kill them before retrying")


# mae sure that every metrics file is complete by checking the shutdown marker from the record writer
def assert_records_complete(records_dir: Path) -> list[str]:

    incomplete = []
    for path in sorted(records_dir.glob("node-*.jsonl")):
        last = None
        with open(path, "rb") as records_file:
            for line in records_file:
                if line.strip():
                    last = line
        if last is None:
            incomplete.append(f"{path.name}: empty")
            continue
        try:
            if json.loads(last).get("type") != "writer_shutdown":
                incomplete.append(f"{path.name}: no shutdown marker")
        except json.JSONDecodeError:
            incomplete.append(f"{path.name}: truncated last line")
    return incomplete


# blocks until every node listens on its unix socket and its tcp port and is ready and give up if one of them dies
def wait_until_ready(nodes: list[NodeInfo], procs: dict[int, subprocess.Popen] | None = None, timeout: float = 60.0) -> float:
 
    deadline = time.monotonic() + timeout
    started = time.monotonic()
    while time.monotonic() < deadline:
        if procs:
            dead = {node_id: process.returncode for node_id, process in procs.items()
                    if process.poll() is not None}
            if dead:
                raise RuntimeError(
                    f"node process(es) exited during startup: {dead}")
        missing = [node for node in nodes
                   if not (os.path.exists(node.unix_socket) and tcp_ready(node.tcp)
                           and (node.overlay is None or tcp_ready(node.overlay)))]
        if not missing:
            return time.monotonic() - started
        time.sleep(0.2)
    raise TimeoutError(f"nodes not ready after {timeout}s: {[n.node_id for n in missing]}")


def remove_sockets(nodes: list[NodeInfo]) -> None:
    for node in nodes:
        try:
            os.unlink(node.unix_socket)
        except OSError:
            pass


LAUNCHED_PROCESSES: dict[int, subprocess.Popen] = {}


def register_process(proc: subprocess.Popen) -> None:
    LAUNCHED_PROCESSES[proc.pid] = proc


# signal a process group (nodes are setsid() so they are each in their own process group)
def _signal_process(proc: subprocess.Popen, sig: int) -> None:

    try:
        os.killpg(os.getpgid(proc.pid), sig)
    except (ProcessLookupError, PermissionError):
        # fall back to regular process signal if the group is already gone
        try:
            proc.send_signal(sig)
        except (ProcessLookupError, OSError):
            pass


# send SIGTERM and escalate to SIGKILL if node does not shut down
def stop_processes(procs: dict[int, subprocess.Popen], grace: float = 8.0) -> list[int]:

    for proc in procs.values():
        if proc.poll() is None:
            _signal_process(proc, signal.SIGTERM)
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline and any(p.poll() is None for p in procs.values()):
        time.sleep(0.1)

    killed = []
    for node_id, proc in procs.items():
        if proc.poll() is None:
            killed.append(node_id)
            _signal_process(proc, signal.SIGKILL)
    for proc in procs.values():
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        LAUNCHED_PROCESSES.pop(proc.pid, None)
    return killed


def stop_all_launched(grace: float = 3.0) -> int:

    alive = {pid: proc for pid, proc in LAUNCHED_PROCESSES.items() if proc.poll() is None}
    if alive:
        stop_processes(dict(alive), grace=grace)
    LAUNCHED_PROCESSES.clear()
    return len(alive)


# kills all processes started by the node binary that are still running
def kill_stale_nodes(node_bin: str | Path) -> int:

    pattern = str(node_bin)
    found = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True)
    pids = [int(pid) for pid in found.stdout.split()]
    for pid in pids:
        try:
            os.killpg(os.getpgid(pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
    return len(pids)


# samples /proc periodically and writes it to a jsonl file
class ProcSampler(threading.Thread):

    def __init__(self, targets: dict[int, dict], out_path: Path, interval: float = 0.25):
        super().__init__(daemon=True)
        self.targets = targets          # pid -> (node id, role)
        self.out_path = out_path
        self.interval = interval
        self._shutdown = threading.Event()

    def stop(self) -> None:
        self._shutdown.set()

    @staticmethod
    def _read_proc(pid: int) -> dict | None:
        try:
            with open(f"/proc/{pid}/stat") as fh:
                data = fh.read()
        except OSError:
            return None
        try:
            rest = data[data.rindex(")") + 2:].split()
            return {
                "utime_ticks": int(rest[11]),
                "stime_ticks": int(rest[12]),
                "threads": int(rest[17]),
                "rss_kb": int(rest[21]) * PAGE_SIZE // 1024,
            }
        except (ValueError, IndexError):
            return None

    @staticmethod
    def _read_host() -> dict | None:
        try:
            with open("/proc/stat") as fh:
                parts = fh.readline().split()
        except OSError:
            return None
        values = [int(v) for v in parts[1:]]
        return {"cpu_total_ticks": sum(values), "cpu_idle_ticks": values[3] + (values[4] if len(values) > 4 else 0)}

    def run(self) -> None:
        self.out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.out_path, "w") as fh:
            while not self._shutdown.is_set():
                t = now_us()
                host = self._read_host()
                if host:
                    fh.write(json.dumps({"t_us": t, "kind": "host", **host, "clk_tck": CLK_TCK}) + "\n")
                for pid, meta in self.targets.items():
                    sample = self._read_proc(pid)
                    if sample is None:
                        continue
                    fh.write(json.dumps({"t_us": t, "kind": "proc", "pid": pid, **meta, **sample}) + "\n")
                fh.flush()
                self._shutdown.wait(self.interval)


def sqlite_ro(db_path: str | Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)


# check how many (case, constraint) pairs are in the db
# used to detect when processing has finished
def db_progress(db_path: str | Path) -> tuple[int, int]:
    
    if not Path(db_path).exists():
        return (0, 0)
    try:
        con = sqlite_ro(db_path)
        pairs = con.execute("SELECT COUNT(*) FROM (SELECT DISTINCT case_id, constraint_id FROM results)").fetchone()[0]
        rows = con.execute("SELECT COUNT(*) FROM results").fetchone()[0]
        con.close()
        return (pairs, rows)
    except sqlite3.Error:
        return (0, 0)


def load_expected(path: str | Path) -> dict[tuple[str, str], str]:
    out = {}
    for row in read_jsonl(path):
        out[(row["case_id"], row["constraint_id"])] = row["expected"]
    return out


# add a netem RTT to the tcp messages through the local network stack (define a qdisc)
def netem_apply(delay_ms: float, jitter_ms: float = 0.0, dev: str = "lo", sudo: bool = True) -> None:
    cmd = (["sudo"] if sudo else []) + ["tc", "qdisc", "replace", "dev", dev, "root", "netem",
                                        "delay", f"{delay_ms}ms"]
    if jitter_ms:
        cmd += [f"{jitter_ms}ms", "distribution", "normal"]
    subprocess.run(cmd, check=True)


# remove the netem RTT qdisc
def netem_clear(dev: str = "lo", sudo: bool = True) -> None:
    cmd = (["sudo"] if sudo else []) + ["tc", "qdisc", "del", "dev", dev, "root"]
    subprocess.run(cmd, check=False, capture_output=True)