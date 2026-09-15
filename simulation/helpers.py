#!/usr/bin/env python3
"""
helper functions for the other scripts

needs python >= 3.11 (tomllib)

"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import math
import threading
import time
import fnmatch
import gzip
import tomllib
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

CLK_TCK = os.sysconf("SC_CLK_TCK")
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")

SYSTEMS = ("decentralized", "central")

# microseconds
def now_us() -> int:
    return int(time.time() * 1_000_000)


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# which commit the code of a run was built from
def git_revision(repo: Path) -> dict:
    def git(*args: str) -> str | None:
        try:
            done = subprocess.run(["git", "-C", str(repo), *args],
                                  capture_output=True, text=True, timeout=10)
        except (OSError, subprocess.SubprocessError):
            return None
        return done.stdout.strip() if done.returncode == 0 else None

    commit = git("rev-parse", "HEAD")
    if not commit:
        return {"commit": None}
    return {"commit": commit, "committed_at": git("log", "-1", "--format=%cI")}


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
               env: dict | None = None, check: bool = True, timeout: float | None = None,
               on_start=None) -> subprocess.CompletedProcess:
    log(f"  $ {' '.join(str(c) for c in cmd)}")
    full_env = {**os.environ, **(env or {})}
    popen = subprocess.Popen([str(c) for c in cmd], cwd=str(cwd), env=full_env,
                             stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True)
    if on_start is not None:
        try:
            on_start(popen)
        except Exception as error:
            log(f"  !! on_start hook failed: {error!r}")
    try:
        stdout, stderr = popen.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        popen.kill()
        stdout, stderr = popen.communicate()
        raise
    proc = subprocess.CompletedProcess(cmd, popen.returncode, stdout, stderr)
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(f"$ {' '.join(str(substring) for substring in cmd)}\n"
                            f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}\n")
    if check and proc.returncode != 0:
        sys.stderr.write(proc.stdout[-4000:] + "\n" + proc.stderr[-4000:] + "\n")
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(str(substring) for substring in cmd)}")
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


# make sure that every metrics file is complete by checking the shutdown marker from the record writer
def check_records_complete(records_dir: Path, trailing_record_warning: int = 8) -> list[str]:

    incomplete = []
    records_dir = Path(records_dir)
    if not records_dir.exists():
        return [f"{records_dir}: records directory does not exist"]
    files = sorted(records_dir.glob("**/node-*.jsonl"))
    if not files:
        # metrics disabled
        return []
    for path in files:
        name = f"{path.parent.name}/{path.name}"
        last = None
        marker_seen = False
        records_after_marker = 0
        with open(path, "rb") as records_file:
            for line in records_file:
                if not line.strip():
                    continue
                last = line
                if marker_seen:
                    records_after_marker += 1
                elif b'"writer_shutdown"' in line:
                    marker_seen = True
        if last is None:
            incomplete.append(f"{name}: empty")
            continue
        # check if line is complete (must parse as json)
        try:
            json.loads(last)
        except json.JSONDecodeError:
            incomplete.append(f"{name}: truncated last line")
            continue
        if not marker_seen:
            incomplete.append(f"{name}: no shutdown marker (file is truncated)")
        elif records_after_marker > trailing_record_warning:
            incomplete.append(f"{name}: {records_after_marker} records after the shutdown marker (more than the expected max of {trailing_record_warning}")
    return incomplete


def assert_records_complete(records_dir: Path) -> None:
    incomplete = check_records_complete(records_dir)
    if incomplete:
        raise RuntimeError(f"incomplete metric record files in {records_dir}: {incomplete}")


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


# the shutdown of a node is 'sleep(500ms)' + SHUTDOWN_TIMEOUT (10s - see record_writer.rs)
# the stop grace has to cover that period
DEFAULT_STOP_GRACE_S = 15.0


# cap the cpu resource in a run
# 'cpuset' caps the amount of available cores
# 'quote' caps the amount of cpu-time, but the kernel enforces it by freezing the processes for the rest of its 100ms intervals, which disturbs latency numbers
@dataclass
class CpuCap:
    mode: str = "off"                 # off | cpuset | quota
    scope: str = "system"             # 'system' cap whole system | 'node' cap each node
    cores: float = 0.0                # cpuset: cores per scope unit; quota: cores worth of runtime
    cpus: str | None = None           # explicit cpuset list (overrides cores)
    tooling_cores: int = 2            # cpus held back for the tooling to run on
    isolate_tooling: bool = True
    exclude_roles: tuple[str, ...] = ("sink",)

    @property
    def enabled(self) -> bool:
        return self.mode != "off"


def _cpu_list(cpus: list[int]) -> str:
    return ",".join(str(cpu) for cpu in cpus)


def parse_cpu_cap(params: dict, defaults: dict) -> CpuCap:
    def pick(key, fallback):
        return params.get(key, defaults.get(key, fallback))

    mode = str(pick("cpu_cap_mode", "off")).lower()
    if mode not in ("off", "cpuset", "quota"):
        raise ValueError(f"cpu_cap_mode must be off | cpuset | quota, got {mode!r}")
    scope = str(pick("cpu_cap_scope", "system")).lower()
    if scope not in ("system", "node"):
        raise ValueError(f"cpu_cap_scope must be system | node, got {scope!r}")
    cap = CpuCap(mode=mode, scope=scope,
                 cores=float(pick("cpu_cap_cores", 0) or 0),
                 cpus=pick("cpu_cap_cpus", None),
                 tooling_cores=int(pick("cpu_cap_tooling_cores", 2)),
                 isolate_tooling=bool(pick("cpu_cap_isolate_tooling", True)),
                 exclude_roles=tuple(pick("cpu_cap_exclude_roles", ["sink"])))
    if cap.enabled and not cap.cores and not cap.cpus:
        raise ValueError("cpu_cap_mode is set but neither cpu_cap_cores nor cpu_cap_cpus was given")
    if cap.mode == "cpuset" and cap.cpus is None and cap.cores != int(cap.cores):
        raise ValueError(f"cpu_cap_mode=cpuset needs a whole number of cores, got {cap.cores}. "
                         "use cpu_cap_mode=quota for fractional caps (but read the throttling note)")
    return cap


# ensure that the configured cpu cap is possible 
def ensure_cpu_cap_possible(cap: CpuCap, node_count: int) -> dict:
    if not cap.enabled:
        return {"mode": "off"}
    available = sorted(os.sched_getaffinity(0))
    info = {"mode": cap.mode, "scope": cap.scope, "cores": cap.cores, "cpus": cap.cpus,
            "isolate_tooling": cap.isolate_tooling, "exclude_roles": list(cap.exclude_roles),
            "host_cpus_available": len(available)}

    if cap.mode == "cpuset":
        if shutil.which("taskset") is None:
            raise RuntimeError("cpu_cap_mode=cpuset needs taskset (util-linux), which is not on PATH")
        system_cpus = _resolve_system_cpus(cap, available)
        info["system_cpus"] = _cpu_list(system_cpus)
        if cap.scope == "system":
            info["system_cpus"] = _cpu_list(system_cpus)
        else:
            per_node = int(cap.cores)
            info["node_cpu_pool"] = _cpu_list(system_cpus)
            info["cpus_per_node"] = per_node
            info["tooling_cores_reserved"] = cap.tooling_cores if cap.isolate_tooling else 0
            if per_node * node_count > len(system_cpus):
                info["note"] = (f"{node_count} nodes x {per_node} cpu(s) exceeds the "
                                f"{len(system_cpus)} in the pool, so node cpusets overlap - a per "
                                f"node cap is a ceiling, not a reservation")

    else:
        _assert_cgroup_writable()
        info["quota_pct_per_unit"] = int(round(cap.cores * 100))
        info["cgroup_root"] = str(CGROUP_ROOT)
        info["warning"] = ("quota is enforced by freezing the cgroup for the rest of each 100ms "
                           "period once its budget is spent - do not use this for latency experiments")
    return info


DEFAULT_WORKER_POLICY = "per-cpu"


# how many tokio worker threads a node process should build its runtime with
def _usable_cpu_count(cap: "CpuCap") -> int:
    available = sorted(os.sched_getaffinity(0))
    if not cap.enabled:
        return len(available)
    if cap.mode == "cpuset":
        if cap.scope == "node":
            return max(1, int(cap.cores))
        return len(_resolve_system_cpus(cap, available))
    return max(1, int(round(cap.cores))) # quota


# resolved per node so roles excluded from the cap (the sink) keep the binary's own default
def worker_threads_for_node(cap: "CpuCap", policy, node_count: int, role: str = "") -> int | None:
    if policy is None:
        policy = DEFAULT_WORKER_POLICY
    if isinstance(policy, str):
        policy = policy.strip().lower()
        policy = {"cap": "per-cpu", "percpu": "per-cpu"}.get(policy, policy)  # accepted aliases

    if policy in ("auto", "off"):
        return None
    if isinstance(policy, (int, float)) or (isinstance(policy, str) and policy.isdigit()):
        return max(1, int(policy))
    if policy not in ("per-cpu", "share"):
        raise ValueError(f"worker_threads must be auto | per-cpu | share | <int>, got {policy!r}")

    if cap.enabled and role in cap.exclude_roles:
        return None

    usable = _usable_cpu_count(cap)
    if policy == "per-cpu":
        return max(1, usable)
    if cap.enabled and cap.scope == "node":
        return max(1, usable)  # each node already owns its own cpus, nothing to share
    return max(1, usable // max(1, node_count))


# summary for the run metadata (timeline["cpu_cap"])
def worker_threads_info(cap: "CpuCap", policy, node_count: int) -> dict:
    resolved = worker_threads_for_node(cap, policy, node_count)
    info = {"worker_threads_policy": policy if policy is not None else DEFAULT_WORKER_POLICY,
            "worker_threads_per_node": resolved,
            "worker_threads_nodes": node_count,
            "usable_cpus_per_node": _usable_cpu_count(cap),
            "cpu_cap_mode": cap.mode}
    if resolved is not None:
        info["worker_threads_total"] = resolved * node_count
    if cap.enabled and cap.mode == "quota" and policy in (None, "auto", "off"):
        info["worker_threads_warning"] = (
            "quota mode with worker_threads=auto: available_parallelism() does not see cpu.max, so "
            "each node builds a full host sized runtime and is then throttled. use \"cap\" or \"share\"")
    return info


CGROUP_ROOT = Path("/sys/fs/cgroup")
CGROUP_PREFIX = "cc-sim"


def _assert_cgroup_writable() -> None:
    if not (CGROUP_ROOT / "cgroup.controllers").exists():
        raise RuntimeError("cpu_cap_mode=quota needs cgroup v2 at /sys/fs/cgroup. "
                           "use cpu_cap_mode=cpuset, which needs no privileges")
    try:
        controllers = (CGROUP_ROOT / "cgroup.subtree_control").read_text().split()
    except OSError as error:
        raise RuntimeError(f"cannot read cgroup.subtree_control ({error}). use cpu_cap_mode=cpuset")
    if "cpu" not in controllers:
        raise RuntimeError("the cpu controller is not enabled in /sys/fs/cgroup/cgroup.subtree_control. "
                           "enable it as root (echo +cpu > ...) or use cpu_cap_mode=cpuset")
    probe = CGROUP_ROOT / f"{CGROUP_PREFIX}-probe"
    try:
        probe.mkdir(exist_ok=True)
        (probe / "cpu.max").write_text("max 100000")
    except OSError as error:
        raise RuntimeError(f"cannot create a cgroup under {CGROUP_ROOT} ({error}). run as root, "
                           "delegate the subtree, or use cpu_cap_mode=cpuset")
    finally:
        try:
            probe.rmdir()
        except OSError:
            pass


def make_quota_cgroup(name: str, cores: float, period_us: int = 100_000) -> Path:
    path = CGROUP_ROOT / f"{CGROUP_PREFIX}-{name}"
    path.mkdir(exist_ok=True)
    (path / "cpu.max").write_text(f"{int(round(cores * period_us))} {period_us}")
    return path


def remove_quota_cgroups() -> None:
    for path in CGROUP_ROOT.glob(f"{CGROUP_PREFIX}-*"):
        try:
            path.rmdir()
        except OSError:
            pass


# returned as preexec_fn so the child writes its own pid, so keep the Popen.pid as the nodes pid
def join_cgroup_preexec(path: Path):
    def _join():
        with open(path / "cgroup.procs", "w") as cgroup_file:
            cgroup_file.write(str(os.getpid()))
    return _join


# the pool of cpus the capped deployment may use
def _resolve_system_cpus(cap: CpuCap, available: list[int]) -> list[int]:
    if cap.cpus:
        wanted = _expand_cpu_list(cap.cpus)
        missing = [cpu for cpu in wanted if cpu not in available]
        if missing:
            raise RuntimeError(f"cpu_cap_cpus asks for cpus {missing} which this process may not use")
        return wanted

    if cap.scope == "node":
        reserve = cap.tooling_cores if cap.isolate_tooling else 0
        pool = available[:len(available) - reserve] if reserve else list(available)
        if len(pool) < int(cap.cores):
            raise RuntimeError(f"cpu_cap_scope=node wants {int(cap.cores)} cpu(s) per node but only "
                               f"{len(pool)} are left after reserving {reserve} for the sender and "
                               f"sink. lower cpu_cap_cores or cpu_cap_tooling_cores")
        return pool

    count = min(int(cap.cores), len(available))
    if count < 1:
        raise RuntimeError("cpu_cap resolves to zero cpus")
    if cap.isolate_tooling and count >= len(available):
        raise RuntimeError(f"the cap wants all {len(available)} cpus, leaving none for the sender "
                           f"and sink. lower cpu_cap_cores or set cpu_cap_isolate_tooling=false")
    return available[:count]


def _tooling_cpus(cap: CpuCap, available: list[int], system_cpus: list[int]) -> list[int]:
    if not cap.isolate_tooling:
        return available
    rest = [cpu for cpu in available if cpu not in set(system_cpus)]
    return rest or available


def _expand_cpu_list(spec: str) -> list[int]:
    cpus: list[int] = []
    for part in str(spec).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            cpus += list(range(int(lo), int(hi) + 1))
        else:
            cpus.append(int(part))
    return sorted(set(cpus))


# command prefix that applies the cap to one process
def cpu_cap_prefix(cap: CpuCap, role: str, node_index: int = 0, tooling: bool = False) -> list[str]:
    if not cap.enabled:
        return []
    available = sorted(os.sched_getaffinity(0))
    system_cpus = _resolve_system_cpus(cap, available)

    if tooling or role in cap.exclude_roles:
        if cap.mode == "cpuset" and cap.isolate_tooling:
            return ["taskset", "-c", _cpu_list(_tooling_cpus(cap, available, system_cpus))]
        return []

    if cap.mode == "cpuset":
        if cap.scope == "system":
            return ["taskset", "-c", _cpu_list(system_cpus)]
        per_node = max(1, int(cap.cores))
        start = (node_index * per_node) % len(system_cpus)
        block = [system_cpus[(start + offset) % len(system_cpus)] for offset in range(per_node)]
        return ["taskset", "-c", _cpu_list(sorted(set(block)))]

    # quota adds no command prefix
    return []


# cgroup join hook for quota mode; None for cpuset and for uncapped
def cpu_cap_preexec(cap: CpuCap, system: str, role: str, node_index: int, tooling: bool = False):
    if not cap.enabled or cap.mode != "quota" or tooling or role in cap.exclude_roles:
        return None
    name = system if cap.scope == "system" else f"{system}-node{node_index}"
    return join_cgroup_preexec(make_quota_cgroup(name, cap.cores))


# send SIGTERM and escalate to SIGKILL if node does not shut down
def stop_processes(procs: dict[int, subprocess.Popen], grace: float = DEFAULT_STOP_GRACE_S) -> list[int]:

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


# separate \proc stats of conformance checking system and simulation tooling (mainly send_event_stream.py)
GROUP_SYSTEM = "system"
GROUP_TOOLING = "tooling"


class ProcSampler(threading.Thread):

    def __init__(self, targets: dict[int, dict], out_path: Path, interval: float = 0.25, per_thread: bool = True):
        super().__init__(daemon=True)
        self._lock = threading.Lock()
        self.targets = {pid: {"group": GROUP_SYSTEM, **meta} for pid, meta in targets.items()}
        self.out_path = out_path
        self.interval = interval
        self.per_thread = per_thread
        self._shutdown = threading.Event()

    # register an extra process (e.g. the send script) after the sampler already started
    def add_target(self, pid: int, group: str = GROUP_TOOLING, **meta) -> None:
        with self._lock:
            self.targets[pid] = {"group": group, **meta}

    # stop sampling a process
    def remove_target(self, pid: int) -> None:
        with self._lock:
            self.targets.pop(pid, None)

    # return a snapshot of the current list of sampling targets
    def _snapshot_targets(self) -> list[tuple[int, dict]]:
        with self._lock:
            return list(self.targets.items())

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
            rest = data[data.rindex(") ") + 2:].split()
            return {
                "utime_ticks": int(rest[11]),
                "stime_ticks": int(rest[12]),
                "threads": int(rest[17]),
                "rss_kb": int(rest[21]) * PAGE_SIZE // 1024,
            }
        except (ValueError, IndexError):
            return None

    # per thread cpu from /proc/<pid>/task/<tid>/stat because a node is a multi threaded tokio runtime and 
    # the bottleneck is the single serialized event_q_processor task -> looking for busiest thread
    @staticmethod
    def _read_threads(pid: int) -> list[dict] | None:
        try:
            thread_ids = os.listdir(f"/proc/{pid}/task")
        except OSError:
            return None
        threads = []
        for thread_id in thread_ids:
            try:
                with open(f"/proc/{pid}/task/{thread_id}/stat") as thread_stat_file:
                    data = thread_stat_file.read()
            except OSError:
                continue
            try:
                name = data[data.index("(") + 1:data.rindex(")")]
                rest = data[data.rindex(") ") + 2:].split()
                threads.append({"tid": int(thread_id), "name": name,
                                "utime_ticks": int(rest[11]), "stime_ticks": int(rest[12])})
            except (ValueError, IndexError):
                continue
        return threads or None

    @staticmethod
    def _read_host() -> dict | None:
        try:
            with open("/proc/stat") as process_stat_file:
                parts = process_stat_file.readline().split()
        except OSError:
            return None
        values = [int(value) for value in parts[1:]]
        return {"cpu_total_ticks": sum(values), "cpu_idle_ticks": values[3] + (values[4] if len(values) > 4 else 0)}

    def run(self) -> None:
        self.out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.out_path, "w") as resource_sampling_file:
            while not self._shutdown.is_set():
                time = now_us()
                host = self._read_host()
                if host:
                    resource_sampling_file.write(json.dumps({"t_us": time, "kind": "host", **host, "clk_tck": CLK_TCK}) + "\n")
                for pid, meta in self._snapshot_targets():
                    sample = self._read_proc(pid)
                    if sample is None:
                        continue
                    resource_sampling_file.write(json.dumps({"t_us": time, "kind": "proc", "pid": pid, **meta, **sample}) + "\n")
                    if not self.per_thread:
                        continue
                    threads = self._read_threads(pid)
                    if not threads:
                        continue
                    # one line per process (not per thread - would be too much sampling overhead)
                    resource_sampling_file.write(json.dumps({
                        "t_us": time, "kind": "threads", "pid": pid,
                        "node_id": meta.get("node_id"), "role": meta.get("role"),
                        "group": meta.get("group"), "thread_count": len(threads),
                        "threads": [[thread["tid"], thread["name"],
                                     thread["utime_ticks"] + thread["stime_ticks"]]
                                    for thread in threads],
                    }) + "\n")
                resource_sampling_file.flush()
                self._shutdown.wait(self.interval)


def sqlite_ro(db_path: str | Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)


# create a checkpoint to fold the WAL into the database
def checkpoint_wal(db_path: str | Path) -> dict:

    db_path = Path(db_path)
    if not db_path.exists():
        return {"checkpointed": False, "reason": "no database file"}
    wal_filepath = Path(str(db_path) + "-wal")
    wal_bytes = wal_filepath.stat().st_size if wal_filepath.exists() else 0
    try:
        db_connection = sqlite3.connect(str(db_path), timeout=30)
        try:
            # 'TRUNCATE' blocks until every frame is in the main db and then empties the wal
            busy, moved, checkpointed = db_connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            rows = db_connection.execute("SELECT COUNT(*) FROM results").fetchone()[0]
        finally:
            db_connection.close()
        return {"checkpointed": busy == 0, "wal_bytes_before": wal_bytes,
                "wal_frames": moved, "wal_frames_checkpointed": checkpointed, "rows_after": rows}
    except sqlite3.Error as error:
        return {"checkpointed": False, "wal_bytes_before": wal_bytes, "error": str(error)}


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
    cmd = (["sudo", "-n"] if sudo else []) + ["tc", "qdisc", "replace", "dev", dev, "root", "netem", "delay", f"{delay_ms}ms"]
    if jitter_ms:
        cmd += [f"{jitter_ms}ms", "distribution", "normal"]
    # add increase limit of netem packet queue to not drop packets under load here
    cmd += ["limit", "100000"]
    subprocess.run(cmd, check=True)


# remove the netem RTT qdisc
def netem_clear(dev: str = "lo", sudo: bool = True) -> None:
    cmd = (["sudo", "-n"] if sudo else []) + ["tc", "qdisc", "del", "dev", dev, "root"]
    subprocess.run(cmd, check=False, capture_output=True)


def percentiles(values: list[float], points=(50, 95, 99)) -> dict:
    if not values:
        return {"n": 0}
    ordered = sorted(values)
    percentile_batch = {"n": len(ordered), "min": ordered[0], "max": ordered[-1], "mean": sum(ordered) / len(ordered)}
    for point in points:
        # nearest rank, so every reported value is an observed one
        index = min(len(ordered) - 1, max(0, math.ceil(point / 100 * len(ordered)) - 1))
        percentile_batch[f"p{point}"] = ordered[index]
    return percentile_batch


def quantile(values: list[float], quantil_level: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(quantil_level * len(ordered)) - 1))
    return ordered[index]


# least squares slope
# used to tell a queue that grows without bound from one that just jitters
def slope_per_second(points: list[tuple[float, float]]) -> float:
    if len(points) < 3:
        return 0.0
    num_points = len(points)
    mean_time = sum(time for time, _ in points) / num_points
    mean_peak_value = sum(peak_value for _, peak_value in points) / num_points
    denominator = sum((time - mean_time) ** 2 for time, _ in points)
    if denominator == 0:
        return 0.0
    return sum((time - mean_time) * (peak_value - mean_peak_value) for time, peak_value in points) / denominator * 1e6


# deterministic fraction float from a case id for reproducible case subsampling
def case_id_hash_fraction(case_id: str) -> float:
    digest = hashlib.blake2b(case_id.encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big") / float(1 << 64)


# discover runs from run specs or observations
def discover_runs(results: Path, pattern: str | None, filename: str) -> list[Path]:
    runs = [path.parent for path in sorted(results.rglob(filename))]
    if pattern:
        runs = [run for run in runs if fnmatch.fnmatch(run.parent.name, pattern)]
    return runs


# lightweight progress reporting for the long running evaluation scripts
class Progress:

    def __init__(self, total: int, label: str = "", stream=sys.stderr, enabled: bool = True, quiet_interval_s: float = 5.0):
        self.total = max(0, int(total))
        self.label = label
        self.stream = stream
        self.enabled = enabled and self.total > 0
        self.quiet_interval_s = quiet_interval_s
        self.done = 0
        self.started_at = time.monotonic()
        self.last_emit = 0.0
        self.interactive = bool(getattr(stream, "isatty", lambda: False)())
        self._line_open = False
        self._last_detail = ""
        self._partial = 0.0     # progress inside the unit currently in flight

    @staticmethod
    def _duration(seconds: float) -> str:
        seconds = int(max(0, seconds))
        if seconds < 60:
            return f"{seconds}s"
        if seconds < 3600:
            return f"{seconds // 60}m{seconds % 60:02d}s"
        return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"

    def _render(self, detail: str, force: bool) -> None:
        if not self.enabled:
            return
        self._last_detail = detail
        now = time.monotonic()
        if not force and not self.interactive and (now - self.last_emit) < self.quiet_interval_s:
            return
        self.last_emit = now

        elapsed = now - self.started_at
        width = len(str(self.total))
        effective = min(float(self.total), self.done + self._partial)
        percent = (effective / self.total * 100.0) if self.total else 0.0
        text = f"[{self.done:>{width}}/{self.total}] {percent:3.0f}%"
        if self.label:
            text += f" {self.label}"
        if detail:
            text += f" {detail}"
        text += f"  ({self._duration(elapsed)} elapsed"
        if effective > 0 and self.total - effective > 0:
            remaining = elapsed / effective * (self.total - effective)
            text += f", ~{self._duration(remaining)} left"
        text += ")"

        if self.interactive:
            self.stream.write("\r\033[2K" + text[:200])
            self._line_open = True
        else:
            self.stream.write(text + "\n")
        self.stream.flush()

    # one unit of work finished
    def step(self, detail: str = "") -> None:
        self.done += 1
        self._partial = 0.0
        self._render(detail, force=(self.done >= self.total))

    def note(self, detail: str = "", fraction: float | None = None) -> None:
        if fraction is not None:
            self._partial = min(1.0, max(0.0, float(fraction)))
        self._render(detail, force=False)

    def finish(self, detail: str = "") -> None:
        if not self.enabled:
            return
        if self.interactive and self._line_open:
            self.stream.write("\r\033[2K")
            self._line_open = False
        elapsed = time.monotonic() - self.started_at
        message = f"{self.label or 'done'}: {self.done}/{self.total} in {self._duration(elapsed)}"
        if detail:
            message += f" {detail}"
        self.stream.write(message + "\n")
        self.stream.flush()

    # print a normal line without destroying the progress line
    def log(self, message: str) -> None:
        if self.interactive and self._line_open:
            self.stream.write("\r\033[2K")
            self.stream.flush()
            self._line_open = False
        print(message, flush=True)
        self._render(self._last_detail, force=True)


BK_TYPES = ("set", "mult", "seq")
DISCLOSURE_INDEX_NAME = "disclosure_index.json.gz"
DISCLOSURE_INDEX_SCHEMA = 1


def load_ground_truth(manifest: Path) -> dict:

    cases: dict[str, dict] = {}
    with open(manifest) as manifest_file:
        for line in manifest_file:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            case_id, event_id = row.get("case_id"), row.get("event_id")
            if case_id is None or event_id is None:
                continue
            case = cases.setdefault(case_id, {"activities": Counter(), "trace": []})
            case["activities"][event_id] += 1
            case["trace"].append(event_id)
    return {case_id: {"activities": dict(case["activities"]), "trace": tuple(case["trace"])}
            for case_id, case in cases.items()}



# the disclosure measures of rafiei and van der aalst (2021) evaluated against the ground truth log
#
# the log is collapsed to its variants (unique traces) with their frequencies
# a candidate background knowledge selects a subset of variants and the multiset of matching traces comes from their frequencies
# the variants containing a given activity are precomputed as bitmasks, so resolving a candidate intersects a few integers instead of scanning the log
class DisclosureIndex:

    def __init__(self, ground_truth_event_log: dict | None = None, variants=None, counts=None):
        if variants is not None:
            # rebuilt from a prebuilt index file
            self.variants = [tuple(variant) for variant in variants]
            self.counts = list(counts or [])
        else:
            variant_counts = Counter(case["trace"] for case in (ground_truth_event_log or {}).values())
            self.variants = list(variant_counts.keys())
            self.counts = [variant_counts[variant] for variant in self.variants]
        self.total_traces = sum(self.counts)

        self.activity_mask: dict[str, int] = {} # activity -> bitmask of variants containing it
        self.count_mask: dict[tuple[str, int], int] = {} # (activity, k) -> variants with >= k of it
        for index, variant in enumerate(self.variants):
            bit = 1 << index
            for activity, occurrences in Counter(variant).items():
                self.activity_mask[activity] = self.activity_mask.get(activity, 0) | bit
                for k in range(1, occurrences + 1):
                    self.count_mask[(activity, k)] = self.count_mask.get((activity, k), 0) | bit

        self._mask_cache: dict[tuple, int] = {}
        self._stats_cache: dict[int, tuple[int, float]] = {}

    # bitmask of the variants matching a candidate background knowledge
    def _mask(self, bk_type: str, candidate) -> int:
        cache_key = (bk_type, candidate)
        cached = self._mask_cache.get(cache_key)
        if cached is not None:
            return cached

        mask = (1 << len(self.variants)) - 1
        if bk_type == "set":
            for activity in candidate:
                mask &= self.activity_mask.get(activity, 0)
                if not mask:
                    break
        elif bk_type == "mult":
            for activity, occurrences in candidate:
                mask &= self.count_mask.get((activity, occurrences), 0)
                if not mask:
                    break
        else: # seq: prune with the set mask first, then verify the subsequence directly
            for activity in set(candidate):
                mask &= self.activity_mask.get(activity, 0)
                if not mask:
                    break
            for index in list(_bits(mask)):
                if not _is_subsequence(candidate, self.variants[index]):
                    mask &= ~(1 << index)

        self._mask_cache[cache_key] = mask
        return mask

    # |proj(x)| and ent(proj(x)) / max_ent(proj(x)) for one candidate
    def stats(self, bk_type: str, candidate) -> tuple[int, float]:
        mask = self._mask(bk_type, candidate)
        cached = self._stats_cache.get(mask)
        if cached is not None:
            return cached

        matching = [self.counts[index] for index in _bits(mask)]
        total = sum(matching)
        if total == 0:
            result = (0, 0.0)
        elif len(matching) <= 1:
            # a single matching variant: no uncertainty about the trace is left (entropy ratio = 0, trace disclosure contribution = 1)
            result = (total, 0.0)
        else:
            entropy = 0.0
            for count in matching:
                frequency = count / total
                entropy -= frequency * math.log2(frequency)
            ratio = min(1.0, max(0.0, entropy / math.log2(len(matching))))
            result = (total, ratio)

        self._stats_cache[mask] = result
        return result


# iterate the set bit positions of a bitmask
def _bits(mask: int):
    while mask:
        lowest = mask & -mask
        yield lowest.bit_length() - 1
        mask ^= lowest


def _is_subsequence(candidate: tuple, trace: tuple) -> bool:
    position = 0
    for activity in trace:
        if position < len(candidate) and activity == candidate[position]:
            position += 1
    return position == len(candidate)


# the background knowledge that a set of named activities gives about one case, per type
def build_candidates(named: set[str], truth_case: dict, bk_types) -> dict:
    candidates = {}
    if "set" in bk_types:
        candidates["set"] = frozenset(named)
    if "mult" in bk_types:
        candidates["mult"] = tuple(sorted((activity, count)
                                          for activity, count in truth_case["activities"].items()
                                          if activity in named))
    if "seq" in bk_types:
        candidates["seq"] = tuple(activity for activity in truth_case["trace"] if activity in named)
    return candidates


def log_baseline_disclosure(ground_truth_event_log: dict, index, bk_types) -> dict:
    distinct_traces = (len(index.variants) if index is not None
                       else len({case["trace"] for case in ground_truth_event_log.values()}))
    activities_per_case = [float(len(case["activities"])) for case in ground_truth_event_log.values()]
    events_per_case = [float(len(case["trace"])) for case in ground_truth_event_log.values()]
    baseline = {"cases": len(ground_truth_event_log),
                "events": int(sum(events_per_case)),
                "distinct_traces": distinct_traces,
                "trace_uniqueness": (distinct_traces / len(ground_truth_event_log))
                                    if ground_truth_event_log else 0.0,
                "distinct_activities": len({activity for case in ground_truth_event_log.values()
                                            for activity in case["activities"]}),
                "background_knowledge_size_max": percentiles(activities_per_case, points=(5, 50, 95)),
                "events_per_case": percentiles(events_per_case, points=(5, 50, 95))}
    if index is None:
        return baseline
    for bk_type in bk_types:
        uniqueness, entropy_ratios = [], []
        for case in ground_truth_event_log.values():
            candidate = build_candidates(set(case["activities"]), case, (bk_type,))[bk_type]
            matching_traces, entropy_ratio = index.stats(bk_type, candidate)
            if matching_traces:
                uniqueness.append(1.0 / matching_traces)
                entropy_ratios.append(entropy_ratio)
        baseline[f"case_disclosure_{bk_type}"] = (sum(uniqueness) / len(uniqueness)) if uniqueness else 0.0
        baseline[f"trace_disclosure_{bk_type}"] = (max(0.0, 1.0 - sum(entropy_ratios) / len(entropy_ratios)) if entropy_ratios else 0.0)
    return baseline


def load_disclosure_index(path: Path) -> dict | None:
    try:
        with gzip.open(path, "rt") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if (payload.get("schema_version") or 0) < 1 or "cases" not in payload:
        return None
    return payload


def index_path_for(manifest: Path) -> Path:
    return manifest.parent / DISCLOSURE_INDEX_NAME


# how unique the traces are decides whether the variant index is worth building at all: on a
# synthetic stream nearly every trace is its own variant, so the disclosure measures describe the
# generator rather than the deployment. counting distinct traces is cheap and needs no index
def trace_uniqueness(log: dict) -> float:
    if not log:
        return 0.0
    return len({case["trace"] for case in log.values()}) / len(log)


# build the index next to the manifest unless it is already there. returns (path, action) where
# action is one of "existing", "built", "skipped"
def ensure_index(manifest: Path, bk_types: tuple[str, ...] | None = ("set", "mult", "seq"),
                 force: bool = False, uniqueness_max: float = 0.9,
                 out_path: Path | None = None, log=print) -> tuple[Path | None, str]:
    if manifest is None or not Path(manifest).exists():
        return None, "skipped"
    manifest = Path(manifest)
    out_path = out_path or index_path_for(manifest)
    if out_path.exists() and not force:
        return out_path, "existing"

    parsed = load_ground_truth(manifest)
    if not parsed:
        if log:
            log(f"    disclosure index: {manifest} parsed to no cases, skipped")
        return None, "skipped"

    wanted = tuple(bk_types or ())
    uniqueness = trace_uniqueness(parsed)
    # bk_types=None means "decide from the log itself"
    if bk_types is None:
        wanted = () if uniqueness > uniqueness_max else ("set", "mult", "seq")
    if wanted and uniqueness > uniqueness_max and log:
        log(f"    disclosure index: trace uniqueness {uniqueness:.3f} is above {uniqueness_max:.2f}, "
            f"so the disclosure measures mostly describe the stream generator here")

    payload = build_payload(manifest, parsed, wanted)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out_path, "wt") as handle:
        json.dump(payload, handle)
    if log:
        baseline = payload["baselines"]["|".join(wanted)]
        log(f"    disclosure index: wrote {out_path.name} "
            f"({out_path.stat().st_size / 1e6:.1f} MB, {baseline['cases']} cases, "
            f"trace uniqueness {baseline['trace_uniqueness']:.3f}"
            + (f", variant index for {', '.join(wanted)})" if wanted else ", log only)"))
    return out_path, "built"


def build_payload(manifest: Path, log: dict, bk_types: tuple[str, ...]) -> dict:
    index = DisclosureIndex(log) if bk_types else None
    baseline = log_baseline_disclosure(log, index, bk_types)

    payload = {
        "schema_version": DISCLOSURE_INDEX_SCHEMA,
        "manifest": str(manifest.resolve()),
        "manifest_bytes": manifest.stat().st_size,
        "bk_types": list(bk_types),
        # the compacted log
        "cases": {case_id: {"activities": case["activities"], "trace": list(case["trace"])}
                  for case_id, case in log.items()},
        # the log baseline under the bk types this file was built with
        "baselines": {"|".join(bk_types): baseline},
    }
    if index is not None:
        # activity_mask and count_mask are rebuilt from the variants in a single pass, so only the variants and their frequencies are stored
        payload["variants"] = [list(variant) for variant in index.variants]
        payload["variant_counts"] = list(index.counts)
    return payload


def _cpu_count_from_list(cpu_list) -> int | None:
    if not cpu_list:
        return None
    total = 0
    for chunk in str(cpu_list).split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "-" in chunk:
            low, _, high = chunk.partition("-")
            try:
                total += int(high) - int(low) + 1
            except ValueError:
                return None
        else:
            total += 1
    return total or None


def cpu_ceiling(cap_info: dict, point: dict) -> dict:
    out = {
        "cpu_cap_mode": (cap_info or {}).get("mode", "off"),
        "cpu_cap_scope": (cap_info or {}).get("scope"),
        "cpu_cap_cores": (cap_info or {}).get("cores"),
        "cpu_cap_note": (cap_info or {}).get("note"),
        "cpu_ceiling_basis": None,      # which number is compared against the ceiling
        "cpu_ceiling_label": None,
        "cpu_ceiling_pct": None, 
        "cpu_used_pct": None,
        "cpu_of_ceiling": None,         # cpu_used_pct / cpu_ceiling_pct
    }
    mode = str((cap_info or {}).get("mode") or "off").lower()

    if mode == "off":
        out["cpu_ceiling_label"] = "uncapped - no cpu ceiling, so cpu cannot signal saturation here"
        return out

    scope = str((cap_info or {}).get("scope") or "system").lower()
    cores = (cap_info or {}).get("cores")
    if scope == "node":
        per_node = (cap_info or {}).get("cpus_per_node") or cores
        ceiling = float(per_node) * 100.0 if per_node else None
        used = point.get("sys_cpu_max_pct")
        out["cpu_ceiling_basis"] = "process"
        out["cpu_ceiling_label"] = (f"{mode}/node: busiest node process vs its "
                                    f"{_fmt_cores(per_node)} cpu(s)")
    else:
        cpus = _cpu_count_from_list((cap_info or {}).get("system_cpus")) or cores
        ceiling = float(cpus) * 100.0 if cpus else None
        used = point.get("sys_cpu_sum_pct")
        out["cpu_ceiling_basis"] = "system"
        out["cpu_ceiling_label"] = (f"{mode}/system: all node processes vs the shared "
                                    f"{_fmt_cores(cpus)} cpu(s)")
    out["cpu_ceiling_pct"] = ceiling
    out["cpu_used_pct"] = used
    if ceiling and used is not None:
        out["cpu_of_ceiling"] = used / ceiling
    return out


def _fmt_cores(value) -> str:
    if value is None:
        return "?"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return str(int(number)) if number == int(number) else f"{number:g}"


# saturation decision code
DEFAULT_SATURATION_THRESHOLDS = {
    "queue_fill": 0.25,       # standing p95 depth as a fraction of the queues capacity
    "queue_growth": 0.05,     # depth trend over the window as a fraction of capacity
    "cpu_pct": 80.0,          # % of the cpu ceiling that counts as exhausted with a rate shortfall
    "cpu_pinned": 95.0,       # % of the ceiling that counts as exhausted on its own
    "achieved_ratio": 0.98,   # delivered / offered rate below which the shortfall flags cpu
    "stall_ms": 1000.0,       # a single socket write blocked at least this long
    "only_max_node": False,   # name only the worst node instead of every node over the line
}


def saturation_thresholds(**overrides) -> dict:
    merged = dict(DEFAULT_SATURATION_THRESHOLDS)
    merged.update({key: value for key, value in overrides.items() if value is not None})
    return merged


def _fmt(value, spec="", dash="-"):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return dash
    try:
        return format(value, spec)
    except (TypeError, ValueError):
        return str(value)


# returns {"saturated", "reasons", "signals", "cpu_fired", "queue_fired"}
def saturation_signals(facts: dict, thresholds: dict | None = None) -> dict:
    thresholds = thresholds or DEFAULT_SATURATION_THRESHOLDS
    reasons: list[str] = []
    signals: list[dict] = []

    # LOSS (message/event drops)
    if facts.get("input_drops"):
        detail = ", ".join(f"{count}x {reason}" for reason, count
                           in (facts.get("input_drop_reasons") or {}).items())
        by_node = facts.get("input_drops_by_node") or {}
        where = ("" if thresholds.get("only_max_node") or not by_node else
                 " on " + ", ".join(f"{node} ({count})" for node, count in by_node.items()))
        signals.append({"kind": "LOSS", "where": ", ".join(by_node) or "unattributed",
                        "value": facts["input_drops"]})
        split = f"{facts.get('event_drops') or 0} event(s), {facts.get('message_drops') or 0} message(s)"
        reasons.append(f"{facts['input_drops']} input record(s) dropped{where} - {split} "
                       f"({detail or 'reason not recorded'}) - work was destroyed, and a late "
                       f"revision cannot repair a message or event that never arrived")

    # BACKLOG (queue fills)
    standing_fill = facts.get("queue_fill_p95_max")
    standing_queue = facts.get("queue_fill_p95_max_queue")
    standing_node = facts.get("queue_fill_p95_max_node")
    standing_slope = facts.get("queue_slope_at_p95_max")
    standing_growth = facts.get("queue_growth_at_p95_max")
    standing_p99 = facts.get("queue_fill_p99_at_p95_max")
    queue_fired = False
    if (standing_fill is not None and standing_fill > thresholds["queue_fill"]
            and standing_growth is not None and standing_growth > thresholds["queue_growth"]):
        queue_fired = True
        crossed = [entry for entry in (facts.get("queue_profile") or [])
                   if (entry.get("p95_fill") or 0) > thresholds["queue_fill"]
                   and (entry.get("growth") or 0) > thresholds["queue_growth"]]
        signals.append({"kind": "BACKLOG", "where": f"{standing_queue} on {standing_node}",
                        "value": standing_fill, "growth": standing_growth, "crossed": len(crossed)})
        if thresholds.get("only_max_node") or not crossed:
            reasons.append(f"{standing_queue} on {standing_node} is a standing backlog: p95 depth "
                           f"{standing_fill:.0%} of capacity (> {thresholds['queue_fill']:.0%}) and "
                           f"the depth trend rises by {standing_growth:.0%} of capacity across the "
                           f"send window ({_fmt(standing_slope, '+.1f')} slots/s)"
                           + (f", p99 {standing_p99:.0%}" if standing_p99 is not None else "")
                           + (f"  [{len(crossed)} queues over the threshold in total, hidden]"
                              if len(crossed) > 1 else ""))
        else:
            reasons.append(f"{len(crossed)} queue(s) over the backlog threshold "
                           f"(p95 > {thresholds['queue_fill']:.0%} of capacity AND trend "
                           f"> {thresholds['queue_growth']:.0%}), deepest first:")
            for entry in crossed:
                reasons.append(f"     {entry['queue']} on {entry['node']}: p95 "
                               f"{entry['p95_fill']:.0%} p99 {_fmt(entry.get('p99_fill'), '.0%')} of "
                               f"{entry['capacity']} slots, trend {entry['growth']:+.0%} of capacity "
                               f"over the window ({_fmt(entry.get('slope_per_s'), '+.1f')} slots/s)")

    # CPU CAP
    of_cap = facts.get("cpu_of_ceiling")
    ratio = facts.get("achieved_ratio")
    where = (f"{facts.get('sys_cpu_max_node')} at {_fmt(facts.get('cpu_used_pct'), '.0f')}%"
             if facts.get("cpu_ceiling_basis") == "process"
             else f"the node processes together at {_fmt(facts.get('cpu_used_pct'), '.0f')}%")
    cap_text = (f"{where} of the {_fmt(facts.get('cpu_ceiling_pct'), '.0f')}% cpu cap "
                f"({_fmt(of_cap, '.0%')} of it)")
    cpu_fired = False
    if of_cap is not None:
        if of_cap >= thresholds["cpu_pinned"] / 100.0:
            cpu_fired = True
            reasons.append(f"{cap_text} - pinned at the cap "
                           f"(>= {thresholds['cpu_pinned']:.0f}%), there is no cpu left to go faster")
        elif (of_cap >= thresholds["cpu_pct"] / 100.0 and ratio is not None
              and ratio < thresholds["achieved_ratio"]):
            cpu_fired = True
            reasons.append(f"{cap_text} AND only {ratio:.3f} of the target rate was delivered "
                           f"- the cpu it was given ran out and the shortfall shows it "
                           f"(cap >= {thresholds['cpu_pct']:.0f}%, "
                           f"rate < {thresholds['achieved_ratio']:.2f})")
        if cpu_fired:
            signals.append({"kind": "CPU CAP", "where": facts.get("sys_cpu_max_node") or "system",
                            "value": of_cap})
        # under a per node cap the same threshold applies to every node, so name all of them
        if cpu_fired and facts.get("cpu_ceiling_basis") == "process" and facts.get("cpu_ceiling_pct"):
            ceiling = facts["cpu_ceiling_pct"]
            hot = [entry for entry in (facts.get("sys_cpu_profile") or [])
                   if entry["cpu_pct"] / ceiling >= thresholds["cpu_pct"] / 100.0]
            if len(hot) > 1:
                if thresholds.get("only_max_node"):
                    reasons.append(f"     {len(hot)} nodes are at or above "
                                   f"{thresholds['cpu_pct']:.0f}% of the cap, hidden")
                else:
                    reasons.append(f"     {len(hot)} nodes are at or above "
                                   f"{thresholds['cpu_pct']:.0f}% of the cap:")
                    for entry in hot:
                        reasons.append(f"       {entry['node']}: {entry['cpu_pct']:.0f}% "
                                       f"({entry['cpu_pct'] / ceiling:.0%} of the cap), "
                                       f"{_fmt(entry.get('rss_mb'), '.0f')} MB")

    # STALL (blocked sends)
    max_send = facts.get("max_send_ms")
    if max_send is not None and max_send >= thresholds["stall_ms"]:
        signals.append({"kind": "STALL", "where": "sender socket", "value": max_send / 1000})
        reasons.append(f"one socket write blocked for {max_send / 1000:.1f}s "
                       f"(>= {thresholds['stall_ms'] / 1000:.1f}s) - a node stopped reading its "
                       f"socket altogether")

    return {"saturated": bool(reasons), "reasons": reasons, "signals": signals,
            "cpu_fired": cpu_fired, "queue_fired": queue_fired}


# the worst (node, queue) out of a per node queue profile, in the shape saturation_signals expects
def worst_queue(queue_profile: list[dict]) -> dict:
    if not queue_profile:
        return {}
    deepest = max(queue_profile, key=lambda entry: entry.get("p95_fill") or 0)
    return {"queue_fill_p95_max": deepest.get("p95_fill"),
            "queue_fill_p95_max_queue": deepest.get("queue"),
            "queue_fill_p95_max_node": deepest.get("node"),
            "queue_slope_at_p95_max": deepest.get("slope_per_s"),
            "queue_growth_at_p95_max": deepest.get("growth"),
            "queue_fill_p99_at_p95_max": deepest.get("p99_fill")}