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


def worker_threads_for_node(cap: "CpuCap", policy, capped_nodes: int, role: str = "") -> int | None:
    if policy is None:
        policy = "cap"
    if isinstance(policy, str):
        policy = policy.strip().lower()

    if policy == "auto" or policy == "off":
        return None
    if isinstance(policy, (int, float)) or (isinstance(policy, str) and policy.isdigit()):
        return max(1, int(policy))
    if policy not in ("cap", "share"):
        raise ValueError(f"worker_threads must be auto | cap | share | <int>, got {policy!r}")

    if cap.enabled and role in cap.exclude_roles:
        return None

    usable = _usable_cpu_count(cap)
    if policy == "cap":
        return max(1, usable)
    if cap.enabled and cap.scope == "node":
        return max(1, usable)
    return max(1, usable // max(1, capped_nodes))


# summary for the run metadata (timeline["cpu_cap"])
def worker_threads_info(cap: "CpuCap", policy, capped_nodes: int) -> dict:
    resolved = worker_threads_for_node(cap, policy, capped_nodes)
    info = {"worker_threads_policy": policy if policy is not None else "cap",
            "worker_threads_per_node": resolved,
            "capped_nodes": capped_nodes,
            "usable_cpus_per_node": _usable_cpu_count(cap)}
    if resolved is not None:
        info["worker_threads_total"] = resolved * capped_nodes
    if cap.enabled and cap.mode == "quota" and (policy in (None, "auto", "off")):
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