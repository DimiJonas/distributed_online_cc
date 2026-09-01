#!/usr/bin/env python3
"""
!!! needs at least python 3.11 for tomllib !!!

send a generated event stream to the right node sockets in a local deployment and log every delivery

reads the configs to work out routing
reads the manifest from gen-events.rs
delivers to each nodes unix socket

routing is by event_id in both systems:
    decentralized : an event goes to the node whose constraints watch its event_id
    central     : an event goes to the forwarder whose include_event_ids lists it

usage:
  send_stream.py --config-dir run/decentralized --stream events_gen/manifest.jsonl
  send_stream.py --config-dir run/decentralized --print-routes
  send_stream.py --config-dir run/decentralized --stream events_gen/manifest.jsonl --rate 50
  send_stream.py --config-dir run/decentralized --stream events_gen/manifest.jsonl --rate 200 --burst 10 --jitter 0.1
  send_stream.py --config-dir run/decentralized --stream events_gen/manifest.jsonl --rate 8000 --wire framed

"""
import argparse
import atexit
import json
import os
import random
import signal
import socket
import struct
import sys
import time
import tomllib
from glob import glob
from pathlib import Path


# ---- read configs -> nodes + routing ----

def load_nodes(config_dir):
    nodes = []
    for path in sorted(glob(os.path.join(config_dir, "*-config.toml"))):
        with open(path, "rb") as config_file:
            config = tomllib.load(config_file)
        mode = config.get("mode", "decentralized")
        constraints = [
            {
                "role": constraint["constraint_role"],
                "constraint_id": constraint["constraint_id"],
                "event_id": constraint["event_id"],
            }
            for constraint in config.get("constraints", [])
        ]
        if mode == "central" and "forwarder" in config:
            kind = "forwarder"
        elif mode == "central":
            kind = "central"
        elif constraints:
            kind = "constraint-node"
        else:
            kind = "sink"
        if kind == "forwarder":
            events = list(config.get("forwarder", {}).get("include_event_ids") or [])
        elif kind == "constraint-node":
            events = list({constraint["event_id"] for constraint in constraints})
        else:
            events = []
        nodes.append({
            "node_id": int(config.get("node_id", -1)),
            "kind": kind, "mode": mode,
            "tcp": str(config.get("tcp_listen_address", "")),
            "socket": str(config.get("unix_socket", "")),
            "constraints": constraints,
            "events": events,
        })
    return nodes


def system_kind(nodes):
    modes = {node["mode"] for node in nodes}
    return "central" if "central" in modes else "decentralized"


def build_routing(nodes):
    # event_id -> nodes that accept it on their unix socket
    by_event = {}
    for node in nodes:
        for event_id in node["events"]:
            by_event.setdefault(event_id, []).append(node)
    return by_event


# ---- pacing ----

# paces the send loop at a fixed rate
#   interval <= 0 means no pacing, send as fast as possible
class Pacer:

    def __init__(self, interval, burst=1, jitter=0.0):
        self.interval = max(0.0, interval)
        self.burst = max(1, burst)
        self.jitter = max(0.0, jitter)
        self.count = 0
        self.starttime = time.monotonic()
        self.next_t = self.starttime
        self.max_lag = 0.0
        self.late = 0
        self.resyncs = 0
        self.wall_start_us = int(time.time() * 1_000_000)
        self.wall_end_us = None
        self.endtime = None

    # record stream end time
    def finish(self):
        if self.wall_end_us is None:
            self.wall_end_us = int(time.time() * 1_000_000)
            self.endtime = time.monotonic()
        return self.wall_end_us

    def _elapsed(self):
        end = self.endtime if self.endtime is not None else time.monotonic()
        return end - self.starttime

    def wait(self):
        # block until the next event is due, return seconds since start
        now = time.monotonic()
        if self.interval > 0 and self.count and self.count % self.burst == 0:
            self.next_t += self.interval * self.burst
            target = self.next_t
            if self.jitter:
                target += random.uniform(-self.jitter, self.jitter) * self.interval
            delay = target - now
            if delay > 0:
                time.sleep(delay)
                now = time.monotonic()
            else:
                # behind schedule: the sends cannot keep up with the rate
                lag = -delay
                self.max_lag = max(self.max_lag, lag)
                self.late += 1
                if lag > self.interval * self.burst:
                    # too far behind to catch up, restart the schedule from here
                    self.next_t = now
                    self.resyncs += 1
        self.count += 1
        return now - self.starttime

    @staticmethod
    def _rate_str(rate):
        if rate >= 1000:
            return f"{rate:.0f}"
        if rate >= 1:
            return f"{rate:.1f}".rstrip("0").rstrip(".")
        return f"{rate:.3g}"

    # pacer summary as json
    def json_summary(self):
        elapsed = self._elapsed()
        return {
            "target_rate": (1.0 / self.interval) if self.interval > 0 else None,
            "effective_rate": (self.count / elapsed) if elapsed > 0 else 0.0,
            "elapsed_s": elapsed,
            "events_paced": self.count,
            "burst": self.burst,
            "burst_seconds": (self.interval * self.burst) if self.interval > 0 else None,
            "jitter": self.jitter,
            "late": self.late,
            "max_lag_ms": self.max_lag * 1000.0,
            "resyncs": self.resyncs,
            "pacer_start_us": self.wall_start_us,
            "pacer_end_us": self.wall_end_us,
        }

    def summary(self):
        elapsed = self._elapsed()
        eff = self.count / elapsed if elapsed > 0 else 0.0

        if self.interval <= 0:
            return f"rate=unlimited {self._rate_str(eff)}/s (effective) elapsed={elapsed:.2f}s"
        summary = (f"rate={self._rate_str(1.0 / self.interval)}/s (target) "
             f"{self._rate_str(eff)}/s (effective) elapsed={elapsed:.2f}s")
        if self.late:
            summary += f"  late={self.late} max_lag={self.max_lag * 1000:.1f}ms"
        if self.resyncs:
            summary += f" resyncs={self.resyncs}"
        return summary


# ---- progress ----

def count_events(path):
    # number of non-blank lines in the stream, for the x/N denominator
    num_events = 0
    with open(path) as manifest:
        for line in manifest:
            if line.strip():
                num_events += 1
    return num_events


def fmt_duration(sec):
    if sec < 60:
        return f"{sec:.0f}s"
    if sec < 3600:
        return f"{int(sec // 60)}m{int(sec % 60):02d}s"
    return f"{int(sec // 3600)}h{int(sec % 3600 // 60):02d}m"


def progress_enabled(when, stream=sys.stderr):
    if when == "off":
        return False
    if when == "on":
        return True
    # auto: only when someone is watching, so calls from another script stay quiet
    return hasattr(stream, "isatty") and stream.isatty()


# the progress of the send in one line in stderr and redrawn in place
class Progress:
    BAR = 24

    def __init__(self, total, enabled=True, stream=sys.stderr):
        self.total = total
        self.stream = stream
        self.enabled = enabled
        self.tty = hasattr(stream, "isatty") and stream.isatty()
        self.every = 0.1 if self.tty else 2.0
        self.send_count = 0
        self.starttime = time.monotonic()
        self.last = -1.0
        self.width = 0
        self.drawn = 0

    def _render(self, counts):
        elapsed = time.monotonic() - self.starttime
        rate = self.send_count / elapsed if elapsed > 0 else 0.0
        if self.total:
            done = int(self.BAR * self.send_count / self.total)
            bar = "#" * done + "." * (self.BAR - done)
            progress_bar = (f"event {self.send_count}/{self.total} [{bar}] "
                 f"{100.0 * self.send_count / self.total:3.0f}%")
        else:
            progress_bar = f"event {self.send_count}"
        if elapsed >= 0.5:
            progress_bar += f"  {Pacer._rate_str(rate)}/s"
            if self.total and self.send_count < self.total:
                progress_bar += f"  eta {fmt_duration((self.total - self.send_count) / rate)}"
        extra = " ".join(f"{k}={v}" for k, v in counts.items() if v)
        if extra:
            progress_bar += f"  {extra}"
        return progress_bar

    def _draw(self, progress_bar):
        if self.tty:
            pad = " " * max(0, self.width - len(progress_bar))
            self.width = len(progress_bar)
            self.stream.write("\r" + progress_bar + pad)
        else:
            self.stream.write(progress_bar + "\n")
        self.stream.flush()

    def advance(self, counts):
        self.send_count += 1
        if not self.enabled:
            return
        now = time.monotonic()
        if now - self.last < self.every and self.send_count != self.total:
            return
        self.last = now
        self.drawn = self.send_count
        self._draw(self._render(counts))

    def note(self, msg):
        # write a message without leaving half a progress line behind it
        if self.tty and self.width:
            self.stream.write("\r" + " " * self.width + "\r")
            self.width = 0
        self.stream.write(msg if msg.endswith("\n") else msg + "\n")
        self.stream.flush()

    def close(self, counts):
        if not self.enabled or not self.send_count:
            return
        if self.drawn != self.send_count:
            self._draw(self._render(counts))
        if self.tty:
            self.stream.write("\n")
            self.width = 0
        self.stream.flush()


# ---- send log ----

# in memory jsonl writer
class SendLog:

    def __init__(self, path, flush_every=0, max_buffer=0):
        self.path = path
        self.flush_every = max(0, flush_every)
        self.max_buffer = max(0, max_buffer)
        self.buffer = []
        self.written = 0
        self.spills = 0
        self.peak_buffer = 0
        self.spill_us_total = 0
        self.max_spill_us = 0
        self.spill_positions: list[int] = []
        self.send_log_file = open(path, "w")
        self.closed = False

    @property
    def deferred(self):
        return self.flush_every == 0

    def write(self, record):
        self.buffer.append(record)
        if len(self.buffer) > self.peak_buffer:
            self.peak_buffer = len(self.buffer)
        if self.flush_every and len(self.buffer) >= self.flush_every:
            self.flush()
        elif self.max_buffer and len(self.buffer) >= self.max_buffer:
            self.spills += 1
            self.spill_positions.append(self.written + len(self.buffer))
            started = time.perf_counter()
            self.flush()
            spill_us = int((time.perf_counter() - started) * 1e6)
            self.spill_us_total += spill_us
            self.max_spill_us = max(self.max_spill_us, spill_us)

    def flush(self, chunk=8192):
        if not self.buffer or self.closed:
            return
        for start in range(0, len(self.buffer), chunk):
            part = self.buffer[start:start + chunk]
            self.send_log_file.write("\n".join(json.dumps(record) for record in part) + "\n")
        self.send_log_file.flush()
        self.written += len(self.buffer)
        self.buffer.clear()

    def close(self):
        if self.closed:
            return
        try:
            self.flush()
            os.fsync(self.send_log_file.fileno())
        finally:
            self.closed = True
            self.send_log_file.close()


# ---- delivery ----

# add a 4 byte big endian length prefix to a payload to be able to send multiple XES files in one unix socket connection
# this matches the 'framed' mode that the event_input_handler expects
def frame(payload):
    return struct.pack(">I", len(payload)) + payload


# keep one persistent unix socket connection for a node
class NodeConnection:

    def __init__(self, sock_path, connect_timeout, send_timeout):
        self.sock_path = sock_path
        self.connect_timeout = connect_timeout
        self.send_timeout = send_timeout
        self.sock = None
        self.reconnects = 0

    def _connect(self):
        new_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        new_socket.settimeout(self.connect_timeout)
        new_socket.connect(self.sock_path)
        # this timeout is needed so if a node stops reading events for a while that can be recorded as pacer lag
        new_socket.settimeout(self.send_timeout)
        self.sock = new_socket

    def send(self, payload):
        if self.sock is None:
            self._connect()
        framed_payload = frame(payload)
        try:
            self.sock.sendall(framed_payload)
        except (BrokenPipeError, ConnectionResetError):
            self.close()
            self.reconnects += 1
            self._connect()
            self.sock.sendall(framed_payload)

    def close(self):
        if self.sock is not None:
            try:
                self.sock.close()
            finally:
                self.sock = None


# contains the unix socket connections (one persistent connection per node)
class ConnectionPool:

    def __init__(self, wire_mode, connect_timeout=5.0, send_timeout=60.0):
        # see src/conformance_checking/event_input_handler.rs
        self.wire_mode = wire_mode
        self.connect_timeout = connect_timeout
        self.send_timeout = send_timeout
        self.connections = {}

    # create the persistent connections up front
    def connect_all(self, sock_paths):
        if self.wire_mode != "framed":
            return
        for path in sock_paths:
            connection = NodeConnection(path, self.connect_timeout, self.send_timeout)
            connection._connect()
            self.connections[path] = connection

    def send(self, sock_path, payload):
        if self.wire_mode == "framed":
            persistent_connection = self.connections.get(sock_path)
            if persistent_connection is None:
                persistent_connection = NodeConnection(sock_path, self.connect_timeout, self.send_timeout)
                self.connections[sock_path] = persistent_connection
            persistent_connection.send(payload)
        else:
            oneshot_connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            oneshot_connection.settimeout(self.send_timeout)
            oneshot_connection.connect(sock_path)
            try:
                oneshot_connection.sendall(payload)
            finally:
                oneshot_connection.close()

    def reconnects(self):
        return sum(connection.reconnects for connection in self.connections.values())

    def close_all(self):
        for connection in self.connections.values():
            connection.close()
        self.connections.clear()


# ---- stream loading ----

# read the payloads into memory before sending starts
# returns (seq, case_id, event_id, payload) tuples
def load_stream(stream_path, stream_dir, preload, max_preload_bytes, notify):

    entries = []
    total_bytes = 0
    with open(stream_path) as manifest_file:
        for line in manifest_file:
            line = line.strip()
            if not line:
                continue
            manifest_event = json.loads(line)
            event_file_path = None
            if manifest_event.get("file"):
                event_file_path = manifest_event["file"] if os.path.isabs(manifest_event["file"]) \
                    else os.path.join(stream_dir, manifest_event["file"])
            if preload:
                payload = manifest_event["xes"].encode() if manifest_event.get("xes") is not None \
                    else (open(event_file_path, "rb").read() if event_file_path else b"")
                total_bytes += len(payload)
                if total_bytes > max_preload_bytes:
                    notify(f"preload exceeded {max_preload_bytes} bytes, falling back to reading payloads during the run")
                    return load_stream(stream_path, stream_dir, False, max_preload_bytes, notify)
            else:
                payload = ("inline", manifest_event["xes"]) if manifest_event.get("xes") is not None \
                    else ("file", event_file_path)
            entries.append((manifest_event["seq"], manifest_event["case_id"], manifest_event["event_id"], payload))

    if preload:
        notify(f"preloaded {len(entries)} payloads ({total_bytes / 1e6:.1f} MB)")
    return entries, preload


def resolve_payload(payload, preloaded):
    if preloaded:
        return payload
    kind, value = payload
    if kind == "inline":
        return value.encode()
    return open(value, "rb").read() if value else b""


# ---- main ----

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config-dir", required=True)
    ap.add_argument("--stream", help="manifest.jsonl from gen-events (or an embedded-xes .jsonl stream)")
    ap.add_argument("--rate", type=float, default=0.0, help="events per second (0 = as fast as possible)")
    ap.add_argument("--interval", type=float, default=None, help="seconds between events (alternative to --rate)")
    ap.add_argument("--burst", type=int, default=1, help="release this many events at once, then wait (default 1)")
    ap.add_argument("--burst-ms", type=float, default=None,
                    help="derive --burst from the rate so that one burst covers this many milliseconds (burst = round(rate * burst_ms / 1000)). holding the burst DURATION fixed keeps the "
                         "shape of the arrival process identical across a rate sweep, which a fixed --burst does not: burst=32 is a 4ms burst at 8000/s but a 0.64ms burst at 50000/s. it also keeps the "
                         "inter burst sleep above the ~1ms granularity of time.sleep(). overrides --burst")
    ap.add_argument("--jitter", type=float, default=0.0, help="random jitter as a fraction of the interval, e.g. 0.1")
    ap.add_argument("--pace-dry-run", action="store_true", help="also apply pacing during --dry-run")
    ap.add_argument("--dry-run", action="store_true", help="resolve + log routing without sending")
    ap.add_argument("--print-routes", action="store_true", help="print event_id -> node/socket and exit")
    ap.add_argument("--progress", choices=["auto", "on", "off"], default="auto",
                    help="show send progress on stderr (auto = only when stderr is a terminal)")
    ap.add_argument("--wire", choices=["framed", "raw"], default="framed",
                    help="framed = one persistent length-prefixed connection per node (default), raw = one connection per event delimited by EOF")
    ap.add_argument("--preload", choices=["on", "off"], default="on", help="read every xes payload into memory before the pacing loop (default on)")
    ap.add_argument("--max-preload-bytes", type=int, default=2 * 1024 ** 3, help="give up on preloading above this many bytes (default 2 GiB)")
    ap.add_argument("--log-flush-every", type=int, default=0,
                    help="write the send log to disk every N records. 0 (the default) keeps the whole log in memory and writes it once at the end")
    ap.add_argument("--log-max-buffer", type=int, default=0,
                    help="safety valve for --log-flush-every 0: write to disk if the in memory log exceeds this many records. 0 (the default) = no limit. a spill happens INSIDE the pacing loop and "
                         "serialising the whole buffer at once stalls it for seconds, so only set this if memory is actually tight (roughly 1 GB per million events)")
    ap.add_argument("--send-timeout", type=float, default=60.0,
                    help="socket timeout for a single send. long on purpose: blocking here is backpressure, not an error (default 60s)")
    ap.add_argument("--log", default="send_log.jsonl")
    ap.add_argument("--json-summary", default=None, help="write the pacer/send counters to this path as json (for the evaluation scripts)")
    args = ap.parse_args()

    nodes = load_nodes(args.config_dir)
    kind = system_kind(nodes)

    by_event = build_routing(nodes)

    if args.print_routes:
        print(f"system: {kind}")
        print("event routing:")
        for event_id in sorted(by_event):
            where = "  ".join(f"node {node['node_id']} ({node['socket']})" for node in by_event[event_id])
            print(f"  {event_id:<40} -> {where}")
        return

    if not args.stream:
        sys.exit("need --stream manifest.jsonl (or --print-routes)")
    stream_dir = os.path.dirname(os.path.abspath(args.stream))

    if args.interval is not None and args.rate:
        ap.error("use either --rate or --interval, not both")
    if args.interval is not None:
        interval = args.interval
    elif args.rate > 0:
        interval = 1.0 / args.rate
    else:
        interval = 0.0
    if args.dry_run and not args.pace_dry_run:
        interval = 0.0

    # --burst-ms fixes the burst duration instead of the burst size
    burst = max(1, args.burst)
    burst_source = "--burst"
    if args.burst_ms is not None:
        if args.burst_ms <= 0:
            ap.error("--burst-ms has to be > 0")
        if interval <= 0:
            print("--burst-ms needs a rate (--rate/--interval), ignoring it", file=sys.stderr)
        else:
            burst = max(1, round((1.0 / interval) * args.burst_ms / 1000.0))
            burst_source = f"--burst-ms {args.burst_ms:g}"

    show_progress = progress_enabled(args.progress)
    total = count_events(args.stream) if show_progress else 0
    prog = Progress(total, enabled=show_progress)

    entries, preloaded = load_stream(
        args.stream, stream_dir,
        preload=(args.preload == "on"),
        max_preload_bytes=args.max_preload_bytes,
        notify=prog.note,
    )

    log = SendLog(args.log, flush_every=args.log_flush_every, max_buffer=args.log_max_buffer)
    pool = ConnectionPool(args.wire, send_timeout=args.send_timeout)

    def _terminate(signum, _frame):
        raise SystemExit(128 + signum)

    for sig in (signal.SIGTERM, signal.SIGHUP):
        try:
            signal.signal(sig, _terminate)
        except (ValueError, AttributeError):
            pass
    atexit.register(log.close)
    atexit.register(pool.close_all)

    counts = {"sent": 0, "dry-run": 0, "no_route": 0, "error": 0}
    interrupted = False
    slow_sends = 0
    max_send_us = 0

    if not args.dry_run:
        try:
            pool.connect_all(sorted({node["socket"] for nodes in by_event.values() for node in nodes}))
        except OSError as e:
            sys.exit(f"cannot connect to a node socket: {e}\n"
                     f"are the nodes running? check --config-dir {args.config_dir}")

    pacer = Pacer(interval, burst=burst, jitter=args.jitter)
    if interval > 0:
        prog.note(f"pacing at {1.0 / interval:.0f}/s in bursts of {pacer.burst} "
                  f"({interval * pacer.burst * 1000:.2f}ms per burst, from {burst_source})")

    try:
        for seq, case_id, event_id, raw_payload in entries:
            payload = resolve_payload(raw_payload, preloaded)

            # wait for this events slot
            send_slot = pacer.wait()

            target_nodes = by_event.get(event_id, [])

            if not target_nodes:
                counts["no_route"] += 1
                log.write({"seq": seq, "case_id": case_id, "event_id": event_id, "node_id": None, "socket": None, "status": "no_route"})
                prog.note(f"no route for '{event_id}' (seq {seq})")
                prog.advance(counts)
                continue

            for node in target_nodes:
                status = "dry-run"
                send_us = 0
                if not args.dry_run:
                    started = time.perf_counter()
                    try:
                        pool.send(node["socket"], payload)
                        status = "sent"
                    except OSError as e:
                        status = f"error:{e}"
                    send_us = int((time.perf_counter() - started) * 1e6)
                    if send_us > 1000:
                        slow_sends += 1
                    max_send_us = max(max_send_us, send_us)
                counts["sent" if status == "sent" else
                       "dry-run" if status == "dry-run" else "error"] += 1
                log.write({"seq": seq, "case_id": case_id, "event_id": event_id,
                           "node_id": node["node_id"], "socket": node["socket"],
                           "bytes": len(payload), "status": status,
                           "send_us": send_us,
                           "t": round(send_slot, 6)})

            prog.advance(counts)

    except KeyboardInterrupt:
        interrupted = True
        prog.note("interrupted, flushing the send log")
    except SystemExit:
        interrupted = True
        prog.note("terminated, flushing the send log")
        raise
    finally:
        pacer.finish()
        pacer_summary = pacer.summary()
        pacer_stats = pacer.json_summary()
        prog.close(counts)
        log.close()
        pool.close_all()

        if args.json_summary:
            summary_payload = {
                "schema_version": 1,
                "system": kind,
                "interrupted": interrupted,
                "dry_run": bool(args.dry_run),
                "wire": args.wire,
                "preloaded": bool(preloaded),
                "stream": str(args.stream) if args.stream else None,
                "counts": dict(counts),
                "pacer": pacer_stats,
                "sends": {"blocked_over_1ms": slow_sends, "max_send_us": max_send_us, "reconnects": pool.reconnects()},
                "send_log": {"path": str(args.log), "lines": log.written,
                             "deferred": log.deferred, "spills": log.spills,
                             "peak_buffer": log.peak_buffer,
                             "spill_positions": log.spill_positions,
                             "spill_us_total": log.spill_us_total,
                             "max_spill_us": log.max_spill_us},
            }
            try:
                summary_path = Path(args.json_summary)
                summary_path.parent.mkdir(parents=True, exist_ok=True)
                summary_path.write_text(json.dumps(summary_payload, indent=2))
            except OSError as error:
                print(f"could not write --json-summary to {args.json_summary}: {error}", file=sys.stderr)

        print(f"system={kind}  sent={counts['sent']} dry-run={counts['dry-run']} "
              f"no_route={counts['no_route']} error={counts['error']}"
              + ("  INTERRUPTED" if interrupted else ""))
        print(pacer_summary)
        if not args.dry_run:
            print(f"wire={args.wire} preload={'on' if preloaded else 'off'} "
                  f"blocked_sends(>1ms)={slow_sends} max_send={max_send_us / 1000:.1f}ms "
                  f"reconnects={pool.reconnects()}")
        log_mode = "deferred" if log.deferred else f"streamed every {log.flush_every}"
        spilled = (f", {log.spills} SPILL(S) mid stream costing {log.spill_us_total / 1000:.0f}ms "
                   f"(worst {log.max_spill_us / 1000:.0f}ms) at event(s) "
                   f"{', '.join(str(position) for position in log.spill_positions[:5])}"
                   f"{' ...' if len(log.spill_positions) > 5 else ''}") if log.spills else ""
        print(f"log -> {args.log} ({log.written} lines, peak buffer {log.peak_buffer}, {log_mode}{spilled})")
        if log.spills:
            print("  !! a spill stalls the pacing loop and shows up as pacer lag / a fake throughput dip - raise --log-max-buffer (0 = no limit) or stream the log with --log-flush-every", file=sys.stderr)

    if interrupted:
        sys.exit(130)


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        try:
            sys.stdout.close()
        except Exception:
            pass