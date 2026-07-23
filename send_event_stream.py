#!/usr/bin/env python3
"""
!!! needs at least python 3.11 for tomllib !!!

send a generated event stream to the right node sockets in a local deployment and log every delivery 


reads the configs to work out routing
reads the manifest from gen-events.rs
delivers to each nodes unix socket

    distributed : each event goes to the nodes whose constraints watch its event_id
        a split sensor event that carries a constraint_id goes to the one node holding that exact constraint
    central     : every forwarder accepts any event -> round-robin the forwarders

events are delivered to each nodes unix socket

usage:
  send_stream.py --config-dir run/distributed --stream events_gen/manifest.jsonl
  send_stream.py --config-dir run/distributed --print-routes
  send_stream.py --config-dir run/distributed --stream events_gen/manifest.jsonl --rate 50
  send_stream.py --config-dir run/distributed --stream events_gen/manifest.jsonl --rate 200 --burst 10 --jitter 0.1

"""
import argparse
import json
import os
import random
import socket
import subprocess
import sys
import time
import tomllib
from glob import glob
from itertools import cycle


# ---- read configs -> nodes + routing ----

def load_nodes(config_dir):
    nodes = []
    for path in sorted(glob(os.path.join(config_dir, "*-config.toml"))):
        with open(path, "rb") as fh:
            cfg = tomllib.load(fh)
        mode = cfg.get("mode", "distributed")
        cons = [
            {
                "role": c["constraint_role"],
                "constraint_id": c["constraint_id"],
                "event_id": c["event_id"],
            }
            for c in cfg.get("constraints", [])
        ]
        if mode == "central" and "forwarder" in cfg:
            kind = "forwarder"
        elif mode == "central":
            kind = "central"
        elif cons:
            kind = "constraint-node"
        else:
            kind = "sink"
        nodes.append({
            "node_id": int(cfg.get("node_id", -1)),
            "kind": kind, "mode": mode,
            "tcp": str(cfg.get("tcp_listen_address", "")),
            "socket": str(cfg.get("unix_socket", "")),
            "constraints": cons,
        })
    return nodes


def system_kind(nodes):
    modes = {n["mode"] for n in nodes}
    return "central" if "central" in modes else "distributed"


def build_routing(nodes):
    """(event_id -> nodes), ((event_id,constraint_id) -> nodes), forwarders."""
    by_event, by_constraint = {}, {}
    for n in nodes:
        seen_e, seen_c = set(), set()
        for c in n["constraints"]:
            if c["event_id"] not in seen_e:
                seen_e.add(c["event_id"])
                by_event.setdefault(c["event_id"], []).append(n)
            key = (c["event_id"], c["constraint_id"])
            if key not in seen_c:
                seen_c.add(key)
                by_constraint.setdefault(key, []).append(n)
    fwd = [n for n in nodes if n["kind"] == "forwarder"]
    return by_event, by_constraint, fwd


# ---- pacing ----
 
class Pacer:
    """paces the send loop at a fixed rate without drifting
 
    interval <= 0 : no pacing, send as fast as possible
    burst n       : release n events back to back, then wait n * interval
    jitter f      : add uniform noise of +/- f * interval to each wake-up
 
    the schedule is absolute (next_t += interval), not "sleep(interval) after
    each send", so the time spent building payloads and writing to sockets does
    not accumulate into a growing drift
    """
 
    def __init__(self, interval, burst=1, jitter=0.0):
        self.interval = max(0.0, interval)
        self.burst = max(1, burst)
        self.jitter = max(0.0, jitter)
        self.n = 0
        self.t0 = time.monotonic()
        self.next_t = self.t0
        self.max_lag = 0.0
        self.late = 0
        self.resyncs = 0
 
    def wait(self):
        # block until the next event is due, return seconds since start
        now = time.monotonic()
        if self.interval > 0 and self.n and self.n % self.burst == 0:
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
        self.n += 1
        return now - self.t0
 
    def summary(self):
        if self.interval <= 0:
            return "rate=unlimited"
        elapsed = time.monotonic() - self.t0
        eff = self.n / elapsed if elapsed > 0 else 0.0
        s = (f"rate={1.0 / self.interval:.3g}/s (target) "
             f"{eff:.3g}/s (effective) elapsed={elapsed:.2f}s")
        if self.late:
            s += f"  late={self.late} max_lag={self.max_lag * 1000:.1f}ms"
        if self.resyncs:
            s += f" resyncs={self.resyncs}"
        return s


# ---- delivery ----

def deliver_event(sock_path, payload):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(5.0)
    s.connect(sock_path)
    try:
        s.sendall(payload)
    finally:
        s.close()


# ---- main ----

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config-dir", required=True)
    ap.add_argument("--stream", help="manifest.jsonl from gen-events (or an embedded-xes .jsonl stream)")
    ap.add_argument("--mode", choices=["auto", "distributed", "central"], default="auto")
    ap.add_argument("--rate", type=float, default=0.0, help="events per second (0 = as fast as possible)")
    ap.add_argument("--interval", type=float, default=None, help="seconds between events (alternative to --rate)")
    ap.add_argument("--burst", type=int, default=1, help="release this many events at once, then wait (default 1)")
    ap.add_argument("--jitter", type=float, default=0.0, help="random jitter as a fraction of the interval, e.g. 0.1")
    ap.add_argument("--pace-dry-run", action="store_true", help="also apply pacing during --dry-run")
    ap.add_argument("--dry-run", action="store_true", help="resolve + log routing without sending")
    ap.add_argument("--print-routes", action="store_true", help="print event_id -> node/socket and exit")
    ap.add_argument("--log", default="send_log.jsonl")
    args = ap.parse_args()


    nodes = load_nodes(args.config_dir)
    kind = args.mode if args.mode != "auto" else system_kind(nodes)
    by_event, by_constraint, fwd = build_routing(nodes)

    if args.print_routes:
        print(f"system: {kind}")
        if kind == "central":
            print("forwarders (any event -> any forwarder):")
            for n in fwd:
                print(f"  node {n['node_id']}  {n['socket']}")
        print("event routing:")
        for ev in sorted(by_event):
            where = "  ".join(f"node {n['node_id']} ({n['socket']})" for n in by_event[ev])
            print(f"  {ev:<40} -> {where}")
        return

    if not args.stream:
        sys.exit("need --stream manifest.jsonl (or --print-routes)")
    stream_dir = os.path.dirname(os.path.abspath(args.stream))
    fwd_cycle = cycle(fwd) if fwd else None

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
    pacer = Pacer(interval, burst=args.burst, jitter=args.jitter)


    counts = {"sent": 0, "dry-run": 0, "no_route": 0, "error": 0}
    with open(args.log, "w") as logf, open(args.stream) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            ev = json.loads(line)
            event_id = ev["event_id"]
            constraint_id = ev.get("constraint_id")

            # payload + file (embedded xes or a referenced file)
            file_path = None
            if ev.get("file"):
                file_path = ev["file"] if os.path.isabs(ev["file"]) \
                    else os.path.join(stream_dir, ev["file"])
            payload = ev["xes"].encode() if ev.get("xes") is not None \
                else (open(file_path, "rb").read() if file_path else b"")

            # wait for this event's slot
            t = pacer.wait()

            # resolve targets
            if kind == "central":
                targets = [next(fwd_cycle)] if fwd_cycle else []
            elif constraint_id:
                targets = by_constraint.get((event_id, constraint_id), [])
            else:
                targets = by_event.get(event_id, [])

            if not targets:
                counts["no_route"] += 1
                logf.write(json.dumps({"seq": ev["seq"], "case_id": ev["case_id"],
                    "event_id": event_id, "constraint_id": constraint_id,
                    "node_id": None, "socket": None, "status": "no_route"}) + "\n")
                sys.stderr.write(f"no route for '{event_id}' (seq {ev['seq']})\n")
                continue

            for n in targets:
                status = "dry-run"
                if not args.dry_run:
                    try:
                        deliver_event(n["socket"], payload)
                        status = "sent"
                    except OSError as e:
                        status = f"error:{e}"
                counts["sent" if status == "sent" else
                       "dry-run" if status == "dry-run" else "error"] += 1
                logf.write(json.dumps({"seq": ev["seq"], "case_id": ev["case_id"],
                    "event_id": event_id, "constraint_id": constraint_id,
                    "node_id": n["node_id"], "socket": n["socket"],
                    "bytes": len(payload), "status": status,
                    "t": round(t, 6)}) + "\n")


    print(f"system={kind}  sent={counts['sent']} dry-run={counts['dry-run']} "
          f"no_route={counts['no_route']} error={counts['error']}")
    print(pacer.summary())
    print(f"log -> {args.log}")


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        try:
            sys.stdout.close()
        except Exception:
            pass