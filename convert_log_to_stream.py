#!/usr/bin/env python3
"""
!!! needs at least python 3.11 for tomllib !!!


turn an event log (e.g. sepsis or traffic fines) into a stream for send_event_stream.py

reads the original xes, flattens all traces into one stream ordered by timestamp
closes every case with a synthetic end event

writes the manifest.jsonl and event xes files (e.g. 00000_A_ER_Registration.xes) into --out

the end event has the timestamp of the cases last real event and is placed directly after it in the stream
its manifest line is marked with "synthetic": true to filter it out for analysis
pass --end-activity "" to skip generating end events

usage:
  convert_log_to_stream.py --xes sepsis.xes.gz --out events_sepsis
  convert_log_to_stream.py --xes sepsis.xes.gz --out events_sepsis --activities
  convert_log_to_stream.py --xes sepsis.xes.gz --out events_sepsis --cases 20
  convert_log_to_stream.py --xes sepsis.xes.gz --out events_sepsis --end-activity "End Activity"
"""
import argparse
import datetime
import gzip
import json
import os
import re
import xml.etree.ElementTree as ET
from glob import glob
from xml.sax.saxutils import quoteattr

XES_NS = "{http://www.xes-standard.org/}"
ATTR_TAGS = ("string", "date", "int", "float", "boolean", "id")
CORE = ("concept:name", "time:timestamp")
FAR_FUTURE = datetime.datetime.max.replace(tzinfo=datetime.timezone.utc)


# ---- read the sepsis XES ----

def _open(path):
    return gzip.open(path, "rb") if path.endswith(".gz") else open(path, "rb")


def _strip(tag):
    return tag[len(XES_NS):] if tag.startswith(XES_NS) else tag


def _attrs(elem):
    out = {}
    for child in elem:
        t = _strip(child.tag)
        if t in ATTR_TAGS:
            k = child.get("key")
            if k is not None:
                out[k] = (t, child.get("value", ""))
    return out


def read_log(path):
    # yield (case_id, trace_attrs, event_attrs) for every event in the log
    with _open(path) as fh:
        for _, trace in ET.iterparse(fh, events=("end",)):
            if _strip(trace.tag) != "trace":
                continue
            tattrs = _attrs(trace)
            case_id = tattrs.get("concept:name", ("string", "?"))[1]
            for ev in trace:
                if _strip(ev.tag) == "event":
                    yield case_id, tattrs, _attrs(ev)
            trace.clear()


def parse_ts(s):
    # sort key - sepsis mixes +01:00 and +02:00 offsets, so compare instants rather than strings
    try:
        return datetime.datetime.fromisoformat(s)
    except ValueError:
        return FAR_FUTURE


# ---- output ----

def safe(name):
    # filename-safe fragment because the event_ids have spaces
    return re.sub(r"_+", "_", re.sub(r"[^0-9A-Za-z]+", "_", str(name))).strip("_") or "x"


def render_event_xes(case_id, trace_attrs, event_attrs):
    def line(indent, t, k, v):
        return f'{indent}<{t} key={quoteattr(k)} value={quoteattr(v)}/>'

    out = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<log xes.version="1.0" xmlns="http://www.xes-standard.org/">',
           '  <trace>',
           line('    ', 'string', 'concept:name', case_id)]
    for k, (t, v) in trace_attrs.items():
        if k != "concept:name":
            out.append(line('    ', t, k, v))
    out.append('    <event>')
    for k, (t, v) in event_attrs.items():
        out.append(line('      ', t, k, v))
    out += ['    </event>', '  </trace>', '</log>', '']
    return "\n".join(out)


def typed(t, v):
    # xes attribute value -> json value
    try:
        if t == "float":
            return float(v)
        if t == "int":
            return int(v)
        if t == "boolean":
            return v.strip().lower() == "true"
    except ValueError:
        pass
    return v


# ---- what the deployment listens for ----

def routable_event_ids(config_dir):
    # route by constraint event_ids + forwarder include_event_ids"""
    import tomllib
    ids = set()
    for path in sorted(glob(os.path.join(config_dir, "*-config.toml"))):
        with open(path, "rb") as fh:
            cfg = tomllib.load(fh)
        for c in cfg.get("constraints", []):
            ids.add(c["event_id"])
        ids.update(cfg.get("forwarder", {}).get("include_event_ids") or [])
    return ids


# ---- records ----

def close_cases(records, end_activity):
    # append one end event per case
    # timestamp is taken from cases last event and is_end = 1 breaks the tie so the end event follows the real event
    last = {}
    for r in records:
        case_id = r[3]
        if case_id not in last or r[0] > last[case_id][0]:
            last[case_id] = r
    out = []
    for case_id, (sort_ts, _, ts, _, tattrs, eattrs, _) in last.items():
        end_attrs = {"concept:name": ("string", end_activity),
                     "time:timestamp": ("date", ts)}
        if "lifecycle:transition" in eattrs:
            end_attrs["lifecycle:transition"] = eattrs["lifecycle:transition"]
        out.append((sort_ts, 1, ts, case_id, tattrs, end_attrs, end_activity))
    return out


# ---- main ----

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xes", required=True, help="sepsis .xes or .xes.gz")
    ap.add_argument("--out", required=True, help="output directory")
    ap.add_argument("--end-activity", default="Case End", help="activity name of the synthetic end event appended to every case"
    " (default 'Case End', empty string to skip)")
    ap.add_argument("--activities", action="store_true", help="list the activities in the log and exit")
    ap.add_argument("--config-dir", help="report which event_ids the node configs actually route")
    ap.add_argument("--cases", type=int, default=0, help="keep only the first N cases")
    ap.add_argument("--max-events", type=int, default=0, help="stop after N events")
    args = ap.parse_args()

    # collect and order by timestamp
    records = []
    for case_id, tattrs, eattrs in read_log(args.xes):
        event_id = eattrs.get("concept:name", ("string", "unknown"))[1]
        ts = eattrs.get("time:timestamp", ("date", ""))[1]
        records.append((parse_ts(ts), 0, ts, case_id, tattrs, eattrs, event_id))
    records.sort(key=lambda r: (r[0], r[1]))

    # keep the first N cases to appear in the stream
    if args.cases:
        kept = {}
        for r in records:
            if r[3] not in kept:
                if len(kept) >= args.cases:
                    continue
                kept[r[3]] = True
        records = [r for r in records if r[3] in kept]

    # close every case, then re-sort so the end events land in place
    if args.end_activity:
        records += close_cases(records, args.end_activity)
        records.sort(key=lambda r: (r[0], r[1]))

    known = routable_event_ids(args.config_dir) if args.config_dir else None

    if args.activities:
        seen = {}
        for r in records:
            seen[r[6]] = seen.get(r[6], 0) + 1
        print(f"{'event_id (= activity)':<28} {'count':>6}  routed")
        for a in sorted(seen, key=lambda x: -seen[x]):
            mark = "-" if known is None else ("yes" if a in known else "no")
            print(f"{a:<28} {seen[a]:>6}  {mark}")
        print(f"\n{len(seen)} activities, {len(records)} events, "
              f"{len({r[3] for r in records})} cases")
        return

    os.makedirs(args.out, exist_ok=True)
    seq, ends, unrouted = 0, 0, set()

    with open(os.path.join(args.out, "manifest.jsonl"), "w") as mf:
        for sort_ts, is_end, ts, case_id, tattrs, eattrs, event_id in records:
            if known is not None and event_id not in known:
                unrouted.add(event_id)

            fname = f"{seq:05d}_{safe(case_id)}_{safe(event_id)}.xes"
            with open(os.path.join(args.out, fname), "w") as fh:
                fh.write(render_event_xes(case_id, tattrs, eattrs))

            row = {"attributes": {k: typed(t, v) for k, (t, v) in eattrs.items()
                                  if k not in CORE},
                   "case_id": case_id, "constraint_id": None,
                   "event_id": event_id, "file": fname, "seq": seq,
                   "timestamp": ts}
            if is_end:
                row["synthetic"] = True
                ends += 1
            mf.write(json.dumps(row) + "\n")
            seq += 1
            if args.max_events and seq >= args.max_events:
                break

    cases = len({r[3] for r in records})
    print(f"wrote {seq} events -> {os.path.join(args.out, 'manifest.jsonl')}")
    if args.end_activity:
        print(f"cases: {cases}, '{args.end_activity}' events written: {ends}")
        if ends < cases:
            print(f"warning: --max-events cut the stream short, {cases - ends} "
                  f"case(s) have no end event")
    if known is not None:
        if unrouted:
            print(f"{len(unrouted)} event_id(s) no node listens for, they will be "
                  f"logged as no_route: {', '.join(sorted(unrouted))}")
        else:
            print("all event_ids are routable")


if __name__ == "__main__":
    main()