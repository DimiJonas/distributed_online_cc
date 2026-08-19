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
ATTRIBUTE_TAGS = ("string", "date", "int", "float", "boolean", "id")
CORE_KEYS = ("concept:name", "time:timestamp")
FAR_FUTURE = datetime.datetime.max.replace(tzinfo=datetime.timezone.utc)


# ---- read the sepsis XES ----

def _open(path):
    return gzip.open(path, "rb") if path.endswith(".gz") else open(path, "rb")


def _strip(tag):
    return tag[len(XES_NS):] if tag.startswith(XES_NS) else tag


def _attributes(element):
    attributes = {}
    for child in element:
        tag = _strip(child.tag)
        if tag in ATTRIBUTE_TAGS:
            key = child.get("key")
            if key is not None:
                attributes[key] = (tag, child.get("value", ""))
    return attributes


def read_log(path):
    # yield (case_id, trace_attributes, event_attributes) for every event in the log
    with _open(path) as log_file:
        for _, trace in ET.iterparse(log_file, events=("end",)):
            if _strip(trace.tag) != "trace":
                continue
            trace_attributes = _attributes(trace)
            case_id = trace_attributes.get("concept:name", ("string", "?"))[1]
            for event in trace:
                if _strip(event.tag) == "event":
                    yield case_id, trace_attributes, _attributes(event)
            trace.clear()


def parse_timestamp(s):
    # sort key - sepsis mixes +01:00 and +02:00 offsets, so compare instants rather than strings
    try:
        return datetime.datetime.fromisoformat(s)
    except ValueError:
        return FAR_FUTURE


# ---- output ----

def safe_name(name):
    # filename-safe fragment because the event_ids have spaces
    return re.sub(r"_+", "_", re.sub(r"[^0-9A-Za-z]+", "_", str(name))).strip("_") or "x"


def render_event_xes(case_id, trace_attributes, event_attributes):
    def line(indent, tag, key, value):
        return f'{indent}<{tag} key={quoteattr(key)} value={quoteattr(value)}/>'

    xes = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<log xes.version="1.0" xmlns="http://www.xes-standard.org/">',
           '  <trace>',
           line('    ', 'string', 'concept:name', case_id)]
    for key, (tag, value) in trace_attributes.items():
        if key != "concept:name":
            xes.append(line('    ', tag, key, value))
    xes.append('    <event>')
    for key, (tag, value) in event_attributes.items():
        xes.append(line('      ', tag, key, value))
    xes += ['    </event>', '  </trace>', '</log>', '']
    return "\n".join(xes)


def typed(attribute_type, value):
    # xes attribute value -> json value
    try:
        if attribute_type == "float":
            return float(value)
        if attribute_type == "int":
            return int(value)
        if attribute_type == "boolean":
            return value.strip().lower() == "true"
    except ValueError:
        pass
    return value


# ---- what the deployment listens for ----

def routable_event_ids(config_dir):
    # route by constraint event_ids + forwarder include_event_ids"""
    import tomllib
    event_ids = set()
    for path in sorted(glob(os.path.join(config_dir, "*-config.toml"))):
        with open(path, "rb") as config_file:
            config = tomllib.load(config_file)
        for constraint in config.get("constraints", []):
            event_ids.add(constraint["event_id"])
        event_ids.update(config.get("forwarder", {}).get("include_event_ids") or [])
    return event_ids


# ---- records ----

def add_case_ends(records, end_activity):
    # append one end event per case
    # timestamp is taken from cases last event and is_end = 1 breaks the tie so the end event follows the real event
    last_events_per_case = {}
    for record in records:
        case_id = record[3]
        if case_id not in last_events_per_case or record[0] > last_events_per_case[case_id][0]:
            last_events_per_case[case_id] = record
    case_with_end = []
    for case_id, (sort_timestamp, _, timestamp, _, trace_attributes, event_attributes, _) in last_events_per_case.items():
        end_attributes = {"concept:name": ("string", end_activity), "time:timestamp": ("date", timestamp)}
        if "lifecycle:transition" in event_attributes:
            end_attributes["lifecycle:transition"] = event_attributes["lifecycle:transition"]
        case_with_end.append((sort_timestamp, 1, timestamp, case_id, trace_attributes, end_attributes, end_activity))
    return case_with_end


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
    for case_id, trace_attributes, event_attributes in read_log(args.xes):
        event_id = event_attributes.get("concept:name", ("string", "unknown"))[1]
        timestamp = event_attributes.get("time:timestamp", ("date", ""))[1]
        records.append((parse_timestamp(timestamp), 0, timestamp, case_id, trace_attributes, event_attributes, event_id))
    records.sort(key=lambda record: (record[0], record[1]))

    # keep only the first n cases to appear in the stream if configured
    if args.cases:
        kept_cases = {}
        for record in records:
            if record[3] not in kept_cases:
                if len(kept_cases) >= args.cases:
                    continue
                kept_cases[record[3]] = True
        records = [record for record in records if record[3] in kept_cases]

    # close every case, then re-sort so the end events land in place (at the end of each case)
    if args.end_activity:
        records += add_case_ends(records, args.end_activity)
        records.sort(key=lambda record: (record[0], record[1]))

    known_event_ids = routable_event_ids(args.config_dir) if args.config_dir else None

    if args.activities:
        seen_event_ids = {}
        for record in records:
            seen_event_ids[record[6]] = seen_event_ids.get(record[6], 0) + 1
        print(f"{'event_id (= activity)':<28} {'count':>6}  routed")
        for event_id in sorted(seen_event_ids, key=lambda x: -seen_event_ids[x]):
            mark = "-" if known_event_ids is None else ("yes" if event_id in known_event_ids else "no")
            print(f"{event_id:<28} {seen_event_ids[event_id]:>6}  {mark}")
        print(f"\n{len(seen_event_ids)} activities, {len(records)} events, "
              f"{len({record[3] for record in records})} cases")
        return

    os.makedirs(args.out, exist_ok=True)
    seq, ends, unrouted = 0, 0, set()

    with open(os.path.join(args.out, "manifest.jsonl"), "w") as manifest_file:
        for sort_ts, is_end, timestamp, case_id, trace_attributes, event_attributes, event_id in records:
            if known_event_ids is not None and event_id not in known_event_ids:
                unrouted.add(event_id)

            file_name = f"{seq:05d}_{safe_name(case_id)}_{safe_name(event_id)}.xes"
            with open(os.path.join(args.out, file_name), "w") as event_file:
                event_file.write(render_event_xes(case_id, trace_attributes, event_attributes))

            row = {"attributes": {attribute_key: typed(attribute_type, value) for attribute_key, (attribute_type, value) in event_attributes.items()
                                  if attribute_key not in CORE_KEYS},
                   "case_id": case_id, "constraint_id": None,
                   "event_id": event_id, "file": file_name, "seq": seq,
                   "timestamp": timestamp}
            if is_end:
                row["synthetic"] = True
                ends += 1
            manifest_file.write(json.dumps(row) + "\n")
            seq += 1
            if args.max_events and seq >= args.max_events:
                break

    cases = len({record[3] for record in records})
    print(f"wrote {seq} events -> {os.path.join(args.out, 'manifest.jsonl')}")
    if args.end_activity:
        print(f"cases: {cases}, '{args.end_activity}' events written: {ends}")
        if ends < cases:
            print(f"warning: --max-events cut the stream short, {cases - ends} "
                  f"case(s) have no end event")
    if known_event_ids is not None:
        if unrouted:
            print(f"{len(unrouted)} event_id(s) no node listens for, they will be "
                  f"logged as no_route: {', '.join(sorted(unrouted))}")
        else:
            print("all event_ids are routable")


if __name__ == "__main__":
    main()