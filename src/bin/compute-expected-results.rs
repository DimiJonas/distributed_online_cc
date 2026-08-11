//! compute the 'ground truth' for a real event log
//!
//! it reads
//!   * `--config-dir`  the node configs
//!   * `--manifest`    the `manifest.jsonl` written by `convert_log_to_stream.py` (or by `gen-events`)
//!
//! and writes `expected_results.jsonl`
//!
//! usage:
//!   compute-expected-results --config-dir run/decentralized/configs --manifest sepsis/events/manifest.jsonl
//!

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDateTime, Utc};
use clap::Parser;
use online_cc::config::node_config::{
    AppearanceCounterReset, DistributedAppearanceCounterReset, MinMaxMode,
};
use online_cc::types::{
    AttributeKey, CaseId, ConstraintId, ConstraintResultType, EventId, EventTime,
};
use online_cc::util::constraint_catalog::{
    AppearanceSpec, Catalog, DistAppearanceSpec, PairSpec, SensorSpec, TimeSpec, build_catalog,
    load_configs,
};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Parser)]
#[command(about = "Derive the expected conformance results for an event stream")]
struct Args {
    /// directory of *-config.toml files (decentralized or central)
    #[arg(long)]
    config_dir: PathBuf,

    /// manifest.jsonl of the event stream or the directory containing it
    #[arg(long)]
    manifest: PathBuf,

    /// where to write the ground truth (defaults to expected_results.jsonl next to the manifest)
    #[arg(long)]
    out: Option<PathBuf>,

    /// also evaluate cases that never reach the configured end activity
    #[arg(long, default_value_t = false)]
    include_unterminated_cases: bool,

    /// drop the reason from the output
    #[arg(long, default_value_t = false)]
    no_reasons: bool,

    /// print the per-constraint violation counts
    #[arg(long, default_value_t = false)]
    summary: bool,
}

// ---------------
// event stream
// ---------------

#[derive(Deserialize)]
struct ManifestLine {
    seq: u64,
    case_id: CaseId,
    event_id: EventId,
    timestamp: String,
    #[serde(default)]
    attributes: BTreeMap<AttributeKey, Value>,
}

struct ManifestEvent {
    seq: u64,
    event_id: EventId,
    timestamp: EventTime,
    attributes: BTreeMap<AttributeKey, Value>,
}

fn parse_timestamp(raw_str: &str) -> Result<EventTime, String> {
    // XES writes RFC3339; be forgiving about the space-separated variant too
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(raw_str) {
        return Ok(timestamp.with_timezone(&Utc));
    }
    for format in [
        "%Y-%m-%d %H:%M:%S%.f%:z",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
    ] {
        if let Ok(timestamp) = DateTime::parse_from_str(raw_str, format) {
            return Ok(timestamp.with_timezone(&Utc));
        }
        if let Ok(timestamp) = NaiveDateTime::parse_from_str(raw_str, format) {
            return Ok(timestamp.and_utc());
        }
    }
    Err(format!("unparseable timestamp: {raw_str:?}"))
}

fn manifest_path(path: &Path) -> PathBuf {
    if path.is_dir() {
        path.join("manifest.jsonl")
    } else {
        path.to_path_buf()
    }
}

/// read the manifest into per-case traces, ordered by (timestamp, seq)
fn read_cases(path: &Path) -> BTreeMap<CaseId, Vec<ManifestEvent>> {
    let manifest_file =
        File::open(path).unwrap_or_else(|e| panic!("cannot open manifest {}: {e}", path.display()));
    let mut cases: BTreeMap<CaseId, Vec<ManifestEvent>> = BTreeMap::new();

    for (line_number, line) in BufReader::new(manifest_file).lines().enumerate() {
        let line = line.unwrap_or_else(|e| panic!("cannot read {}: {}", path.display(), e));
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let manifest_line: ManifestLine = serde_json::from_str(line).unwrap_or_else(|e| {
            panic!(
                "{}:{}: cannot parse manifest line: {}",
                path.display(),
                line_number + 1,
                e
            )
        });
        let event_time = parse_timestamp(&manifest_line.timestamp)
            .unwrap_or_else(|e| panic!("{}:{}: {}", path.display(), line_number + 1, e));
        cases
            .entry(manifest_line.case_id)
            .or_default()
            .push(ManifestEvent {
                seq: manifest_line.seq,
                event_id: manifest_line.event_id,
                timestamp: event_time,
                attributes: manifest_line.attributes,
            });
    }

    for events in cases.values_mut() {
        events.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then(a.seq.cmp(&b.seq)));
    }
    cases
}

fn elapsed_secs(from: DateTime<Utc>, to: DateTime<Utc>) -> f64 {
    (to - from).num_milliseconds() as f64 / 1000.0
}

fn times_of(events: &[ManifestEvent], event_id: &str) -> Vec<EventTime> {
    events
        .iter()
        .filter(|e| e.event_id == event_id)
        .map(|e| e.timestamp)
        .collect()
}

fn to_sensor_attribute_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// per template evaluation
// ---------------------------------------------------------------------------

type Reason = String;
type Verdict = (ConstraintResultType, Reason);

fn satisfied(reason: impl Into<String>) -> Verdict {
    (ConstraintResultType::Satisfied, reason.into())
}

fn violated(reason: impl Into<String>) -> Verdict {
    (ConstraintResultType::Violated, reason.into())
}

/// order: the order-first event must not happen at or after the order-second event
fn evaluate_order(spec: &PairSpec, events: &[ManifestEvent]) -> Verdict {
    let first_times = times_of(events, &spec.event_id_1);
    if first_times.is_empty() {
        return satisfied("order first event never happened");
    }
    let Some(second_time) = times_of(events, &spec.event_id_2).into_iter().min() else {
        return satisfied("order second event never happened");
    };
    match first_times.iter().find(|time| **time >= second_time) {
        Some(first_time) => violated(format!(
            "order first event at {} happened at or after order second event at {}",
            first_time, second_time
        )),
        None => satisfied(format!(
            "all {} order first event(s) happened before the order second event at {}",
            first_times.len(),
            second_time
        )),
    }
}

/// response: after the producer event the consumer event must follow
fn evaluate_response(spec: &PairSpec, events: &[ManifestEvent]) -> Verdict {
    let mut open_token: Option<EventTime> = None;
    let mut last_consumer: Option<EventTime> = None;

    for event in events {
        if event.event_id == spec.event_id_1 {
            open_token = match last_consumer {
                Some(consumed) if consumed > event.timestamp => None,
                _ => Some(event.timestamp),
            };
        }
        if event.event_id == spec.event_id_2 {
            last_consumer = Some(match last_consumer {
                Some(previous) if previous > event.timestamp => previous,
                _ => event.timestamp,
            });
            if open_token.is_some_and(|opened| opened < event.timestamp) {
                open_token = None;
            }
        }
    }

    match open_token {
        Some(opened) => violated(format!("producer token from {} was not consumed", opened)),
        None => satisfied("no producer tokens are open at case end"),
    }
}

/// precedence: the consumer event may only happen after the producer event
fn evaluate_precedence(spec: &PairSpec, events: &[ManifestEvent]) -> Verdict {
    let mut earliest_producer: Option<EventTime> = None;
    let mut consumer_seen = false;

    for event in events {
        if event.event_id == spec.event_id_1 {
            earliest_producer = Some(match earliest_producer {
                Some(previous) if previous < event.timestamp => previous,
                _ => event.timestamp,
            });
        }
        if event.event_id == spec.event_id_2 {
            consumer_seen = true;
            match earliest_producer {
                Some(producer) if event.timestamp > producer => {}
                Some(producer) => {
                    return violated(format!(
                        "consumer event at {} happened before or at the producer event at {}",
                        event.timestamp, producer
                    ));
                }
                None => {
                    return violated(format!(
                        "consumer event at {} happened but the producer event never did",
                        event.timestamp
                    ));
                }
            }
        }
    }

    if consumer_seen {
        satisfied("every consumer event was preceded by the producer event")
    } else {
        satisfied("consumer event never happened")
    }
}

/// not coexistence: the two events must not both appear in a case
fn evaluate_notcoexistence(spec: &PairSpec, events: &[ManifestEvent]) -> Verdict {
    let a = !times_of(events, &spec.event_id_1).is_empty();
    let b = !times_of(events, &spec.event_id_2).is_empty();
    match (a, b) {
        (true, true) => violated("both not coexistence events happened"),
        (true, false) => satisfied("only the not coexistence A event happened"),
        (false, true) => satisfied("only the not coexistence B event happened"),
        (false, false) => satisfied("none of the not coexistence events happened"),
    }
}

fn counter_verdict(mode: &MinMaxMode, count: u64, frequency: u64) -> Option<Reason> {
    match mode {
        MinMaxMode::Min if count < frequency => Some(format!(
            "CounterMin: counter {} < limit {}",
            count, frequency
        )),
        MinMaxMode::Max if count > frequency => Some(format!(
            "CounterMax: counter {} > limit {}",
            count, frequency
        )),
        _ => None,
    }
}

/// appearance: the counter event must appear min/max `frequency` times per trigger window
fn evaluate_appearance(spec: &AppearanceSpec, events: &[ManifestEvent]) -> Verdict {
    let mut count: u64 = 0;
    let mut triggered = false;
    let mut violation: Option<Reason> = None;

    for event in events {
        if event.event_id == spec.counter_event_id {
            count += 1;
        }
        if event.event_id == spec.trigger_event_id {
            triggered = true;
            if let Some(reason) = counter_verdict(&spec.mode, count, spec.frequency) {
                violation = Some(reason);
            }
            count = match spec.reset {
                AppearanceCounterReset::Zero => 0,
                AppearanceCounterReset::Subtract => count.saturating_sub(spec.frequency),
                AppearanceCounterReset::Keep => count,
            };
        }
    }

    match violation {
        Some(reason) => violated(reason),
        None if triggered => satisfied("every counter evaluation was within its limit"),
        None => satisfied("counter evaluation was never triggered"),
    }
}

/// distributed appearance: same as appearance but the counter is the sum over all counter events
fn evaluate_dist_appearance(spec: &DistAppearanceSpec, events: &[ManifestEvent]) -> Verdict {
    let counter_event_ids: BTreeSet<&str> = spec
        .counter_event_ids
        .iter()
        .map(|event_id| event_id.as_str())
        .collect();
    let mut count: u64 = 0;
    let mut triggered = false;
    let mut violation: Option<Reason> = None;

    for event in events {
        if counter_event_ids.contains(event.event_id.as_str()) {
            count += 1;
        }
        if event.event_id == spec.trigger_event_id {
            triggered = true;
            if let Some(reason) = counter_verdict(&spec.mode, count, spec.frequency) {
                violation = Some(reason);
            }
            count = match spec.reset {
                DistributedAppearanceCounterReset::Zero => 0,
                DistributedAppearanceCounterReset::Keep => count,
            };
        }
    }

    match violation {
        Some(reason) => violated(reason),
        None if triggered => satisfied("every distributed counter evaluation was within its limit"),
        None => satisfied("counter evaluation was never triggered"),
    }
}

/// sensor: at every trigger the newest recorded value must satisfy the condition
fn evaluate_sensor(spec: &SensorSpec, events: &[ManifestEvent]) -> Verdict {
    let mut newest: Option<(EventTime, f64)> = None;
    let mut triggered = false;
    let mut violation: Option<Reason> = None;

    for event in events {
        if event.event_id == spec.collector_event_id
            && let Some(value) = event
                .attributes
                .get(&spec.attribute_key)
                .and_then(to_sensor_attribute_value)
        {
            // the store keeps the first value seen for an event time and reads back the newest value at or before the trigger
            let replace = match newest {
                Some((known, _)) => event.timestamp > known,
                None => true,
            };
            if replace {
                newest = Some((event.timestamp, value));
            }
        }

        if event.event_id == spec.trigger_event_id {
            triggered = true;
            match newest {
                None => {
                    violation = Some(format!(
                        "no sensor value exists for key {} at the trigger event {}",
                        spec.attribute_key, event.timestamp
                    ));
                }
                Some((value_time, value)) => {
                    let age = elapsed_secs(value_time, event.timestamp);
                    if age > spec.max_value_age_secs {
                        violation = Some(format!(
                            "sensor value for key {} is outdated: age {}s > {}s",
                            spec.attribute_key, age, spec.max_value_age_secs
                        ));
                    } else if !spec.mode.evaluate_condition(value, spec.threshold) {
                        violation = Some(format!(
                            "{} {} {} is false",
                            value, spec.mode, spec.threshold
                        ));
                    }
                }
            }
        }
    }

    match violation {
        Some(reason) => violated(reason),
        None if triggered => satisfied("condition was met"),
        None => satisfied("sensor trigger event never happened"),
    }
}

/// time: the gap between start and stop must respect the configured duration
/// the last verdict is the correct result
fn evaluate_time(spec: &TimeSpec, events: &[ManifestEvent], case_end: EventTime) -> Verdict {
    let mut live_starts: Vec<EventTime> = Vec::new();
    let mut last: Option<Verdict> = None;

    let judge = |elapsed: f64| -> Verdict {
        match spec.mode {
            MinMaxMode::Max if elapsed <= spec.duration_secs => satisfied(format!(
                "TimerMax: elapsed {}s <= limit {}s",
                elapsed, spec.duration_secs
            )),
            MinMaxMode::Max => violated(format!(
                "TimerMax: elapsed {}s > limit {}s",
                elapsed, spec.duration_secs
            )),
            MinMaxMode::Min if elapsed >= spec.duration_secs => satisfied(format!(
                "TimerMin: elapsed {}s >= minimum {}s",
                elapsed, spec.duration_secs
            )),
            MinMaxMode::Min => violated(format!(
                "TimerMin: elapsed {}s < minimum {}s",
                elapsed, spec.duration_secs
            )),
        }
    };

    for event in events {
        if event.event_id == spec.start_event_id {
            live_starts.push(event.timestamp);
        }
        if event.event_id == spec.stop_event_id {
            // a stop settles every live timer it qualifies for, i.e. every start at or before it (earliest_stop_after uses >=)
            let stop_time = event.timestamp;
            let mut settled: Vec<EventTime> = Vec::new();
            live_starts.retain(|start| {
                if *start <= stop_time {
                    settled.push(*start);
                    false
                } else {
                    true
                }
            });
            for start in settled {
                last = Some(judge(elapsed_secs(start, stop_time)));
            }
        }
    }

    // timers still running at case end never saw their stop event
    for start in &live_starts {
        last = Some(match spec.mode {
            MinMaxMode::Max => violated(format!(
                "TimerMax: stop event never arrived; case ended {}s after the start event, limit {}s",
                elapsed_secs(*start, case_end),
                spec.duration_secs
            )),
            MinMaxMode::Min => satisfied(
                "TimerMin: stop event never arrived, so the minimum gap cannot be undercut"
                    .to_string(),
            ),
        });
    }

    last.unwrap_or_else(|| satisfied("no time constraint result was produced until case end"))
}

// ---------------------------------------------------------------------------
// case evaluation
// ---------------------------------------------------------------------------

fn evaluate_case(catalog: &Catalog, events: &[ManifestEvent]) -> BTreeMap<ConstraintId, Verdict> {
    let case_end = events
        .last()
        .map(|event| event.timestamp)
        .unwrap_or_else(Utc::now);

    let mut verdicts: BTreeMap<ConstraintId, Verdict> = BTreeMap::new();
    for spec in &catalog.order {
        verdicts.insert(spec.constraint_id.clone(), evaluate_order(spec, events));
    }
    for spec in &catalog.response {
        verdicts.insert(spec.constraint_id.clone(), evaluate_response(spec, events));
    }
    for spec in &catalog.precedence {
        verdicts.insert(
            spec.constraint_id.clone(),
            evaluate_precedence(spec, events),
        );
    }
    for spec in &catalog.notcoex {
        verdicts.insert(
            spec.constraint_id.clone(),
            evaluate_notcoexistence(spec, events),
        );
    }
    for spec in &catalog.appearance {
        verdicts.insert(
            spec.constraint_id.clone(),
            evaluate_appearance(spec, events),
        );
    }
    for spec in &catalog.dist_appearance {
        verdicts.insert(
            spec.constraint_id.clone(),
            evaluate_dist_appearance(spec, events),
        );
    }
    for spec in &catalog.sensor {
        verdicts.insert(spec.constraint_id.clone(), evaluate_sensor(spec, events));
    }
    for spec in &catalog.time {
        verdicts.insert(
            spec.constraint_id.clone(),
            evaluate_time(spec, events, case_end),
        );
    }
    verdicts
}

fn main() {
    let args = Args::parse();

    let configs = load_configs(&args.config_dir);
    if configs.is_empty() {
        panic!("no *.toml configs found in {}", args.config_dir.display());
    }
    let catalog = build_catalog(&configs);
    let constraint_ids = catalog.result_constraint_ids();
    if constraint_ids.is_empty() {
        panic!(
            "the configs in {} do not define a single complete constraint",
            args.config_dir.display()
        );
    }

    let manifest = manifest_path(&args.manifest);
    let cases = read_cases(&manifest);
    let out_path = args.out.clone().unwrap_or_else(|| {
        manifest
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("expected_results.jsonl")
    });

    let end_event_id = catalog.end_event_id.clone();
    let mut skipped_cases: Vec<CaseId> = Vec::new();
    let mut violations_per_constraint: HashMap<ConstraintId, u64> = HashMap::new();
    let mut written = 0u64;
    let mut evaluated_cases = 0u64;

    let file = File::create(&out_path)
        .unwrap_or_else(|e| panic!("cannot write {}: {e}", out_path.display()));
    let mut out = BufWriter::new(file);

    for (case_id, events) in &cases {
        if let Some(end) = &end_event_id
            && !args.include_unterminated_cases
            && !events.iter().any(|event| &event.event_id == end)
        {
            skipped_cases.push(case_id.clone());
            continue;
        }
        evaluated_cases += 1;

        let verdicts = evaluate_case(&catalog, events);
        for constraint_id in &constraint_ids {
            let (expected, reason) = verdicts.get(constraint_id).cloned().unwrap_or_else(|| {
                panic!(
                    "constraint {constraint_id} was not evaluated - catalog and evaluator disagree"
                )
            });
            if expected == ConstraintResultType::Violated {
                *violations_per_constraint
                    .entry(constraint_id.clone())
                    .or_default() += 1;
            }
            let mut record = json!({
                "case_id": case_id,
                "constraint_id": constraint_id,
                "expected": expected,
            });
            if !args.no_reasons {
                record["reason"] = json!(reason);
            }
            writeln!(out, "{}", record).expect("cannot write ground truth line");
            written += 1;
        }
    }
    out.flush().expect("cannot flush ground truth file");

    println!(
        "wrote {} expected result(s) for {} case(s) x {} constraint(s) to {}",
        written,
        evaluated_cases,
        constraint_ids.len(),
        out_path.display()
    );
    if !skipped_cases.is_empty() {
        let end = end_event_id.unwrap_or_default();
        println!(
            "skipped {} case(s) without the end activity {}",
            skipped_cases.len(),
            end
        );
    }

    if args.summary {
        println!(
            "\nviolations per constraint (of {} case(s)):",
            evaluated_cases
        );
        for constraint_id in &constraint_ids {
            let violation_count = violations_per_constraint
                .get(constraint_id)
                .copied()
                .unwrap_or(0);
            let violation_share = if evaluated_cases > 0 {
                100.0 * violation_count as f64 / evaluated_cases as f64
            } else {
                0.0
            };
            println!("  {constraint_id:<45} {violation_count:>6}  ({violation_share:>5.1}%)");
        }
    }

    // sanity check for faulty specified catalogs: event ids the model (constraint catalog) refers to but the manifest doesnt contain
    let present_events: BTreeSet<&str> = cases
        .values()
        .flat_map(|events| events.iter().map(|e| e.event_id.as_str()))
        .collect();
    let missing_events: Vec<EventId> = catalog
        .all_event_ids()
        .into_iter()
        .filter(|id| !present_events.contains(id.as_str()))
        .collect();
    if !missing_events.is_empty() {
        println!(
            "\nwarning: {} configured event id(s) never occur in the stream: {}",
            missing_events.len(),
            missing_events.join(", ")
        );
    }
}
