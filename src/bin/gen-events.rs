//! generate a realistic XES event stream from a systems node configs
//!
//! the configs are read to build a constraint catalog.
//! from the catalog the generator derives a satisfying base trace per case: producers before consumers,
//! order-first before order-second, at most one of the not-coexistence pair, counter frequencies within their limits,
//! sensor values inside their thresholds, timer gaps respecting their durations, and the end activity at the end.
//! cases are mostly this happy trace (with randomized internal order, counts, values and timing jitter), but a configurable fraction of cases deviate:
//! events get swapped or dropped, counters over/undershoot, sensor values go out of range, timers get squeezed, both not-coexistence events appear
//! or the end activity comes early.
//!
//! output (into --out-dir):
//!   * one .xes file per event
//!   * manifest.jsonl - one line per event (seq, case_id, event_id, file, attributes, timestamp); the global stream order (by timestamp)
//!   * cases.jsonl - which deviations were applied to which case

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use clap::Parser;
use online_cc::config::node_config::{MinMaxMode, SensorConstraintMode};
use online_cc::types::{EventId, SensorAttributeThreshold};
use online_cc::util::constraint_catalog::{Catalog, SensorSpec, build_catalog, load_configs};
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Generate an XES event stream from a systems node configs")]
struct Args {
    /// directory of *-config.toml files (decentralized or central - only the constraint definitions are read)
    #[arg(long)]
    config_dir: PathBuf,

    /// output directory for the .xes files, manifest and 'ground truth'
    #[arg(long, default_value = "run/events")]
    out_dir: PathBuf,

    /// number of cases
    #[arg(long, default_value_t = 10)]
    cases: u64,

    #[arg(long, default_value = "case")]
    case_prefix: String,

    /// fraction of cases that deviate from the happy trace (0.0 - 1.0)
    #[arg(long, default_value_t = 0.2)]
    violation_rate: f64,

    /// how deviant cases are chosen:
    ///     exact = exactly round(cases * violation_rate) cases deviate so the deviant count identical for every seed
    ///     bernoulli: every case deviates independently with p = violation_rate, which is more realistic because the deviant count varies binomially between seeds
    #[arg(long, value_enum, default_value_t = ViolationSampling::Exact)]
    violation_sampling: ViolationSampling,

    /// rng seed (the whole stream is deterministic given the seed)
    /// defaults to a random seed that will be printed (so it can be reproduced)
    #[arg(long)]
    seed: Option<u64>,

    /// RFC3339 base timestamp of the stream e.g. "2026-07-23T00:00:00Z"
    #[arg(long, default_value = "2026-07-30T00:00:00Z")]
    start: String,

    /// mean time seconds between consecutive events of a case
    #[arg(long, default_value_t = 1.0)]
    step_secs: f64,

    /// seconds between case starts (cases overlap when this is smaller than a cases duration)
    #[arg(long, default_value_t = 10.0)]
    case_gap_secs: f64,

    /// disable the +-40% jitter on event gaps
    #[arg(long, default_value_t = false)]
    no_jitter: bool,

    /// file name for the manifest file that will be written
    /// the manifest file fixes the order of the event stream and provides metadata for routing the events to the nodes that expect them
    #[arg(long, default_value = "manifest.jsonl")]
    manifest: String,
}

#[derive(Clone, Copy, PartialEq, clap::ValueEnum)]
enum ViolationSampling {
    Exact,
    Bernoulli,
}

/// a value that satisfies the constraint
fn happy_sensor_value(spec: &SensorSpec) -> SensorAttributeThreshold {
    match spec.mode {
        SensorConstraintMode::Gt => spec.threshold + 1.0,
        SensorConstraintMode::Lt => spec.threshold - 1.0,
        SensorConstraintMode::Gte | SensorConstraintMode::Lte | SensorConstraintMode::Eq => {
            spec.threshold
        }
    }
}

/// a value that violates the constraint
fn violating_sensor_value(spec: &SensorSpec) -> SensorAttributeThreshold {
    match spec.mode {
        SensorConstraintMode::Gt | SensorConstraintMode::Gte => spec.threshold - 1.0,
        SensorConstraintMode::Lt | SensorConstraintMode::Lte | SensorConstraintMode::Eq => {
            spec.threshold + 1.0
        }
    }
}

// ---------------------------------------------------------------------------
// deviations
// ---------------------------------------------------------------------------

// one way a case can deviate from the happy trace
// indices refer into the catalog vectors
#[derive(Clone, Debug)]
enum Deviation {
    /// order-second before order-first
    OrderSwap(usize),
    /// the response consumer never happens -> open producer token at case end
    ResponseDropConsumer(usize),
    /// response consumer before its producer -> token never consumed
    ResponseSwap(usize),
    /// precedence consumer without a prior producer
    PrecedenceDropProducer(usize),
    /// precedence consumer before its producer
    PrecedenceSwap(usize),
    /// both not coexistence events appear
    Coexist(usize),
    /// appearance counter over/undershoots its frequency limit
    AppearanceOffCount(usize),
    /// distributed appearance counters over/undershoot their limit
    DistAppearanceOffCount(usize),
    /// sensor value outside at least one threshold
    SensorBadValue(usize),
    /// the collector event never happens -> no value at trigger time
    SensorMissingValue(usize),
    /// timer duration on the wrong side of its limit
    TimeOffDuration(usize),
    /// end activity fires before the last events of the case (last events still arrive within the grace period)
    EarlyEnd,
}

fn deviation_pool(catalog: &Catalog) -> Vec<Deviation> {
    let mut pool = Vec::new();
    for i in 0..catalog.order.len() {
        pool.push(Deviation::OrderSwap(i));
    }
    for i in 0..catalog.response.len() {
        pool.push(Deviation::ResponseDropConsumer(i));
        pool.push(Deviation::ResponseSwap(i));
    }
    for i in 0..catalog.precedence.len() {
        pool.push(Deviation::PrecedenceDropProducer(i));
        pool.push(Deviation::PrecedenceSwap(i));
    }
    for i in 0..catalog.notcoex.len() {
        pool.push(Deviation::Coexist(i));
    }
    for i in 0..catalog.appearance.len() {
        pool.push(Deviation::AppearanceOffCount(i));
    }
    for i in 0..catalog.dist_appearance.len() {
        pool.push(Deviation::DistAppearanceOffCount(i));
    }
    for i in 0..catalog.sensor.len() {
        pool.push(Deviation::SensorBadValue(i));
        pool.push(Deviation::SensorMissingValue(i));
    }
    for i in 0..catalog.time.len() {
        pool.push(Deviation::TimeOffDuration(i));
    }
    if catalog.end_event_id.is_some() {
        pool.push(Deviation::EarlyEnd);
    }
    pool
}

// the constraint this deviation touches
// at most one deviation per constraint is applied to a case
fn deviation_target(deviation: &Deviation, catalog: &Catalog) -> Option<String> {
    match deviation {
        Deviation::OrderSwap(i) => Some(catalog.order[*i].constraint_id.clone()),
        Deviation::ResponseDropConsumer(i) | Deviation::ResponseSwap(i) => {
            Some(catalog.response[*i].constraint_id.clone())
        }
        Deviation::PrecedenceDropProducer(i) | Deviation::PrecedenceSwap(i) => {
            Some(catalog.precedence[*i].constraint_id.clone())
        }
        Deviation::Coexist(i) => Some(catalog.notcoex[*i].constraint_id.clone()),
        Deviation::AppearanceOffCount(i) => Some(catalog.appearance[*i].constraint_id.clone()),
        Deviation::DistAppearanceOffCount(i) => {
            Some(catalog.dist_appearance[*i].constraint_id.clone())
        }
        Deviation::SensorBadValue(i) | Deviation::SensorMissingValue(i) => {
            Some(catalog.sensor[*i].constraint_id.clone())
        }
        Deviation::TimeOffDuration(i) => Some(catalog.time[*i].constraint_id.clone()),
        Deviation::EarlyEnd => None,
    }
}

fn deviation_name(deviation: &Deviation) -> &'static str {
    match deviation {
        Deviation::OrderSwap(_) => "order_swap",
        Deviation::ResponseDropConsumer(_) => "response_drop_consumer",
        Deviation::ResponseSwap(_) => "response_swap",
        Deviation::PrecedenceDropProducer(_) => "precedence_drop_producer",
        Deviation::PrecedenceSwap(_) => "precedence_swap",
        Deviation::Coexist(_) => "coexistence",
        Deviation::AppearanceOffCount(_) => "appearance_off_count",
        Deviation::DistAppearanceOffCount(_) => "dist_appearance_off_count",
        Deviation::SensorBadValue(_) => "sensor_bad_value",
        Deviation::SensorMissingValue(_) => "sensor_missing_value",
        Deviation::TimeOffDuration(_) => "time_off_duration",
        Deviation::EarlyEnd => "early_end",
    }
}

// ---------------------------------------------------------------------------
// trace building
// ---------------------------------------------------------------------------

// planned event of a case before timestamps are assigned
struct Planned {
    event_id: EventId,
    attributes: HashMap<String, f64>,
    /// (time constraint id) this event stops - used for timestamp planning
    stops_timer: Option<usize>,
    /// (time constraint id) this event starts
    starts_timer: Option<usize>,
}

impl Planned {
    fn plain(event_id: &str) -> Self {
        Planned {
            event_id: event_id.to_string(),
            attributes: HashMap::new(),
            stops_timer: None,
            starts_timer: None,
        }
    }
}

struct CasePlan {
    events: Vec<Planned>,
    deviations: Vec<Deviation>,
}

fn build_case(catalog: &Catalog, deviations: Vec<Deviation>, rng: &mut StdRng) -> CasePlan {
    let mut early: Vec<Planned> = Vec::new(); // producers / setup
    let mut mid: Vec<Planned> = Vec::new(); // counters / independent events
    let mut late: Vec<Planned> = Vec::new(); // consumers / triggers

    let dev = |pred: &dyn Fn(&Deviation) -> bool| deviations.iter().any(pred);

    // order
    for (i, pair_spec) in catalog.order.iter().enumerate() {
        if dev(&|deviation| matches!(deviation, Deviation::OrderSwap(j) if *j == i)) {
            late.push(Planned::plain(&pair_spec.event_id_1));
            early.push(Planned::plain(&pair_spec.event_id_2));
        } else {
            early.push(Planned::plain(&pair_spec.event_id_1));
            late.push(Planned::plain(&pair_spec.event_id_2));
        }
    }
    // response
    for (i, pair_spec) in catalog.response.iter().enumerate() {
        if dev(&|deviation| matches!(deviation, Deviation::ResponseDropConsumer(j) if *j == i)) {
            early.push(Planned::plain(&pair_spec.event_id_1));
        } else if dev(&|deviation| matches!(deviation, Deviation::ResponseSwap(j) if *j == i)) {
            early.push(Planned::plain(&pair_spec.event_id_2));
            late.push(Planned::plain(&pair_spec.event_id_1));
        } else {
            early.push(Planned::plain(&pair_spec.event_id_1));
            late.push(Planned::plain(&pair_spec.event_id_2));
        }
    }
    // precedence
    for (i, pair_spec) in catalog.precedence.iter().enumerate() {
        if dev(&|d| matches!(d, Deviation::PrecedenceDropProducer(j) if *j == i)) {
            late.push(Planned::plain(&pair_spec.event_id_2));
        } else if dev(&|d| matches!(d, Deviation::PrecedenceSwap(j) if *j == i)) {
            early.push(Planned::plain(&pair_spec.event_id_2));
            late.push(Planned::plain(&pair_spec.event_id_1));
        } else {
            early.push(Planned::plain(&pair_spec.event_id_1));
            late.push(Planned::plain(&pair_spec.event_id_2));
        }
    }
    // not coexistence: normally at most one of the two events (or neither)
    for (i, pair_spec) in catalog.notcoex.iter().enumerate() {
        if dev(&|d| matches!(d, Deviation::Coexist(j) if *j == i)) {
            mid.push(Planned::plain(&pair_spec.event_id_1));
            mid.push(Planned::plain(&pair_spec.event_id_2));
        } else {
            match rng.random_range(0..3) {
                0 => mid.push(Planned::plain(&pair_spec.event_id_1)),
                1 => mid.push(Planned::plain(&pair_spec.event_id_2)),
                _ => {} // neither
            }
        }
    }
    // appearance counters
    for (i, appear_spec) in catalog.appearance.iter().enumerate() {
        let off = dev(&|d| matches!(d, Deviation::AppearanceOffCount(j) if *j == i));
        let count = match (&appear_spec.mode, off) {
            (MinMaxMode::Max, false) => rng.random_range(1..=appear_spec.frequency),
            (MinMaxMode::Max, true) => {
                rng.random_range(appear_spec.frequency + 1..=appear_spec.frequency + 3)
            }
            (MinMaxMode::Min, false) => {
                rng.random_range(appear_spec.frequency..=appear_spec.frequency + 2)
            }
            (MinMaxMode::Min, true) => rng.random_range(0..appear_spec.frequency),
        };
        for _ in 0..count {
            mid.push(Planned::plain(&appear_spec.counter_event_id));
        }
        late.push(Planned::plain(&appear_spec.trigger_event_id));
    }
    // distributed appearance counters
    for (i, dist_appear_spec) in catalog.dist_appearance.iter().enumerate() {
        let off = dev(&|d| matches!(d, Deviation::DistAppearanceOffCount(j) if *j == i));
        let total = match (&dist_appear_spec.mode, off) {
            (MinMaxMode::Min, false) => {
                rng.random_range(dist_appear_spec.frequency..=dist_appear_spec.frequency + 2)
            }
            (MinMaxMode::Min, true) => rng.random_range(0..dist_appear_spec.frequency),
            (MinMaxMode::Max, false) => rng.random_range(0..=dist_appear_spec.frequency),
            (MinMaxMode::Max, true) => {
                rng.random_range(dist_appear_spec.frequency + 1..=dist_appear_spec.frequency + 3)
            }
        };
        for _ in 0..total {
            let event_id = dist_appear_spec.counter_event_ids
                [rng.random_range(0..dist_appear_spec.counter_event_ids.len())]
            .clone();
            mid.push(Planned::plain(&event_id));
        }
        late.push(Planned::plain(&dist_appear_spec.trigger_event_id));
    }
    // sensors
    for (i, sensor_spec) in catalog.sensor.iter().enumerate() {
        let missing = dev(&|d| matches!(d, Deviation::SensorMissingValue(j) if *j == i));
        let bad = dev(&|d| matches!(d, Deviation::SensorBadValue(j) if *j == i));
        if !missing {
            let value = if bad {
                violating_sensor_value(sensor_spec)
            } else {
                happy_sensor_value(sensor_spec)
            };
            let mut event = Planned::plain(&sensor_spec.collector_event_id);
            event
                .attributes
                .insert(sensor_spec.attribute_key.clone(), value);
            early.push(event);
        }
        late.push(Planned::plain(&sensor_spec.trigger_event_id));
    }
    // timers
    for (i, time_spec) in catalog.time.iter().enumerate() {
        let off = dev(&|deviation| matches!(deviation, Deviation::TimeOffDuration(j) if *j == i));
        let mut start = Planned::plain(&time_spec.start_event_id);
        start.starts_timer = Some(i);
        let mut stop = Planned::plain(&time_spec.stop_event_id);
        stop.stops_timer = Some(i);
        // Min: gap must be >= duration (stop goes late, padded past duration)
        //      violation = stop right after start (gap << duration)
        // Max: gap must be <= duration (stop right after start)
        //      violation = stop late, padded past duration
        let stop_adjacent = match (&time_spec.mode, off) {
            (MinMaxMode::Min, false) => false,
            (MinMaxMode::Min, true) => true,
            (MinMaxMode::Max, false) => true,
            (MinMaxMode::Max, true) => false,
        };
        if stop_adjacent {
            // keep the pair adjacent so the natural ~1s step stays far below the duration
            early.push(start);
            early.push(stop);
        } else {
            early.push(start);
            late.push(stop);
        }
    }

    // shuffle within phases while cross-phase requirements stay intact
    // adjacent timer pairs in `early` must stay together so shuffle around them
    shuffle_keeping_adjacent_pairs(&mut early, rng);
    mid.shuffle(rng);
    late.shuffle(rng);

    let mut events: Vec<Planned> = Vec::new();
    events.extend(early);
    events.extend(mid);
    events.extend(late);

    // end activity: normally last; with EarlyEnd it fires before the last
    // 1..=2 events of the case (which then arrive during the grace period)
    if let Some(end_event_id) = &catalog.end_event_id {
        let end_event = Planned::plain(end_event_id);
        if dev(&|d| matches!(d, Deviation::EarlyEnd)) && events.len() >= 2 {
            let back = rng.random_range(1..=2usize.min(events.len()));
            let pos = events.len() - back;
            events.insert(pos, end_event);
        } else {
            events.push(end_event);
        }
    }

    CasePlan { events, deviations }
}

// shuffle a list but keep (start, stop) timer pairs that were inserted next to each other
fn shuffle_keeping_adjacent_pairs(event_list: &mut Vec<Planned>, rng: &mut StdRng) {
    let mut blocks: Vec<Vec<Planned>> = Vec::new();
    let drained: Vec<Planned> = std::mem::take(event_list);
    let mut it = drained.into_iter().peekable();
    while let Some(event) = it.next() {
        let pair_with_next = match (event.starts_timer, it.peek().and_then(|n| n.stops_timer)) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        };
        if pair_with_next {
            let stop = it.next().unwrap();
            blocks.push(vec![event, stop]);
        } else {
            blocks.push(vec![event]);
        }
    }
    blocks.shuffle(rng);
    for block in blocks {
        event_list.extend(block);
    }
}

fn fmt_num(v: f64) -> String {
    if v.fract() == 0.0 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

fn render_xes(
    case_id: &str,
    event_id: &str,
    attributes: &HashMap<String, f64>,
    ts: &str,
) -> String {
    let mut xes_event = String::new();
    xes_event.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xes_event.push_str("<log xes.version=\"1.0\" xmlns=\"http://www.xes-standard.org/\">\n");
    xes_event.push_str("  <trace>\n");
    xes_event.push_str(&format!(
        "    <string key=\"concept:name\" value=\"{case_id}\"/>\n"
    ));
    xes_event.push_str("    <event>\n");
    xes_event.push_str(&format!(
        "      <string key=\"concept:name\" value=\"{event_id}\"/>\n"
    ));
    let mut attribute_keys: Vec<&String> = attributes.keys().collect();
    attribute_keys.sort();
    for key in attribute_keys {
        xes_event.push_str(&format!(
            "      <float key=\"{key}\" value=\"{}\"/>\n",
            fmt_num(attributes[key])
        ));
    }
    xes_event.push_str(&format!(
        "      <date key=\"time:timestamp\" value=\"{ts}\"/>\n"
    ));
    xes_event.push_str("    </event>\n  </trace>\n</log>\n");
    xes_event
}

fn main() {
    let args = Args::parse();
    let configs = load_configs(&args.config_dir);
    let catalog = build_catalog(&configs);

    let constraint_ids = catalog.result_constraint_ids();
    if constraint_ids.is_empty() && catalog.end_event_id.is_none() {
        eprintln!("no constraints found in {}", args.config_dir.display());
        std::process::exit(1);
    }

    let base_time: DateTime<Utc> = DateTime::parse_from_rfc3339(&args.start)
        .unwrap_or_else(|e| panic!("bad --start {:?}: {e}", args.start))
        .with_timezone(&Utc);

    let seed = args.seed.unwrap_or_else(|| rand::rng().random());
    let mut rng = StdRng::seed_from_u64(seed);
    let pool = deviation_pool(&catalog);

    // choose which cases deviate before any trace is built
    let violation_rate = args.violation_rate.clamp(0.0, 1.0);
    let deviant_cases: HashSet<u64> = match args.violation_sampling {
        // exactly round(cases * rate) deviate
        ViolationSampling::Exact => {
            let n_deviant = ((args.cases as f64) * violation_rate).round() as usize;
            let mut indices: Vec<u64> = (0..args.cases).collect();
            indices.shuffle(&mut rng);
            indices.into_iter().take(n_deviant).collect()
        }
        // independent coin flip per case, drawn in case order
        ViolationSampling::Bernoulli => (0..args.cases)
            .filter(|_| rng.random_bool(violation_rate))
            .collect(),
    };

    // plan all cases
    struct EventWithTimestamp {
        timestamp_millis: i64,
        case_id: String,
        event_id: String,
        attributes: HashMap<String, f64>,
    }
    let mut stream: Vec<EventWithTimestamp> = Vec::new();
    let mut cases_meta = String::new();

    for case_number in 0..args.cases {
        let case_id = format!("{}-{}", args.case_prefix, case_number);

        // pick deviations for a deviant case: 1..=2 distinct-target deviations
        let mut deviations: Vec<Deviation> = Vec::new();
        if !pool.is_empty() && deviant_cases.contains(&case_number) {
            let max_amount_of_deviations = rng.random_range(1..=2usize);
            let mut taken_targets: HashSet<String> = HashSet::new();
            let mut attempts = 0;
            while deviations.len() < max_amount_of_deviations && attempts < 20 {
                attempts += 1;
                let deviation = pool[rng.random_range(0..pool.len())].clone();
                // at most one deviation per constraint target, and EarlyEnd
                // at most once per case
                if let Some(t) = deviation_target(&deviation, &catalog)
                    && !taken_targets.insert(t)
                {
                    continue;
                }
                if matches!(deviation, Deviation::EarlyEnd)
                    && deviations.iter().any(|e| matches!(e, Deviation::EarlyEnd))
                {
                    continue;
                }
                deviations.push(deviation);
            }
        }

        let case_plan = build_case(&catalog, deviations, &mut rng);

        // assign business timestamps
        let case_start_ms = (case_number as f64 * args.case_gap_secs * 1000.0) as i64
            + if args.no_jitter {
                0
            } else {
                rng.random_range(0..(args.case_gap_secs * 300.0).max(1.0) as i64)
            };
        let mut t_ms = case_start_ms;
        let mut timer_starts: HashMap<usize, i64> = HashMap::new();
        for (position, event) in case_plan.events.iter().enumerate() {
            if position > 0 {
                let gap = if args.no_jitter {
                    args.step_secs
                } else {
                    args.step_secs * rng.random_range(0.6..=1.4)
                };
                t_ms += (gap * 1000.0) as i64;
            }
            if let Some(ti) = event.starts_timer {
                timer_starts.insert(ti, t_ms);
            }
            if let Some(ti) = event.stops_timer {
                // a non-adjacent stop must land past the timer duration
                // an adjacent stop keeps its natural (small) gap
                if let Some(&start_ms) = timer_starts.get(&ti) {
                    let spec = &catalog.time[ti];
                    let needed = start_ms + (spec.duration_secs * 1000.0) as i64;
                    let elapsed = t_ms - start_ms;
                    let adjacent = elapsed < (spec.duration_secs * 500.0) as i64;
                    if !adjacent && t_ms <= needed {
                        t_ms = needed + (args.step_secs * 1000.0) as i64;
                    }
                }
            }
            stream.push(EventWithTimestamp {
                timestamp_millis: t_ms,
                case_id: case_id.clone(),
                event_id: event.event_id.clone(),
                attributes: event.attributes.clone(),
            });
        }

        let names: Vec<&str> = case_plan.deviations.iter().map(deviation_name).collect();
        cases_meta.push_str(&json!({"case_id": case_id, "deviations": names}).to_string());
        cases_meta.push('\n');
    }

    // global stream order = timestamp order
    stream.sort_by(|a, b| a.timestamp_millis.cmp(&b.timestamp_millis));

    fs::create_dir_all(&args.out_dir).unwrap();
    let mut manifest = String::new();
    for (seq, event) in stream.iter().enumerate() {
        let timestamp = base_time + TimeDelta::milliseconds(event.timestamp_millis);
        let timestamp_str = timestamp.to_rfc3339_opts(SecondsFormat::Millis, false);
        let filename = format!("{seq:05}_{}_{}.xes", event.case_id, event.event_id);
        let xes_event = render_xes(
            &event.case_id,
            &event.event_id,
            &event.attributes,
            &timestamp_str,
        );
        fs::write(args.out_dir.join(&filename), &xes_event).unwrap();
        let record = json!({
            "seq": seq,
            "case_id": event.case_id,
            "event_id": event.event_id,
            "constraint_id": null,
            "file": filename,
            "attributes": event.attributes,
            "timestamp": timestamp_str,
        });
        manifest.push_str(&record.to_string());
        manifest.push('\n');
    }

    let mut seed_text = String::new();
    seed_text.push_str(&json!({"seed": seed}).to_string());
    seed_text.push('\n');

    fs::write(args.out_dir.join(&args.manifest), manifest).unwrap();
    fs::write(args.out_dir.join("cases.jsonl"), cases_meta).unwrap();
    fs::write(args.out_dir.join("seed.txt"), seed_text).unwrap();

    println!(
        "wrote {} events across {} case(s), {} deviant ({} constraints, seed {}) to {}\n  manifest: {}\n  deviations: cases.jsonl",
        stream.len(),
        args.cases,
        deviant_cases.len(),
        constraint_ids.len(),
        seed,
        args.out_dir.display(),
        args.out_dir.join(&args.manifest).display(),
    );
}
