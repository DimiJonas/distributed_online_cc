//! gen-events: generate an xes event stream from a systems node configs
//!
//! event id is taken from the configs
//!
//! output (into --out-dir):
//!   * one .xes file per event
//!   * manifest.jsonl: one line per event with seq, case_id, event_id, file, attributes, timestamp to fix the events as an ordered stream
//!
//! the same stream can be replayed against both the distributed and the central system

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use clap::Parser;
use online_cc::config::node_config::{ConstraintConfig, NodeConfig, SensorConstraintMode};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(about = "Generate an XES event stream from a systems node configs")]
struct Args {
    // directory of *-config.toml files
    // it does not matter if the configs are from a distributed or a centralized system
    // because the configs are only read to collect the set of configured constraints with their event ids
    #[arg(long)]
    config_dir: PathBuf,

    // output directory for the .xes files and the manifest
    #[arg(long, default_value = "run/events")]
    out_dir: PathBuf,

    // number of cases
    #[arg(long, default_value_t = 1)]
    cases: u64,

    #[arg(long, default_value = "case")]
    case_prefix: String,

    // which side of each sensor threshold to emit: happy (satisfies constraint) | sad (violates constraint) | both
    #[arg(long, default_value = "happy")]
    sensor: String,

    // emit one event per sensor constraint instead of one bundled event per event_id carrying all sibling sensor attributes
    // splits let the sender route by constraint_id to the exact node
    #[arg(long, default_value_t = false)]
    split_sensors: bool,

    // file listing event_ids to fix the order within a case
    #[arg(long)]
    order: Option<PathBuf>,

    // shuffle event order within each case
    #[arg(long, default_value_t = false)]
    shuffle: bool,
    // used to generate the event order
    #[arg(long, default_value_t = 0)]
    seed: u64,

    // add a time:timestamp to every event
    #[arg(long, default_value_t = false)]
    timestamps: bool,
    // RFC3339 base timestamp (with --timestamps)
    #[arg(long, default_value = "2024-01-01T00:00:00Z")]
    start: String,
    // seconds between consecutive events (with --timestamps).
    #[arg(long, default_value_t = 1.0)]
    step_secs: f64,

    // route map filename (written to --out-dir)
    #[arg(long, default_value = "manifest.jsonl")]
    manifest: String,
}

#[derive(Clone)]
struct Sensor {
    constraint_id: String,
    attribute_key: String,
    mode: SensorConstraintMode,
    value: f64,
}

fn fmt_num(v: f64) -> String {
    if v.fract() == 0.0 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

// a value for attr <op> threshold that satisfies (happy) or violates (sad) it
fn sensor_value(mode: &SensorConstraintMode, threshold: f64, happy: bool) -> f64 {
    match mode {
        SensorConstraintMode::Lt => {
            if happy {
                threshold - 1.0
            } else {
                threshold
            }
        }
        SensorConstraintMode::Gt => {
            if happy {
                threshold + 1.0
            } else {
                threshold
            }
        }
        SensorConstraintMode::Lte => {
            if happy {
                threshold
            } else {
                threshold + 1.0
            }
        }
        SensorConstraintMode::Gte => {
            if happy {
                threshold
            } else {
                threshold - 1.0
            }
        }
        SensorConstraintMode::Eq => {
            if happy {
                threshold
            } else {
                threshold + 1.0
            }
        }
    }
}

fn render_xes(
    case_id: &str,
    event_id: &str,
    attrs: &HashMap<String, f64>,
    ts: Option<&str>,
) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    s.push_str("<log xes.version=\"1.0\" xmlns=\"http://www.xes-standard.org/\">\n");
    s.push_str("  <trace>\n");
    s.push_str(&format!(
        "    <string key=\"concept:name\" value=\"{case_id}\"/>\n"
    ));
    s.push_str("    <event>\n");
    s.push_str(&format!(
        "      <string key=\"concept:name\" value=\"{event_id}\"/>\n"
    ));
    for (k, v) in attrs {
        s.push_str(&format!(
            "      <float key=\"{k}\" value=\"{}\"/>\n",
            fmt_num(*v)
        ));
    }
    if let Some(ts) = ts {
        s.push_str(&format!(
            "      <date key=\"time:timestamp\" value=\"{ts}\"/>\n"
        ));
    }
    s.push_str("    </event>\n  </trace>\n</log>\n");
    s
}

// parse every *-config.toml in a dir
fn load_configs(dir: &Path) -> Vec<NodeConfig> {
    let mut out = Vec::new();
    let mut paths: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .collect();
    paths.sort();
    for p in paths {
        if p.extension().and_then(|e| e.to_str()) == Some("toml") {
            let text = fs::read_to_string(&p).unwrap();
            let cfg: NodeConfig =
                toml::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", p.display()));
            out.push(cfg);
        }
    }
    out
}

fn main() {
    let args = Args::parse();

    let configs = load_configs(&args.config_dir);

    // event ids + sensor specs, taken from the constraints
    let mut event_ids: HashSet<String> = HashSet::new();
    let mut sensors: HashMap<String, Vec<Sensor>> = HashMap::new();
    for cfg in &configs {
        for c in &cfg.constraints {
            event_ids.insert(c.event_id().to_string());
            if let ConstraintConfig::SensorCollector {
                constraint_id,
                event_id,
                attribute_key,
                value,
                mode,
                ..
            } = c
            {
                sensors.entry(event_id.clone()).or_default().push(Sensor {
                    constraint_id: constraint_id.clone(),
                    attribute_key: attribute_key.clone(),
                    mode: mode.clone(),
                    value: *value,
                });
            }
        }
    }
    if event_ids.is_empty() {
        eprintln!(
            "no constraints/event ids found in {}",
            args.config_dir.display()
        );
        std::process::exit(1);
    }

    // within-case order
    let mut base_order: Vec<String> = event_ids.iter().cloned().collect();
    if let Some(order_file) = &args.order {
        let listed: Vec<String> = fs::read_to_string(order_file)
            .unwrap()
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect();
        for e in &listed {
            if !event_ids.contains(e) {
                eprintln!("warning: --order lists unknown event id {e:?}");
            }
        }
        base_order = listed
            .into_iter()
            .filter(|e| event_ids.contains(e))
            .collect();
    }

    let sides: Vec<bool> = match args.sensor.as_str() {
        "happy" => vec![true],
        "sad" => vec![false],
        "both" => vec![true, false],
        other => {
            eprintln!("--sensor must be happy|sad|both (got {other:?})");
            std::process::exit(1);
        }
    };

    let base_time: DateTime<Utc> = DateTime::parse_from_rfc3339(&args.start)
        .unwrap_or_else(|e| panic!("bad --start {:?}: {e}", args.start))
        .with_timezone(&Utc);

    fs::create_dir_all(&args.out_dir).unwrap();
    let manifest_path = args.out_dir.join(&args.manifest);
    let mut manifest = String::new();

    let mut seq: u64 = 0;
    let mut tick: u64 = 0;
    for ci in 0..args.cases {
        let case_id = format!("{}-{}", args.case_prefix, ci);
        let mut order = base_order.clone();
        if args.shuffle {
            let mut rng = StdRng::seed_from_u64(args.seed.wrapping_add(ci));
            order.shuffle(&mut rng);
        }
        for event_id in &order {
            let specs: Vec<Sensor> = sensors.get(event_id).cloned().unwrap_or_default();
            // emission units: bundled = one unit with all specs; split = one per spec
            let units: Vec<(Option<String>, Vec<Sensor>)> =
                if args.split_sensors && !specs.is_empty() {
                    specs
                        .iter()
                        .map(|s| (Some(s.constraint_id.clone()), vec![s.clone()]))
                        .collect()
                } else {
                    vec![(None, specs.clone())]
                };
            for (cid, unit_specs) in &units {
                for &happy in &sides {
                    let mut attrs: HashMap<String, f64> = HashMap::new();
                    for spec in unit_specs {
                        attrs.insert(
                            spec.attribute_key.clone(),
                            sensor_value(&spec.mode, spec.value, happy),
                        );
                    }
                    let ts_str = if args.timestamps {
                        let ms = (args.step_secs * tick as f64 * 1000.0) as i64;
                        let t = base_time + TimeDelta::milliseconds(ms);
                        tick += 1;
                        Some(t.to_rfc3339_opts(SecondsFormat::Millis, false))
                    } else {
                        None
                    };

                    let xes = render_xes(&case_id, event_id, &attrs, ts_str.as_deref());
                    let tag = if !attrs.is_empty() && !happy {
                        "_sad"
                    } else {
                        ""
                    };
                    let cid_tag = cid.as_ref().map(|c| format!("_{c}")).unwrap_or_default();
                    let fname = format!("{seq:05}_{case_id}_{event_id}{cid_tag}{tag}.xes");
                    fs::write(args.out_dir.join(&fname), &xes).unwrap();

                    let rec = json!({
                        "seq": seq,
                        "case_id": case_id,
                        "event_id": event_id,
                        "constraint_id": cid,
                        "file": fname,
                        "attributes": attrs,
                        "timestamp": ts_str,
                    });
                    manifest.push_str(&rec.to_string());
                    manifest.push('\n');
                    seq += 1;
                }
            }
        }
    }

    fs::write(&manifest_path, manifest).unwrap();
    println!(
        "wrote {seq} events ({} case(s), {} event ids/case) to {}\n  manifest: {}",
        args.cases,
        base_order.len(),
        args.out_dir.display(),
        manifest_path.display()
    );
}
