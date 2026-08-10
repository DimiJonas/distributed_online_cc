use std::net::SocketAddr;

use chrono::Utc;

use crate::{
    config::node_config::ConfigMode,
    conformance_checking::case_closing_task::case_closing_task,
    measurement::{api::case_end_seen, record::CaseEndSource},
    state::{app_state::AppState, case_end_gossip_cache::MergeResult},
    types::CaseEnd,
};

pub async fn handle_case_end(state: AppState, case_end: CaseEnd, source_peer: Option<SocketAddr>) {
    log::debug!(
        "case end signal handling: received case end info: {}",
        case_end
    );
    match state.config.mode {
        ConfigMode::Decentralized => {
            // case end might already be known - handle new and known information correctly
            handle_case_end_info(
                state.clone(),
                vec![case_end],
                CaseEndSource::EndEvent,
                source_peer,
            )
            .await;
        }
        ConfigMode::Central => {
            // no gossip - spawn case closing task directly
            spawn_case_closing_task(state.clone(), &case_end);
        }
    }
}

pub async fn handle_case_end_info(
    state: AppState,
    case_end_info: Vec<CaseEnd>,
    source: CaseEndSource,
    source_peer: Option<SocketAddr>,
) {
    // update stream clock because new event time was observed
    state
        .stream_clock
        .advance_to_max(case_end_info.iter().map(|case_end| case_end.timestamp));

    let merge_result = {
        // the ended case record is the deduplication rule that needs to be considered at the merge
        let case_state = state.case_state.lock().unwrap();
        let mut ce_cache = state.dist().ce_gossip_cache.lock().unwrap();
        ce_cache.merge(case_end_info, Utc::now(), |case_id| {
            case_state.has_ended_record(case_id)
        })
    };
    log::debug!("merge result: {:?}", merge_result);
    apply_merge(merge_result, state.clone(), source, source_peer).await;
}

async fn apply_merge(
    merge_result: MergeResult,
    state: AppState,
    source: CaseEndSource,
    source_peer: Option<SocketAddr>,
) {
    if merge_result.straggler_count > 0 {
        log::debug!(
            "case end signal handling: ignored {} straggler case end(s) for already closed cases",
            merge_result.straggler_count
        );
    }

    // spawn a case closing task for newly observed case ends
    for case_end in &merge_result.new {
        // record case end
        match source {
            CaseEndSource::PushGossip | CaseEndSource::AntiEntropy => {
                // record case end for measurements
                case_end_seen(case_end.case_id.clone(), source, source_peer);
            }
            CaseEndSource::EndEvent => {
                // end acivity handler already recorded case end
            }
        }

        spawn_case_closing_task(state.clone(), case_end);
    }

    // spread gossip of new and updated case ends
    let mut to_push = merge_result.new;
    to_push.extend(merge_result.updated);
    if let Err(e) = state.dist().outbound_gossip_q.send(to_push).await {
        log::error!("error passing push gossip to gossip manager: {}", e);
    }
}

fn spawn_case_closing_task(state: AppState, case_end: &CaseEnd) {
    let case_id = case_end.case_id.clone();

    // check if case end already known in which case a case closing task was already spawned previously
    {
        let mut case_state = state.case_state.lock().unwrap();
        if !case_state.mark_ended(&case_id, case_end.timestamp, Utc::now()) {
            log::debug!(
                "case end signal handling: case {} has already ended; not spawning another closing task",
                case_id
            );
            return;
        }
        case_state.end_case(case_id.clone());
    }

    // spawn case closing task
    let handle = tokio::spawn(case_closing_task(state.clone(), case_id.clone()));
    // spawn a task that logs the error if the case closing task panics
    tokio::spawn(async move {
        if let Err(e) = handle.await {
            log::error!("case closing task for case {} failed: {}", case_id, e);
        }
    });
}
