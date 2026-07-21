use chrono::Utc;

use crate::{
    config::node_config::ConfigMode,
    conformance_checking::case_closing_task::case_closing_task,
    state::{app_state::AppState, case_end_gossip_cache::MergeResult},
    types::{CaseEnd, CaseId},
};

pub async fn handle_case_end(state: AppState, case_end: CaseEnd) {
    log::debug!(
        "case end signal handling: received case end info: {}",
        case_end
    );
    match state.config.mode {
        ConfigMode::Distributed => {
            // case end might already be known - handle new and known information correctly
            handle_case_end_info(state.clone(), vec![case_end]).await;
        }
        ConfigMode::Central => {
            // spawn case closing task directly
            spawn_case_closing_task(state.clone(), case_end.case_id.clone());
        }
    }
}

pub async fn handle_case_end_info(state: AppState, case_end_info: Vec<CaseEnd>) {
    let now = Utc::now();
    let merge_result = {
        // update cache if needed
        let mut ce_cache = state.dist().ce_gossip_cache.lock().unwrap();
        ce_cache.merge(case_end_info, now)
    };
    log::debug!("merge result: {:?}", merge_result);
    apply_merge(merge_result, state.clone()).await;
}

async fn apply_merge(res: MergeResult, state: AppState) {
    // spawn a case closing task for newly observed case ends
    for case_end in &res.new {
        spawn_case_closing_task(state.clone(), case_end.case_id.clone());
    }
    // spread gossip of new and updated case ends
    let mut to_push = res.new;
    to_push.extend(res.updated);
    if let Err(e) = state.dist().outbound_gossip_q.send(to_push).await {
        log::error!("error passing push gossip to gossip manager: {}", e);
    }
}

fn spawn_case_closing_task(state: AppState, case_id: CaseId) {
    // spawn case closing task
    let handle = tokio::spawn(case_closing_task(state.clone(), case_id.clone()));
    // spawn a task that logs the error if the case closing task panics
    tokio::spawn(async move {
        if let Err(e) = handle.await {
            log::error!("case closing task for case {} failed: {}", case_id, e);
        }
    });
}
