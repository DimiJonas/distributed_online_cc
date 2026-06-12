use std::time::Duration;

use tokio::time::sleep;

use crate::{state::app_state::AppState, types::CaseId};

/// this task handles case end
/// so it makes sure, that all state (tokens and results) are removed at some point
///
/// should utilize the grace period for closing a case, that is needed so tasks can finish up their results
/// via handle_case_end methods and emit them and then the tokens and results are cleared AFTER this
///
/// probably:
/// calls case end on constraint handlers
/// calls clean up on constraint handlers
///
/// for some time after the clean up the closed case id has to be remembered so that a super late event does not open the case again and pollute the memory
/// so probably has to spawn some other task with a timer that is supposed to also clear the completed cases memory at some point
pub async fn case_closing_task(state: AppState, case_id: CaseId) {
    for handler in state.handlers.all() {
        handler.handle_case_end(&case_id, state.clone()).await
    }

    // TODO: what if no end activity is configured/received on a node?
    //  -> need some kind of end signal handling
    //      -> should be propagated through the overlay
    // TODO: make grace period configurable
    let case_end_grace_period = Duration::from_secs(10);
    let case_id = case_id.clone();
    tokio::spawn(async move {
        sleep(case_end_grace_period).await;
        log::info!("closing case after case end grace period - removing results from store");
        let mut result_store = state.cc_results.lock().unwrap();
        result_store.remove_case(&case_id);

        // TODO: remember case id for some time so a super late event for that case does not open the case again but then delete is also
    });
}
