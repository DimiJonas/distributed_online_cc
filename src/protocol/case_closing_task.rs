use tokio::time::sleep;

use crate::{state::app_state::AppState, types::CaseId};

/// this task handles case end
///
/// calls case end on constraint handlers which makes sure each constraint emitted at least 1 result
/// then sleeps for a configured amount of time in case late events stil come in
/// then closes the case (which blocks further reult emitting) and calls clean up state methods on constraint handlers
/// sleeps for another period (this period resembles the time the case is still remembered for so an even later event does not open it again)
/// then forgets about the case by removing it from case state
pub async fn case_closing_task(state: AppState, case_id: CaseId) {
    for handler in state.handlers.all() {
        // TODO: implement where needed
        handler.handle_case_end(&case_id, state.clone()).await;
    }

    sleep(state.config.case_end_grace_period).await;

    log::info!("closing case {} after case end grace period", case_id);
    {
        let mut case_state = state.case_state.lock().unwrap();
        case_state.close_case(case_id.clone());
    }
    {
        let mut result_store = state.cc_results.lock().unwrap();
        result_store.remove_case(&case_id);
    }

    for handler in state.handlers.all() {
        // TODO: implemented for all handlers
        handler.clean_up_state(&case_id, state.clone()).await;
    }

    sleep(state.config.case_retention_period).await;

    log::debug!("forgetting case {}", case_id);
    {
        let mut case_state = state.case_state.lock().unwrap();
        case_state.remove(&case_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::constraint_logic::constraint_handler_trait::ConstraintHandler;
    // case_closing_task
    use crate::state::app_state::AppState;
    use crate::types::{ConstraintHandlerId, ConstraintResultType, ConstraintType, TracedEvent};
    use crate::util::test_tools::{make_config_with_port, make_state_from_config};
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// Records the order in which its hooks are called, with a tag so
    /// multiple recording handlers can be told apart in assertions.
    struct RecordingHandler {
        tag: &'static str,
        event_id: String,
        constraint_id: String,
        handler_id: ConstraintHandlerId,
        calls: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl ConstraintHandler for RecordingHandler {
        fn event_id(&self) -> &str {
            &self.event_id
        }
        fn constraint_id(&self) -> &str {
            &self.constraint_id
        }
        fn handler_id(&self) -> ConstraintHandlerId {
            self.handler_id
        }

        async fn handle_event(&self, _event: &TracedEvent, _state: AppState) {}

        async fn handle_case_end(&self, _case_id: &CaseId, _state: AppState) {
            self.calls
                .lock()
                .unwrap()
                .push(format!("{}:handle_case_end", self.tag));
        }

        async fn clean_up_state(&self, _case_id: &CaseId, _state: AppState) {
            self.calls
                .lock()
                .unwrap()
                .push(format!("{}:clean_up_state", self.tag));
        }
    }

    /// Builds an AppState with the given grace/retention periods and a
    /// single RecordingHandler registered, sharing `calls` so the test can
    /// inspect call order after running the task.
    fn make_state_with_recording_handler(
        grace_period: Duration,
        retention_period: Duration,
        calls: Arc<Mutex<Vec<String>>>,
    ) -> AppState {
        let mut config = make_config_with_port(0);
        config.case_end_grace_period = grace_period;
        config.case_retention_period = retention_period;
        // No ConstraintConfig variant maps to a test mock, so build the
        // base state from config (empty handler registry)...
        let mut state = make_state_from_config(config);

        // ...then register the recording handler directly into a fresh
        // registry and swap it in. HandlerRegistry::register is pub, and
        // AppState.handlers is `pub handlers: Arc<HandlerRegistry>`, so we
        // can just replace the Arc wholesale.
        let mut registry = crate::config::handler_registry::HandlerRegistry::new();
        registry.register(Box::new(RecordingHandler {
            tag: "h1",
            event_id: "end".to_string(),
            constraint_id: "c1".to_string(),
            handler_id: ConstraintHandlerId::EndActivity,
            calls,
        }));
        state.handlers = Arc::new(registry);
        state
    }

    /// Advances simulated time by `step`, yielding repeatedly before and
    /// after so the spawned task under test actually gets polled and
    /// reaches/consumes its timer.
    ///
    /// `tokio::time::advance` only fires timers the runtime is already
    /// parked on. If the spawned task hasn't reached its `sleep().await`
    /// yet (e.g. it's still working through a handler loop when we call
    /// `advance`), the clock jumps past the timer's deadline without the
    /// task ever being woken to observe it — `advance` does not retroactively
    /// wake tasks that register a timer "in the past" relative to the new
    /// clock in every executor configuration, so we yield first to let the
    /// task reach the timer, then advance, then yield again to let it run
    /// past the now-elapsed sleep and reach the next await point.
    async fn advance_and_settle(step: Duration) {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(step).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn calls_handle_case_end_before_sleeping() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let state = make_state_with_recording_handler(
            Duration::from_secs(10),
            Duration::from_secs(15),
            calls.clone(),
        );
        let case_id: CaseId = "case-1".to_string();

        let handle = tokio::spawn(case_closing_task(state, case_id));

        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.lock().unwrap().as_slice(), ["h1:handle_case_end"]);

        advance_and_settle(Duration::from_secs(10)).await; // grace period
        advance_and_settle(Duration::from_secs(15)).await; // retention period
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn closes_case_only_after_grace_period() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let state = make_state_with_recording_handler(
            Duration::from_secs(10),
            Duration::from_secs(15),
            calls,
        );
        let case_id: CaseId = "case-1".to_string();

        state.case_state.lock().unwrap().open_case(case_id.clone());

        let handle = tokio::spawn(case_closing_task(state.clone(), case_id.clone()));

        // Not yet closed: still inside the grace period.
        advance_and_settle(Duration::from_secs(9)).await;
        assert!(!state.case_state.lock().unwrap().is_closed(&case_id));

        // Crosses the grace period boundary: now closed.
        advance_and_settle(Duration::from_secs(2)).await;
        assert!(state.case_state.lock().unwrap().is_closed(&case_id));

        // Drive to completion so the spawned task doesn't outlive the test.
        advance_and_settle(Duration::from_secs(15)).await;
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn clean_up_state_runs_after_close_and_before_retention_sleep() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let state = make_state_with_recording_handler(
            Duration::from_secs(10),
            Duration::from_secs(15),
            calls.clone(),
        );
        let case_id: CaseId = "case-1".to_string();
        state.case_state.lock().unwrap().open_case(case_id.clone());

        let handle = tokio::spawn(case_closing_task(state.clone(), case_id.clone()));

        // Advance past the grace period only.
        advance_and_settle(Duration::from_secs(10)).await;

        assert_eq!(
            calls.lock().unwrap().as_slice(),
            ["h1:handle_case_end", "h1:clean_up_state"]
        );
        assert!(state.case_state.lock().unwrap().is_closed(&case_id));
        // Not removed yet — retention period hasn't elapsed.
        assert!(state.case_state.lock().unwrap().has_case(&case_id));

        advance_and_settle(Duration::from_secs(15)).await;
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn removes_case_only_after_retention_period() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let state = make_state_with_recording_handler(
            Duration::from_secs(10),
            Duration::from_secs(15),
            calls,
        );
        let case_id: CaseId = "case-1".to_string();
        state.case_state.lock().unwrap().open_case(case_id.clone());

        let handle = tokio::spawn(case_closing_task(state.clone(), case_id.clone()));

        advance_and_settle(Duration::from_secs(10)).await; // grace period
        assert!(state.case_state.lock().unwrap().has_case(&case_id));

        advance_and_settle(Duration::from_secs(14)).await; // not quite retention
        assert!(
            state.case_state.lock().unwrap().has_case(&case_id),
            "case should still be remembered just before retention period elapses"
        );

        advance_and_settle(Duration::from_secs(2)).await; // crosses retention boundary
        assert!(
            !state.case_state.lock().unwrap().has_case(&case_id),
            "case should be forgotten once retention period elapses"
        );

        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn removes_case_from_result_store_after_grace_period() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let state = make_state_with_recording_handler(
            Duration::from_secs(10),
            Duration::from_secs(15),
            calls,
        );
        let case_id: CaseId = "case-1".to_string();

        // Seed a result for this case so we can observe its removal.
        {
            let mut results = state.cc_results.lock().unwrap();
            results.upsert(
                &case_id,
                &"c1".to_string(),
                ConstraintType::Response,
                ConstraintResultType::Satisfied,
                "seed".to_string(),
            );
        }

        let handle = tokio::spawn(case_closing_task(state.clone(), case_id.clone()));

        advance_and_settle(Duration::from_secs(10)).await;

        assert!(
            state
                .cc_results
                .lock()
                .unwrap()
                .remove_case(&case_id)
                .is_none(),
            "result store should already be cleared for this case after the grace period"
        );

        advance_and_settle(Duration::from_secs(15)).await;
        handle.await.unwrap();
    }
}
