use crate::{state::app_state::AppState, types::TracedEvent};

use tokio::sync::mpsc;

/*
   useful for
       - serialization
       - no race conditions
       - easy stateful checks
       - events are in order and processed in order

   it might be better to not spawn new tasks to handle each event and just use the event q processor as a dispatcher because
       - handling is probably fast anyway (lock, mutate, unlock, ...)
       - ordering matters for correctness
       - bottleneck is probably network IO anyway
*/
pub fn spawn_event_processor(state: AppState, mut event_q: mpsc::Receiver<TracedEvent>) {
    tokio::spawn(async move {
        loop {
            if let Some(event) = event_q.recv().await {
                log::info!("processing event: {}", event);
                log::debug!("event ({:?})", event);

                // is the case already closed? -> drop event
                // is it a new case? -> register it in case state and result store
                {
                    let mut case_state = state.case_state.lock().unwrap();
                    if case_state.is_closed(&event.case_id) {
                        log::warn!(
                            "the case of event {} is already closed. dropping event",
                            event
                        );
                        continue;
                    }

                    if !case_state.has_case(&event.case_id) {
                        log::info!(
                            "event queue processor: new case detected; id={}",
                            &event.case_id
                        );
                        case_state.open_case(event.case_id.clone());
                    }
                }
                {
                    let mut result_store = state.cc_results.lock().unwrap();
                    if !result_store.has_case(&event.case_id) {
                        result_store.register_case(event.case_id.clone());
                    }
                }

                // dispatch relevant handlers
                let event_id = &event.event_id;
                for handler in state.handlers.get_by_event_id(event_id) {
                    handler.handle_event(&event, state.clone()).await;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use crate::util::test_tools::{
        make_state_with_config_and_event_q, make_state_with_event_q,
        make_traced_event_with_fixed_timestamp,
    };
    use crate::{
        app::event_q_processor::spawn_event_processor, config::node_config::ConstraintConfig,
        util::test_tools::make_config_with_constraints,
    };

    /// Sending an event for a new case registers it in the result store.
    #[tokio::test]
    async fn new_case_is_registered_in_result_store() {
        let config = make_config_with_constraints(vec![ConstraintConfig::EndActivity {
            constraint_id: "ea".to_string(),
            event_id: "end".to_string(),
        }]);
        let (event_tx, event_rx) = mpsc::channel(16);
        let state = make_state_with_config_and_event_q(config, event_tx.clone());
        spawn_event_processor(state.clone(), event_rx);

        event_tx
            .send(make_traced_event_with_fixed_timestamp(
                "case-new",
                "some_event",
            ))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let store = state.cc_results.lock().unwrap();
        assert!(store.has_case(&"case-new".to_string()));
    }

    /// Sending two events for the same case must NOT create a duplicate entry.
    #[tokio::test]
    async fn same_case_is_not_registered_twice() {
        let (event_tx, event_rx) = mpsc::channel(16);
        let state = make_state_with_event_q(event_tx.clone());

        spawn_event_processor(state.clone(), event_rx);

        event_tx
            .send(make_traced_event_with_fixed_timestamp("case-dup", "e1"))
            .await
            .unwrap();
        event_tx
            .send(make_traced_event_with_fixed_timestamp("case-dup", "e2"))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let store = state.cc_results.lock().unwrap();
        assert!(store.has_case(&"case-dup".to_string()));
    }

    /// Events for different cases each get their own registration.
    #[tokio::test]
    async fn multiple_cases_each_registered() {
        let (event_tx, event_rx) = mpsc::channel(16);
        let state = make_state_with_event_q(event_tx.clone());

        spawn_event_processor(state.clone(), event_rx);

        for id in ["alpha", "beta", "gamma"] {
            event_tx
                .send(make_traced_event_with_fixed_timestamp(id, "e"))
                .await
                .unwrap();
        }

        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        let store = state.cc_results.lock().unwrap();
        for id in ["alpha", "beta", "gamma"] {
            assert!(store.has_case(&id.to_string()), "case {id} missing");
        }
    }

    /// Dropping the sender (channel closed) must not panic the processor task.
    #[tokio::test]
    async fn processor_does_not_panic_when_sender_dropped() {
        let (event_tx, event_rx) = mpsc::channel(16);
        let state = make_state_with_event_q(event_tx.clone());

        spawn_event_processor(state.clone(), event_rx);
        drop(event_tx);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    /// Events that match a configured handler are dispatched to it.
    /// We verify via the token store (a ResponseProducer creates a token on match).
    #[tokio::test]
    async fn matching_event_is_dispatched_to_handler() {
        let config = make_config_with_constraints(vec![ConstraintConfig::ResponseProducer {
            constraint_id: "rp".to_string(),
            event_id: "produce".to_string(),
        }]);
        let (event_tx, event_rx) = mpsc::channel(16);
        let state = make_state_with_config_and_event_q(config, event_tx.clone());

        // Register the case first so handle_case_end doesn't crash.
        {
            let mut store = state.cc_results.lock().unwrap();
            store.register_case("case-dispatch".to_string());
        }

        spawn_event_processor(state.clone(), event_rx);

        event_tx
            .send(make_traced_event_with_fixed_timestamp(
                "case-dispatch",
                "produce",
            ))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        assert!(
            state.token_out.lock().unwrap().contains_out_token(
                &"case-dispatch".to_string(),
                state
                    .handlers
                    .get_handler_index_by_constraint_id(&"rp".to_string())
                    .unwrap()
            ),
            "token should have been created by the handler"
        );
    }
}
