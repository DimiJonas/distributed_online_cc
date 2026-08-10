#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::sleep;

    use crate::{
        app::event_q_processor::spawn_event_processor,
        config::{
            config_builder::NodeConfigBuilder,
            node_config::{
                AppearanceCounterReset, ConstraintConfig, DistributedAppearanceCounterReset,
                MinMaxMode, ResultBufferConfig,
            },
        },
        util::test_tools::make_traced_event_with_fixed_timestamp,
    };

    /// Test A — with seven constraints all configured on the SAME event_id, the
    /// registry must build and index all seven, and a single event pushed through
    /// the real processor must reach every handler (each leaves a distinct local
    /// mark keyed by its own constraint_id).
    #[tokio::test]
    async fn many_constraints_one_event_id_all_created_and_dispatched() {
        const EVENT: &str = "shared";
        const CASE: &str = "caseA";

        let (state, event_tx, event_rx) = NodeConfigBuilder::new_base_config()
            .port(31000)
            .constraints(vec![
                ConstraintConfig::ResponseProducer {
                    constraint_id: "rp1".to_string(),
                    event_id: EVENT.to_string(),
                },
                ConstraintConfig::ResponseProducer {
                    constraint_id: "rp2".to_string(),
                    event_id: EVENT.to_string(),
                },
                ConstraintConfig::ResponseProducer {
                    constraint_id: "rp3".to_string(),
                    event_id: EVENT.to_string(),
                },
                ConstraintConfig::AppearanceCounter {
                    constraint_id: "ap1".to_string(),
                    event_id: EVENT.to_string(),
                    mode: MinMaxMode::Max,
                    frequency: 5,
                    reset: AppearanceCounterReset::Keep,
                },
                ConstraintConfig::AppearanceCounter {
                    constraint_id: "ap2".to_string(),
                    event_id: EVENT.to_string(),
                    mode: MinMaxMode::Max,
                    frequency: 5,
                    reset: AppearanceCounterReset::Keep,
                },
                ConstraintConfig::DistributedAppearanceCounter {
                    constraint_id: "dac1".to_string(),
                    event_id: EVENT.to_string(),
                    reset: DistributedAppearanceCounterReset::Keep,
                },
                ConstraintConfig::OrderFirst {
                    constraint_id: "of1".to_string(),
                    event_id: EVENT.to_string(),
                },
            ])
            .build_state_with_event_q();
        spawn_event_processor(state.clone(), event_rx);

        // every handler was built ...
        assert_eq!(
            state.handlers.all().count(),
            7,
            "all seven handlers must be built from the config"
        );
        // ... and every one is indexed under the shared event_id (the dispatch path
        // consults exactly this index; the bug was handlers missing from here).
        assert_eq!(
            state.handlers.get_by_event_id(EVENT).len(),
            7,
            "all seven handlers must be indexed under the shared event_id"
        );

        // push one event through the real processor
        event_tx
            .send(make_traced_event_with_fixed_timestamp(CASE, EVENT))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // each ResponseProducer left an open token under its own constraint_id
        for cid in ["rp1", "rp2", "rp3"] {
            assert!(
                state.token_out.lock().unwrap().contains_out_token(
                    &CASE.to_string(),
                    state
                        .handlers
                        .get_handler_index_by_constraint_id(&cid.to_string())
                        .unwrap()
                ),
                "ResponseProducer {cid} did not run on the shared event"
            );
        }
        // each AppearanceCounter created its counter meta token
        for cid in ["ap1", "ap2"] {
            assert!(
                state.meta_token_store.lock().unwrap().contains_meta_token(
                    &CASE.to_string(),
                    state
                        .handlers
                        .get_handler_index_by_constraint_id(&cid.to_string())
                        .unwrap()
                ),
                "AppearanceCounter {cid} did not run on the shared event"
            );
        }
        // the distributed appearance counter incremented locally
        assert!(
            state.meta_token_store.lock().unwrap().contains_meta_token(
                &CASE.to_string(),
                state
                    .handlers
                    .get_handler_index_by_constraint_id(&"dac1".to_string())
                    .unwrap()
            ),
            "DistributedAppearanceCounter dac1 did not run on the shared event"
        );
        // the order-first handler recorded the first execution
        assert!(
            state.token_out.lock().unwrap().contains_out_token(
                &CASE.to_string(),
                state
                    .handlers
                    .get_handler_index_by_constraint_id(&"of1".to_string())
                    .unwrap()
            ),
            "OrderFirst of1 did not run on the shared event"
        );
    }

    #[tokio::test]
    async fn shared_event_id_no_missing_results() {
        const EVENT: &str = "evt";
        const CASE: &str = "caseB";

        let constraint_ids = ["r1", "r2", "r3", "a1", "a2", "o1"];

        let (state, event_tx, event_rx) = NodeConfigBuilder::new_base_config()
            .port(31010)
            .default_result_buffer(ResultBufferConfig::Immediate)
            .constraints(vec![
                ConstraintConfig::ResponseProducer {
                    constraint_id: "r1".to_string(),
                    event_id: EVENT.to_string(),
                },
                ConstraintConfig::ResponseProducer {
                    constraint_id: "r2".to_string(),
                    event_id: EVENT.to_string(),
                },
                ConstraintConfig::ResponseProducer {
                    constraint_id: "r3".to_string(),
                    event_id: EVENT.to_string(),
                },
                ConstraintConfig::AppearanceCounter {
                    constraint_id: "a1".to_string(),
                    event_id: EVENT.to_string(),
                    mode: MinMaxMode::Max,
                    frequency: 5,
                    reset: AppearanceCounterReset::Keep,
                },
                ConstraintConfig::AppearanceCounter {
                    constraint_id: "a2".to_string(),
                    event_id: EVENT.to_string(),
                    mode: MinMaxMode::Max,
                    frequency: 5,
                    reset: AppearanceCounterReset::Keep,
                },
                ConstraintConfig::OrderFirst {
                    constraint_id: "o1".to_string(),
                    event_id: EVENT.to_string(),
                },
            ])
            .build_state_with_event_q();
        spawn_event_processor(state.clone(), event_rx);

        // one real event has to reach every handler registered on this event_id
        event_tx
            .send(make_traced_event_with_fixed_timestamp(CASE, EVENT))
            .await
            .unwrap();

        // let the event processor drain the queue and dispatch to all handlers
        sleep(Duration::from_millis(200)).await;

        // drive the case end hooks the way case_closing_task does, but stop before it
        // closes the case and wipes cc_results, so the verdicts stay observable
        for handler in state.handlers.all() {
            handler
                .handle_case_end(&CASE.to_string(), state.clone())
                .await;
        }

        let results = state.cc_results.lock().unwrap();
        let missing: Vec<&str> = constraint_ids
            .iter()
            .copied()
            .filter(|cid| {
                results
                    .get_constraint(&CASE.to_string(), &cid.to_string())
                    .is_none()
            })
            .collect();
        assert!(
            missing.is_empty(),
            "constraints {missing:?} have no result after a real dispatch + case end (missing-result bug)"
        );
    }
}
