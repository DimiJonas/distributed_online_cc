#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        app::event_q_processor::spawn_event_processor,
        config::node_config::{
            AppearanceCounterReset, ConstraintConfig, DistributedAppearanceCounterReset, MinMaxMode,
        },
        types::ConstraintHandlerId,
        util::test_tools::{NodeConfigBuilder, make_traced_event},
    };

    /// Test A — with seven constraints all configured on the SAME event_id, the
    /// registry must build and index all seven, and a single event pushed through
    /// the real processor must reach every handler (each leaves a distinct local
    /// mark keyed by its own constraint_id).
    #[tokio::test]
    async fn many_constraints_one_event_id_all_created_and_dispatched() {
        const EVENT: &str = "shared";
        const CASE: &str = "caseA";

        let (state, event_tx, event_rx) = NodeConfigBuilder::new()
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
        event_tx.send(make_traced_event(CASE, EVENT)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // each ResponseProducer left an open token under its own constraint_id
        for cid in ["rp1", "rp2", "rp3"] {
            assert!(
                state.token_out.lock().unwrap().has_open_token(
                    &CASE.to_string(),
                    &cid.to_string(),
                    ConstraintHandlerId::ResponseProducer,
                ),
                "ResponseProducer {cid} did not run on the shared event"
            );
        }
        // each AppearanceCounter created its counter meta token
        for cid in ["ap1", "ap2"] {
            assert!(
                state.meta_token_store.lock().unwrap().contains_meta_token(
                    &CASE.to_string(),
                    &cid.to_string(),
                    ConstraintHandlerId::AppearanceCounter,
                ),
                "AppearanceCounter {cid} did not run on the shared event"
            );
        }
        // the distributed appearance counter incremented locally
        assert!(
            state.meta_token_store.lock().unwrap().contains_meta_token(
                &CASE.to_string(),
                &"dac1".to_string(),
                ConstraintHandlerId::DistributedAppearanceCounter,
            ),
            "DistributedAppearanceCounter dac1 did not run on the shared event"
        );
        // the order-first handler recorded the first execution
        assert!(
            state.token_out.lock().unwrap().has_open_token(
                &CASE.to_string(),
                &"of1".to_string(),
                ConstraintHandlerId::OrderFirst,
            ),
            "OrderFirst of1 did not run on the shared event"
        );
    }

    /// Test B — with several result-producing constraints sharing one event_id, a
    /// real case end (driven by an EndActivity event that spawns the real
    /// case-closing task) must leave no result missing: every configured
    /// constraint_id must have a recorded result.
    #[tokio::test]
    async fn shared_event_id_no_missing_results() {
        const EVENT: &str = "evt";
        const END: &str = "end";
        const CASE: &str = "caseB";

        let constraint_ids = ["r1", "r2", "r3", "a1", "a2", "o1"];

        let (state, event_tx, event_rx) = NodeConfigBuilder::new()
            .port(31010)
            // long grace so results are still readable when we assert; the test
            // returns well before it elapses (the background task is dropped).
            .grace_period(Duration::from_secs(5))
            .retention_period(Duration::from_millis(50))
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
                // drives the real case-closing task when the "end" event arrives
                ConstraintConfig::EndActivity {
                    constraint_id: "ea".to_string(),
                    event_id: END.to_string(),
                },
            ])
            .build_state_with_event_q();
        spawn_event_processor(state.clone(), event_rx);

        // one real event reaches every handler on this event_id (FIFO: processed
        // before the end event below)
        event_tx.send(make_traced_event(CASE, EVENT)).await.unwrap();
        // then end the case for real -> EndActivity spawns the case-closing task,
        // which runs handle_case_end on every handler
        event_tx.send(make_traced_event(CASE, END)).await.unwrap();

        // give the processor + closing task time to run handle_case_end (well under
        // the 5s grace period, so results are still in the store)
        tokio::time::sleep(Duration::from_millis(200)).await;

        // every configured result-producing constraint left a result — none missing
        let results = state.cc_results.lock().unwrap();
        for cid in constraint_ids {
            assert!(
                results
                    .get_constraint(&CASE.to_string(), &cid.to_string())
                    .is_some(),
                "constraint {cid} has no result after a real dispatch + case end (missing-result bug)"
            );
        }
    }
}
