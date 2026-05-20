use crate::impl_constraint_handler_fields;
use crate::protocol::constraint_logic::constraint_handler_trait::ConstraintHandler;
use crate::state::app_state::AppState;
use crate::types::TracedEvent;
use async_trait::async_trait;

pub struct EndActivityHandler {
    pub event_id: String,
    pub constraint_id: String,
}

#[async_trait]
impl ConstraintHandler for EndActivityHandler {
    impl_constraint_handler_fields!(EndActivityHandler);

    // notify all handlers that this case has ended
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!("received end event for case {}", event.case_id);
        for handler in state.handlers.all() {
            handler.handle_case_end(&event.case_id, state.clone()).await
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        config::node_config::ConstraintConfig,
        protocol::constraint_logic::{
            constraint_handler_trait::ConstraintHandler, end_activity_handler::EndActivityHandler,
        },
        state::app_state::AppState,
        types::ConstraintType,
        util::test_tools::make_config_with_constraints,
    };

    use crate::util::test_tools::make_traced_event;

    /// The handler exposes the correct event_id and constraint_id via the macro.
    #[test]
    fn handler_fields_correct() {
        let h = EndActivityHandler {
            event_id: "end_event".to_string(),
            constraint_id: "ea-1".to_string(),
        };
        assert_eq!(h.event_id(), "end_event");
        assert_eq!(h.constraint_id(), "ea-1");
    }

    /// handle_event triggers handle_case_end on all other handlers.
    /// Verified via the token store: a co-registered ResponseProducer finalises
    /// its result after receiving case-end.
    #[tokio::test]
    async fn handle_event_triggers_case_end_on_all_handlers() {
        let config = make_config_with_constraints(vec![
            ConstraintConfig::EndActivity {
                constraint_id: "ea".to_string(),
                event_id: "end".to_string(),
            },
            ConstraintConfig::ResponseProducer {
                constraint_id: "rp".to_string(),
                event_id: "produce".to_string(),
            },
        ]);
        let (event_tx, _rx) = tokio::sync::mpsc::channel(16);
        let state = AppState::new(config, event_tx);

        {
            let mut store = state.cc_results.lock().unwrap();
            store.register_case(
                "case-1".to_string(),
                &[
                    ("ea".to_string(), ConstraintType::EndActivity),
                    ("rp".to_string(), ConstraintType::Response),
                ],
            );
        }

        let handler = EndActivityHandler {
            event_id: "end".to_string(),
            constraint_id: "ea".to_string(),
        };
        handler
            .handle_event(&make_traced_event("case-1", "end"), state.clone())
            .await;

        let store = state.cc_results.lock().unwrap();
        assert!(
            store
                .get_constraint(&"case-1".to_string(), &"rp".to_string())
                .is_some()
        );
    }

    /// handle_event does not panic when no other handlers are registered.
    #[tokio::test]
    async fn handle_event_no_other_handlers_does_not_panic() {
        let config = make_config_with_constraints(vec![ConstraintConfig::EndActivity {
            constraint_id: "ea".to_string(),
            event_id: "end".to_string(),
        }]);
        let (event_tx, _rx) = tokio::sync::mpsc::channel(16);
        let state = AppState::new(config, event_tx);

        {
            let mut store = state.cc_results.lock().unwrap();
            store.register_case(
                "case-x".to_string(),
                &[("ea".to_string(), ConstraintType::EndActivity)],
            );
        }

        let handler = EndActivityHandler {
            event_id: "end".to_string(),
            constraint_id: "ea".to_string(),
        };
        handler
            .handle_event(&make_traced_event("case-x", "end"), state)
            .await;
    }
}
