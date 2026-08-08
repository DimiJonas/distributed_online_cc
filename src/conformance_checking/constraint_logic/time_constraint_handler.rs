use std::{net::SocketAddr, time::Duration};

use async_trait::async_trait;

use crate::{
    config::node_config::MinMaxMode,
    conformance_checking::{
        constraint_logic::constraint_handler_trait::ConstraintHandler, result_emitter::emit_result,
        watchdog_task::watchdog_task,
    },
    impl_constraint_handler_fields,
    measurement::record::ResultTrigger,
    network::{
        message_types::{CCMessage, CCMessageType, PeerMessage},
        tcp_router::send_to_conformance_peer,
    },
    state::app_state::AppState,
    types::{
        CaseId, ConstraintId, ConstraintResultType, ConstraintRole, ConstraintType, EventId,
        EventInstanceId, EventTime, HandlerIndex, RequestId, Token, TracedEvent,
    },
};

pub struct TimeStartHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub constraint_id: ConstraintId,
    pub event_id: EventId,
    pub duration: Duration,
    pub mode: MinMaxMode,
}

#[async_trait]
impl ConstraintHandler for TimeStartHandler {
    impl_constraint_handler_fields!(TimeStartHandler);

    /// start timer
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "time start handler: received configured time start event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        // create a timer token for this event instance
        let token = Token::new(
            event.case_id.clone(),
            self.constraint_id.clone(),
            ConstraintType::Time,
            Some(event.timestamp),
            state.config.node_id,
        );
        let token_id = token.id;

        {
            let mut timer_store = state.timer_token_store.lock().unwrap();
            timer_store.insert_timer_token(token);
        }

        log::info!(
            "time start handler: inserted timer token {} for constraint {}/case {}",
            token_id,
            self.constraint_id,
            event.case_id
        );

        // register watchdog (insert cancel token)
        let cancel_token = {
            let mut cancel_tokens = state.watchdog_cancel_tokens.lock().unwrap();
            cancel_tokens.get_or_create_cancel_token(&event.case_id, &self.constraint_id, token_id)
        };

        // spawn watchdog task
        tokio::spawn(watchdog_task(
            cancel_token,
            token_id,
            event.case_id.clone(),
            self.constraint_id.clone(),
            self.duration,
            self.mode.clone(),
            state.clone(),
        ));
    }

    /// stop timer
    /// evaluate constraint:
    ///     - no timer start events happened -> satisfied
    ///     - all timers already fired -> the timers already emitted their results
    ///     - there are still running watchdog tasks -> cancel them and evaluate
    async fn handle_message(
        &self,
        state: AppState,
        message: CCMessage,
        _request_id: Option<RequestId>,
    ) {
        log::info!(
            "received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);

        if message.message_type != CCMessageType::TriggerCheck {
            log::warn!(
                "time start handler: unexpected message type {:?} for constraint {}, ignoring",
                message.message_type,
                self.constraint_id
            );
            return;
        }

        let stop_time = match message.timestamp {
            Some(t) => t,
            None => {
                log::error!(
                    "time start handler: TriggerCheck for constraint {} has no timestamp, cannot evaluate",
                    self.constraint_id
                );
                return;
            }
        };

        // cancel all watchdogs for this (case, constraint) at once via parent cancel token
        {
            let mut cancel_tokens = state.watchdog_cancel_tokens.lock().unwrap();
            cancel_tokens.cancel_constraint(&message.case_id, &self.constraint_id);
            cancel_tokens.remove_constraint(&message.case_id, &self.constraint_id);
        }

        // evaluate timer data
        let timer_data = {
            let mut timer_store = state.timer_token_store.lock().unwrap();
            timer_store.take_timer_tokens_for_evaluation(&message.case_id, &self.constraint_id)
        };

        log::info!(
            "time start handler: evaluating {} live and {} fired tokens for ({}/{})",
            timer_data.live_tokens.len(),
            timer_data.fired_tokens.len(),
            message.case_id,
            self.constraint_id
        );

        // evaluate for each token (live and fired - which allows for corrections) separately
        // if violated or satisfied depending on event times and time constraint mode (min/max)
        for token in timer_data
            .live_tokens
            .iter()
            .chain(timer_data.fired_tokens.iter())
        {
            let was_fired = timer_data.live_tokens.iter().all(|t| t.id != token.id);
            let (result, info) = self.evaluate_token(token, stop_time);
            log::info!(
                "time start handler: : token {} ({}) -> {:?}: {}",
                token.id,
                if was_fired { "late-correction" } else { "live" },
                result,
                info
            );
            emit_result(
                &message.case_id,
                &self.constraint_id,
                ConstraintType::Time,
                result,
                info,
                state.clone(),
                ResultTrigger::Event,
                message.triggering_event,
            )
            .await;
        }
    }

    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "time start handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );

        let result_exists = {
            let results = state.cc_results.lock().unwrap();
            results
                .get_constraint(case_id, &self.constraint_id)
                .is_some()
        };

        // if a timer is still running its watchdog task will emit the result
        // once it fires (or a late stop event cancels it and evaluates)
        let live_timers = {
            let timer_store = state.timer_token_store.lock().unwrap();
            timer_store.timer_token_count(case_id, &self.constraint_id)
        };
        if live_timers > 0 {
            log::info!(
                "time start handler: case end for ({}/{}) with {} live timer(s) - watchdog will emit the result",
                case_id,
                self.constraint_id,
                live_timers
            );
            return;
        }

        if !result_exists {
            // produce result because none was emitted yet
            emit_result(
                case_id,
                &self.constraint_id,
                ConstraintType::Time,
                ConstraintResultType::Satisfied,
                "no time constraint result was produced until case end".to_string(),
                state.clone(),
                ResultTrigger::CaseEnd,
                None,
            )
            .await;
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::debug!(
            "time start handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
        // remove timer tokens and clear 'seen' flag
        {
            let mut timer_store = state.timer_token_store.lock().unwrap();
            timer_store.clear_timer_state(case_id, &self.constraint_id);
        }
        {
            let mut watchdog_store = state.watchdog_cancel_tokens.lock().unwrap();
            watchdog_store.remove_constraint(case_id, &self.constraint_id);
        }
    }
}

impl TimeStartHandler {
    fn evaluate_token(
        &self,
        token: &Token,
        stop_time: EventTime,
    ) -> (ConstraintResultType, String) {
        let start_time = token.event_time.unwrap_or(token.created_at);
        let elapsed_ms = (stop_time - start_time).num_milliseconds();
        let elapsed = if elapsed_ms >= 0 {
            Duration::from_millis(elapsed_ms as u64)
        } else {
            log::warn!(
                "ttime start handler: negative elapsed for token {} \
                 (stop={} before start={}); treating as zero",
                token.id,
                stop_time,
                start_time
            );
            Duration::ZERO
        };

        match self.mode {
            MinMaxMode::Max => {
                if elapsed <= self.duration {
                    (
                        ConstraintResultType::Satisfied,
                        format!(
                            "TimerMax: elapsed {:?} <= limit {:?} (token {})",
                            elapsed, self.duration, token.id
                        ),
                    )
                } else {
                    (
                        ConstraintResultType::Violated,
                        format!(
                            "TimerMax: elapsed {:?} > limit {:?} (token {})",
                            elapsed, self.duration, token.id
                        ),
                    )
                }
            }
            MinMaxMode::Min => {
                if elapsed >= self.duration {
                    (
                        ConstraintResultType::Satisfied,
                        format!(
                            "TimerMin: elapsed {:?} >= minimum {:?} (token {})",
                            elapsed, self.duration, token.id
                        ),
                    )
                } else {
                    (
                        ConstraintResultType::Violated,
                        format!(
                            "TimerMin: elapsed {:?} < minimum {:?} (token {})",
                            elapsed, self.duration, token.id
                        ),
                    )
                }
            }
        }
    }
}

pub struct TimeStopHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub constraint_id: ConstraintId,
    pub event_id: EventId,
    pub time_start_peers: Vec<SocketAddr>,
}

#[async_trait]
impl ConstraintHandler for TimeStopHandler {
    impl_constraint_handler_fields!(TimeStopHandler);

    fn configured_peers(&self) -> Vec<SocketAddr> {
        self.time_start_peers.clone()
    }

    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "time stop handler: received configured time stop event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        // notify timer start peers
        log::info!("time stop handler: notifying time start peers that time stop event happened");
        for &peer_addr in self.time_start_peers.iter() {
            send_cc_trigger_message(
                peer_addr,
                event.case_id.clone(),
                self.constraint_id.clone(),
                event.timestamp,
                state.clone(),
                event.instance_id,
            )
            .await;
        }
    }
}

async fn send_cc_trigger_message(
    peer_address: SocketAddr,
    case_id: CaseId,
    constraint_id: ConstraintId,
    event_timestamp: EventTime,
    state: AppState,
    triggering_event: EventInstanceId,
) {
    let message = PeerMessage::conformance(CCMessage::trigger_check(
        case_id,
        constraint_id.clone(),
        event_timestamp,
        triggering_event,
    ));

    log::info!(
        "time stop handler: sending cc trigger message for constraint {} to peer {}",
        constraint_id,
        peer_address
    );

    if let Err(e) = send_to_conformance_peer(peer_address, message, state.clone()).await {
        log::error!(
            "time stop handler: error sending cc trigger message to {}: {}",
            peer_address,
            e
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::node_config::{ConstraintConfig, ResultBufferConfig};
    use crate::network::message_types::CCMessage;
    use crate::types::ConstraintResultType;
    use crate::util::test_tools::{
        make_config_with_buffers, make_state_from_config, make_traced_event_at,
    };
    use chrono::TimeZone;
    use std::collections::HashMap;
    use std::time::Duration;

    const CASE: &str = "case-1";
    const CID: &str = "tc";

    fn base_time() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()
    }

    fn make_handler(duration_secs: u64, mode: MinMaxMode) -> TimeStartHandler {
        TimeStartHandler {
            index: 0,
            role: ConstraintRole::TimeStart,
            constraint_id: CID.into(),
            event_id: "start".into(),
            duration: Duration::from_secs(duration_secs),
            mode,
        }
    }

    /// Build state with a registered TimeStart constraint matching `handler`,
    /// the case pre-registered in cc_results, and the buffering policy set.
    fn make_test_state(
        duration_secs: u64,
        mode: MinMaxMode,
        buffer: ResultBufferConfig,
    ) -> AppState {
        let mut buffers = HashMap::new();
        buffers.insert(CID.to_string(), buffer);
        // Register the handler in the registry too — needed so finalize calls land.
        let constraints = vec![ConstraintConfig::TimeStart {
            constraint_id: CID.into(),
            event_id: "start".into(),
            duration: Duration::from_secs(duration_secs),
            mode,
        }];
        let cfg = make_config_with_buffers(constraints, buffers, ResultBufferConfig::Immediate);
        let state = make_state_from_config(cfg);
        state.cc_results.lock().unwrap().register_case(CASE.into());
        state
    }

    fn read_result(state: &AppState) -> Option<ConstraintResultType> {
        Some(
            state
                .cc_results
                .lock()
                .unwrap()
                .get_constraint(&CASE.to_string(), &CID.to_string())?
                .constraint_result,
        )
    }

    fn live_count(state: &AppState) -> usize {
        state
            .timer_token_store
            .lock()
            .unwrap()
            .timer_token_count(&CASE.to_string(), &CID.to_string())
    }

    fn fired_count(state: &AppState) -> usize {
        state
            .timer_token_store
            .lock()
            .unwrap()
            .fired_timer_token_count(&CASE.to_string(), &CID.to_string())
    }

    /// Yield before and after advancing time so spawned tasks (watchdog,
    /// emitter timer) fully register their sleeps and then run to completion.
    /// Pre-yields: ensure the spawned task reaches its sleep and registers
    ///   with the runtime's timer wheel before we advance.
    /// Post-yields: drive chained async calls (watchdog → emit_result →
    ///   grace_period_timer etc.) through all their poll points.
    async fn advance_and_settle(secs: u64) {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(std::time::Duration::from_secs(secs)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }

    // ── handle_event ─────────────────────────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn handle_event_creates_timer_token_and_registers_watchdog() {
        let state = make_test_state(10, MinMaxMode::Max, ResultBufferConfig::Immediate);
        let handler = make_handler(10, MinMaxMode::Max);
        let event = make_traced_event_at(CASE, "start", 0);

        handler.handle_event(&event, state.clone()).await;

        assert_eq!(live_count(&state), 1);
        assert!(
            state
                .watchdog_cancel_tokens
                .lock()
                .unwrap()
                .has_parent(&CASE.to_string(), &CID.to_string())
        );
    }

    // ── watchdog timeout: Max → Violated, Min → Satisfied ────────────────

    #[tokio::test(start_paused = true)]
    async fn max_watchdog_timeout_sets_violated() {
        let state = make_test_state(10, MinMaxMode::Max, ResultBufferConfig::Immediate);
        let handler = make_handler(10, MinMaxMode::Max);
        let event = make_traced_event_at(CASE, "start", 0);

        handler.handle_event(&event, state.clone()).await;

        advance_and_settle(11).await;

        assert!(matches!(
            read_result(&state),
            Some(ConstraintResultType::Violated)
        ));
        assert_eq!(fired_count(&state), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn min_watchdog_timeout_sets_satisfied() {
        let state = make_test_state(10, MinMaxMode::Min, ResultBufferConfig::Immediate);
        let handler = make_handler(10, MinMaxMode::Min);
        let event = make_traced_event_at(CASE, "start", 0);

        handler.handle_event(&event, state.clone()).await;

        advance_and_settle(11).await;

        assert!(matches!(
            read_result(&state),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    // ── handle_message: no start ever → satisfied immediately ────────────

    #[tokio::test]
    async fn message_with_no_start_ever_is_no_violation() {
        let state = make_test_state(10, MinMaxMode::Max, ResultBufferConfig::Immediate);
        let handler = make_handler(10, MinMaxMode::Max);

        let stop_time = base_time() + chrono::Duration::seconds(5);
        let msg = CCMessage::trigger_check(
            CASE.into(),
            CID.into(),
            stop_time,
            EventInstanceId::new_v4(),
        );
        handler.handle_message(state.clone(), msg, None).await;

        assert!(read_result(&state).is_none(),);
    }

    // ── live evaluation: Max in time / out of time ───────────────────────

    #[tokio::test(start_paused = true)]
    async fn max_in_time_stop_satisfies() {
        let state = make_test_state(10, MinMaxMode::Max, ResultBufferConfig::Immediate);
        let handler = make_handler(10, MinMaxMode::Max);
        let event = make_traced_event_at(CASE, "start", 0);
        handler.handle_event(&event, state.clone()).await;

        // Stop event happened at base + 3s (well within 10s limit)
        let stop_time = base_time() + chrono::Duration::seconds(3);
        let msg = CCMessage::trigger_check(
            CASE.into(),
            CID.into(),
            stop_time,
            EventInstanceId::new_v4(),
        );
        handler.handle_message(state.clone(), msg, None).await;

        assert!(matches!(
            read_result(&state),
            Some(ConstraintResultType::Satisfied)
        ));
        // live token was consumed
        assert_eq!(live_count(&state), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn max_late_stop_within_live_window_violates() {
        let state = make_test_state(5, MinMaxMode::Max, ResultBufferConfig::Immediate);
        let handler = make_handler(5, MinMaxMode::Max);
        let event = make_traced_event_at(CASE, "start", 0);
        handler.handle_event(&event, state.clone()).await;

        // Stop event timestamp is past the limit; watchdog hasn't fired yet
        // (we don't advance time), so the token is still live.
        let stop_time = base_time() + chrono::Duration::seconds(20);
        let msg = CCMessage::trigger_check(
            CASE.into(),
            CID.into(),
            stop_time,
            EventInstanceId::new_v4(),
        );
        handler.handle_message(state.clone(), msg, None).await;

        assert!(matches!(
            read_result(&state),
            Some(ConstraintResultType::Violated)
        ));
    }

    // ── live evaluation: Min ─────────────────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn min_sufficient_elapsed_satisfies() {
        let state = make_test_state(5, MinMaxMode::Min, ResultBufferConfig::Immediate);
        let handler = make_handler(5, MinMaxMode::Min);
        let event = make_traced_event_at(CASE, "start", 0);
        handler.handle_event(&event, state.clone()).await;

        let stop_time = base_time() + chrono::Duration::seconds(7);
        let msg = CCMessage::trigger_check(
            CASE.into(),
            CID.into(),
            stop_time,
            EventInstanceId::new_v4(),
        );
        handler.handle_message(state.clone(), msg, None).await;

        assert!(matches!(
            read_result(&state),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn min_too_early_stop_violates() {
        let state = make_test_state(10, MinMaxMode::Min, ResultBufferConfig::Immediate);
        let handler = make_handler(10, MinMaxMode::Min);
        let event = make_traced_event_at(CASE, "start", 0);
        handler.handle_event(&event, state.clone()).await;

        let stop_time = base_time() + chrono::Duration::seconds(3);
        let msg = CCMessage::trigger_check(
            CASE.into(),
            CID.into(),
            stop_time,
            EventInstanceId::new_v4(),
        );
        handler.handle_message(state.clone(), msg, None).await;

        assert!(matches!(
            read_result(&state),
            Some(ConstraintResultType::Violated)
        ));
    }

    // ── late-arrival correction via fired tokens + grace period ──────────

    #[tokio::test(start_paused = true)]
    async fn late_stop_after_watchdog_fired_corrects_via_grace_period() {
        // Max timer, 5s limit, 60s grace period. The watchdog fires at 5s
        // wall-clock with Violated (tentative). Then within the grace period
        // a stop arrives whose event-time is well within 5s of the start.
        // The handler must re-evaluate using the preserved fired token and
        // correct the result to Satisfied before the grace window expires.
        let state = make_test_state(
            5,
            MinMaxMode::Max,
            ResultBufferConfig::GracePeriod {
                duration: Duration::from_secs(60),
            },
        );
        let handler = make_handler(5, MinMaxMode::Max);
        let event = make_traced_event_at(CASE, "start", 0);
        handler.handle_event(&event, state.clone()).await;

        // Let the watchdog fire.
        advance_and_settle(6).await;

        // Tentative result is Violated; the fired token is preserved for correction.
        assert!(matches!(
            read_result(&state),
            Some(ConstraintResultType::Violated)
        ));
        assert_eq!(fired_count(&state), 1);
        assert_eq!(live_count(&state), 0);

        // Late stop arrives carrying an in-time timestamp (start + 2s).
        let stop_time = base_time() + chrono::Duration::seconds(2);
        let msg = CCMessage::trigger_check(
            CASE.into(),
            CID.into(),
            stop_time,
            EventInstanceId::new_v4(),
        );
        handler.handle_message(state.clone(), msg, None).await;

        // cc_results corrected to Satisfied immediately.
        assert!(matches!(
            read_result(&state),
            Some(ConstraintResultType::Satisfied)
        ));
        // The fired-tokens collection was drained for evaluation.
        assert_eq!(fired_count(&state), 0);

        // Run the grace period out — the buffer drains and finalize clears
        // any remaining timer state.
        advance_and_settle(120).await;
        assert!(
            !state
                .result_send_buffer
                .lock()
                .unwrap()
                .has_pending(&CASE.to_string(), &CID.to_string())
        );
    }

    // ── finalize directly ────────────────────────────────────────────────

    #[tokio::test]
    async fn finalize_clears_timer_state() {
        let state = make_test_state(10, MinMaxMode::Max, ResultBufferConfig::Immediate);
        let handler = make_handler(10, MinMaxMode::Max);

        // Plant some state to be cleared
        let event = make_traced_event_at(CASE, "start", 0);
        handler.handle_event(&event, state.clone()).await;
        assert_eq!(live_count(&state), 1);

        handler
            .clean_up_state(&CASE.to_string(), state.clone())
            .await;
        assert_eq!(live_count(&state), 0);
        assert_eq!(fired_count(&state), 0);
    }

    // ── default trait finalize is a no-op ────────────────────────────────

    #[tokio::test]
    async fn default_trait_finalize_does_nothing() {
        // The TimeStopHandler doesn't override finalize, so it should be a no-op.
        let stop = TimeStopHandler {
            index: 1,
            role: ConstraintRole::TimeStop,
            constraint_id: CID.into(),
            event_id: "stop".into(),
            time_start_peers: vec![],
        };
        let state = make_test_state(10, MinMaxMode::Max, ResultBufferConfig::Immediate);
        // Should not panic, should not touch token_store
        stop.clean_up_state(&CASE.to_string(), state.clone()).await;
    }

    // ── case end fallback ────────────────────────────────────────────────

    // regression: case end arriving while a timer is still live must not  emit the fallback result - the watchdog owns the result
    // emitting here would produce a second (misleading) result row in the central system
    // while the distributed system produces only the watchdog result because case end arrives later via gossip
    #[tokio::test]
    async fn case_end_with_live_timer_emits_nothing() {
        let state = make_test_state(10, MinMaxMode::Min, ResultBufferConfig::Immediate);
        let handler = make_handler(10, MinMaxMode::Min);

        // start a timer, then signal case end while it is still live
        let event = make_traced_event_at(CASE, "start", 0);
        handler.handle_event(&event, state.clone()).await;

        handler
            .handle_case_end(&CASE.to_string(), state.clone())
            .await;

        assert!(
            read_result(&state).is_none(),
            "no fallback result may be emitted while a timer is live"
        );
    }

    /// case end with no timer activity and no prior result emits the
    /// satisfied fallback so every constraint has at least one result per case
    #[tokio::test]
    async fn case_end_without_activity_emits_satisfied_fallback() {
        let state = make_test_state(10, MinMaxMode::Min, ResultBufferConfig::Immediate);
        let handler = make_handler(10, MinMaxMode::Min);

        handler
            .handle_case_end(&CASE.to_string(), state.clone())
            .await;

        assert!(matches!(
            read_result(&state),
            Some(ConstraintResultType::Satisfied)
        ));
    }
}
