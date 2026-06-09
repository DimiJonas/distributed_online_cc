use std::{net::SocketAddr, time::Duration};

use async_trait::async_trait;
use chrono::Utc;

use crate::{
    impl_constraint_handler_fields,
    network::{
        message_types::{CCMessage, CCMessageType, PeerMessage},
        tcp_router::send_to_peer,
    },
    protocol::{
        constraint_logic::constraint_handler_trait::ConstraintHandler, result_emitter::emit_result,
        watchdog_task::watchdog_task,
    },
    state::app_state::AppState,
    types::{
        CaseId, ConstraintHandlerId, ConstraintId, ConstraintResultType, ConstraintType, EventId,
        EventTime, TimeConstraintMode, Token, TracedEvent,
    },
};

pub struct TimeStartHandler {
    pub handler_id: ConstraintHandlerId,
    pub constraint_id: ConstraintId,
    pub event_id: EventId,
    pub duration: Duration,
    pub mode: TimeConstraintMode,
}

#[async_trait]
impl ConstraintHandler for TimeStartHandler {
    impl_constraint_handler_fields!(TimeStartHandler);

    /// start timer
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "received configured time start event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let start_time = event.timestamp_utc().unwrap_or_else(Utc::now);

        // create a timer token for this event instance
        let token = Token::new(
            event.case_id.clone(),
            self.constraint_id.clone(),
            match self.mode {
                TimeConstraintMode::Max => ConstraintType::TimerStartMax,
                TimeConstraintMode::Min => ConstraintType::TimerStartMin,
            },
            Some(start_time),
            state.config.node_id,
        );
        let token_id = token.id;

        {
            let mut timer_store = state.timer_token_store.lock().unwrap();
            timer_store.insert_timer_token(token);
        }

        log::info!(
            "time start: inserted timer token {} for constraint {}/case {}",
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
    async fn handle_message(&self, state: AppState, message: CCMessage) {
        log::info!(
            "received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!(
            "received message for constraint {}; message={:?}",
            self.constraint_id,
            message
        );

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
        }

        // evaluate timer data
        let timer_data = {
            let mut timer_store = state.timer_token_store.lock().unwrap();
            timer_store.take_timer_tokens_for_evaluation(&message.case_id, &self.constraint_id)
        };

        // satisfied if no time start event happened
        if !timer_data.start_ever_seen && !timer_data.has_tokens() {
            log::info!(
                "time start handler: stop event arrived for ({}/{}) but no start event was ever recorded - satisfied immediately",
                message.case_id,
                self.constraint_id
            );
            emit_result(
                &message.case_id,
                &self.constraint_id,
                ConstraintResultType::Satisfied,
                "TimerStop: no start event was ever observed".to_string(),
                state.clone(),
            )
            .await;
            return;
        }

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
                "time start: token {} ({}) -> {:?}: {}",
                token.id,
                if was_fired { "late-correction" } else { "live" },
                result,
                info
            );
            emit_result(
                &message.case_id,
                &self.constraint_id,
                result,
                info,
                state.clone(),
            )
            .await;
        }
    }

    async fn finalize(&self, case_id: &CaseId, state: AppState) {
        log::debug!(
            "time start handler: finalize ({}/{}) - clearing timer state",
            case_id,
            self.constraint_id
        );
        let mut timer_store = state.timer_token_store.lock().unwrap();
        timer_store.clear_timer_state(case_id, &self.constraint_id);
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
                "time start: negative elapsed for token {} \
                 (stop={} before start={}); treating as zero",
                token.id,
                stop_time,
                start_time
            );
            Duration::ZERO
        };

        match self.mode {
            TimeConstraintMode::Max => {
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
            TimeConstraintMode::Min => {
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
    pub handler_id: ConstraintHandlerId,
    pub constraint_id: ConstraintId,
    pub event_id: EventId,
    pub time_start_peers: Vec<SocketAddr>,
}

#[async_trait]
impl ConstraintHandler for TimeStopHandler {
    impl_constraint_handler_fields!(TimeStopHandler);

    // notify all handlers that this case has ended
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "received configured time stop event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let stop_time = event.timestamp_utc().unwrap_or(Utc::now());

        // notify timer start peers
        log::info!("notifying time start peers that time stop event happened");
        log::debug!("time start peers: {:?}", self.time_start_peers);
        for &peer_addr in self.time_start_peers.iter() {
            send_cc_trigger_message(
                peer_addr,
                event.case_id.clone(),
                self.constraint_id.clone(),
                stop_time,
                state.clone(),
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
) {
    let message = PeerMessage::conformance(CCMessage::trigger_check(
        case_id,
        constraint_id.clone(),
        event_timestamp,
    ));

    log::info!(
        "sending cc trigger message for constraint {} to peer {}",
        constraint_id,
        peer_address
    );

    send_to_peer(peer_address, message, state.clone()).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::node_config::{ConstraintConfig, ResultBufferConfig};
    use crate::network::message_types::CCMessage;
    use crate::types::{ConstraintResultType, ConstraintType, TimeConstraintMode};
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

    fn make_handler(duration_secs: u64, mode: TimeConstraintMode) -> TimeStartHandler {
        TimeStartHandler {
            handler_id: ConstraintHandlerId::TimeStart,
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
        mode: TimeConstraintMode,
        buffer: ResultBufferConfig,
    ) -> AppState {
        let constraint_type = match &mode {
            TimeConstraintMode::Max => ConstraintType::TimerStartMax,
            TimeConstraintMode::Min => ConstraintType::TimerStartMin,
        };
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
        state
            .cc_results
            .lock()
            .unwrap()
            .register_case(CASE.into(), &[(CID.into(), constraint_type)]);
        state
    }

    fn read_result(state: &AppState) -> ConstraintResultType {
        state
            .cc_results
            .lock()
            .unwrap()
            .get_constraint(&CASE.to_string(), &CID.to_string())
            .expect("not registered")
            .constraint_result
            .clone()
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
        let state = make_test_state(10, TimeConstraintMode::Max, ResultBufferConfig::Immediate);
        let handler = make_handler(10, TimeConstraintMode::Max);
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
        let state = make_test_state(10, TimeConstraintMode::Max, ResultBufferConfig::Immediate);
        let handler = make_handler(10, TimeConstraintMode::Max);
        let event = make_traced_event_at(CASE, "start", 0);

        handler.handle_event(&event, state.clone()).await;

        advance_and_settle(11).await;

        assert!(matches!(
            read_result(&state),
            ConstraintResultType::Violated
        ));
        // Immediate policy → finalize runs → fired tokens cleared
        assert_eq!(fired_count(&state), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn min_watchdog_timeout_sets_satisfied() {
        let state = make_test_state(10, TimeConstraintMode::Min, ResultBufferConfig::Immediate);
        let handler = make_handler(10, TimeConstraintMode::Min);
        let event = make_traced_event_at(CASE, "start", 0);

        handler.handle_event(&event, state.clone()).await;

        advance_and_settle(11).await;

        assert!(matches!(
            read_result(&state),
            ConstraintResultType::Satisfied
        ));
    }

    // ── handle_message: no start ever → satisfied immediately ────────────

    #[tokio::test]
    async fn message_with_no_start_ever_is_satisfied_immediately() {
        let state = make_test_state(10, TimeConstraintMode::Max, ResultBufferConfig::Immediate);
        let handler = make_handler(10, TimeConstraintMode::Max);

        let stop_time = base_time() + chrono::Duration::seconds(5);
        let msg = CCMessage::trigger_check(CASE.into(), CID.into(), stop_time);
        handler.handle_message(state.clone(), msg).await;

        assert!(matches!(
            read_result(&state),
            ConstraintResultType::Satisfied
        ));
    }

    // ── live evaluation: Max in time / out of time ───────────────────────

    #[tokio::test(start_paused = true)]
    async fn max_in_time_stop_satisfies() {
        let state = make_test_state(10, TimeConstraintMode::Max, ResultBufferConfig::Immediate);
        let handler = make_handler(10, TimeConstraintMode::Max);
        let event = make_traced_event_at(CASE, "start", 0);
        handler.handle_event(&event, state.clone()).await;

        // Stop event happened at base + 3s (well within 10s limit)
        let stop_time = base_time() + chrono::Duration::seconds(3);
        let msg = CCMessage::trigger_check(CASE.into(), CID.into(), stop_time);
        handler.handle_message(state.clone(), msg).await;

        assert!(matches!(
            read_result(&state),
            ConstraintResultType::Satisfied
        ));
        // live token was consumed
        assert_eq!(live_count(&state), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn max_late_stop_within_live_window_violates() {
        let state = make_test_state(5, TimeConstraintMode::Max, ResultBufferConfig::Immediate);
        let handler = make_handler(5, TimeConstraintMode::Max);
        let event = make_traced_event_at(CASE, "start", 0);
        handler.handle_event(&event, state.clone()).await;

        // Stop event timestamp is past the limit; watchdog hasn't fired yet
        // (we don't advance time), so the token is still live.
        let stop_time = base_time() + chrono::Duration::seconds(20);
        let msg = CCMessage::trigger_check(CASE.into(), CID.into(), stop_time);
        handler.handle_message(state.clone(), msg).await;

        assert!(matches!(
            read_result(&state),
            ConstraintResultType::Violated
        ));
    }

    // ── live evaluation: Min ─────────────────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn min_sufficient_elapsed_satisfies() {
        let state = make_test_state(5, TimeConstraintMode::Min, ResultBufferConfig::Immediate);
        let handler = make_handler(5, TimeConstraintMode::Min);
        let event = make_traced_event_at(CASE, "start", 0);
        handler.handle_event(&event, state.clone()).await;

        let stop_time = base_time() + chrono::Duration::seconds(7);
        let msg = CCMessage::trigger_check(CASE.into(), CID.into(), stop_time);
        handler.handle_message(state.clone(), msg).await;

        assert!(matches!(
            read_result(&state),
            ConstraintResultType::Satisfied
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn min_too_early_stop_violates() {
        let state = make_test_state(10, TimeConstraintMode::Min, ResultBufferConfig::Immediate);
        let handler = make_handler(10, TimeConstraintMode::Min);
        let event = make_traced_event_at(CASE, "start", 0);
        handler.handle_event(&event, state.clone()).await;

        let stop_time = base_time() + chrono::Duration::seconds(3);
        let msg = CCMessage::trigger_check(CASE.into(), CID.into(), stop_time);
        handler.handle_message(state.clone(), msg).await;

        assert!(matches!(
            read_result(&state),
            ConstraintResultType::Violated
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
            TimeConstraintMode::Max,
            ResultBufferConfig::GracePeriod {
                duration: Duration::from_secs(60),
            },
        );
        let handler = make_handler(5, TimeConstraintMode::Max);
        let event = make_traced_event_at(CASE, "start", 0);
        handler.handle_event(&event, state.clone()).await;

        // Let the watchdog fire.
        advance_and_settle(6).await;

        // Tentative result is Violated; the fired token is preserved for correction.
        assert!(matches!(
            read_result(&state),
            ConstraintResultType::Violated
        ));
        assert_eq!(fired_count(&state), 1);
        assert_eq!(live_count(&state), 0);

        // Late stop arrives carrying an in-time timestamp (start + 2s).
        let stop_time = base_time() + chrono::Duration::seconds(2);
        let msg = CCMessage::trigger_check(CASE.into(), CID.into(), stop_time);
        handler.handle_message(state.clone(), msg).await;

        // cc_results corrected to Satisfied immediately.
        assert!(matches!(
            read_result(&state),
            ConstraintResultType::Satisfied
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
        let state = make_test_state(10, TimeConstraintMode::Max, ResultBufferConfig::Immediate);
        let handler = make_handler(10, TimeConstraintMode::Max);

        // Plant some state to be cleared
        let event = make_traced_event_at(CASE, "start", 0);
        handler.handle_event(&event, state.clone()).await;
        assert_eq!(live_count(&state), 1);

        handler.finalize(&CASE.to_string(), state.clone()).await;
        assert_eq!(live_count(&state), 0);
        assert_eq!(fired_count(&state), 0);
    }

    // ── default trait finalize is a no-op ────────────────────────────────

    #[tokio::test]
    async fn default_trait_finalize_does_nothing() {
        // The TimeStopHandler doesn't override finalize, so it should be a no-op.
        let stop = TimeStopHandler {
            handler_id: ConstraintHandlerId::TimeStop,
            constraint_id: CID.into(),
            event_id: "stop".into(),
            time_start_peers: vec![],
        };
        let state = make_test_state(10, TimeConstraintMode::Max, ResultBufferConfig::Immediate);
        // Should not panic, should not touch token_store
        stop.finalize(&CASE.to_string(), state.clone()).await;
    }
}
