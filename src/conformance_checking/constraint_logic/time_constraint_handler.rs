use std::{net::SocketAddr, time::Duration};

use async_trait::async_trait;
use chrono::Utc;

use crate::{
    config::node_config::MinMaxMode,
    conformance_checking::{
        constraint_logic::constraint_handler_trait::ConstraintHandler,
        result_emitter::emit_result_at, watchdog_task::watchdog_task,
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

        // the verdict below is decided by this lookup
        let decided_at = Utc::now();
        // a matching time stop event can already have been observed. if so evaluate it right away
        let pending_stop = {
            let timer_store = state.timer_token_store.lock().unwrap();
            timer_store.earliest_pending_stop_after(
                &event.case_id,
                &self.constraint_id,
                event.timestamp,
            )
        };

        if let Some(stop_time) = pending_stop {
            // remember the token as settled for late corrections
            {
                let mut timer_store = state.timer_token_store.lock().unwrap();
                timer_store.insert_settled_token(token.clone());
            }

            let (result, info) = self.evaluate_token(&token, stop_time);
            let info = format!("{info} (late correction: stop event observed before start event)");
            log::info!(
                "time start handler: token {} (stop event already observed at {}) -> {:?}: {}",
                token_id,
                stop_time,
                result,
                info
            );
            emit_result_at(
                decided_at,
                &event.case_id,
                &self.constraint_id,
                ConstraintType::Time,
                result,
                info,
                state.clone(),
                ResultTrigger::Event,
                Some(event.instance_id),
            )
            .await;
            return;
        }

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

        // deadline in event stream time (based on stream clock)
        let deadline = match chrono::Duration::from_std(self.duration) {
            Ok(limit) => event.timestamp + limit,
            Err(e) => {
                log::error!(
                    "time start handler: duration {:?} for constraint {} is out of range ({e}). not arming a watchdog",
                    self.duration,
                    self.constraint_id
                );
                return;
            }
        };

        // spawn watchdog task
        tokio::spawn(watchdog_task(
            cancel_token,
            token_id,
            event.case_id.clone(),
            self.constraint_id.clone(),
            deadline,
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

        // the verdict below is decided by this lookup
        let decided_at = Utc::now();

        // record the stop time and collect the tokens it settles
        let timer_data = {
            let mut timer_store = state.timer_token_store.lock().unwrap();
            timer_store.record_pending_stop(&message.case_id, &self.constraint_id, stop_time);
            timer_store.take_tokens_settled_by(&message.case_id, &self.constraint_id, stop_time)
        };

        // cancel only the watchdogs of the tokens this stop event actually settles
        // tokens it does not qualify for stay live and keep their watchdogs running
        if !timer_data.newly_settled.is_empty() {
            let mut cancel_tokens = state.watchdog_cancel_tokens.lock().unwrap();
            for token in timer_data.newly_settled.iter() {
                cancel_tokens.cancel_watchdog(token.id);
                cancel_tokens.remove_token(&token.id);
            }
        }

        log::info!(
            "time start handler: stop event at {} settles {} token(s) and corrects {} token(s) for ({}/{})",
            stop_time,
            timer_data.newly_settled.len(),
            timer_data.corrections.len(),
            message.case_id,
            self.constraint_id
        );

        // evaluate for each token separately if violated or satisfied depending on event times and time constraint mode
        for token in timer_data
            .newly_settled
            .iter()
            .chain(timer_data.corrections.iter())
        {
            let is_correction = timer_data.newly_settled.iter().all(|t| t.id != token.id);
            let (result, info) = self.evaluate_token(token, stop_time);
            let info = if is_correction {
                format!("{info} (late correction: an earlier stop event arrived)")
            } else {
                info
            };
            log::info!(
                "time start handler: : token {} ({}) -> {:?}: {}",
                token.id,
                if is_correction {
                    "late-correction"
                } else {
                    "live"
                },
                result,
                info
            );
            emit_result_at(
                decided_at,
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

        // the verdict below is decided by this lookup
        let decided_at = Utc::now();
        let result_exists = {
            let results = state.cc_results.lock().unwrap();
            results
                .get_constraint(case_id, &self.constraint_id)
                .is_some()
        };

        // a timer still running at case end means the stop event never arrived
        let case_end_time = {
            let case_state = state.case_state.lock().unwrap();
            case_state.case_end_time(case_id)
        };

        let live_timers = {
            let timer_store = state.timer_token_store.lock().unwrap();
            timer_store.timer_token_count(case_id, &self.constraint_id)
        };

        if live_timers > 0 {
            // cancel the watchdogs first so they cannot emit a second result later
            {
                let mut cancel_tokens = state.watchdog_cancel_tokens.lock().unwrap();
                cancel_tokens.cancel_constraint(case_id, &self.constraint_id);
                cancel_tokens.remove_constraint(case_id, &self.constraint_id);
            }

            let live_tokens = {
                let mut timer_store = state.timer_token_store.lock().unwrap();
                timer_store.take_live_timer_tokens(case_id, &self.constraint_id)
            };

            log::info!(
                "time start handler: case end for ({}/{}) with {} live timer(s) - settling now",
                case_id,
                self.constraint_id,
                live_timers
            );

            for token in live_tokens.iter() {
                let (result, verdict) = match self.mode {
                    MinMaxMode::Max => (ConstraintResultType::Violated, "TimerMax"),
                    MinMaxMode::Min => (ConstraintResultType::Satisfied, "TimerMin"),
                };
                let start_time = token.event_time.unwrap_or(token.created_at);
                let elapsed = match case_end_time {
                    Some(end) => {
                        humantime::format_duration((end - start_time).to_std().unwrap_or_default())
                            .to_string()
                    }
                    None => "unknown time".to_string(),
                };
                let info = format!(
                    "{}: stop event never arrived; case ended {} after the start event, limit {:?} (token {})",
                    verdict, elapsed, self.duration, token.id
                );
                log::info!(
                    "time start handler: token {} (case end) -> {:?}: {}",
                    token.id,
                    result,
                    info
                );
                emit_result_at(
                    decided_at,
                    case_id,
                    &self.constraint_id,
                    ConstraintType::Time,
                    result,
                    info,
                    state.clone(),
                    ResultTrigger::CaseEnd,
                    None,
                )
                .await;
            }
            return;
        }

        if !result_exists {
            // produce result because none was emitted yet
            emit_result_at(
                decided_at,
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
    use chrono::{TimeDelta, TimeZone};
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

        // stream clock
        state.stream_clock.advance(
            event
                .timestamp
                .checked_add_signed(TimeDelta::seconds(11))
                .unwrap(),
        );
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

        // stream clock
        state.stream_clock.advance(
            event
                .timestamp
                .checked_add_signed(TimeDelta::seconds(11))
                .unwrap(),
        );
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

    // ── pairing is decided by event time, not by arrival order ───────────

    /// the sepsis log contains traces where the lab result carries an earlier timestamp than the
    /// triage event that starts the timer. such a stop must not settle the timer: doing so produced
    /// a negative elapsed time that got clamped to zero and reported as trivially satisfied
    #[tokio::test(start_paused = true)]
    async fn stop_before_the_start_does_not_settle_the_timer() {
        let state = make_test_state(10, MinMaxMode::Max, ResultBufferConfig::Immediate);
        let handler = make_handler(10, MinMaxMode::Max);
        let event = make_traced_event_at(CASE, "start", 100);
        handler.handle_event(&event, state.clone()).await;

        let stop_time = base_time() + chrono::Duration::seconds(40);
        let msg = CCMessage::trigger_check(
            CASE.into(),
            CID.into(),
            stop_time,
            EventInstanceId::new_v4(),
        );
        handler.handle_message(state.clone(), msg, None).await;

        assert!(
            read_result(&state).is_none(),
            "a stop before the start must not produce a verdict"
        );
        assert_eq!(live_count(&state), 1, "the timer has to stay live");
    }

    /// two stop events both qualify - the verdict has to be the one of the earlier stop whichever
    /// order the messages arrive in
    #[tokio::test(start_paused = true)]
    async fn earliest_qualifying_stop_decides_regardless_of_arrival_order() {
        for descending in [false, true] {
            let state = make_test_state(10, MinMaxMode::Max, ResultBufferConfig::Immediate);
            let handler = make_handler(10, MinMaxMode::Max);
            handler
                .handle_event(&make_traced_event_at(CASE, "start", 0), state.clone())
                .await;

            // 3s after the start is within the 10s limit, 40s is not
            let mut offsets = [3i64, 40i64];
            if descending {
                offsets.reverse();
            }
            for offset in offsets {
                let msg = CCMessage::trigger_check(
                    CASE.into(),
                    CID.into(),
                    base_time() + chrono::Duration::seconds(offset),
                    EventInstanceId::new_v4(),
                );
                handler.handle_message(state.clone(), msg, None).await;
            }

            assert!(
                matches!(read_result(&state), Some(ConstraintResultType::Satisfied)),
                "the earliest qualifying stop has to decide (descending arrival: {descending})"
            );
        }
    }

    /// a start event processed after a stop that does not qualify still arms a watchdog
    #[tokio::test(start_paused = true)]
    async fn start_after_a_non_qualifying_stop_still_arms_a_watchdog() {
        let state = make_test_state(10, MinMaxMode::Max, ResultBufferConfig::Immediate);
        let handler = make_handler(10, MinMaxMode::Max);

        // the stop arrives first and is earlier in event time than the start that follows
        let msg = CCMessage::trigger_check(
            CASE.into(),
            CID.into(),
            base_time() + chrono::Duration::seconds(40),
            EventInstanceId::new_v4(),
        );
        handler.handle_message(state.clone(), msg, None).await;

        let event = make_traced_event_at(CASE, "start", 100);
        handler.handle_event(&event, state.clone()).await;
        assert_eq!(live_count(&state), 1);
        assert!(read_result(&state).is_none());

        // push the stream past the deadline (start at +100s, limit 10s)
        state.stream_clock.advance(
            event
                .timestamp
                .checked_add_signed(TimeDelta::seconds(11))
                .unwrap(),
        );
        advance_and_settle(11).await;

        assert!(matches!(
            read_result(&state),
            Some(ConstraintResultType::Violated)
        ));
    }

    /// a stop observed before the start settles it right away when it qualifies, and the retained
    /// token is still correctable by an even earlier stop afterwards
    #[tokio::test(start_paused = true)]
    async fn start_after_a_qualifying_stop_settles_immediately_and_stays_correctable() {
        let state = make_test_state(10, MinMaxMode::Max, ResultBufferConfig::Immediate);
        let handler = make_handler(10, MinMaxMode::Max);

        // stop at +40s arrives before the start at +0s, so it is out of the 10s limit
        let msg = CCMessage::trigger_check(
            CASE.into(),
            CID.into(),
            base_time() + chrono::Duration::seconds(40),
            EventInstanceId::new_v4(),
        );
        handler.handle_message(state.clone(), msg, None).await;

        handler
            .handle_event(&make_traced_event_at(CASE, "start", 0), state.clone())
            .await;
        assert!(matches!(
            read_result(&state),
            Some(ConstraintResultType::Violated)
        ));
        assert_eq!(live_count(&state), 0, "no watchdog may be armed");

        // an earlier stop turns up afterwards and has to correct the verdict
        let earlier = CCMessage::trigger_check(
            CASE.into(),
            CID.into(),
            base_time() + chrono::Duration::seconds(4),
            EventInstanceId::new_v4(),
        );
        handler.handle_message(state.clone(), earlier, None).await;

        assert!(matches!(
            read_result(&state),
            Some(ConstraintResultType::Satisfied)
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
        // stream clock
        state.stream_clock.advance(
            event
                .timestamp
                .checked_add_signed(TimeDelta::seconds(6))
                .unwrap(),
        );
        // also advance test time
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
        // The fired token is retained rather than drained: a stop that is even earlier in event
        // time can still arrive and would have to correct the verdict again.
        assert_eq!(fired_count(&state), 1);

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
