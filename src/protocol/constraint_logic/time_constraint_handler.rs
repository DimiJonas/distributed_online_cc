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
        constraint_logic::constraint_handler_trait::ConstraintHandler,
        result_emitter::{emit_result, emit_result_immediate},
        watchdog_task::watchdog_task,
    },
    state::{app_state::AppState, token_store::TimerStopOutcome},
    types::{
        CaseId, ConstraintId, ConstraintResultType, ConstraintType, EventId, EventTime,
        TimeConstraintMode, Token, TracedEvent,
    },
};

pub struct TimeStartHandler {
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
                TimeConstraintMode::Max => ConstraintType::TimerMax,
                TimeConstraintMode::Min => ConstraintType::TimerMin,
            },
            Some(start_time),
            state.config.node_id,
        );
        let token_id = token.id;

        {
            let mut store = state.token_store.lock().unwrap();
            store.insert_timer_token(token);
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

        // determine if tokens exist
        let outcome = {
            let mut store = state.token_store.lock().unwrap();
            store.take_timer_tokens_with_outcome(&message.case_id, &self.constraint_id)
        };

        match outcome {
            TimerStopOutcome::NeverStarted => {
                log::info!(
                    "time start handler: stop event arrived for ({}/{}) but no start event was ever recorded - satisfied immediately",
                    message.case_id,
                    self.constraint_id
                );
                emit_result_immediate(
                    &message.case_id,
                    &self.constraint_id,
                    ConstraintResultType::Satisfied,
                    "TimerStop: no start event was ever observed (vacuously satisfied)".to_string(),
                    state.clone(),
                );
            }
            TimerStopOutcome::AllTimersFired => {
                log::info!(
                    "time start handler: stop event arrived for ({}/{}) but all timers already fired - checking for late arrival correction",
                    message.case_id,
                    self.constraint_id
                );
                self.maybe_correct_late_arrival(&message.case_id, stop_time, state.clone())
                    .await;
            }
            TimerStopOutcome::LiveTokensFound(tokens) => {
                log::info!(
                    "time start handler: evaluating {} live tokens for ({}/{})",
                    tokens.len(),
                    message.case_id,
                    self.constraint_id
                );
                for token in &tokens {
                    let (result, info) = self.evaluate_token(token, stop_time);
                    log::info!("time start: token {} → {:?}: {}", token.id, result, info);
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

    /// Called in the `AllTimersFired` case. The original tokens are gone (the
    /// watchdogs removed them), so exact elapsed times aren't available here.
    /// Best-effort correction: for Max, if the current result is Violated and a
    /// stop event has now arrived, overwrite with Satisfied. For Min, a fired
    /// timer already means the minimum elapsed, so it stays Satisfied.
    async fn maybe_correct_late_arrival(
        &self,
        case_id: &CaseId,
        stop_time: EventTime,
        state: AppState,
    ) {
        let current = {
            let results = state.cc_results.lock().unwrap();
            results
                .get_constraint(case_id, &self.constraint_id)
                .cloned()
        };

        let Some(current) = current else {
            log::warn!(
                "time start: late-arrival correction: no result found for ({}/{})",
                case_id,
                self.constraint_id
            );
            return;
        };

        match self.mode {
            TimeConstraintMode::Max => {
                if matches!(current.constraint_result, ConstraintResultType::Violated) {
                    log::info!(
                        "time start: late-arrival correction for MAX ({}/{}) — \
                         stop_time={} arrived after timeout; overwriting Violated -> Satisfied",
                        case_id,
                        self.constraint_id,
                        stop_time
                    );
                    emit_result(
                        case_id,
                        &self.constraint_id,
                        ConstraintResultType::Satisfied,
                        format!(
                            "TimerMax: late stop event correction — stop arrived at {} \
                             after timer fired; result updated to Satisfied (start times \
                             no longer available for exact elapsed)",
                            stop_time
                        ),
                        state.clone(),
                    )
                    .await;
                } else {
                    log::debug!(
                        "time start: no correction needed for ({}/{}) — already {:?}",
                        case_id,
                        self.constraint_id,
                        current.constraint_result
                    );
                }
            }
            TimeConstraintMode::Min => {
                log::debug!(
                    "time start: MIN late arrival for ({}/{}) — timer already fired as \
                     Satisfied; stop_time={} noted, no correction needed",
                    case_id,
                    self.constraint_id,
                    stop_time
                );
            }
        }
    }
}

pub struct TimeStopHandler {
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
