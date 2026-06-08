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
    state::app_state::AppState,
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

        // evaluate timer data
        let timer_data = {
            let mut store = state.token_store.lock().unwrap();
            store.take_timer_tokens_for_evaluation(&message.case_id, &self.constraint_id)
        };

        // satisfied if no time start event happened
        if !timer_data.start_ever_seen && !timer_data.has_tokens() {
            log::info!(
                "time start handler: stop event arrived for ({}/{}) but no start event was ever recorded - satisfied immediately",
                message.case_id,
                self.constraint_id
            );
            emit_result_immediate(
                &message.case_id,
                &self.constraint_id,
                ConstraintResultType::Satisfied,
                "TimerStop: no start event was ever observed".to_string(),
                state.clone(),
            );
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
