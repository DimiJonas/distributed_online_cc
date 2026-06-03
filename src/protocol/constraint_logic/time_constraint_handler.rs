use std::{net::SocketAddr, ops::Add, time::Duration};

use async_trait::async_trait;
use chrono::Utc;

use crate::{
    impl_constraint_handler_fields,
    network::{
        message_types::{CCMessage, PeerMessage},
        tcp_router::send_to_peer,
    },
    protocol::constraint_logic::constraint_handler_trait::ConstraintHandler,
    state::app_state::AppState,
    types::{CaseId, ConstraintId, EventId, EventTime, TimeConstraintMode, TracedEvent},
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

    // start timer
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "received configured time start event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let start_event_time = event.timestamp_utc().unwrap_or(Utc::now());
        let constraint_end_time = start_event_time.add(self.duration);
    }

    // stop timer
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

        let time_stop_event_time = event.timestamp_utc().unwrap_or(Utc::now());

        // notify timer start peers
        log::info!("notifying time start peers that time stop event happened");
        log::debug!("time start peers: {:?}", self.time_start_peers);
        for &peer_addr in self.time_start_peers.iter() {
            send_cc_trigger_message(
                peer_addr,
                event.case_id.clone(),
                self.constraint_id.clone(),
                time_stop_event_time,
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
