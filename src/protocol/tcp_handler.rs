/*
    note: tcp handlers have to use the outbound router queue to send messages back to other peers using tcp
    this is needed to break a cycle that would emerge when tcp handlers would call tcp channel methods since tcp channels also call the tcp handlers when receiving a message
*/

use crate::network::message_types::{
    CCMessage, CliTestMessage, OverlayMessage, PeerMessage, TerminationDetectionMessage,
};
use crate::network::peer_manager_task::PeerManagerMessage;
use crate::state::app_state::AppState;
use uuid::Uuid;

pub async fn handle_cli_message(msg: CliTestMessage, state: AppState, msg_id: Uuid) {
    log::debug!("received cli message id={} msg={}", msg_id, msg);

    if msg.message.starts_with("ACK") {
        // its a response

        // remove pending request
        let (sender, len) = {
            let mut pending = state.pending.lock().unwrap();
            (pending.remove(&msg.request_id), pending.len())
        };

        if let Some(waiting_channel) = sender {
            log::info!(
                "matched response with pending request. {} pending requests left",
                len
            );
            // notofy task that is waiting for response
            waiting_channel.send(PeerMessage::cli(msg)).unwrap();
        } else {
            log::warn!(
                "no pending request found for response with id {}",
                msg.request_id
            );
        }
    } else {
        // its a request

        // send response
        let response = PeerMessage::cli(CliTestMessage {
            request_id: msg.request_id,
            message: format!("ACK '{}'", msg.message),
            sender_addr: state.config.listen_address,
        });

        let manager_tx = {
            let managers = state.peer_managers.lock().unwrap();
            managers.get(&msg.sender_addr)
        };
        match manager_tx {
            Some(tx) => {
                if let Err(e) = tx
                    .send(PeerManagerMessage::Outbound(Box::new(response)))
                    .await
                {
                    log::error!("failed to enqueue cli response: {}", e);
                }
            }
            None => {
                log::error!(
                    "no peer manager for {} found to send cli response to",
                    msg.sender_addr
                );
            }
        }
    }
}

pub async fn handle_cc_message(msg: CCMessage, state: AppState, msg_id: Uuid) {
    log::debug!(
        "handling conformance message with id={} msg={}",
        &msg_id,
        msg
    );

    if let Some(handler) = state.handlers.get_by_constraint_id(&msg.constraint_id) {
        handler.handle_message(state.clone(), msg).await;
    }
}

pub async fn handle_td_message(msg: TerminationDetectionMessage, _state: AppState, msg_id: Uuid) {
    log::debug!(
        "handling termination detection message message with id={} msg={}",
        &msg_id,
        msg
    );
    // TODO
}

pub async fn handle_overlay_message(msg: OverlayMessage, _state: AppState, msg_id: Uuid) {
    log::debug!("handling overlay message with id={} msg={}", &msg_id, msg);
    // TODO
}
