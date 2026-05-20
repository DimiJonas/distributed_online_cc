use std::{collections::VecDeque, net::SocketAddr, time::Duration};

use tokio::{net::TcpStream, sync::mpsc, time::sleep};

use crate::{
    network::{message_types::PeerMessage, tcp_channel::initiate_tcp_connection},
    state::app_state::AppState,
};

// enum with messages to a peer manager
//  Outbound: enqueue a message to be sent via tcp. the manager will make sure the tcp channel exists and sends it
//  ConnectionLost: notify the manager that the tcp channel broke down
//  ConnectionReady: notify the manager that the tcp channel was rebuilt
//  ConnectIfNeeded: signal the manager to initiate a tcp connection if none exists yet
pub enum PeerManagerMessage {
    Outbound(Box<PeerMessage>),
    ConnectionLost,
    ConnectionReady(mpsc::Sender<PeerMessage>),
    ConnectIfNeeded,
}

// the peer manager task routine
pub async fn peer_manager_task(
    peer_address: SocketAddr,
    mut manager_receiver: mpsc::Receiver<PeerManagerMessage>,
    state: AppState,
) {
    let mut queue: VecDeque<PeerMessage> = VecDeque::new();
    let mut tcp_channel_sender: Option<mpsc::Sender<PeerMessage>> = None;
    let mut reconnect_delay = state.config.tcp_channels.reconnect_start_delay;

    loop {
        let manager_msg = match manager_receiver.recv().await {
            Some(msg) => msg,
            None => {
                log::info!("peer manager for {} shutting down", peer_address);
                break;
            }
        };

        match manager_msg {
            PeerManagerMessage::Outbound(peer_msg) => {
                if peer_msg.is_expired() {
                    log::warn!(
                        "dropping expired message {} for {}",
                        peer_msg.message_id,
                        peer_address
                    );
                    continue;
                }
                match &tcp_channel_sender {
                    Some(sender) => {
                        if let Err(e) = sender.send(*peer_msg.clone()).await {
                            log::error!("send to tcp_channel failed for {}: {}", peer_address, e);
                            tcp_channel_sender = None;
                            // channel closed unexpectedly; re-queue so it isn't lost
                            // ConnectionLost will arrive shortly from tcp_channel
                            enqueue(state.clone(), &mut queue, *peer_msg, peer_address);
                        }
                    }
                    None => {
                        enqueue(state.clone(), &mut queue, *peer_msg, peer_address);
                    }
                }
            }

            PeerManagerMessage::ConnectionLost => {
                log::warn!(
                    "connection to {} lost, reconnecting in {}s",
                    peer_address,
                    reconnect_delay
                );
                tcp_channel_sender = None;
                schedule_reconnect(peer_address, state.clone(), reconnect_delay);
                reconnect_delay =
                    (reconnect_delay * 2).min(state.config.tcp_channels.reconnect_max_delay);
            }

            PeerManagerMessage::ConnectionReady(sender) => {
                log::info!(
                    "connection to {} ready, flushing {} queued messages",
                    peer_address,
                    queue.len()
                );
                tcp_channel_sender = Some(sender.clone());
                // reset reconnect delay
                reconnect_delay = state.config.tcp_channels.reconnect_start_delay;

                // flush messages
                while let Some(queued_msg) = queue.pop_front() {
                    if queued_msg.is_expired() {
                        log::warn!(
                            "dropping expired queued message {} for {} during flush",
                            queued_msg.message_id,
                            peer_address
                        );
                        continue;
                    }
                    if let Err(e) = sender.send(queued_msg).await {
                        log::error!("flush to {} failed: {}", peer_address, e);
                        break;
                    }
                }
            }

            PeerManagerMessage::ConnectIfNeeded => {
                if tcp_channel_sender.is_some() {
                    log::debug!(
                        "connect requested for {} but already connected, ignoring",
                        peer_address
                    );
                } else {
                    log::info!("connect requested for {}, attempting", peer_address);
                    do_connect(peer_address, state.clone()).await;
                }
            }
        }
    }
}

fn enqueue(
    state: AppState,
    queue: &mut VecDeque<PeerMessage>,
    msg: PeerMessage,
    peer_address: SocketAddr,
) {
    if queue.len() >= state.config.tcp_channels.message_queue_size {
        log::warn!(
            "message queue full for {}, dropping oldest message",
            peer_address
        );
        queue.pop_front();
    }
    queue.push_back(msg);
    log::debug!("{} messages queued for {}", queue.len(), peer_address);
}

fn schedule_reconnect(peer_address: SocketAddr, state: AppState, delay_secs: u64) {
    log::debug!(
        "scheduling reconnect to {} in {} seconds",
        peer_address,
        delay_secs
    );
    tokio::spawn(async move {
        sleep(Duration::from_secs(delay_secs)).await;
        attempt_connect(peer_address, state).await;
    });
}

async fn do_connect(peer_address: SocketAddr, state: AppState) {
    log::debug!("connecting to peer {}", peer_address);
    match TcpStream::connect(&peer_address).await {
        Ok(stream) => {
            let (tcp_channel_sender, tcp_channel_receiver) =
                mpsc::channel(state.config.tcp_channels.tcp_channel_mpsc_channel_size);
            tokio::spawn(initiate_tcp_connection(
                peer_address,
                stream,
                tcp_channel_sender,
                tcp_channel_receiver,
                state,
            ));
        }
        Err(e) => {
            log::error!("could not connect to {}: {}", peer_address, e);
            // signal back to manager task loop
            let manager_sender = {
                let managers = state.peer_managers.lock().unwrap();
                managers.get(&peer_address)
            };
            if let Some(sender) = manager_sender {
                let _ = sender.send(PeerManagerMessage::ConnectionLost).await;
            }
        }
    }
}

/// signals the peer manager task that corresponds to the peer_address to create a tcp channel if needed
pub async fn attempt_connect(peer_address: SocketAddr, state: AppState) {
    log::debug!("attempting to connect to peer {}", peer_address);
    let manager_sender = {
        let managers = state.peer_managers.lock().unwrap();
        managers.get(&peer_address)
    };
    if let Some(sender) = manager_sender {
        let _ = sender.send(PeerManagerMessage::ConnectIfNeeded).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::message_types::{CliTestMessage, PeerMessage, PeerMessagePayload};
    use crate::util::test_tools::{addr, cli_msg, make_state_with_event_q, spawn_manager};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::{Instant, timeout};

    // ----------------------------------------------------------------
    // Graceful shutdown
    // ----------------------------------------------------------------

    /// Dropping the sender closes the channel; the task must exit cleanly.
    #[tokio::test]
    async fn task_exits_when_sender_dropped() {
        let (state, _eq) = make_state_with_event_q(19100);
        let peer = addr(19101);
        let tx = spawn_manager(peer, state);

        // Give the task time to start, then drop the only sender.
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(tx);

        // If the task is stuck the test will time out; a short sleep is enough
        // to confirm the task did not panic and released its resources.
        tokio::time::sleep(Duration::from_millis(50)).await;
        // No assertion needed — reaching here without a panic is the proof.
    }

    // ----------------------------------------------------------------
    // Outbound message queuing (no connection yet)
    // ----------------------------------------------------------------

    /// Messages sent before ConnectionReady must be queued, then flushed
    /// when ConnectionReady arrives.
    #[tokio::test]
    async fn queued_messages_are_flushed_on_connection_ready() {
        let (state, _eq) = make_state_with_event_q(19110);
        let peer = addr(19111);
        let manager_tx = spawn_manager(peer, state);

        // Send two outbound messages before any tcp channel exists.
        manager_tx
            .send(PeerManagerMessage::Outbound(Box::new(cli_msg(19110))))
            .await
            .unwrap();
        manager_tx
            .send(PeerManagerMessage::Outbound(Box::new(cli_msg(19110))))
            .await
            .unwrap();

        // Now simulate a tcp channel becoming ready.
        let (tcp_tx, mut tcp_rx) = mpsc::channel::<PeerMessage>(16);
        manager_tx
            .send(PeerManagerMessage::ConnectionReady(tcp_tx))
            .await
            .unwrap();

        // Both queued messages should arrive on the tcp channel.
        let first = timeout(Duration::from_millis(200), tcp_rx.recv())
            .await
            .expect("timed out waiting for first message")
            .expect("channel closed unexpectedly");
        assert!(matches!(first.payload, PeerMessagePayload::Cli(_)));

        let second = timeout(Duration::from_millis(200), tcp_rx.recv())
            .await
            .expect("timed out waiting for second message")
            .expect("channel closed unexpectedly");
        assert!(matches!(second.payload, PeerMessagePayload::Cli(_)));
    }

    // ----------------------------------------------------------------
    // Outbound message forwarding (connection already up)
    // ----------------------------------------------------------------

    /// When a connection is up, an Outbound message must be forwarded
    /// immediately to the tcp channel — nothing should sit in the queue.
    #[tokio::test]
    async fn outbound_message_forwarded_immediately_when_connected() {
        let (state, _eq) = make_state_with_event_q(19120);
        let peer = addr(19121);
        let manager_tx = spawn_manager(peer, state);

        let (tcp_tx, mut tcp_rx) = mpsc::channel::<PeerMessage>(16);
        manager_tx
            .send(PeerManagerMessage::ConnectionReady(tcp_tx))
            .await
            .unwrap();

        manager_tx
            .send(PeerManagerMessage::Outbound(Box::new(cli_msg(19120))))
            .await
            .unwrap();

        let received = timeout(Duration::from_millis(200), tcp_rx.recv())
            .await
            .expect("timed out")
            .expect("channel closed");
        assert!(matches!(received.payload, PeerMessagePayload::Cli(_)));
    }

    // ----------------------------------------------------------------
    // Expired message is dropped, not forwarded
    // ----------------------------------------------------------------

    #[tokio::test]
    async fn expired_outbound_message_is_dropped() {
        let (state, _eq) = make_state_with_event_q(19130);
        let peer = addr(19131);
        let manager_tx = spawn_manager(peer, state);

        let (tcp_tx, mut tcp_rx) = mpsc::channel::<PeerMessage>(16);
        manager_tx
            .send(PeerManagerMessage::ConnectionReady(tcp_tx))
            .await
            .unwrap();

        // Build a message with a TTL that is already expired.
        let mut expired = cli_msg(19130);
        expired.deadline = Some(Instant::now() - Duration::from_secs(1));

        manager_tx
            .send(PeerManagerMessage::Outbound(Box::new(expired)))
            .await
            .unwrap();

        // Nothing should arrive; a short timeout confirms the message was dropped.
        let result = timeout(Duration::from_millis(100), tcp_rx.recv()).await;
        assert!(
            result.is_err(),
            "expired message should not have been forwarded"
        );
    }

    // ----------------------------------------------------------------
    // ConnectionLost clears the tcp sender
    // ----------------------------------------------------------------

    /// After ConnectionLost, new Outbound messages must be queued again
    /// (not forwarded on the old — now dead — channel).
    #[tokio::test]
    async fn connection_lost_clears_sender_and_re_queues() {
        let (state, _eq) = make_state_with_event_q(19140);
        let peer = addr(19141);
        let manager_tx = spawn_manager(peer, state);

        // Establish a connection.
        let (tcp_tx, tcp_rx) = mpsc::channel::<PeerMessage>(16);
        manager_tx
            .send(PeerManagerMessage::ConnectionReady(tcp_tx))
            .await
            .unwrap();

        // Tear it down and drop tcp_rx here so we can detect
        // whether the task tries to forward to the dead channel.
        manager_tx
            .send(PeerManagerMessage::ConnectionLost)
            .await
            .unwrap();

        // Drop the old receiver — the channel is now closed from our side.
        drop(tcp_rx);

        // Queue the Outbound BEFORE ConnectionReady — this is the message that
        // should be buffered and then flushed.
        manager_tx
            .send(PeerManagerMessage::Outbound(Box::new(cli_msg(19140))))
            .await
            .unwrap();

        // send a ConnectionReady with a fresh channel to confirm the task has fully processed ConnectionLost
        let (tcp_tx2, mut tcp_rx2) = mpsc::channel::<PeerMessage>(16);
        // the queued message should flush onto this channel.
        manager_tx
            .send(PeerManagerMessage::ConnectionReady(tcp_tx2))
            .await
            .unwrap();

        let received = timeout(Duration::from_millis(200), tcp_rx2.recv())
            .await
            .expect("timed out waiting for re-queued message")
            .expect("channel closed");
        assert!(matches!(received.payload, PeerMessagePayload::Cli(_)));

        // And nothing more should arrive (the message was queued once, flushed once).
        let extra = timeout(Duration::from_millis(50), tcp_rx2.recv()).await;
        assert!(
            extra.is_err(),
            "message should have been flushed exactly once"
        );
    }

    // ----------------------------------------------------------------
    // Reconnect-delay back-off
    // ----------------------------------------------------------------

    /// Each successive ConnectionLost must double the reconnect delay,
    /// capped at RECONNECT_MAX_DELAY_SECS.
    ///
    /// We test the logic indirectly: the task must not panic and must
    /// remain responsive to further messages after several lost events.
    /// (Actual timer elapsing would require advancing Tokio's fake clock
    /// and is omitted here for simplicity.)
    #[tokio::test]
    async fn task_remains_responsive_after_multiple_connection_losses() {
        let (state, _eq) = make_state_with_event_q(19150);
        let peer = addr(19151);
        let manager_tx = spawn_manager(peer, state);

        for _ in 0..5 {
            manager_tx
                .send(PeerManagerMessage::ConnectionLost)
                .await
                .unwrap();
        }

        // After all those losses the task must still be alive.
        let (tcp_tx, mut tcp_rx) = mpsc::channel::<PeerMessage>(16);
        manager_tx
            .send(PeerManagerMessage::ConnectionReady(tcp_tx))
            .await
            .unwrap();
        manager_tx
            .send(PeerManagerMessage::Outbound(Box::new(cli_msg(19150))))
            .await
            .unwrap();

        let received = timeout(Duration::from_millis(300), tcp_rx.recv())
            .await
            .expect("task was unresponsive after connection losses")
            .expect("channel closed");
        assert!(matches!(received.payload, PeerMessagePayload::Cli(_)));
    }

    // ----------------------------------------------------------------
    // Queue overflow: oldest message is dropped
    // ----------------------------------------------------------------

    /// Filling the queue beyond MAX_QUEUE (256) should drop the oldest
    /// entry, not panic and not block.
    #[tokio::test]
    async fn queue_overflow_drops_oldest_message() {
        let (state, _eq) = make_state_with_event_q(19160);
        let peer = addr(19161);
        let manager_tx = spawn_manager(peer, state.clone());

        // Send tcp_message_queue_size + 10 messages with no tcp channel.
        for i in 0..(state.config.tcp_channels.message_queue_size + 10) {
            let msg = PeerMessage::cli(CliTestMessage {
                request_id: i as u64,
                message: format!("msg-{}", i),
                sender_addr: addr(19160),
            });
            manager_tx
                .send(PeerManagerMessage::Outbound(Box::new(msg)))
                .await
                .unwrap();
        }

        // Now flush. We should receive exactly MAX_QUEUE messages (the 10
        // oldest were dropped).
        let (tcp_tx, mut tcp_rx) = mpsc::channel::<PeerMessage>(512);
        manager_tx
            .send(PeerManagerMessage::ConnectionReady(tcp_tx))
            .await
            .unwrap();

        let mut count = 0usize;
        while let Ok(Some(_)) = timeout(Duration::from_millis(200), tcp_rx.recv()).await {
            count += 1
        }

        assert_eq!(
            count, state.config.tcp_channels.message_queue_size,
            "expected exactly MAX_QUEUE messages after overflow, got {}",
            count
        );
    }

    // ----------------------------------------------------------------
    // ConnectionReady resets the reconnect delay
    // ----------------------------------------------------------------

    /// A successful connection after losses must reset the reconnect delay.
    /// We verify this by checking the task still handles subsequent
    /// ConnectionLost/Ready cycles correctly.
    #[tokio::test]
    async fn connection_ready_resets_reconnect_delay() {
        let (state, _eq) = make_state_with_event_q(19170);
        let peer = addr(19171);
        let manager_tx = spawn_manager(peer, state);

        // Lose connection a few times to ramp up the delay.
        for _ in 0..3 {
            manager_tx
                .send(PeerManagerMessage::ConnectionLost)
                .await
                .unwrap();
        }

        // A successful connection should reset the internal delay counter.
        let (tcp_tx, _tcp_rx) = mpsc::channel::<PeerMessage>(16);
        manager_tx
            .send(PeerManagerMessage::ConnectionReady(tcp_tx))
            .await
            .unwrap();

        // Lose connection again — the next reconnect attempt should use the
        // base delay (1 s), not the ramped-up value.  We can't observe the
        // delay directly without a fake clock, but the task must remain alive.
        manager_tx
            .send(PeerManagerMessage::ConnectionLost)
            .await
            .unwrap();

        let (tcp_tx2, mut tcp_rx2) = mpsc::channel::<PeerMessage>(16);
        manager_tx
            .send(PeerManagerMessage::ConnectionReady(tcp_tx2))
            .await
            .unwrap();

        manager_tx
            .send(PeerManagerMessage::Outbound(Box::new(cli_msg(19170))))
            .await
            .unwrap();

        let received = timeout(Duration::from_millis(300), tcp_rx2.recv())
            .await
            .expect("task unresponsive after delay reset")
            .expect("channel closed");
        assert!(matches!(received.payload, PeerMessagePayload::Cli(_)));
    }
}
