use std::{collections::VecDeque, net::SocketAddr, time::Duration};

use tokio::{net::TcpStream, sync::mpsc, time::sleep};

use crate::{
    network::{message_types::PeerMessage, tcp_channel::initiate_tcp_connection},
    state::app_state::AppState,
};

// TODO: make this configurable
const MAX_QUEUE: usize = 256;
const RECONNECT_BASE_DELAY_SECS: u64 = 1;
const RECONNECT_MAX_DELAY_SECS: u64 = 30;

// enum with messages to a peer manager
//  Outbound: enqueue a message to be sent via tcp. the manager will make sure the tcp channel exists and sends it
//  ConnectionLost: notify the manager that the tcp channel broke down
//  ConnectionReady: notify the manager that the tcp channel was rebuilt
pub enum PeerManagerMessage {
    Outbound(Box<PeerMessage>),
    ConnectionLost,
    ConnectionReady(mpsc::Sender<PeerMessage>),
}

// the peer manager task routine
pub async fn peer_manager_task(
    peer_address: SocketAddr,
    mut manager_receiver: mpsc::Receiver<PeerManagerMessage>,
    state: AppState,
) {
    let mut queue: VecDeque<PeerMessage> = VecDeque::new();
    let mut tcp_channel_sender: Option<mpsc::Sender<PeerMessage>> = None;
    let mut reconnect_delay = RECONNECT_BASE_DELAY_SECS;

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
                            enqueue(&mut queue, *peer_msg, peer_address);
                        }
                    }
                    None => {
                        enqueue(&mut queue, *peer_msg, peer_address);
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
                reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX_DELAY_SECS);
            }

            PeerManagerMessage::ConnectionReady(sender) => {
                log::info!(
                    "connection to {} ready, flushing {} queued messages",
                    peer_address,
                    queue.len()
                );
                tcp_channel_sender = Some(sender.clone());
                // reset reconnect delay
                reconnect_delay = RECONNECT_BASE_DELAY_SECS;

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
        }
    }
}

fn enqueue(queue: &mut VecDeque<PeerMessage>, msg: PeerMessage, peer_address: SocketAddr) {
    if queue.len() >= MAX_QUEUE {
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
    tokio::spawn(async move {
        sleep(Duration::from_secs(delay_secs)).await;
        attempt_connect(peer_address, state).await;
    });
}

pub async fn attempt_connect(peer_address: SocketAddr, state: AppState) {
    match TcpStream::connect(&peer_address).await {
        Ok(stream) => {
            let (tcp_channel_sender, tcp_channel_receiver) =
                mpsc::channel(state.config.tcp_channel_queue_size);
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
