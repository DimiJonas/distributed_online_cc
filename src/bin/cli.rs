use clap::Parser;
// use distributed_online_cc::cli_command::*;
// use distributed_online_cc::peer_message::*;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::net::{TcpStream, TcpListener};
use std::collections::HashMap;
use std::sync::Arc;
use bytes::{BytesMut, BufMut, Buf};
use serde::{Serialize, Deserialize};

type PeerId = u8;
type RequestId = u64;

// parse command line arguments
#[derive(Parser)]
struct Input {
    id: PeerId,
}

#[derive(Serialize, Deserialize, Debug)]
enum PeerMessage {
    Request { id: RequestId, payload: String },
    Response { id: RequestId, payload: String },
}

#[derive(Debug)]
enum CliCommand {
    Send { peer_id: PeerId, msg: String },
    Connect { peer_id: PeerId },
}

struct PeerHandle {
    sender: mpsc::Sender<PeerMessage>,
}

struct PeerManager {
    peers: HashMap<PeerId, PeerHandle>,
}

impl PeerManager {
    fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }
}

struct RequestIdContainer {
    next_id: RequestId,
}

impl RequestIdContainer {
    fn new(initial_value: RequestId) -> Self {
        Self { next_id: initial_value }
    }

    fn get_next_id(&mut self) -> RequestId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

struct PendingRequestManager {
    requests: HashMap<RequestId, oneshot::Sender<PeerMessage>>,
}

impl PendingRequestManager {
    fn new() -> Self {
        Self {
            requests: HashMap::new(),
        }
    }
}

fn get_address_from_id(id: PeerId) -> String {
    format!("127.0.0.1:{}", 7000 + id as u16)
}

fn get_id_from_address(addr: &std::net::SocketAddr) -> PeerId {
    (addr.port() - 7000) as PeerId
}

// spawn tasks that communicate with a certain peer
async fn peer_task(
    peer_id: PeerId,
    mut stream: TcpStream,
    mut rx: mpsc::Receiver<PeerMessage>,
    peer_manager: Arc<Mutex<PeerManager>>,
    pending_request_manager: Arc<Mutex<PendingRequestManager>>,
) {
    let mut read_buf = BytesMut::with_capacity(1024);

    loop {
        tokio::select! {
            // send msg
            Some(msg) = rx.recv() => {
                let msg_bytes = serde_json::to_vec(&msg).unwrap();

                // prefix the json data with its length for raw framing
                let mut framed_msg = BytesMut::with_capacity(4 + msg_bytes.len());
                framed_msg.put_u32(msg_bytes.len() as u32);
                framed_msg.extend_from_slice(&msg_bytes);

                if stream.write_all(&framed_msg).await.is_err() {
                    eprintln!("error sending to peer {}", peer_id);
                    break;
                }
            }
            // receive msg
            result = stream.read_buf(&mut read_buf) => {
                match result {
                    Ok(0) => {
                        // connection closed
                        println!("got zero bytes. peer {} disconnected", peer_id);
                        break;
                    }
                    Ok(_) => {
                        // some bytes received
                        loop {
                            if read_buf.len() < 4 {
                                // not even enough for length prefix
                                break;
                            }

                            let len = u32::from_be_bytes([read_buf[0], read_buf[1], read_buf[2], read_buf[3]]) as usize;

                            if read_buf.len() < 4 + len {
                                // (some) message bytes (still) missing
                                break;
                            }

                            read_buf.advance(4);
                            let msg_bytes = read_buf.split_to(len);

                            let msg: PeerMessage = serde_json::from_slice(&msg_bytes).unwrap();

                            handle_message(peer_id, msg, peer_manager.clone(), pending_request_manager.clone()).await;
                        }
                    }
                    Err(e) => {
                        eprintln!("error reading from peer: {}", e);
                        break;
                    }
                }
            }
        }
    }

    println!("Peer connection closed");

}

async fn handle_message(peer_id: PeerId, msg: PeerMessage, peer_manager: Arc<Mutex<PeerManager>>, pending_request_manager: Arc<Mutex<PendingRequestManager>>) {
    match msg {
        PeerMessage::Request { id, payload } => {
            println!("received request; peer_id: {}, request_id: {}, payload: {}", peer_id, id, payload);
            
            // send response
            let response = PeerMessage::Response {
                id: id,
                payload: format!("ACK '{}'", payload),
            };
            
            let sender = {
                let pm = peer_manager.lock().await;
                pm.peers.get(&peer_id).map(|handle| handle.sender.clone())
            };
            if let Some(sender) = sender {
                sender.send(response).await.unwrap();
            }
        }
        PeerMessage::Response { id, payload } => {
            println!("received response; peer_id: {}, request_id: {}, payload: {}", peer_id, id, payload);
            
            let sender = {
                let mut prm = pending_request_manager.lock().await;
                prm.requests.remove(&id)
            };
            if let Some(tx) = sender {
                tx.send(PeerMessage::Response { id, payload }).unwrap();
                println!("matched response with pending request. {} pending requests left", pending_request_manager.lock().await.requests.len());
            } else {
                println!("no pending request found for response with id {}", id);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Input::parse();
    let host_id = args.id;
    let host_address = get_address_from_id(host_id);

    let request_id_container = Arc::new(Mutex::new(RequestIdContainer::new(host_id as u64 * 100)));
    let peer_manager = Arc::new(Mutex::new(PeerManager::new()));
    let pending_request_manager = Arc::new(Mutex::new(PendingRequestManager::new()));

    // peer listener task
    let listener = TcpListener::bind(&host_address).await.unwrap();
    println!("listening for peer messages on {}", host_address);
    let pm_clone = peer_manager.clone();
    let prm_clone = pending_request_manager.clone();
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            println!("new peer connection from {}", addr);

            let peer_id = get_id_from_address(&addr);

            let (tx, rx) = mpsc::channel(100);
            pm_clone.lock().await.peers.insert(peer_id, PeerHandle { sender: tx.clone() });
            tokio::spawn(peer_task(peer_id, stream, rx, pm_clone.clone(), prm_clone.clone()));
        }
    });

    // cli command processing task
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let (cmd_tx, mut cmd_rx) = mpsc::channel(100);

    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            let parts = match shell_words::split(&line) {
                Ok(parts) => parts,
                Err(e) => {
                    println!("error parsing command: {}", e);
                    continue;
                }
            };

            match parts.as_slice() {
                [cmd, id_str, msg] if cmd == "send" => {
                    let command = CliCommand::Send {
                        peer_id: id_str.parse().unwrap(),
                        msg: msg.to_string(),
                    };
                    // send message
                    cmd_tx.send(command).await.unwrap();
                }
                [cmd, id_str] if cmd == "connect"=> {
                    let command = CliCommand::Connect {
                        peer_id: id_str.parse().unwrap(),
                    };
                    cmd_tx.send(command).await.unwrap();
                }
                _ => println!("unknown command"),
            }
        }
    });
        
    // task that receives the command and interprets it to send a message to the peer
    let prm_clone_clone = pending_request_manager.clone();
    tokio::spawn(async move {
        while let Some(command) = cmd_rx.recv().await {
            match command {
                CliCommand::Send { peer_id, msg } => {
                    let request_id = request_id_container.lock().await.get_next_id();
                    let message = PeerMessage::Request {
                        id: request_id,
                        payload: msg.clone(),
                    };
                    // waiting channel for response
                    let (tx, rx) = oneshot::channel();
                    
                    // send msg to peer_address
                    let sender = {
                        let pm = peer_manager.lock().await;
                        pm.peers.get(&peer_id).map(|handle| handle.sender.clone())
                    };
                    if let Some(sender) = sender {
                        prm_clone_clone.lock().await.requests.insert(request_id, tx);
                        sender.send(message).await.unwrap();
                        println!("sent request. now {} pending requests in total", pending_request_manager.lock().await.requests.len());
                        match rx.await {
                            Ok(resp) => println!("got response: {:?}", resp),
                            Err(_) => println!("error when trying to receive response for request id {}", request_id),
                        }
                    } else {
                        println!("peer {} not connected yet", peer_id);
                    }
                }
                CliCommand::Connect { peer_id } => {
                    if peer_id == host_id {
                        println!("cannot connect to self");
                        continue;
                    }

                    let peer_address = get_address_from_id(peer_id);

                    let stream = TcpStream::connect(&peer_address).await.unwrap();
                    println!("connected to peer {}" , peer_id);

                    let (tx, rx) = mpsc::channel(100);
                    peer_manager.lock().await.peers.insert(peer_id, PeerHandle { sender: tx.clone() });
                    tokio::spawn(peer_task(peer_id, stream, rx, peer_manager.clone(), pending_request_manager.clone()));
                }
            }
        }
    });

    // keeps main alive until i shut it down
    tokio::signal::ctrl_c().await.unwrap();
    println!("shutting down main");
}
