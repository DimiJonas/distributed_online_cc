use crate::types::PeerId;

pub fn get_id_from_address(addr: &std::net::SocketAddr) -> PeerId {
    (addr.port() - 7000) as PeerId
}

pub fn get_address_from_id(id: PeerId) -> String {
    format!("127.0.0.1:{}", 7000 + id as u16)
}
