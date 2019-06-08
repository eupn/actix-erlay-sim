use actix::prelude::*;
use crate::peer::{Peer, PeerId};
use crate::recset::ShortId;
use siphasher::sip::SipHasher;
use std::hash::Hasher;

#[derive(Debug, Copy, Clone)]
pub struct Tx(pub [u8; 32]);

impl ShortId<u64> for Tx {
    fn short_id(&self) -> u64 {
        let mut hasher = SipHasher::new_with_keys(0xDEu64, 0xADu64);
        hasher.write(&self.0);
        hasher.finish()
    }
}

#[derive(Debug, Copy, Clone, Message)]
pub struct PeerTx {
    pub from: PeerId,
    pub data: Tx,
}

#[derive(Debug, Clone, Message)]
pub struct Connect {
    pub from_addr: Addr<Peer>,
    pub from_id: PeerId,
}

#[derive(Debug, Clone, Message)]
pub struct ReconcileRequest {
    pub from_addr: Addr<Peer>,
    pub from_id: PeerId,
    pub sketch: Vec<u8>,
}

#[derive(Debug, Clone, Message)]
pub struct ReconcileResult {
    pub from_addr: Addr<Peer>,
    pub from_id: PeerId,
    pub missing: Vec<u64>
}

#[derive(Debug, Clone, Message)]
pub struct TxRequest {
    pub from_addr: Addr<Peer>,
    pub from_id: PeerId,
    pub txid: u64,
}
