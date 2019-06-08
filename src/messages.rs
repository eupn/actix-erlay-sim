use crate::peer::{Peer, PeerId};
use crate::recset::ShortId;
use actix::prelude::*;
use siphasher::sip::SipHasher;
use std::hash::Hasher;

#[derive(Copy, Clone)]
pub struct Tx(pub [u8; 1024]);

impl ShortId<u64> for Tx {
    fn short_id(&self) -> u64 {
        let mut hasher = SipHasher::new_with_keys(0xDEu64, 0xADu64);
        hasher.write(&self.0);
        hasher.finish()
    }
}

#[derive(Copy, Clone, Message)]
pub struct PeerTx {
    pub from: PeerId,
    pub data: Tx,
}

#[derive(Clone, Message)]
pub struct Connect {
    pub from_addr: Addr<Peer>,
    pub from_id: PeerId,
}

#[derive(Clone, Message)]
pub struct ReconcileRequest {
    pub from_addr: Addr<Peer>,
    pub from_id: PeerId,
    pub sketch: Vec<u8>,
}

#[derive(Clone, Message)]
pub struct ReconcileResult {
    pub from_addr: Addr<Peer>,
    pub from_id: PeerId,
    pub missing: Vec<u64>,
}

#[derive(Clone, Message)]
pub struct TxRequest {
    pub from_addr: Addr<Peer>,
    pub from_id: PeerId,
    pub txid: u64,
}

#[derive(Debug, Clone, Message)]
pub struct TrafficReport {
    pub from_id: PeerId,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

pub trait Traffic {
    fn size_bytes(&self) -> u64;
}

impl Traffic for PeerTx {
    fn size_bytes(&self) -> u64 {
        (std::mem::size_of::<Tx>() + std::mem::size_of::<PeerId>()) as u64
    }
}

impl Traffic for Connect {
    fn size_bytes(&self) -> u64 {
        std::mem::size_of::<PeerId>() as u64
    }
}

impl Traffic for ReconcileRequest {
    fn size_bytes(&self) -> u64 {
        (std::mem::size_of::<PeerId>() + self.sketch.len()) as u64
    }
}

impl Traffic for ReconcileResult {
    fn size_bytes(&self) -> u64 {
        (std::mem::size_of::<PeerId>() + self.missing.len() * std::mem::size_of::<u64>()) as u64
    }
}

impl Traffic for TxRequest {
    fn size_bytes(&self) -> u64 {
        (std::mem::size_of::<PeerId>() + std::mem::size_of::<u64>()) as u64
    }
}
