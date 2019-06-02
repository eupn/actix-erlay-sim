use actix::prelude::*;

use rand::{self, Rng, thread_rng, seq::SliceRandom};

use std::collections::HashMap;
use std::time::Duration;
use std::fmt::{Debug, Error, Formatter};

#[derive(Debug, Copy, Clone, Message)]
pub struct Tx(pub [u8; 32]);

#[derive(Debug, Copy, Clone, Message)]
pub struct Connect(pub PeerId);

/// Defines possible states of the peer.
#[derive(Debug, Copy, Clone)]
pub enum PeerState {
    Idle,
}

#[derive(Copy, Clone, Hash, PartialEq, Eq)]
pub enum PeerId {
    Public(u32),
    Private(u32),
}

/// Describes single independent peer in the network.
#[derive(Debug)]
pub struct Peer {
    /// ID of this peer.
    pub id: PeerId,

    /// Outbound connections
    pub outbound: HashMap<PeerId, Addr<Peer>>,

    /// Inbound connections
    pub inbound: HashMap<PeerId, Addr<Peer>>,
}

impl Debug for PeerId {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match self {
            PeerId::Public(id) => write!(f, "pub{}", id),
            PeerId::Private(id) => write!(f, "priv{}", id),
        }
    }
}

impl Peer {
    pub fn new(id: PeerId) -> Self {
        Peer {
            id,
            outbound: HashMap::new(),
            inbound: HashMap::new()
        }
    }

    fn is_connected_to(&self, id: PeerId) -> bool {
        self.outbound.contains_key(&id)
    }

    pub fn add_outbound_peer(&mut self, id: PeerId, addr: Addr<Peer>) {
        self.outbound.insert(id, addr);
    }

    fn is_public(&self) -> bool {
        !self.inbound.is_empty()
    }
}

/// Make actor from `Peer`
impl Actor for Peer {
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {}

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        Running::Stop
    }
}

impl Handler<Tx> for Peer {
    type Result = ();

    fn handle(&mut self, msg: Tx, ctx: &mut Context<Self>) {
        // Perform low-fanout flooding if it is a public node
        if self.is_public() {
            let mut rng = thread_rng();
            let mut peers = self.outbound.clone();
            {
                let mut peers = peers.values().collect::<Vec<_>>();
                peers.shuffle(&mut rng);

                for peer in peers.into_iter().take(8) {
                    peer.do_send(msg);
                }
            }
        }
    }
}

impl Handler<Connect> for Peer {
    type Result = ();

    fn handle(&mut self, msg: Connect, ctx: &mut Context<Self>) {
        // Don't connect to self
        if msg.0 == self.id {
            return
        }

        println!("{:?} -> {:?};", msg.0, self.id);

        // Register inbound connection
        self.inbound.insert(msg.0, ctx.address());

        // Connect back
        let is_private = match msg.0 { PeerId::Private(_) => true, _ => false };
        if !is_private && !self.is_connected_to(msg.0) {
            self.add_outbound_peer(msg.0, ctx.address());
            ctx.address().do_send(Connect(self.id));
        }
    }
}

fn hash(bytes: &[u8]) -> Vec<u8> {
    use sha2::Digest;

    let mut sha = sha2::Sha256::new();
    sha.input(bytes);
    sha.result().to_vec()
}
