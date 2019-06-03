use actix::prelude::*;

use byteorder::{ByteOrder, LittleEndian};
use rand::{self, seq::SliceRandom, Rng, SeedableRng};
use rand_xorshift::XorShiftRng;

use std::collections::HashMap;
use std::fmt::{Debug, Error, Formatter};
use std::time::Duration;

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

    seed: u64,
}

impl Debug for PeerId {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match self {
            PeerId::Public(id) => write!(f, "pub{}", id),
            PeerId::Private(id) => write!(f, "priv{}", id),
        }
    }
}

impl Into<u64> for PeerId {
    fn into(self) -> u64 {
        let id = match self {
            PeerId::Public(id) => id + 1,
            PeerId::Private(id) => (id + 1) << 16,
        };

        id as u64
    }
}

impl Peer {
    pub fn new(id: PeerId) -> Self {
        Peer {
            id,
            outbound: HashMap::new(),
            inbound: HashMap::new(),

            seed: id.into(),
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

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_later(Duration::from_secs(5), |peer, _ct| {
            println!(
                "Peer {:?} outbound connections: {:?}",
                peer.id,
                peer.outbound.keys().collect::<Vec<_>>()
            );
            println!(
                "Peer {:?} inbound connections: {:?}",
                peer.id,
                peer.inbound.keys().collect::<Vec<_>>()
            );
        });
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        Running::Stop
    }
}

impl Handler<Tx> for Peer {
    type Result = ();

    fn handle(&mut self, msg: Tx, _ctx: &mut Context<Self>) {
        // Perform low-fanout flooding if it is a public node
        if self.is_public() {
            let mut seed = [0u8; 16];
            LittleEndian::write_u64(&mut seed, self.seed);

            let mut rng = XorShiftRng::from_seed(seed);

            let peers = self.outbound.clone();
            {
                let mut peers = peers.values().collect::<Vec<_>>();
                peers.shuffle(&mut rng);

                for peer in peers.into_iter().take(8) {
                    peer.do_send(msg);
                }
            }

            self.seed = rng.gen();
        } else {
            // TODO: fill reconciliation set
        }
    }
}

impl Handler<Connect> for Peer {
    type Result = ();

    fn handle(&mut self, msg: Connect, ctx: &mut Context<Self>) {
        // Don't connect to self
        if msg.0 == self.id {
            return;
        }

        println!("{:?} -> {:?};", msg.0, self.id);

        // Register inbound connection
        self.inbound.insert(msg.0, ctx.address());

        // Connect back
        let is_private = match msg.0 {
            PeerId::Private(_) => true,
            _ => false,
        };

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
