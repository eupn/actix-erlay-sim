use actix::prelude::*;

use byteorder::{ByteOrder, LittleEndian};
use rand::{self, seq::SliceRandom, Rng, SeedableRng};
use rand_xorshift::XorShiftRng;

use std::collections::HashMap;
use std::fmt::{Debug, Error, Formatter};
use std::time::Duration;

use crate::recset::{RecSet, ShortId};
use crate::RECONCIL_TIMEOUT_SEC;

use crate::messages::{Connect, PeerTx, ReconcileRequest, ReconcileResult, Tx, TxRequest};

const RECONCILIATION_CAPACITY: usize = 128;

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

    /// Holds a mempool, set of transactions by txid
    pub mempool: HashMap<u64, Tx>,

    /// Holds set of received transactions ID from an individual peer.
    pub received_txs: HashMap<PeerId, Vec<u64>>,

    /// Set of transactions for reconciliation with any peer
    pub reconciliation_set: RecSet<u64>,

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

impl From<u64> for PeerId {
    fn from(v: u64) -> Self {
        if v < 1 << 16 {
            PeerId::Public(v as u32 - 1)
        } else {
            PeerId::Private(v as u32)
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

            mempool: Default::default(),
            received_txs: Default::default(),
            reconciliation_set: RecSet::new(RECONCILIATION_CAPACITY),
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
        ctx.run_later(Duration::from_secs(1), |act, _| {
            if !act.is_public() {
                let mut tx_data = [0u8; 32];
                let mut seed = [0u8; 16];
                LittleEndian::write_u64(&mut seed, act.seed);
                let mut rng = XorShiftRng::from_seed(seed);
                rng.fill(&mut tx_data);
                let tx = Tx(tx_data);

                if let Some(addr) = act.outbound.values().collect::<Vec<_>>().first() {
                    addr.do_send(PeerTx {
                        from: act.id,
                        data: tx,
                    });
                }

                act.seed = rng.gen();
            }
        });

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

            let mut txs = peer.mempool.values().map(|tx| tx.short_id()).collect::<Vec<_>>();
            txs.sort();
            println!("Peer {:?} txs: {:?}", peer.id, txs);
        });

        ctx.run_interval(Duration::from_secs(RECONCIL_TIMEOUT_SEC), |peer, ctx| {
            for (_, peer_addr) in peer.outbound.iter() {
                let sketch = peer.reconciliation_set.sketch();
                let msg = ReconcileRequest {
                    from_addr: ctx.address(),
                    from_id: peer.id,
                    sketch,
                };

                peer_addr.do_send(msg);
            }
        });
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        Running::Stop
    }
}

impl Handler<PeerTx> for Peer {
    type Result = ();

    fn handle(&mut self, msg: PeerTx, _ctx: &mut Context<Self>) {
        let txid = msg.data.short_id();

        // Don't relay nor save already processed transaction
        if self.mempool.contains_key(&txid) {
            return;
        }

        if !self.reconciliation_set.contains(&txid) {
            self.reconciliation_set.insert(txid);
        }

        self.mempool.insert(txid, msg.data);

        if !self.received_txs.contains_key(&msg.from) {
            self.received_txs.insert(msg.from, vec![]);
        }

        if let Some(txs) = self.received_txs.get_mut(&msg.from) {
            txs.push(txid);
        }

        // Perform low-fanout flooding if it's a public node
        if self.is_public() {
            let mut seed = [0u8; 16];
            LittleEndian::write_u64(&mut seed, self.seed);

            let mut rng = XorShiftRng::from_seed(seed);

            let peers = self.outbound.clone();
            {
                let mut peers = peers.iter().collect::<Vec<_>>();
                peers.shuffle(&mut rng);

                for (_, peer) in peers.into_iter().take(8) {
                    let new_msg = PeerTx {
                        from: self.id,
                        data: msg.data,
                    };

                    peer.do_send(new_msg);
                }
            }

            self.seed = rng.gen();
        }
    }
}

impl Handler<Connect> for Peer {
    type Result = ();

    fn handle(&mut self, msg: Connect, ctx: &mut Context<Self>) {
        // Don't connect to self
        if msg.from_id == self.id {
            return;
        }

        // Don't connect if already connected
        if self.is_connected_to(msg.from_id) {
            return;
        }

        // Register inbound connection
        self.inbound.insert(msg.from_id, msg.from_addr.clone());

        // Connect back
        let is_private = match msg.from_id {
            PeerId::Private(_) => true,
            _ => false,
        };

        println!("{:?} -> {:?};", msg.from_id, self.id);

        if !is_private && !self.is_connected_to(msg.from_id) {
            self.add_outbound_peer(msg.from_id, msg.from_addr.clone());
            msg.from_addr.do_send(Connect {
                from_addr: ctx.address(),
                from_id: self.id,
            });
        }
    }
}

impl Handler<ReconcileRequest> for Peer {
    type Result = ();

    fn handle(&mut self, msg: ReconcileRequest, ctx: &mut Self::Context) -> Self::Result {
        if let Ok(missing) = self.reconciliation_set.reconcile_with(&msg.sketch) {
            let rec_res = ReconcileResult {
                from_addr: ctx.address(),
                from_id: self.id,
                missing,
            };

            msg.from_addr.do_send(rec_res);
        }
    }
}

impl Handler<ReconcileResult> for Peer {
    type Result = ();

    fn handle(&mut self, msg: ReconcileResult, ctx: &mut Self::Context) -> Self::Result {
        for txid in msg.missing {
            let req_tx = TxRequest {
                from_addr: ctx.address(),
                from_id: self.id,
                txid,
            };

            msg.from_addr.do_send(req_tx);
        }
    }
}

impl Handler<TxRequest> for Peer {
    type Result = ();

    fn handle(&mut self, msg: TxRequest, _ctx: &mut Self::Context) -> Self::Result {
        if let Some(tx) = self.mempool.get(&msg.txid) {
            let tx_msg = PeerTx {
                from: self.id,
                data: *tx,
            };

            msg.from_addr.do_send(tx_msg);
            return;
        }
    }
}
