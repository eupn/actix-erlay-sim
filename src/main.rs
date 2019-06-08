mod messages;
mod peer;
mod recset;

use crate::messages::Connect;
use crate::peer::PeerId;

use actix::prelude::*;

const NUM_PRIVATE_NODES: u32 = 8;
const NUM_PUBLIC_NODES: u32 = 2;

pub const RECONCIL_TIMEOUT_SEC: u64 = 2;
pub const USE_RECONCILIATION: bool = true;

fn main() {
    actix::System::run(|| {
        let mut public_nodes = vec![];
        for id in 0u32..NUM_PUBLIC_NODES {
            let peer_id = PeerId::Public(id);
            let peer = peer::Peer::new(peer_id, USE_RECONCILIATION);
            public_nodes.push((peer_id, Arbiter::start(|_| peer)));
        }

        let mut private_nodes = vec![];
        for id in 0u32..NUM_PRIVATE_NODES {
            let peer_id = PeerId::Private(id);
            let mut peer = peer::Peer::new(peer_id, USE_RECONCILIATION);
            for (id, pub_peer) in public_nodes.iter() {
                peer.add_outbound_peer(*id, pub_peer.clone());
            }

            private_nodes.push((peer_id, Arbiter::start(|_| peer)));
        }

        // Interconnect public nodes
        for (this_id, public_peer) in public_nodes.iter() {
            for (other_id, other_public_peer) in public_nodes.iter() {
                if *this_id != *other_id {
                    other_public_peer.do_send(Connect {
                        from_addr: public_peer.clone(),
                        from_id: this_id.clone(),
                    });
                }
            }
        }

        // Connect all private nodes to the all public nodes
        for (this_id, private_peer) in private_nodes.iter() {
            for (_other_id, other_public_peer) in public_nodes.iter() {
                other_public_peer.do_send(Connect {
                    from_addr: private_peer.clone(),
                    from_id: this_id.clone(),
                });
            }
        }
    });
}
