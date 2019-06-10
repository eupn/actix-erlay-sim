mod messages;
mod peer;
mod recset;
mod traffic_counter;

use crate::messages::Connect;
use crate::peer::PeerId;

use actix::prelude::*;

use crate::traffic_counter::TrafficCounter;
use structopt::*;

pub const RECONCIL_TIMEOUT_SEC: u64 = 1;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "simulator",
    about = "An Erlay transaction propagation technique simulator"
)]
struct SimulatorParameters {
    /// Use reconciliation (Erlay)
    #[structopt(short = "r", long = "reconciliation")]
    pub use_reconciliation: bool,

    /// Number of private nodes that doesn't have inbound connections.
    #[structopt(long = "numprivate", default_value = "8")]
    pub num_private_nodes: u32,

    /// Number of public nodes that have inbound connections.
    #[structopt(long = "numpublic", default_value = "2")]
    pub num_public_nodes: u32,

    /// Seed for a random number generator.
    #[structopt(short = "s", long = "seed")]
    pub seed: Option<u64>,
}

fn estimate_traffic_timeout_sec(parameters: &SimulatorParameters) -> u64 {
    (parameters.num_private_nodes as f32 * 0.57f32 + parameters.num_public_nodes as f32 * 1f32)
        as u64
}

fn main() {
    let parameters = SimulatorParameters::from_args();

    let traffic_timeout = estimate_traffic_timeout_sec(&parameters);

    let _ = actix::System::run(move || {
        let tcounter = TrafficCounter::new(traffic_timeout).start();

        let mut public_nodes = vec![];
        for id in 0u32..parameters.num_public_nodes {
            let peer_id = PeerId::Public(id);
            let peer = peer::Peer::new(
                peer_id,
                parameters.use_reconciliation,
                parameters.num_private_nodes as usize,
                tcounter.clone(),
                traffic_timeout,
                parameters.seed,
            );
            public_nodes.push((peer_id, peer.start()));
        }

        let mut private_nodes = vec![];
        for id in 0u32..parameters.num_private_nodes {
            let peer_id = PeerId::Private(id);
            let mut peer = peer::Peer::new(
                peer_id,
                parameters.use_reconciliation,
                parameters.num_private_nodes as usize,
                tcounter.clone(),
                traffic_timeout,
                parameters.seed,
            );
            for (id, pub_peer) in public_nodes.iter() {
                peer.add_outbound_peer(*id, pub_peer.clone());
            }

            private_nodes.push((peer_id, peer.start()));
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
