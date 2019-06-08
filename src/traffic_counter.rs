use crate::messages::TrafficReport;
use crate::peer::PeerId;
use actix::prelude::*;
use std::collections::HashMap;
use std::time::Duration;
use std::process;

#[derive(Debug, Clone, Default)]
pub struct TrafficData {
    pub bytes_received: u64,
    pub bytes_sent: u64,
}

pub struct TrafficCounter {
    pub traffic: HashMap<PeerId, TrafficData>,
}

impl TrafficCounter {
    pub fn new() -> Self {
        TrafficCounter {
            traffic: Default::default(),
        }
    }
}

impl Actor for TrafficCounter {
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_later(Duration::from_secs(31), |act, _| {
            let total_traffic = act
                .traffic
                .values()
                .fold(0, |v, next| v + (next.bytes_sent + next.bytes_received));

            println!("{}", total_traffic);

            /*
            println!("Traffic per peer:");
            let mut traffic = act.traffic.iter().collect::<Vec<_>>();
            traffic.sort_by_key(|(id, _)| Into::<u64>::into(**id));

            for (id, traffic) in traffic {
                println!(
                    "{:?}: {} ↑ {} ↓ (bytes)",
                    id, traffic.bytes_sent, traffic.bytes_received
                );
            }*/

            process::exit(0);
        });
    }
}

impl Handler<TrafficReport> for TrafficCounter {
    type Result = ();

    fn handle(&mut self, msg: TrafficReport, _: &mut Self::Context) -> Self::Result {
        if !self.traffic.contains_key(&msg.from_id) {
            self.traffic.insert(msg.from_id, TrafficData::default());
        }

        if let Some(data) = self.traffic.get_mut(&msg.from_id) {
            *data = TrafficData {
                bytes_received: msg.bytes_received,
                bytes_sent: msg.bytes_sent,
            };
        }
    }
}
