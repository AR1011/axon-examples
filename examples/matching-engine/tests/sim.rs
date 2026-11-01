//! Order entry as a protocol, on the seeded simulator: three gateways send seeded order flows to one sequencer over
//! links that lose, duplicate and delay messages, and resend what was not acked. The sequencer is the book: it
//! journals what it accepts, then acks, and it crashes once mid-run, coming back from its journal.
//!
//! Per seed: every order is acked; no order id is accepted twice however often it arrived; replaying the journal
//! rebuilds the book and its trades exactly; and the same seed gives the same journal and trades twice.

use std::collections::BTreeMap;
use std::time::Duration;

use axon_testkit::{SimConfig, Simulator, check_seeds};
use matching_engine::book::{Book, Command, Fill, Flow, Outcome, Reason};

#[derive(Clone, Debug)]
enum Wire {
    Order(Command),
    Ack(Command),
}

const GATEWAYS: usize = 3;
const ORDERS: usize = 300;
const WINDOW: usize = 16;

/// A cancel carries the id of the order it cancels, so the two share an id.
fn key(command: &Command) -> (bool, u64) {
    match command {
        Command::Limit { id, .. } | Command::Market { id, .. } => (false, *id),
        Command::Cancel { id } => (true, *id),
    }
}

struct Sequencer {
    book: Book,
    /// Survives a crash: the disk.
    journal: Vec<Command>,
    trades: Vec<Fill>,
}

impl Sequencer {
    fn restart(journal: Vec<Command>) -> Self {
        let mut sequencer = Self { book: Book::new(10_000, 10_000), journal: Vec::new(), trades: Vec::new() };
        for command in journal {
            sequencer.handle(command);
        }
        sequencer
    }

    fn handle(&mut self, command: Command) -> Outcome {
        let outcome = self.book.apply(command, &mut Vec::new());
        if !matches!(outcome, Outcome::Rejected(_)) {
            self.journal.push(command);
        }
        if let Outcome::Accepted { fills, .. } = &outcome {
            self.trades.extend(fills);
        }
        outcome
    }
}

struct Gateway {
    flow: Flow,
    made: usize,
    /// Sent and not acked, by order id. Resent every round until acked.
    open: BTreeMap<(bool, u64), Command>,
    acked: usize,
}

/// One run; returns the journal and the trades, for the same-seed check.
fn run(seed: u64) -> (Vec<Command>, Vec<Fill>) {
    let cfg = SimConfig {
        seed,
        jitter: Duration::from_millis(3),
        drop_every: 7,
        duplicate_every: 5,
        ordered: false,
        ..Default::default()
    };
    let mut net = Simulator::new(cfg);
    let sequencer_id = net.add_node("sequencer").unwrap();
    let gateway_ids: Vec<_> = (0..GATEWAYS).map(|g| net.add_node(&format!("gateway-{g}")).unwrap()).collect();
    let mut gateways: Vec<_> = (0..GATEWAYS as u64)
        .map(|g| Gateway { flow: Flow::new(seed ^ g, (g + 1) << 32), made: 0, open: BTreeMap::new(), acked: 0 })
        .collect();
    let mut sequencer = Sequencer::restart(Vec::new());
    let mut accepted_ids = std::collections::HashSet::new();
    let crash_round = 3 + seed % 5;

    for round in 0.. {
        assert!(round < 500, "seed {seed}: orders still open after 500 rounds");
        if gateways.iter().all(|g| g.made == ORDERS && g.open.is_empty()) {
            break;
        }
        for (g, gateway) in gateways.iter_mut().enumerate() {
            while gateway.made < ORDERS && gateway.open.len() < WINDOW {
                let command = gateway.flow.next_command();
                gateway.made += 1;
                gateway.open.insert(key(&command), command);
            }
            for command in gateway.open.values() {
                net.send(gateway_ids[g], sequencer_id, Wire::Order(*command)).unwrap();
            }
        }
        if round == crash_round {
            // Some orders are on the wire, some journaled but not acked: both must come out right.
            net.run(20, |net, delivery| {
                deliver(net, delivery, sequencer_id, &mut sequencer, &mut gateways, &gateway_ids, &mut accepted_ids)
            });
            net.crash_node(sequencer_id).unwrap();
            sequencer = Sequencer::restart(std::mem::take(&mut sequencer.journal));
        }
        net.run(usize::MAX, |net, delivery| {
            deliver(net, delivery, sequencer_id, &mut sequencer, &mut gateways, &gateway_ids, &mut accepted_ids)
        });
        let quiet = net.now() + Duration::from_millis(50);
        net.advance_idle_to(quiet).unwrap();
    }

    for gateway in &gateways {
        assert_eq!(gateway.acked, ORDERS, "seed {seed}: every order answered");
    }
    // No command twice in the journal, however many copies arrived. A cancel names the order it cancels.
    let unique: std::collections::HashSet<_> = sequencer.journal.iter().collect();
    assert_eq!(unique.len(), sequencer.journal.len(), "seed {seed}: a command accepted twice");
    // Replay rebuilds the same book and the same trades.
    let replayed = Sequencer::restart(sequencer.journal.clone());
    assert_eq!(replayed.book.digest(), sequencer.book.digest(), "seed {seed}");
    assert_eq!(replayed.trades, sequencer.trades, "seed {seed}");
    (sequencer.journal, sequencer.trades)
}

fn deliver(
    net: &mut Simulator<Wire>,
    delivery: axon_testkit::Delivery<Wire>,
    sequencer_id: usize,
    sequencer: &mut Sequencer,
    gateways: &mut [Gateway],
    gateway_ids: &[usize],
    accepted: &mut std::collections::HashSet<Command>,
) {
    match delivery.payload {
        Wire::Order(command) => {
            let outcome = sequencer.handle(command);
            if !matches!(outcome, Outcome::Rejected(_)) {
                assert!(accepted.insert(command), "{command:?} accepted twice");
            }
            // Journaled before the ack leaves; a lost ack only means a resend, answered `Duplicate`.
            let _ = net.send(sequencer_id, delivery.from, Wire::Ack(command));
        }
        Wire::Ack(command) => {
            let g = gateway_ids.iter().position(|id| *id == delivery.to).expect("an ack goes to a gateway");
            if gateways[g].open.remove(&key(&command)).is_some() {
                gateways[g].acked += 1;
            }
        }
    }
}

#[test]
fn order_entry_survives_loss_duplicates_reordering_and_a_sequencer_crash() {
    check_seeds(0..100, |seed| {
        run(seed);
    })
    .unwrap();
}

#[test]
fn the_same_seed_gives_the_same_journal_and_the_same_trades() {
    for seed in [1, 17, 99] {
        assert_eq!(run(seed), run(seed));
    }
    assert_ne!(run(1).0, run(2).0, "different seeds exercise different orders");
}

#[test]
fn a_resend_of_a_matched_order_is_refused_as_a_duplicate_not_matched_again() {
    let mut sequencer = Sequencer::restart(Vec::new());
    let sell = Command::Limit { id: 1, side: matching_engine::book::Side::Sell, price: 10, qty: 5 };
    let buy = Command::Market { id: 2, side: matching_engine::book::Side::Buy, qty: 5 };
    sequencer.handle(sell);
    assert!(matches!(sequencer.handle(buy), Outcome::Accepted { .. }));
    assert_eq!(sequencer.handle(buy), Outcome::Rejected(Reason::Duplicate));
    assert_eq!(sequencer.trades.len(), 1);
}
