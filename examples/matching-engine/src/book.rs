//! The order book: pure, synchronous, deterministic. The same commands in the same order give the same outcomes,
//! trades and state, which is what lets a journal of commands rebuild a book exactly.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};

pub type OrderId = u64;
pub type Price = u64;
pub type Qty = u64;
/// A price level's price and total quantity.
pub type Depth = (Price, Qty);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Command {
    Limit {
        id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
    },
    /// Immediate or cancel: what does not fill at once is not kept.
    Market {
        id: OrderId,
        side: Side,
        qty: Qty,
    },
    Cancel {
        id: OrderId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    pub maker: OrderId,
    pub taker: OrderId,
    pub price: Price,
    pub qty: Qty,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reason {
    /// This order id was accepted before.
    Duplicate,
    UnknownOrder,
    /// The book holds `max_resting` orders.
    BookFull,
    Invalid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Accepted { fills: Vec<Fill>, resting: Qty, unfilled: Qty },
    Cancelled { qty: Qty },
    Rejected(Reason),
}

/// A price level whose total changed: `qty` 0 means the level is gone.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
}

#[derive(Clone, Copy, Debug)]
struct Resting {
    id: OrderId,
    qty: Qty,
}

#[derive(Default, Debug)]
struct Queue {
    total: Qty,
    orders: VecDeque<Resting>,
}

pub struct Book {
    bids: BTreeMap<Price, Queue>,
    asks: BTreeMap<Price, Queue>,
    /// Lookups only, never iterated: iteration order must not reach an outcome.
    index: HashMap<OrderId, (Side, Price)>,
    seen: HashSet<OrderId>,
    seen_order: VecDeque<OrderId>,
    max_resting: usize,
    max_seen: usize,
}

impl Book {
    /// `max_resting` bounds the orders the book keeps; `max_seen` how many recent ids it refuses as duplicates.
    pub fn new(max_resting: usize, max_seen: usize) -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            index: HashMap::new(),
            seen: HashSet::new(),
            seen_order: VecDeque::new(),
            max_resting,
            max_seen,
        }
    }

    /// Applies one command. `levels` gets every level whose total changed, in the order they changed.
    pub fn apply(&mut self, command: Command, levels: &mut Vec<Level>) -> Outcome {
        match command {
            Command::Cancel { id } => self.cancel(id, levels),
            Command::Limit { id, side, price, qty } => self.take(id, side, Some(price), qty, levels),
            Command::Market { id, side, qty } => self.take(id, side, None, qty, levels),
        }
    }

    fn take(&mut self, id: OrderId, side: Side, limit: Option<Price>, qty: Qty, levels: &mut Vec<Level>) -> Outcome {
        if qty == 0 || limit == Some(0) {
            return Outcome::Rejected(Reason::Invalid);
        }
        if self.seen.contains(&id) {
            return Outcome::Rejected(Reason::Duplicate);
        }
        if limit.is_some() && self.index.len() >= self.max_resting {
            return Outcome::Rejected(Reason::BookFull);
        }
        self.remember(id);
        let (book, maker_side) = match side {
            Side::Buy => (&mut self.asks, Side::Sell),
            Side::Sell => (&mut self.bids, Side::Buy),
        };
        let mut left = qty;
        let mut fills = Vec::new();
        while left > 0 {
            // Best price first: lowest ask for a buy, highest bid for a sell.
            let best = match side {
                Side::Buy => book.first_entry(),
                Side::Sell => book.last_entry(),
            };
            let Some(mut level) = best else { break };
            let price = *level.key();
            let crosses = match (side, limit) {
                (_, None) => true,
                (Side::Buy, Some(limit)) => price <= limit,
                (Side::Sell, Some(limit)) => price >= limit,
            };
            if !crosses {
                break;
            }
            let queue = level.get_mut();
            while left > 0
                && let Some(maker) = queue.orders.front_mut()
            {
                let traded = left.min(maker.qty);
                fills.push(Fill { maker: maker.id, taker: id, price, qty: traded });
                maker.qty -= traded;
                queue.total -= traded;
                left -= traded;
                if maker.qty == 0 {
                    self.index.remove(&maker.id);
                    queue.orders.pop_front();
                }
            }
            levels.push(Level { side: maker_side, price, qty: queue.total });
            if queue.orders.is_empty() {
                level.remove();
            }
        }
        let Some(price) = limit.filter(|_| left > 0) else {
            return Outcome::Accepted { fills, resting: 0, unfilled: left };
        };
        let queue = match side {
            Side::Buy => self.bids.entry(price).or_default(),
            Side::Sell => self.asks.entry(price).or_default(),
        };
        queue.orders.push_back(Resting { id, qty: left });
        queue.total += left;
        levels.push(Level { side, price, qty: queue.total });
        self.index.insert(id, (side, price));
        Outcome::Accepted { fills, resting: left, unfilled: 0 }
    }

    fn cancel(&mut self, id: OrderId, levels: &mut Vec<Level>) -> Outcome {
        let Some((side, price)) = self.index.remove(&id) else { return Outcome::Rejected(Reason::UnknownOrder) };
        let book = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let Some(queue) = book.get_mut(&price) else { return Outcome::Rejected(Reason::UnknownOrder) };
        // ponytail: a linear scan of one level; an intrusive list if levels hold thousands of orders.
        let Some(at) = queue.orders.iter().position(|o| o.id == id) else {
            return Outcome::Rejected(Reason::UnknownOrder);
        };
        let qty = queue.orders.remove(at).map_or(0, |o| o.qty);
        queue.total -= qty;
        levels.push(Level { side, price, qty: queue.total });
        if queue.orders.is_empty() {
            book.remove(&price);
        }
        Outcome::Cancelled { qty }
    }

    fn remember(&mut self, id: OrderId) {
        self.seen.insert(id);
        self.seen_order.push_back(id);
        if self.seen_order.len() > self.max_seen
            && let Some(old) = self.seen_order.pop_front()
        {
            self.seen.remove(&old);
        }
    }

    pub fn resting(&self) -> usize {
        self.index.len()
    }

    pub fn best_bid(&self) -> Option<Price> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<Price> {
        self.asks.keys().next().copied()
    }

    /// The top `depth` levels of each side, best first.
    pub fn depth(&self, depth: usize) -> (Vec<Depth>, Vec<Depth>) {
        let bids = self.bids.iter().rev().take(depth).map(|(p, q)| (*p, q.total)).collect();
        let asks = self.asks.iter().take(depth).map(|(p, q)| (*p, q.total)).collect();
        (bids, asks)
    }

    /// FNV-1a over every resting order in priority order: equal digests mean equal books.
    pub fn digest(&self) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        let mut eat = |v: u64| {
            for byte in v.to_le_bytes() {
                hash = (hash ^ byte as u64).wrapping_mul(0x0000_0100_0000_01b3);
            }
        };
        for (tag, book) in [(1, &self.bids), (2, &self.asks)] {
            eat(tag);
            for (price, queue) in book {
                eat(*price);
                queue.orders.iter().for_each(|o| {
                    eat(o.id);
                    eat(o.qty);
                });
            }
        }
        hash
    }
}

/// A seeded stream of realistic commands: limits around a drifting mid, some markets, cancels of live ids.
pub struct Flow {
    state: u64,
    next_id: OrderId,
    live: Vec<OrderId>,
    mid: Price,
}

impl Flow {
    /// Ids start at `first_id`, so several flows can share a book without colliding.
    pub fn new(seed: u64, first_id: OrderId) -> Self {
        Self { state: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1, next_id: first_id, live: Vec::new(), mid: 10_000 }
    }

    fn rand(&mut self) -> u64 {
        // xorshift64*
        self.state ^= self.state >> 12;
        self.state ^= self.state << 25;
        self.state ^= self.state >> 27;
        self.state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn next_command(&mut self) -> Command {
        let roll = self.rand() % 100;
        if roll < 20 && !self.live.is_empty() {
            let at = (self.rand() % self.live.len() as u64) as usize;
            return Command::Cancel { id: self.live.swap_remove(at) };
        }
        let id = self.next_id;
        self.next_id += 1;
        let side = if self.rand().is_multiple_of(2) { Side::Buy } else { Side::Sell };
        let qty = 1 + self.rand() % 100;
        if roll < 30 {
            return Command::Market { id, side, qty };
        }
        self.mid = (self.mid + self.rand() % 5).saturating_sub(2).max(100);
        let offset = self.rand() % 20;
        let price = match side {
            Side::Buy => self.mid - offset + 5,
            Side::Sell => self.mid + offset - 5,
        };
        self.live.push(id);
        if self.live.len() > 4096 {
            self.live.swap_remove(0);
        }
        Command::Limit { id, side, price, qty }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(book: &mut Book, commands: &[Command]) -> Vec<Outcome> {
        let mut levels = Vec::new();
        commands.iter().map(|c| book.apply(*c, &mut levels)).collect()
    }

    #[test]
    fn price_then_time_priority() {
        let mut book = Book::new(100, 100);
        let out = run(
            &mut book,
            &[
                Command::Limit { id: 1, side: Side::Sell, price: 101, qty: 5 },
                Command::Limit { id: 2, side: Side::Sell, price: 100, qty: 5 },
                Command::Limit { id: 3, side: Side::Sell, price: 100, qty: 5 },
                Command::Limit { id: 4, side: Side::Buy, price: 101, qty: 12 },
            ],
        );
        let Outcome::Accepted { fills, resting, unfilled } = &out[3] else { panic!("{out:?}") };
        let makers: Vec<_> = fills.iter().map(|f| (f.maker, f.price, f.qty)).collect();
        assert_eq!(makers, [(2, 100, 5), (3, 100, 5), (1, 101, 2)]);
        assert_eq!((*resting, *unfilled), (0, 0));
        assert_eq!(book.depth(5), (vec![], vec![(101, 3)]));
    }

    #[test]
    fn a_limit_rests_what_does_not_cross_and_a_market_keeps_nothing() {
        let mut book = Book::new(100, 100);
        let out = run(
            &mut book,
            &[
                Command::Limit { id: 1, side: Side::Sell, price: 100, qty: 5 },
                Command::Limit { id: 2, side: Side::Buy, price: 99, qty: 7 },
                Command::Market { id: 3, side: Side::Buy, qty: 8 },
                Command::Market { id: 4, side: Side::Sell, qty: 10 },
            ],
        );
        assert_eq!(out[1], Outcome::Accepted { fills: vec![], resting: 7, unfilled: 0 });
        assert!(matches!(&out[2], Outcome::Accepted { resting: 0, unfilled: 3, fills } if fills.len() == 1));
        assert!(matches!(&out[3], Outcome::Accepted { resting: 0, unfilled: 3, .. }));
        assert_eq!(book.resting(), 0);
        assert_eq!((book.best_bid(), book.best_ask()), (None, None));
    }

    #[test]
    fn cancels_duplicates_and_bounds() {
        let mut book = Book::new(2, 3);
        let out = run(
            &mut book,
            &[
                Command::Limit { id: 1, side: Side::Buy, price: 10, qty: 5 },
                Command::Limit { id: 1, side: Side::Buy, price: 10, qty: 5 },
                Command::Limit { id: 2, side: Side::Buy, price: 10, qty: 5 },
                Command::Limit { id: 3, side: Side::Buy, price: 9, qty: 5 },
                Command::Cancel { id: 1 },
                Command::Cancel { id: 1 },
                Command::Limit { id: 4, side: Side::Buy, price: 0, qty: 5 },
            ],
        );
        assert_eq!(out[1], Outcome::Rejected(Reason::Duplicate));
        assert_eq!(out[3], Outcome::Rejected(Reason::BookFull));
        assert_eq!(out[4], Outcome::Cancelled { qty: 5 });
        assert_eq!(out[5], Outcome::Rejected(Reason::UnknownOrder));
        assert_eq!(out[6], Outcome::Rejected(Reason::Invalid));
        assert_eq!(book.depth(5).0, vec![(10, 5)]);
    }

    #[test]
    fn quantity_is_conserved_over_a_long_flow() {
        let mut book = Book::new(100_000, 100_000);
        let mut flow = Flow::new(7, 1);
        let mut levels = Vec::new();
        let (mut placed, mut traded, mut gone) = (0u64, 0u64, 0u64);
        for _ in 0..50_000 {
            let command = flow.next_command();
            match (command, book.apply(command, &mut levels)) {
                (
                    Command::Limit { qty, .. } | Command::Market { qty, .. },
                    Outcome::Accepted { fills, unfilled, .. },
                ) => {
                    placed += qty;
                    traded += 2 * fills.iter().map(|f| f.qty).sum::<u64>();
                    gone += unfilled;
                }
                (_, Outcome::Cancelled { qty }) => gone += qty,
                _ => {}
            }
        }
        let (bids, asks) = book.depth(usize::MAX);
        let resting: u64 = bids.iter().chain(&asks).map(|l| l.1).sum();
        assert_eq!(placed, traded + gone + resting);
        assert!(bids.first().zip(asks.first()).is_none_or(|(b, a)| b.0 < a.0), "a crossed book");
    }
}
