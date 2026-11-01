//! The backend service: prices a SKU (read, idempotent) and places orders (write, not idempotent).

use std::collections::VecDeque;
use std::time::Duration;

use axon::{Actor, Context};
use axon_cluster::{ActorProtocol, CallError, Rejection, Reply, WireProtocol};
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Quote(u64),
    Order(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
    Price(u64),
    Placed(u64),
    /// Refused before doing the work: every slot is taken. Safe to send elsewhere.
    Busy,
}

pub struct QuoteApi;

/// Every message and answer: a tag byte, then a big-endian u64.
fn tagged(tag: u8, n: u64) -> Bytes {
    let mut out = BytesMut::with_capacity(9);
    out.put_u8(tag);
    out.put_u64(n);
    out.freeze()
}

impl WireProtocol for QuoteApi {
    type Message = Op;
    type Reply = Answer;
    const ID: u64 = 0xA9_6A7E;
    const SCHEMA: u16 = 2;

    fn encode(op: Op) -> Result<Bytes, CallError> {
        Ok(match op {
            Op::Quote(n) => tagged(0, n),
            Op::Order(n) => tagged(1, n),
        })
    }

    fn decode_reply(mut body: Bytes) -> Result<Answer, CallError> {
        let bad = || CallError::Codec("bad answer".into());
        let tag = body.try_get_u8().map_err(|_| bad())?;
        let n = body.try_get_u64().map_err(|_| bad())?;
        match tag {
            0 => Ok(Answer::Price(n)),
            1 => Ok(Answer::Placed(n)),
            2 => Ok(Answer::Busy),
            _ => Err(bad()),
        }
    }
}

impl ActorProtocol for QuoteApi {
    type Mailbox = Msg;

    fn encode_reply(answer: Answer) -> Result<Bytes, CallError> {
        Ok(match answer {
            Answer::Price(n) => tagged(0, n),
            Answer::Placed(n) => tagged(1, n),
            Answer::Busy => tagged(2, 0),
        })
    }

    fn decode(mut body: Bytes, reply: Option<Reply<Answer>>) -> Result<Msg, Rejection> {
        let bad = |_| Rejection::InvalidMessage;
        let tag = body.try_get_u8().map_err(bad)?;
        let n = body.try_get_u64().map_err(bad)?;
        let op = match tag {
            0 => Op::Quote(n),
            1 => Op::Order(n),
            _ => return Err(Rejection::InvalidMessage),
        };
        Ok(Msg::Call { op, reply })
    }
}

pub enum Msg {
    Call {
        op: Op,
        reply: Option<Reply<Answer>>,
    },
    /// The earliest held answer is due.
    Due,
}

#[derive(Clone, Copy, Debug)]
pub struct ServiceConfig {
    /// Hash rounds per call: the service's own CPU work, done in the handler.
    pub work: u32,
    /// Time each answer waits before it is sent, as a slow dependency would add.
    pub delay: Duration,
    /// Answers held for `delay` at once; past it a call is answered `Busy`.
    pub capacity: usize,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self { work: 200, delay: Duration::ZERO, capacity: 1024 }
    }
}

pub struct Service {
    cfg: ServiceConfig,
    orders: u64,
    /// Answers waiting out `delay`, oldest first; one timer is armed for the head.
    held: VecDeque<(Duration, Reply<Answer>, Answer)>,
    armed: bool,
}

impl Service {
    pub fn new(cfg: ServiceConfig) -> Self {
        Self { cfg, orders: 0, held: VecDeque::new(), armed: false }
    }

    fn arm(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        if let (false, Some(&(due, ..))) = (self.armed, self.held.front()) {
            cx.schedule_at(due, Msg::Due).map_err(|e| format!("timer: {:?}", e.reason))?;
            self.armed = true;
        }
        Ok(())
    }
}

/// FNV-1a over the SKU, `rounds` times: stands in for pricing.
pub fn price(sku: u64, rounds: u32) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for _ in 0..rounds.max(1) {
        for b in sku.to_le_bytes() {
            h = (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3);
        }
    }
    h % 100_000
}

impl Actor for Service {
    type Message = Msg;
    type Error = String;

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Call { op, reply } => {
                let Some(reply) = reply else { return Ok(()) }; // a tell: nobody waits, nothing to do
                // The far node drops a late request only before admission, so one that waited here is checked again.
                if reply.is_abandoned() {
                    return Ok(());
                }
                if !self.cfg.delay.is_zero() && self.held.len() >= self.cfg.capacity {
                    let _ = reply.send(Answer::Busy);
                    return Ok(());
                }
                let answer = match op {
                    Op::Quote(sku) => Answer::Price(price(sku, self.cfg.work)),
                    Op::Order(sku) => {
                        self.orders += 1;
                        Answer::Placed(self.orders << 20 | (sku & 0xF_FFFF))
                    }
                };
                if self.cfg.delay.is_zero() {
                    let _ = reply.send(answer); // a caller that gave up is not an error here
                } else {
                    self.held.push_back((cx.now() + self.cfg.delay, reply, answer));
                    self.arm(cx)?;
                }
            }
            Msg::Due => {
                self.armed = false;
                let now = cx.now();
                while self.held.front().is_some_and(|&(due, ..)| due <= now) {
                    let (_, reply, answer) = self.held.pop_front().expect("a held answer");
                    let _ = reply.send(answer);
                }
                self.arm(cx)?;
            }
        }
        Ok(())
    }
}
