//! A room: the one actor for its name, on the node rendezvous hashing picks. It is the origin of the stream
//! `room/<name>`: posts from any node land here, get the next sequence number, and fan out through the stream tree.
//! A room that restarts on another node opens the same key there, in a higher generation, and the sessions that
//! follow it live see the new generation as a new `Ready`.

use axon::{Actor, Context, StopReason};
use axon_stream::{DeliveryPolicy, StreamKey, StreamNode};
use bytes::Bytes;

use crate::proto::{Record, RoomMsg, pair};

pub const KIND: &str = "room";
pub const NAMESPACE: &str = "room";
pub const MAX_TEXT: usize = 4096;

pub fn key(name: &str) -> StreamKey {
    StreamKey::new(NAMESPACE, name)
}

pub fn now_ms() -> u64 {
    axon::trace::unix_micros() / 1000
}

pub struct Room {
    key: StreamKey,
    streams: StreamNode,
}

impl Room {
    pub fn new(name: &str, streams: StreamNode) -> Self {
        Self { key: key(name), streams }
    }
}

impl Actor for Room {
    type Message = RoomMsg;
    type Error = String;

    /// The start time is the generation: a room that restarts, here or on another node, shows a higher one. Above
    /// the generation this node relayed, if any, so a clock behind the dead host's still moves the room forward.
    /// Posts wait for the open.
    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let relayed = self.streams.cursor(&self.key).map_or(0, |c| c.generation + 1);
        let open = self.streams.open_stream(&self.key, now_ms().max(relayed), DeliveryPolicy::ReplayRequired);
        let open =
            cx.submit_future(async move { RoomMsg::Opened(open.await) }).map_err(|e| format!("{:?}", e.reason))?;
        cx.pause_until(&open).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: RoomMsg, _cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (post, reply) = match msg {
            RoomMsg::Opened(opened) => return opened.map_err(|e| format!("open {e:?}")),
            RoomMsg::Post(post, reply) => (post, reply),
        };
        let posted = if post.text.len() > MAX_TEXT {
            Err("too_long".to_string())
        } else {
            let record = Record { from: post.from, id: post.id, text: post.text, ts: now_ms() };
            let payload = Bytes::from(serde_json::to_vec(&record).map_err(|e| e.to_string())?);
            // A refusal is the hub's bounds; the poster hears it and decides.
            self.streams.publish(&self.key, vec![payload]).map(pair).map_err(|e| format!("{:?}", e.error))
        };
        // The poster may have gone; nothing to do about a reply that cannot be sent.
        drop(reply.map(|r| r.send(posted)));
        Ok(())
    }

    fn on_stop(&mut self, _reason: &StopReason) {
        // Subscribers from a cursor end with StreamClosed; live followers wait for the next open, here or on
        // another node.
        let _ = self.streams.close_stream(&self.key);
    }
}
