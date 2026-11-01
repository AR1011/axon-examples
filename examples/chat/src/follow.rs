//! How one session follows one room, with no I/O: which subscription to hold, what the client is told, and when a
//! client too slow for its outbox is paused. A paused room holds no subscription and no records, only its cursor;
//! it resumes from the node's history ring once the client has caught up, so a slow client costs the room nothing.

use axon_stream::{Cursor, EndReason, RESET_REASONS, ResetReason, SequencedBatch, StartPosition, StreamEvent};

#[derive(Debug, PartialEq, Eq)]
pub enum Act {
    Nothing,
    /// Tell the client where the subscription starts.
    Ready(Option<Cursor>),
    /// Hand the records to the client.
    Deliver(SequencedBatch),
    /// Continuity broke; tell the client why. What comes next is subscribed when the job ends.
    Reset(&'static str),
    /// The client's outbox is full: drop the subscription and keep the cursor.
    Pause,
    /// The job ended: subscribe from here, then call [`Follow::subscribed`].
    Subscribe(StartPosition),
    /// No parent reaches the room's origin: ask for the room, which starts it on its next node. The subscription
    /// stays and shows the new generation as a new `Ready`.
    Lost,
}

#[derive(Debug, Default)]
pub struct Follow {
    /// Tags the current subscription's events; events of a replaced one are ignored.
    pub epoch: u32,
    /// The last record handed to the client.
    pub cursor: Option<Cursor>,
    next: Option<StartPosition>,
    pub paused: bool,
}

impl Follow {
    pub fn new(cursor: Option<Cursor>) -> Self {
        Self { cursor, ..Default::default() }
    }

    /// Where a new subscription starts: after the cursor, or live.
    pub fn start(&self) -> StartPosition {
        self.cursor.map_or(StartPosition::Live, StartPosition::After)
    }

    /// A new subscription replaced the old one; returns its epoch.
    pub fn subscribed(&mut self) -> u32 {
        self.epoch = self.epoch.wrapping_add(1);
        (self.next, self.paused) = (None, false);
        self.epoch
    }

    /// `room` says whether the client's outbox takes another batch.
    pub fn on_event(&mut self, epoch: u32, event: Option<StreamEvent>, room: bool) -> Act {
        if epoch != self.epoch || self.paused {
            return Act::Nothing;
        }
        let Some(event) = event else {
            return Act::Subscribe(self.next.take().unwrap_or_else(|| self.start()));
        };
        match event {
            // A relay that has no position for the room yet: records published before it has one are not
            // delivered, so the client is told where it joined only once it is known.
            StreamEvent::Ready { start: None } => Act::Nothing,
            StreamEvent::Ready { start } => {
                // A live follow shows a room restart as a new generation: the client sees it in the frame.
                self.cursor = start;
                Act::Ready(start)
            }
            StreamEvent::Batch(_) if !room => {
                self.paused = true;
                Act::Pause
            }
            StreamEvent::Batch(batch) => {
                self.cursor = Some(batch.cursor());
                Act::Deliver(batch)
            }
            StreamEvent::ResetRequired(ResetReason::SlowConsumer) => {
                // The branch overflowed while the actor was busy: the ring may still hold what follows.
                self.next = Some(self.start());
                Act::Nothing
            }
            StreamEvent::ResetRequired(ResetReason::GenerationChanged) => {
                // The room restarted: everything in the new generation is new to the client.
                (self.cursor, self.next) = (None, Some(StartPosition::First));
                Act::Reset("generation_changed")
            }
            StreamEvent::End(EndReason::StreamClosed) => {
                (self.cursor, self.next) = (None, Some(StartPosition::First));
                Act::Reset("stream_closed")
            }
            StreamEvent::ResetRequired(reason) => {
                (self.cursor, self.next) = (None, Some(StartPosition::Live));
                Act::Reset(RESET_REASONS[reason as usize])
            }
            StreamEvent::End(_) => {
                self.next = Some(self.start());
                Act::Nothing
            }
            StreamEvent::OriginLost => Act::Lost,
            StreamEvent::CaughtUp { .. } | StreamEvent::Snapshot(_) | StreamEvent::Replacement { .. } => Act::Nothing,
        }
    }

    /// Once the client has room again: where a paused room resumes.
    pub fn resume(&self) -> Option<StartPosition> {
        self.paused.then(|| self.start())
    }
}
