//! Chat rooms as axon streams. A room is a keyed actor that owns the stream `room/<name>` on whichever node
//! rendezvous hashing picks; a session is an actor per websocket on any node, following the rooms it joined
//! through that node's `StreamNode`. See the README.

pub mod client;
pub mod follow;
pub mod load;
pub mod node;
pub mod proto;
pub mod room;
pub mod session;

pub use follow::{Act, Follow};
pub use node::{Node, NodeConfig};
pub use proto::{ClientFrame, Record, ServerFrame};
