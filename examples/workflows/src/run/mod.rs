//! A run: its pure core ([`machine`]), the actor that journals and acts for it ([`actor`]), the calls it takes
//! ([`wire`]), how a caller makes one arrive ([`call`]) and what the API shows of it ([`view`]).

pub mod actor;
pub mod call;
pub mod machine;
pub mod view;
pub mod wire;

pub use actor::{Env, Run, RunMsg, Wall, answer_ended};
pub use call::call_idempotent;
pub use view::{RunView, StepView};
pub use wire::{Caller, Code, RUN, RunAnswer, RunApi, RunCall};
