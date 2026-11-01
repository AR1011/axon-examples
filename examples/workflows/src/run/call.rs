//! `call_idempotent`: a call to a run that must arrive. axon's `request_until` tries again after `Later` and
//! `Resolve` only; a `Timeout`, `OutcomeUnknown` or `NoReply` (`Retry::IfIdempotent`) may have run, so it gives up.
//! A run deduplicates a start by its input, a signal by its id, a completion by its step and a wake by
//! `TimerFired`, and a read changes nothing, so those are tried again too. A cancel, a terminate and an operator's
//! pause, resume, retry or delete are not: sent twice, the second is refused. A run that answers `busy` while it
//! opens or stops is asked again.

use std::time::Duration;

use axon_cluster::{ClusterError, KeyedRef, Retry};
use tokio::time::Instant;

use super::wire::{Code, RunAnswer, RunApi, RunCall};

/// The first wait between tries; it doubles up to `MAX_WAIT`.
const FIRST_WAIT: Duration = Duration::from_millis(20);
const MAX_WAIT: Duration = Duration::from_secs(1);

fn deduplicated(call: &RunCall) -> bool {
    !matches!(
        call,
        RunCall::Cancel { .. }
            | RunCall::Terminate { .. }
            | RunCall::Pause
            | RunCall::Resume
            | RunCall::Retry
            | RunCall::Delete
    )
}

/// Sends `call` to the run until it answers other than `busy`, or the deadline passes: then its last answer.
pub async fn call_idempotent(
    run: &KeyedRef<RunApi>,
    call: RunCall,
    deadline: Instant,
) -> Result<RunAnswer, ClusterError> {
    let mut wait = FIRST_WAIT;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let answer = run.request_within(call.clone(), left).await;
        let again = match &answer {
            Ok(RunAnswer::Refused { code: Code::Busy, .. }) => true,
            Ok(_) => false,
            Err(e) => match e.retry() {
                Retry::Later | Retry::Resolve => true,
                // `RUN_MUTATION=request_until` gives up where `request_until` would: `scripts/mutations.sh`.
                Retry::IfIdempotent => deduplicated(&call) && crate::mutation() != "request_until",
                Retry::Never => false,
            },
        };
        if !again || Instant::now() + wait >= deadline {
            return answer;
        }
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(MAX_WAIT);
    }
}
