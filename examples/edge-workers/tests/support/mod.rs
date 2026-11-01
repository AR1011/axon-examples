//! What the object tests on the fake engine share.

use std::time::Duration;

use edge_workers::engine::fake::FakeRun;
use edge_workers::engine::{Failure, Request, Scope, StorageAnswer, StorageOp};

/// A counter as a script would write it. The worker's handler passes the request to the object its path names;
/// what the stub throws is the handler's exception. The object answers `n` from its storage, and a POST adds one
/// first. Its run computes for 10 ms, or for what a `cpu-ms` header says; with a `then` header it first sends the
/// request on to the object of that id, and fails if that one does.
pub async fn counter(request: Request, scope: Scope) -> FakeRun {
    let host = scope.host.expect("a host");
    if scope.object.is_none() {
        let id = request.url.trim_start_matches('/').to_owned();
        return match host.fetch("Counter".into(), id, request).await {
            Ok(response) => FakeRun::ok(response.body),
            Err(thrown) => FakeRun::fail(Failure::Exception(thrown)),
        };
    }
    let header = |name: &str| request.headers.iter().find(|(header, _)| header == name).map(|(_, value)| value.clone());
    let cpu = header("cpu-ms").map_or(10, |ms| ms.parse().expect("milliseconds"));
    if let Some(next) = header("then") {
        let mut on = request.clone();
        on.headers.retain(|(header, _)| header != "then");
        if let Err(thrown) = host.fetch("Counter".into(), next, on).await {
            return FakeRun::fail(Failure::Exception(thrown));
        }
    }
    let stored = host.storage(StorageOp::Get { key: "n".into() }).await.expect("a get");
    let StorageAnswer::Value(stored) = stored else { panic!("a value") };
    let mut n = stored.map_or(0, |n| n.parse::<u64>().expect("a count"));
    if request.method == "POST" {
        n += 1;
        host.storage(StorageOp::Put { key: "n".into(), value: n.to_string() }).await.expect("a put");
    }
    FakeRun::ok(n.to_string()).cpu(Duration::from_millis(cpu))
}
