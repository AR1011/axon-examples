//! The fault scenario, with real processes: a node killed with SIGKILL while it runs sagas, and an injected
//! shipping failure. Every saga, the killed node's included, must end, and the databases must agree.

use saga::core::Fault;
use saga::crash::{Plan, run};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_saga_ends_consistent_after_a_node_is_killed_mid_saga() {
    let data = std::env::temp_dir().join(format!("saga-crash-test-{}", std::process::id()));
    let plan = Plan {
        exe: env!("CARGO_BIN_EXE_saga").into(),
        data: data.clone(),
        sagas: 1500,
        kill_after: 500,
        fault: Fault { shipping_every: 10 },
        seed: 11,
    };
    let outcome = run(plan).await.unwrap();
    println!("{outcome:#?}");
    let report = &outcome.report;
    assert!(report.violations.is_empty(), "{:#?}", report.violations);
    assert_eq!((report.sagas, report.unfinished), (1500, 0));
    assert!(outcome.orphaned > 0 && outcome.mid_saga > 0, "the kill landed on no running saga: {outcome:?}");
    assert_eq!(outcome.resumed, outcome.orphaned, "{outcome:?}");
    assert!(report.reasons.get("injected failure").is_some_and(|n| *n > 0), "{report:?}");
    std::fs::remove_dir_all(data).unwrap();
}
