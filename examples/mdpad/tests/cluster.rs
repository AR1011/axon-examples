//! The roles across nodes (design §4.3, §5) on real time, in one process: the boot check of a home, the catalogs per
//! home, the accounts on a node of their own and the gate.

mod support;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axon::RuntimeConfig;
use axon::files::{FileOp, Files, MemoryFiles};
use axon_cluster::MemoryDiscovery;
use axon_testkit::eventually;
use mdpad::client::{Client, Stop};
use mdpad::core::accounts::Event;
use mdpad::node::{Node, NodeConfig, Roles, transport};
use mdpad::remote::{AccountsAnswer, AccountsReq};
use mdpad::wire::close;
use support::KEY;

/// A node on `discovery` with `roles`, the homes `h1` and `h2`.
async fn boot(discovery: &MemoryDiscovery, id: &str, roles: Roles, docs: Option<MemoryFiles>) -> Result<Node, String> {
    boot_on(discovery, id, roles, docs, MemoryFiles::default()).await
}

/// The same, on these accounts files when `roles` has the accounts.
async fn boot_on(
    discovery: &MemoryDiscovery,
    id: &str,
    roles: Roles,
    docs: Option<MemoryFiles>,
    accounts: MemoryFiles,
) -> Result<Node, String> {
    let cfg = NodeConfig {
        id: id.into(),
        homes: vec!["h1".into(), "h2".into()],
        docs: docs.map(|d| Arc::new(d) as Arc<dyn Files>),
        accounts: roles.accounts.then(|| Arc::new(accounts) as Arc<dyn Files>),
        http: roles.web.then(|| ([127, 0, 0, 1], 0).into()),
        roles,
        ..NodeConfig::with_key(KEY.to_vec())
    };
    let ccfg = cfg.cluster(([127, 0, 0, 1], 0).into(), transport(id)?, discovery.clone());
    let runtime = cfg.runtime(RuntimeConfig::default());
    Node::boot(cfg, ccfg, runtime).await
}

fn roles(home: Option<&str>, web: bool, accounts: bool) -> Roles {
    Roles { home: home.map(str::to_string), web, accounts }
}

async fn members(nodes: &[&Node]) {
    let n = nodes.len();
    eventually("every node sees the others", async || nodes.iter().all(|x| x.cluster.view().members.len() == n)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_node_for_a_home_is_refused() {
    let discovery = MemoryDiscovery::new();
    let first = boot(&discovery, "h1", Roles::all("h1"), Some(MemoryFiles::default())).await.unwrap();
    let second = boot(&discovery, "h1", roles(Some("h1"), false, false), Some(MemoryFiles::default())).await;
    let refused = second.err().expect("a second h1 is refused");
    assert!(refused.contains("home h1 runs on h1"), "{refused}");
    first.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_directory_of_another_home_is_refused() {
    let docs = MemoryFiles::default();
    docs.apply(vec![FileOp::Replace("home.id".into(), "h2\n".into())]).await.unwrap();
    let refused = boot(&MemoryDiscovery::new(), "h1", Roles::all("h1"), Some(docs.clone())).await.err();
    assert_eq!(refused.as_deref(), Some("docs/home.id names h2, not h1"));
    // Its own home's directory is taken, and a new one is marked.
    let node = boot(&MemoryDiscovery::new(), "h2", Roles::all("h2"), Some(docs)).await.unwrap();
    node.shutdown().await;
    let fresh = MemoryFiles::default();
    let node = boot(&MemoryDiscovery::new(), "h1", Roles::all("h1"), Some(fresh.clone())).await.unwrap();
    assert_eq!(fresh.read("home.id").await.unwrap(), b"h1\n");
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_list_spans_the_homes_and_says_when_one_does_not_answer() {
    let discovery = MemoryDiscovery::new();
    let h1 = boot(&discovery, "h1", Roles::all("h1"), Some(MemoryFiles::default())).await.unwrap();
    let h2 = boot(&discovery, "h2", roles(Some("h2"), false, false), Some(MemoryFiles::default())).await.unwrap();
    members(&[&h1, &h2]).await;
    let ann = support::user(&h1, "ann").await;
    let mut ids = Vec::new();
    while ids.len() < 16 || !["h1", "h2"].iter().all(|h| ids.iter().any(|id: &String| mdpad::doc::home_of(id) == *h)) {
        ids.push(ann.create("# a doc").await.unwrap());
    }
    let page = ann.call(reqwest::Method::GET, "/api/docs?limit=200", None).await.unwrap();
    assert_eq!((page["docs"].as_array().unwrap().len(), &page["partial"]), (ids.len(), &false.into()));

    // Pages run across both homes, newest edit first, each row once.
    let mut paged = Vec::new();
    let mut after = String::new();
    loop {
        let page = ann.call(reqwest::Method::GET, &format!("/api/docs?limit=5{after}"), None).await.unwrap();
        paged.extend(page["docs"].as_array().unwrap().iter().map(|d| d["id"].as_str().unwrap().to_string()));
        match page["next"].as_str() {
            Some(next) => after = format!("&after={next}"),
            None => break,
        }
    }
    paged.sort();
    ids.sort();
    assert_eq!(paged, ids);

    h2.shutdown().await;
    let on_h1 = ids.iter().filter(|id| mdpad::doc::home_of(id) == "h1").count();
    let page = ann.call(reqwest::Method::GET, "/api/docs?limit=200", None).await.unwrap();
    assert_eq!((page["docs"].as_array().unwrap().len(), &page["partial"]), (on_h1, &true.into()));
    h1.shutdown().await;
}

/// A new password closes the user's sessions on every web node with 4401; a client cannot join again with the token
/// it had.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_password_closes_the_sessions_signed_before() {
    let discovery = MemoryDiscovery::new();
    let h1 = boot(&discovery, "h1", Roles::all("h1"), Some(MemoryFiles::default())).await.unwrap();
    let w1 = boot(&discovery, "w1", roles(None, true, false), None).await.unwrap();
    members(&[&h1, &w1]).await;
    let ann = support::user(&h1, "ann").await;
    let id = ann.create("# hi").await.unwrap();
    let mut clients = Vec::new();
    for node in [&h1, &w1] {
        let mut client = Client::new(node.http(), &id, ann.credential());
        support::settle(&mut client, Duration::from_secs(10)).await;
        clients.push(client);
    }

    let hash = mdpad::auth::hash_password("pw-new").unwrap();
    let change = AccountsReq::Record { event: Event::Password { name: "ann".into(), hash } };
    let answer = w1.web().accounts.request_within(change, Duration::from_secs(5)).await.unwrap();
    assert_eq!(answer, AccountsAnswer::Recorded { result: Ok(()) });
    for client in &mut clients {
        assert_eq!(support::closed(client, Duration::from_secs(5)).await, close::UNAUTHORIZED);
    }
    let mut again = Client::new(w1.http(), &id, ann.credential());
    let refused = match again.connect().await {
        Err(_) => close::UNAUTHORIZED,
        Ok(()) => support::closed(&mut again, Duration::from_secs(5)).await,
    };
    assert_eq!(refused, close::UNAUTHORIZED);
    w1.shutdown().await;
    h1.shutdown().await;
}

/// With the accounts node down, a sign-in already made goes on (counted), and a new one is refused with 503.
#[tokio::test(flavor = "multi_thread")]
async fn without_the_accounts_signed_in_users_go_on_and_new_sign_ins_wait() {
    let discovery = MemoryDiscovery::new();
    let h1 = boot(&discovery, "h1", roles(Some("h1"), true, false), Some(MemoryFiles::default())).await.unwrap();
    let a1 = boot(&discovery, "a1", roles(None, false, true), None).await.unwrap();
    members(&[&h1, &a1]).await;
    let add = AccountsReq::Record {
        event: Event::Add { name: "ann".into(), hash: mdpad::auth::hash_password("pw-ann").unwrap() },
    };
    let answer = h1.web().accounts.request_within(add, Duration::from_secs(5)).await.unwrap();
    assert_eq!(answer, AccountsAnswer::Recorded { result: Ok(()) });
    let ann = support::signed_in(&h1, "ann").await;
    let id = ann.create("# hi").await.unwrap();

    a1.shutdown().await;
    let before = mdpad::web::FAIL_OPEN.load(Ordering::Relaxed);
    let me = ann.call(reqwest::Method::GET, "/api/me", None).await.unwrap();
    assert_eq!(me["user"], "ann");
    assert!(mdpad::web::FAIL_OPEN.load(Ordering::Relaxed) > before);
    let mut client = Client::new(h1.http(), &id, ann.credential());
    support::settle(&mut client, Duration::from_secs(10)).await;

    let login = mdpad::client::Api::login(h1.http(), "ann", "pw-ann").await.err().expect("no sign-in");
    assert!(login.starts_with("503"), "{login}");
    match client.run_until(Duration::from_millis(500), |_| false).await {
        Err(Stop::Timeout) => {}
        other => panic!("the session ended: {other:?}"),
    }
    h1.shutdown().await;
}

/// The first sign-in through a web node right after the accounts node restarts goes through: the web node's reference
/// names the boot before (`StaleNode`), and the call is asked again once it looks the name up again.
#[tokio::test(flavor = "multi_thread")]
async fn a_sign_in_right_after_the_accounts_restart_goes_through() {
    let (discovery, accounts) = (MemoryDiscovery::new(), MemoryFiles::default());
    let h1 = boot(&discovery, "h1", roles(Some("h1"), true, false), Some(MemoryFiles::default())).await.unwrap();
    let a1 = boot_on(&discovery, "a1", roles(None, false, true), None, accounts.clone()).await.unwrap();
    members(&[&h1, &a1]).await;
    let add = AccountsReq::Record {
        event: Event::Add { name: "ann".into(), hash: mdpad::auth::hash_password("pw-ann").unwrap() },
    };
    let answer = h1.web().accounts.request_within(add, Duration::from_secs(5)).await.unwrap();
    assert_eq!(answer, AccountsAnswer::Recorded { result: Ok(()) });
    support::signed_in(&h1, "ann").await;

    a1.shutdown().await;
    let a1 = boot_on(&discovery, "a1", roles(None, false, true), None, accounts).await.unwrap();
    let boot = a1.cluster.record().incarnation;
    eventually("h1 sees a1's new boot", async || {
        h1.cluster.view().get("a1").is_some_and(|a| a.record.incarnation == boot)
    })
    .await;
    support::signed_in(&h1, "ann").await;
    a1.shutdown().await;
    h1.shutdown().await;
}
