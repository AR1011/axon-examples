//! The should surfaces of design §4.1 and §4.2 over HTTP: the published page, the operators' routes, and the probes,
//! `/metrics` and axon's console of design §5.1.

mod support;

use std::time::Duration;

use mdpad::client::{Api, Client};
use reqwest::Method;
use serde_json::{Value, json};
use support::{CutFiles, closed, settle, signed_in, start, user};

const SETTLE: Duration = Duration::from_secs(10);

/// An operator: a user the accounts mark as one.
async fn operator(node: &mdpad::node::Node, name: &str) -> Api {
    use mdpad::{accounts::Request, core::accounts::Event, keeper::Msg};
    user(node, name).await;
    let event = Event::Operator { name: name.into(), operator: true };
    let recorded = node.accounts().ask(|reply| Msg::Request(Request::Record { event: event.clone(), reply })).await;
    recorded.expect("the accounts answer").expect("an operator");
    signed_in(node, name).await
}

/// `/api/p/<id>`: 404 until the owner publishes, then the text for anyone, cached privately for 10 s; 404 again in the
/// trash and once unpublished.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_published_page_is_for_anyone_until_it_is_unpublished_or_trashed() {
    let (docs, accounts) = (CutFiles::new(None), CutFiles::new(None));
    let node = start(&docs, &accounts, |_| {}).await;
    let (ann, ben) = (user(&node, "ann").await, user(&node, "ben").await);
    let id = ann.create("# Notes\n\nFor all.\n").await.unwrap();
    let share = format!("/api/docs/{id}/share");
    ann.call(Method::PUT, &format!("/api/docs/{id}/members/ben"), Some(json!({ "level": "edit" }))).await.unwrap();
    let nobody = Api::new(node.http());
    let page = format!("/api/p/{id}");
    let read = async || {
        let response = nobody.http.get(format!("{}{page}", nobody.base)).send().await.unwrap();
        let cache = response.headers().get("cache-control").map(|v| v.to_str().unwrap().to_string());
        (response.status().as_u16(), cache, response.text().await.unwrap())
    };
    assert_eq!(read().await.0, 404, "not published");
    assert_eq!(ann.raw(Method::GET, &page, None).await.unwrap().0, 404, "the owner sees what anyone sees");
    assert_eq!(ann.call(Method::GET, &share, None).await.unwrap()["published"], false);

    let publish = |on: bool| Some(json!({ "published": on }));
    assert_eq!(ben.raw(Method::PUT, &share, publish(true)).await.unwrap().0, 403, "only the owner publishes");
    ann.call(Method::PUT, &share, publish(true)).await.unwrap();
    assert_eq!(ann.call(Method::GET, &share, None).await.unwrap()["published"], true);
    assert_eq!(ann.call(Method::GET, &share, None).await.unwrap()["link"], "off", "the link is as it was");
    let (status, cache, text) = read().await;
    assert_eq!((status, cache.as_deref(), text.as_str()), (200, Some("private, max-age=10"), "# Notes\n\nFor all.\n"));
    let unknown = format!("/api/p/{}", mdpad::catalog::new_id("h1"));
    assert_eq!(nobody.raw(Method::GET, &unknown, None).await.unwrap().0, 404);
    assert_eq!(nobody.raw(Method::GET, "/api/p/nope", None).await.unwrap().0, 404);
    let html = nobody.http.get(format!("{}/p/{id}", nobody.base)).send().await.unwrap();
    assert_eq!(html.status().as_u16(), 200, "the page itself");

    ann.call(Method::DELETE, &format!("/api/docs/{id}"), None).await.unwrap();
    assert_eq!(read().await.0, 404, "in the trash");
    ann.call(Method::POST, &format!("/api/docs/{id}/untrash"), None).await.unwrap();
    assert_eq!(read().await.0, 200, "out of the trash, still published");
    ann.call(Method::PUT, &share, publish(false)).await.unwrap();
    assert_eq!(read().await.0, 404, "unpublished");
    node.shutdown().await;
}

/// The admin routes: only an operator's; the user list, an invite, a disable that ends the user's sockets and
/// sign-ins, a new owner for a document, an enable, and what each home runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_operator_lists_invites_disables_and_gives_documents_a_new_owner() {
    let (docs, accounts) = (CutFiles::new(None), CutFiles::new(None));
    let node = start(&docs, &accounts, |_| {}).await;
    let (ann, ben) = (user(&node, "ann").await, user(&node, "ben").await);
    let dev = operator(&node, "dev").await;
    let nobody = Api::new(node.http());
    for path in ["/api/admin/users", "/api/admin/homes"] {
        assert_eq!(ann.raw(Method::GET, path, None).await.unwrap().0, 403, "{path} for a user");
        assert_eq!(nobody.raw(Method::GET, path, None).await.unwrap().0, 401, "{path} for no one");
    }
    assert_eq!(nobody.call(Method::GET, "/api/me", None).await.unwrap(), json!({ "user": null, "operator": false }));
    assert_eq!(ann.call(Method::GET, "/api/me", None).await.unwrap()["operator"], false);
    assert_eq!(dev.call(Method::GET, "/api/me", None).await.unwrap()["operator"], true);
    let users = dev.call(Method::GET, "/api/admin/users", None).await.unwrap();
    assert_eq!(
        users["users"],
        json!([
            { "name": "ann", "disabled": false, "operator": false },
            { "name": "ben", "disabled": false, "operator": false },
            { "name": "dev", "disabled": false, "operator": true },
        ])
    );

    // An invite: a link that makes the account once.
    let made = dev.call(Method::POST, "/api/admin/users", Some(json!({ "name": "gus" }))).await.unwrap();
    let link = made["link"].as_str().unwrap();
    let token = link.split_once("/i/").expect("an invite link").1;
    let body = json!({ "password": "gus-password" });
    nobody.call(Method::POST, &format!("/api/invite/{token}"), Some(body)).await.unwrap();
    Api::login(node.http(), "gus", "gus-password").await.unwrap();
    let again = dev.raw(Method::POST, "/api/admin/users", Some(json!({ "name": "ann" }))).await.unwrap();
    assert_eq!(again.0, 409, "a name that exists: {}", again.1);
    let bad = dev.raw(Method::POST, "/api/admin/users", Some(json!({ "name": "Not A Name" }))).await.unwrap();
    assert_eq!(bad.0, 400);
    assert_eq!(ann.raw(Method::POST, "/api/admin/users", Some(json!({ "name": "hal" }))).await.unwrap().0, 403);

    // A disable closes the user's sockets and ends their sign-ins; the documents stay.
    let id = ann.create("# Ann's\n").await.unwrap();
    let mut socket = Client::new(node.http(), &id, ann.credential());
    settle(&mut socket, SETTLE).await;
    let off = Some(json!({ "disabled": true }));
    assert_eq!(ann.raw(Method::PUT, "/api/admin/users/ben", off.clone()).await.unwrap().0, 403);
    dev.call(Method::PUT, "/api/admin/users/ann", off.clone()).await.unwrap();
    assert_eq!(closed(&mut socket, Duration::from_secs(2)).await, mdpad::wire::close::UNAUTHORIZED);
    assert_eq!(ann.raw(Method::GET, "/api/me", None).await.unwrap().0, 401);
    assert!(Api::login(node.http(), "ann", "pw-ann").await.is_err(), "no sign-in while disabled");
    assert_eq!(dev.raw(Method::PUT, "/api/admin/users/zed", off.clone()).await.unwrap().0, 404);
    let own = dev.raw(Method::PUT, "/api/admin/users/dev", off).await.unwrap();
    assert_eq!(own.0, 409, "not the operator's own account: {}", own.1);
    assert_eq!(dev.raw(Method::GET, "/api/me", None).await.unwrap().0, 200);

    // A new owner: the one before stays as an edit member, and the list moves the document to the new owner's.
    let owner = format!("/api/admin/docs/{id}/owner");
    assert_eq!(ben.raw(Method::PUT, &owner, Some(json!({ "user": "ben" }))).await.unwrap().0, 403);
    assert_eq!(dev.raw(Method::PUT, &owner, Some(json!({ "user": "zed" }))).await.unwrap().0, 404, "no such user");
    let gone = format!("/api/admin/docs/{}/owner", mdpad::catalog::new_id("h1"));
    assert_eq!(dev.raw(Method::PUT, &gone, Some(json!({ "user": "ben" }))).await.unwrap().0, 404);
    dev.call(Method::PUT, &owner, Some(json!({ "user": "ben" }))).await.unwrap();
    let share = ben.call(Method::GET, &format!("/api/docs/{id}/share"), None).await.unwrap();
    assert_eq!(share["owner"], "ben");
    assert_eq!(share["members"], json!([{ "user": "ann", "level": "edit", "via": "invited" }]));
    axon_testkit::eventually_within("the list moves", Duration::from_secs(5), async || {
        let mine = ben.call(Method::GET, "/api/docs", None).await.unwrap();
        mine["docs"].as_array().unwrap().iter().any(|d| d["id"] == id.as_str())
    })
    .await;

    dev.call(Method::PUT, "/api/admin/users/ann", Some(json!({ "disabled": false }))).await.unwrap();
    let ann = Api::login(node.http(), "ann", "pw-ann").await.unwrap();
    assert_eq!(ann.call(Method::GET, &format!("/api/docs/{id}"), None).await.unwrap()["level"], "edit");

    // What each home runs, and each web node's sockets.
    let mut socket = Client::new(node.http(), &id, ann.credential());
    settle(&mut socket, SETTLE).await;
    let homes = dev.call(Method::GET, "/api/admin/homes", None).await.unwrap();
    let home = &homes["homes"][0];
    assert_eq!(home["home"], "h1", "{homes}");
    assert!(home["open"].as_u64().unwrap() >= 1, "{homes}");
    let open = home["docs"].as_array().unwrap();
    let doc = open.iter().find(|d| d["id"] == id.as_str()).unwrap_or_else(|| panic!("{id} in {homes}"));
    assert_eq!((&doc["title"], &doc["owner"]), (&json!("Ann's"), &json!("ben")));
    assert!(home["stored"].as_u64().unwrap() >= 1 && home["log_bytes"].as_u64().unwrap() > 0, "{homes}");
    assert!(home["streams"].as_u64().unwrap() >= 1, "{homes}");
    let web = &homes["webs"][0];
    assert_eq!(web["node"], "h1", "{homes}");
    assert!(web["sessions"].as_u64().unwrap() >= 1, "{homes}");
    let nodes: Vec<&Value> = homes["nodes"].as_array().unwrap().iter().collect();
    assert_eq!(nodes.len(), 1, "{homes}");
    assert_eq!(nodes[0]["roles"], json!(["home", "web", "accounts"]), "{homes}");
    node.shutdown().await;
}

/// `health=` serves the probes and `/metrics` with the documents' handler labels; `console=` serves axon's console.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_probes_the_metrics_and_the_console_answer() {
    let (docs, accounts) = (CutFiles::new(None), CutFiles::new(None));
    let loopback = || Some(([127, 0, 0, 1], 0).into());
    let node = start(&docs, &accounts, |cfg| (cfg.health, cfg.console) = (loopback(), loopback())).await;
    let ann = user(&node, "ann").await;
    let id = ann.create("# Seen\n").await.unwrap();
    let mut socket = Client::new(node.http(), &id, ann.credential());
    settle(&mut socket, SETTLE).await;
    let http = reqwest::Client::new();
    let get = async |addr: std::net::SocketAddr, path: &str| {
        let response = http.get(format!("http://{addr}{path}")).send().await.unwrap();
        (response.status().as_u16(), response.text().await.unwrap())
    };
    let health = node.health.expect("health=");
    assert_eq!(get(health, "/healthz").await.0, 200);
    axon_testkit::eventually_within("ready", Duration::from_secs(10), async || get(health, "/readyz").await.0 == 200)
        .await;
    axon_testkit::eventually_within("the documents in /metrics", Duration::from_secs(15), async || {
        let (status, text) = get(health, "/metrics").await;
        status == 200 && text.contains(r#"actor_type="mdpad::doc::Doc""#)
    })
    .await;
    let console = node.console.expect("console=");
    assert_eq!(get(console, "/").await.0, 200);
    node.shutdown().await;
}
