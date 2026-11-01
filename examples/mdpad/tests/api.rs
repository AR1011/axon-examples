//! The permission table of design §4.2 over HTTP, one call per cell (I6): the owner, an edit member, a view member,
//! a guest of the view link, a signed-in user with no role and no one. Edits over the socket are `tests/ws.rs`.

mod support;

use std::time::Duration;

use mdpad::client::Api;
use reqwest::Method;
use serde_json::{Value, json};
use support::{CutFiles, start, user};

/// An action, its call, and the status each caller gets.
type Row = (&'static str, Method, String, Option<Value>, [u16; 6]);

/// One caller: a bearer token, a guest token, or nothing.
enum Who<'a> {
    User(&'a Api),
    Guest(&'a Api, &'a str),
    Nobody(&'a Api),
}

/// A JSON body, or a string sent as the body's text.
async fn status(who: &Who<'_>, method: Method, path: &str, body: Option<Value>) -> u16 {
    let (api, authorization) = match who {
        Who::User(api) => (api, api.token.as_ref().map(|t| format!("Bearer {t}"))),
        Who::Guest(api, token) => (api, Some(format!("Guest {token}"))),
        Who::Nobody(api) => (api, None),
    };
    let mut request = api.http.request(method, format!("{}{path}", api.base));
    if let Some(authorization) = authorization {
        request = request.header("authorization", authorization);
    }
    request = match body {
        Some(Value::String(text)) => request.body(text),
        Some(body) => request.json(&body),
        None => request,
    };
    request.send().await.unwrap().status().as_u16()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_cell_of_the_permission_table() {
    let (docs, accounts) = (CutFiles::new(None), CutFiles::new(None));
    let node = start(&docs, &accounts, |_| {}).await;
    let (ann, ben, cid, dee) =
        (user(&node, "ann").await, user(&node, "ben").await, user(&node, "cid").await, user(&node, "dee").await);
    user(&node, "fay").await;
    let id = ann.create("# Table\n").await.unwrap();
    let path = |rest: &str| format!("/api/docs/{id}{rest}");
    ann.call(Method::PUT, &path("/members/ben"), Some(json!({ "level": "edit" }))).await.unwrap();
    ann.call(Method::PUT, &path("/members/cid"), Some(json!({ "level": "view" }))).await.unwrap();
    ann.call(Method::PUT, &path("/share"), Some(json!({ "link": "view" }))).await.unwrap();
    let link = ann.call(Method::GET, &path("/share"), None).await.unwrap()["token"].as_str().unwrap().to_string();
    let nobody = Api::new(node.http());
    let answer = nobody.call(Method::POST, "/api/guest", Some(json!({ "link": link, "name": "Eve" }))).await.unwrap();
    let eve = answer["token"].as_str().unwrap().to_string();

    let callers = [
        ("owner", Who::User(&ann)),
        ("edit member", Who::User(&ben)),
        ("view member", Who::User(&cid)),
        ("view guest", Who::Guest(&nobody, &eve)),
        ("no role", Who::User(&dee)),
        ("no one", Who::Nobody(&nobody)),
    ];
    // Each call changes nothing when the owner makes it: it sets what is already set. The changes come after.
    let rows: [Row; 18] = [
        ("open", Method::GET, path(""), None, [200, 200, 200, 200, 403, 401]),
        ("export", Method::GET, path("/markdown"), None, [200, 200, 200, 200, 403, 401]),
        ("import", Method::PUT, path("/markdown"), Some(json!("# Table\n")), [200, 200, 403, 401, 403, 401]),
        ("history", Method::GET, path("/versions"), None, [200, 200, 200, 403, 403, 401]),
        (
            "name a version",
            Method::POST,
            path("/versions"),
            Some(json!({ "name": "v" })),
            [200, 200, 403, 401, 403, 401],
        ),
        ("see a version", Method::GET, path("/versions/1"), None, [200, 200, 200, 403, 403, 401]),
        ("restore", Method::POST, path("/versions/1/restore"), None, [200, 200, 403, 401, 403, 401]),
        ("copy", Method::POST, path("/copy"), None, [200, 200, 200, 401, 403, 401]),
        ("see people", Method::GET, path("/share"), None, [200, 200, 200, 401, 403, 401]),
        ("link", Method::PUT, path("/share"), Some(json!({ "link": "view" })), [200, 403, 403, 401, 403, 401]),
        ("invite", Method::PUT, path("/members/cid"), Some(json!({ "level": "view" })), [200, 403, 403, 401, 403, 401]),
        ("remove", Method::DELETE, path("/members/fay"), None, [200, 403, 403, 401, 403, 401]),
        ("hide", Method::POST, path("/hide"), None, [403, 200, 200, 401, 403, 401]),
        ("purge", Method::DELETE, path("?purge=1"), None, [403, 403, 403, 401, 403, 401]),
        ("untrash", Method::POST, path("/untrash"), None, [200, 403, 403, 401, 403, 401]),
        ("join", Method::POST, format!("/api/s/{link}/join"), None, [200, 200, 200, 401, 200, 401]),
        ("publish", Method::PUT, path("/share"), Some(json!({ "published": true })), [200, 403, 403, 401, 403, 401]),
        ("read published", Method::GET, format!("/api/p/{id}"), None, [200, 200, 200, 200, 200, 200]),
    ];
    let mut wrong = Vec::new();
    for (action, method, at, body, want) in &rows {
        for ((caller, who), want) in callers.iter().zip(want) {
            let got = status(who, method.clone(), at, body.clone()).await;
            if got != *want {
                wrong.push(format!("{action} by {caller}: {got}, want {want}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    let share = ann.call(Method::GET, &path("/share"), None).await.unwrap();
    let members: Vec<_> = share["members"].as_array().unwrap().iter().map(|m| m["user"].as_str().unwrap()).collect();
    assert_eq!(members, ["ben", "cid", "dee"], "dee joined by the link, nobody else changed");
    assert!(ben.call(Method::GET, &path("/share"), None).await.unwrap()["token"].is_null(), "only the owner sees it");

    // The link and the trash, which do change the table.
    for who in [&ben, &cid, &dee] {
        assert_eq!(status(&Who::User(who), Method::POST, &path("/share/reset"), None).await, 403);
        assert_eq!(status(&Who::User(who), Method::DELETE, &path(""), None).await, 403);
    }
    ann.call(Method::POST, &path("/share/remove-link-members"), None).await.unwrap();
    assert_eq!(status(&Who::User(&dee), Method::GET, &path(""), None).await, 403, "removed with the link members");
    ann.call(Method::POST, &path("/share/reset"), None).await.unwrap();
    assert_eq!(status(&Who::Guest(&nobody, &eve), Method::GET, &path(""), None).await, 403, "the old link's guest");
    assert_eq!(status(&Who::Nobody(&nobody), Method::GET, &format!("/api/s/{link}"), None).await, 404);
    assert_eq!(status(&Who::User(&ben), Method::GET, &path(""), None).await, 200, "members stay (Q9)");

    ann.call(Method::DELETE, &path(""), None).await.unwrap();
    for who in [&ann, &ben] {
        assert_eq!(status(&Who::User(who), Method::GET, &path("/markdown"), None).await, 403, "trashed");
    }
    let (code, refused) = ben.raw(Method::GET, &path(""), None).await.unwrap();
    let trashed = (code, &refused["error"], &refused["trashed"]);
    assert_eq!(trashed, (403, &json!("forbidden"), &json!(true)), "the page shows the trash: {refused}");
    let info = ann.call(Method::GET, &path(""), None).await.unwrap();
    assert!(info["trashed_ms"].is_u64(), "the owner sees it is in the trash: {info}");
    let trash = ann.call(Method::GET, "/api/docs?tab=trash", None).await.unwrap();
    assert_eq!(trash["docs"][0]["id"], id.as_str());
    assert_eq!(status(&Who::User(&ben), Method::DELETE, &path("?purge=1"), None).await, 403);
    ann.call(Method::DELETE, &path("?purge=1"), None).await.unwrap();
    axon_testkit::eventually_within("purged", Duration::from_secs(5), async || {
        status(&Who::User(&ann), Method::GET, &path(""), None).await == 404
    })
    .await;
    node.shutdown().await;
}

/// Design §4.2's account routes: an operator's invite makes the account once, a reset sets a new password once, and a
/// new password ends the other sign-ins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invites_and_new_passwords() {
    use mdpad::auth::{Auth, Invite};
    let (docs, accounts) = (CutFiles::new(None), CutFiles::new(None));
    let node = start(&docs, &accounts, |_| {}).await;
    let auth = Auth::new(&support::KEY).unwrap();
    let later = axon::trace::unix_micros() / 1000 + 60_000;
    let nobody = Api::new(node.http());
    let invite = auth.invite(&Invite { name: "gus".into(), version: 0, expires_ms: later });
    let read = nobody.call(Method::GET, &format!("/api/invite/{invite}"), None).await.unwrap();
    assert_eq!(read, json!({ "user": "gus", "new": true }));
    let short = Some(json!({ "password": "short" }));
    assert_eq!(nobody.raw(Method::POST, &format!("/api/invite/{invite}"), short).await.unwrap().0, 400);
    let body = json!({ "password": "gus-password", "token": true });
    let made = nobody.call(Method::POST, &format!("/api/invite/{invite}"), Some(body.clone())).await.unwrap();
    assert_eq!(made["user"], "gus");
    assert_eq!(nobody.raw(Method::POST, &format!("/api/invite/{invite}"), Some(body)).await.unwrap().0, 404, "once");
    let gus = Api::login(node.http(), "gus", "gus-password").await.unwrap();
    let expired = auth.invite(&Invite { name: "hal".into(), version: 0, expires_ms: 1 });
    assert_eq!(nobody.raw(Method::GET, &format!("/api/invite/{expired}"), None).await.unwrap().0, 404);

    let wrong = json!({ "old": "nope", "new": "gus-second", "token": true });
    assert_eq!(gus.raw(Method::POST, "/api/me/password", Some(wrong)).await.unwrap().0, 403);
    let right = json!({ "old": "gus-password", "new": "gus-second", "token": true });
    let answer = gus.call(Method::POST, "/api/me/password", Some(right)).await.unwrap();
    assert_eq!(gus.raw(Method::GET, "/api/me", None).await.unwrap().0, 401, "the sign-in before the new password");
    let again = Api { token: answer["token"].as_str().map(str::to_string), ..Api::new(node.http()) };
    assert_eq!(again.call(Method::GET, "/api/me", None).await.unwrap()["user"], "gus");

    // A reset is for the version the account is at: it holds once.
    let reset = auth.invite(&Invite { name: "gus".into(), version: 2, expires_ms: later });
    let body = json!({ "password": "gus-third-one" });
    nobody.call(Method::POST, &format!("/api/invite/{reset}"), Some(body.clone())).await.unwrap();
    assert_eq!(nobody.raw(Method::POST, &format!("/api/invite/{reset}"), Some(body)).await.unwrap().0, 404);
    Api::login(node.http(), "gus", "gus-third-one").await.unwrap();
    node.shutdown().await;
}

/// Import, history, restore and copy over HTTP: each change is a forward edit after a version, so the list only grows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn import_history_restore_and_copy() {
    let (docs, accounts) = (CutFiles::new(None), CutFiles::new(None));
    let node = start(&docs, &accounts, |_| {}).await;
    let (ann, ben) = (user(&node, "ann").await, user(&node, "ben").await);
    let id = ann.create("# Plan\n\nIntro.\n").await.unwrap();
    let path = |rest: &str| format!("/api/docs/{id}{rest}");
    ann.call(Method::PUT, &path("/members/ben"), Some(json!({ "level": "view" }))).await.unwrap();
    assert_eq!(ann.call(Method::GET, &path("/versions"), None).await.unwrap()["versions"], json!([]));
    let named = ann.call(Method::POST, &path("/versions"), Some(json!({ "name": "first" }))).await.unwrap();
    assert_eq!((named["version"]["n"].as_u64(), named["version"]["kind"].as_str()), (Some(1), Some("named")));

    let mut put = ann.http.put(format!("{}{}", ann.base, path("/markdown")));
    put = put.bearer_auth(ann.token.as_ref().unwrap()).body("# New\n\nOther 😀\n");
    let imported: Value = put.send().await.unwrap().json().await.unwrap();
    assert_eq!(imported["before"]["kind"], "before_import", "{imported}");
    assert_eq!(ann.markdown(&id).await.unwrap(), "# New\n\nOther 😀\n");

    let one = ann.call(Method::GET, &path("/versions/1"), None).await.unwrap();
    assert_eq!(one["markdown"], "# Plan\n\nIntro.\n");
    assert_eq!(ann.raw(Method::GET, &path("/versions/9"), None).await.unwrap().0, 404);
    let restored = ann.call(Method::POST, &path("/versions/1/restore"), None).await.unwrap();
    assert_eq!(restored["before"]["kind"], "before_restore");
    assert_eq!(ann.markdown(&id).await.unwrap(), "# Plan\n\nIntro.\n");
    let list = ann.call(Method::GET, &path("/versions"), None).await.unwrap();
    let kinds: Vec<&str> = list["versions"].as_array().unwrap().iter().map(|v| v["kind"].as_str().unwrap()).collect();
    assert_eq!(kinds, ["before_restore", "before_import", "named"], "newest first, and only grown (I7)");
    let authors = &list["versions"][0]["authors"];
    assert_eq!(authors, &json!(["ann (import)"]), "who changed the text since the version before");

    // A fresh copy holds the text and the same people; the reader of the original owns the copy.
    let copy = ben.call(Method::POST, &path("/copy"), None).await.unwrap()["id"].as_str().unwrap().to_string();
    assert_ne!(copy, id);
    assert_eq!(ben.markdown(&copy).await.unwrap(), "# Plan\n\nIntro.\n");
    let share = ben.call(Method::GET, &format!("/api/docs/{copy}/share"), None).await.unwrap();
    assert_eq!(share["owner"], "ben");
    assert_eq!(share["members"], json!([{ "user": "ann", "level": "edit", "via": "invited" }]));

    // The list hears the last change of a burst within the catalog's 10 s, not only at the next edit or the stop.
    let mut put = ann.http.put(format!("{}{}", ann.base, path("/markdown")));
    put = put.bearer_auth(ann.token.as_ref().unwrap()).body("# Last\n");
    assert!(put.send().await.unwrap().status().is_success());
    let started = std::time::Instant::now();
    loop {
        let list = ann.call(Method::GET, "/api/docs", None).await.unwrap();
        let row = list["docs"].as_array().unwrap().iter().find(|d| d["id"] == id.as_str()).cloned().unwrap();
        if row["title"] == "Last" {
            break;
        }
        assert!(started.elapsed() < std::time::Duration::from_secs(15), "the list still shows {row}");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    node.shutdown().await;
}
