//! One node and real websockets (design §8): what a client is told was saved outlives a crash of its home (I1), a new
//! document starts once whatever opens it first (I4), no socket changes what its principal may not, nor opens with a
//! cookie from another origin (I6), no update bricks a document (I9), each limit closes with its code (I10) but a
//! client that reads is never closed, and an edit that is durable but cannot be published is published by the
//! document's restart (I14).

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axon_stream::CompactLimits;
use futures_util::{SinkExt, StreamExt};
use mdpad::catalog::{self, new_id};
use mdpad::client::{Api, Client, Credential, Stop};
use mdpad::core::catalog::Event as CatalogEvent;
use mdpad::core::doc::{MAX_STATE, MAX_UPDATE, STREAM_BYTES, STREAM_KEYS};
use mdpad::crdt;
use mdpad::doc;
use mdpad::keeper::Msg;
use mdpad::wire::{ClientFrame, DocAnswer, DocReq, close};
use reqwest::Method;
use serde_json::json;
use support::{CutFiles, closed, settle, signed_in, start, user};
use tokio::time::Instant;

const SETTLE: Duration = Duration::from_secs(15);

fn files() -> (CutFiles, CutFiles) {
    (CutFiles::new(None), CutFiles::new(None))
}

/// A frame that inserts `text` into a copy that holds nothing: a valid update from a client the document never saw.
fn foreign_edit(text: &str) -> Vec<u8> {
    let update = crdt::insert(&crdt::doc(77), 0, text);
    ClientFrame::Edit { seq: 1, offline: 0, update }.encode()
}

/// Types tokens at the end of the text, one at a time, until `stop`; returns the client and the tokens it was told
/// were saved.
async fn typist(mut client: Client, name: String, stop: Arc<AtomicBool>) -> (Client, Vec<String>) {
    let mut tokens = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        if !client.core.is_connected() && client.connect().await.is_err() {
            break;
        }
        if client.core.unsaved() == 0 && client.core.welcome.is_some() {
            let mark = tokens.len() as u64;
            tokens.push(format!(" {name}{mark} "));
            let end = crdt::len(&client.core.doc);
            let _ = client.insert(end, &tokens[mark as usize], mark).await;
        }
        match client.step(Instant::now() + Duration::from_millis(20)).await {
            Ok(()) | Err(Stop::Timeout) => {}
            Err(_) => break,
        }
    }
    let saved = tokens.iter().enumerate().filter(|(mark, _)| client.saved.contains(&(*mark as u64)));
    let saved = saved.map(|(_, token)| token.clone()).collect();
    (client, saved)
}

/// I1 and I3: three clients type while each write takes a few milliseconds; the home crashes with writes in flight,
/// one of them torn. Every token a client was told was saved is in the restarted node's text, and the clients, joined
/// again, end with that text.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn what_was_saved_outlives_a_crash_of_the_home() {
    let (docs, accounts) = (CutFiles::new(Some(Duration::from_millis(3))), CutFiles::new(None));
    let node = start(&docs, &accounts, |_| {}).await;
    let api = user(&node, "ann").await;
    let id = api.create("# Crash\n").await.unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let typists: Vec<_> = (0..3)
        .map(|i| {
            let client = Client::new(node.http(), &id, api.credential());
            tokio::spawn(typist(client, format!("t{i}k"), stop.clone()))
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    docs.crash(true);
    accounts.crash(false);
    node.crash();
    stop.store(true, Ordering::Relaxed);
    let mut clients = Vec::new();
    let mut saved = Vec::new();
    for typist in typists {
        let (mut client, tokens) = typist.await.unwrap();
        client.disconnect().await;
        clients.push(client);
        saved.extend(tokens);
    }
    assert!(saved.len() >= 30, "too few edits saved before the crash to mean anything: {}", saved.len());

    let node = start(&docs.reopen(), &accounts.reopen(), |_| {}).await;
    let api = signed_in(&node, "ann").await;
    let text = api.markdown(&id).await.unwrap();
    let lost: Vec<_> = saved.iter().filter(|token| !text.contains(token.as_str())).collect();
    assert!(lost.is_empty(), "I1: saved and lost: {lost:?}");
    for client in &mut clients {
        client.http = node.http();
        settle(client, SETTLE).await;
    }
    let text = api.markdown(&id).await.unwrap();
    for client in &mut clients {
        let caught_up = client.run_until(SETTLE, |c| c.text() == text).await;
        assert_eq!(caught_up, Ok(()), "I3: {:?} != {text:?}", client.text());
    }
    node.shutdown().await;
}

/// I4: twenty sockets and five creates race for a new document; the template is in it once. An id the catalog never
/// made is refused before anything starts, and a document the catalog lists never loads empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_document_starts_once_and_an_unknown_one_never() {
    let (docs, accounts) = files();
    let node = start(&docs, &accounts, |_| {}).await;
    let api = user(&node, "ann").await;
    // The row as POST /api/docs makes it, and then everything at once.
    let ask = |reply| Msg::Request(catalog::Request::Create { owner: "ann".into(), reply });
    let id = node.catalog().ask(ask).await.unwrap();
    let mut creates = Vec::new();
    for _ in 0..5 {
        let doc = doc::keyed(&node.cluster, &id);
        let req = DocReq::Create { owner: "ann".into(), markdown: "# Template\n".into() };
        creates.push(tokio::spawn(async move { doc.request_within(req, SETTLE).await }));
    }
    let mut sockets = Vec::new();
    for _ in 0..20 {
        let mut client = Client::new(node.http(), &id, api.credential());
        sockets.push(tokio::spawn(async move {
            settle(&mut client, SETTLE).await;
            client
        }));
    }
    for create in creates {
        assert!(matches!(create.await.unwrap(), Ok(DocAnswer::Done)));
    }
    for socket in sockets {
        assert_eq!(socket.await.unwrap().text(), "# Template\n");
    }
    assert_eq!(api.markdown(&id).await.unwrap(), "# Template\n");

    let unknown = new_id("h1");
    let mut client = Client::new(node.http(), &unknown, api.credential());
    client.connect().await.unwrap();
    assert_eq!(closed(&mut client, Duration::from_secs(1)).await, close::NOT_FOUND);
    assert!(api.markdown(&unknown).await.unwrap_err().starts_with("404"));

    // A row the catalog says is ready, with nothing in the journal: an error, not an empty document.
    let ask = |reply| Msg::Request(catalog::Request::Create { owner: "ann".into(), reply });
    let lost = node.catalog().ask(ask).await.unwrap();
    node.catalog().tell(Msg::Request(catalog::Request::Doc(CatalogEvent::Ready { id: lost.clone() }))).unwrap();
    axon_testkit::eventually("the row is ready", async || mdpad::web::ready(node.ids(), &lost)).await;
    let answer = api.markdown(&lost).await;
    assert!(answer.as_ref().is_err_and(|e| e.starts_with("503")), "{answer:?}");
    node.shutdown().await;
}

/// I6: an edit frame from a viewer, from a member just removed, from a guest after a link reset, and from a member by
/// the link after "Remove all via link", closes the socket with 4403 within a second, and the text does not change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_socket_edits_without_the_edit_level() {
    let (docs, accounts) = files();
    let node = start(&docs, &accounts, |_| {}).await;
    let ann = user(&node, "ann").await;
    let (ben, cat) = (user(&node, "ben").await, user(&node, "cat").await);
    let id = ann.create("# Rights\n").await.unwrap();
    let path = |rest: &str| format!("/api/docs/{id}{rest}");
    let forbidden = async |client: &mut Client, send: bool| {
        if send {
            let _ = client.send_raw(foreign_edit("evil")).await;
        }
        let at = Instant::now();
        assert_eq!(closed(client, Duration::from_secs(1)).await, close::FORBIDDEN);
        at.elapsed()
    };

    // A view member.
    ann.call(Method::PUT, &path("/members/ben"), Some(json!({ "level": "view" }))).await.unwrap();
    let mut viewer = Client::new(node.http(), &id, ben.credential());
    settle(&mut viewer, SETTLE).await;
    forbidden(&mut viewer, true).await;

    // An edit member who edits, then is removed.
    ann.call(Method::PUT, &path("/members/ben"), Some(json!({ "level": "edit" }))).await.unwrap();
    let mut editor = Client::new(node.http(), &id, ben.credential());
    settle(&mut editor, SETTLE).await;
    editor.insert(crdt::len(&editor.core.doc), "B", 1).await.unwrap();
    settle(&mut editor, SETTLE).await;
    ann.call(Method::DELETE, &path("/members/ben"), None).await.unwrap();
    forbidden(&mut editor, true).await;

    // A guest of the view link, when the link is reset: closed without sending anything.
    ann.call(Method::PUT, &path("/share"), Some(json!({ "link": "view" }))).await.unwrap();
    let link = ann.call(Method::GET, &path("/share"), None).await.unwrap()["token"].as_str().unwrap().to_string();
    let body = json!({ "link": link, "name": "Gus" });
    let guest = Api::new(node.http()).call(Method::POST, "/api/guest", Some(body)).await.unwrap();
    let guest = guest["token"].as_str().unwrap().to_string();
    let mut gus = Client::new(node.http(), &id, Credential::Guest(guest.clone()));
    settle(&mut gus, SETTLE).await;
    ann.call(Method::POST, &path("/share/reset"), None).await.unwrap();
    forbidden(&mut gus, false).await;
    let mut gus = Client::new(node.http(), &id, Credential::Guest(guest));
    gus.connect().await.unwrap();
    forbidden(&mut gus, false).await;

    // A member by the edit link edits on after a reset (Q9), and is closed by "Remove all via link".
    ann.call(Method::PUT, &path("/share"), Some(json!({ "link": "edit" }))).await.unwrap();
    let link = ann.call(Method::GET, &path("/share"), None).await.unwrap()["token"].as_str().unwrap().to_string();
    cat.call(Method::POST, &format!("/api/s/{link}/join"), None).await.unwrap();
    let mut joined = Client::new(node.http(), &id, cat.credential());
    settle(&mut joined, SETTLE).await;
    ann.call(Method::POST, &path("/share/reset"), None).await.unwrap();
    joined.insert(crdt::len(&joined.core.doc), "C", 1).await.unwrap();
    settle(&mut joined, SETTLE).await;
    ann.call(Method::POST, &path("/share/remove-link-members"), None).await.unwrap();
    forbidden(&mut joined, true).await;

    // The owner's own cookie, from a page of another origin.
    let cookie_socket = async |origin: &str| {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = format!("ws://{}{}", node.http(), path("/ws")).into_client_request().unwrap();
        let token = ann.token.clone().unwrap();
        request.headers_mut().insert("cookie", format!("mdpad={token}").parse().unwrap());
        request.headers_mut().insert("origin", origin.parse().unwrap());
        let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Binary(ClientFrame::Hello { resume: None }.encode().into()))
            .await
            .unwrap();
        // The first frame: a close and its code, or the welcome.
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await.expect("an answer") {
            Some(Ok(tokio_tungstenite::tungstenite::Message::Close(frame))) => frame.map(|f| u16::from(f.code)),
            Some(Ok(_)) => None,
            other => panic!("{other:?}"),
        }
    };
    assert_eq!(cookie_socket("http://elsewhere.example").await, Some(close::FORBIDDEN));
    assert_eq!(cookie_socket(&format!("http://{}", node.http())).await, None, "the same origin joins");

    assert_eq!(ann.markdown(&id).await.unwrap(), "# Rights\nBC");
    node.shutdown().await;
}

/// A small generator, so a failure names its case.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// I9: random frames and mutated updates, each on its own socket: each is refused or applied, and afterwards the
/// document still takes a good edit and loads again after a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_update_bricks_a_document() {
    let (docs, accounts) = files();
    let node = start(&docs, &accounts, |_| {}).await;
    let api = user(&node, "ann").await;
    let id = api.create("# Poison\n").await.unwrap();
    // Eight sockets at a time, each case on a socket of its own, as a refusal closes it.
    let mut tasks = Vec::new();
    for task in 0..8u64 {
        let (http, id, credential) = (node.http(), id.clone(), api.credential());
        tasks.push(tokio::spawn(async move {
            let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (task << 32));
            let mut outcomes = Vec::new();
            for case in 0..20u64 {
                // A valid insert from a client the document never saw, then broken one way or another.
                let mut update = crdt::insert(&crdt::doc(1000 + task * 100 + case), 0, &"p".repeat(1 + rng.below(40)));
                match case % 5 {
                    0 => update = (0..rng.below(64)).map(|_| rng.next() as u8).collect(),
                    1 => {
                        let at = rng.below(update.len());
                        update[at] ^= 1 << rng.below(8);
                    }
                    2 => update.truncate(rng.below(update.len())),
                    3 => update.extend((0..1 + rng.below(16)).map(|_| rng.next() as u8)),
                    _ => {}
                }
                let frame = match case % 7 {
                    6 => (0..1 + rng.below(32)).map(|_| rng.next() as u8).collect(),
                    _ => ClientFrame::Edit { seq: 1, offline: rng.below(2) as u64, update }.encode(),
                };
                let mut client = Client::new(http, &id, credential.clone());
                settle(&mut client, SETTLE).await;
                client.send_raw(frame).await.unwrap();
                outcomes.push(match client.run_until(Duration::from_millis(150), |_| false).await {
                    Err(Stop::Timeout) => "open".to_string(),
                    stop => format!("{stop:?}"),
                });
                client.disconnect().await;
            }
            outcomes
        }));
    }
    let mut outcomes = std::collections::BTreeMap::<String, usize>::new();
    for task in tasks {
        for outcome in task.await.unwrap() {
            *outcomes.entry(outcome).or_default() += 1;
        }
    }
    eprintln!("outcomes: {outcomes:?}");
    let mut good = Client::new(node.http(), &id, api.credential());
    settle(&mut good, SETTLE).await;
    good.insert(0, "GOOD ", 1).await.unwrap();
    settle(&mut good, SETTLE).await;
    let text = api.markdown(&id).await.unwrap();
    assert!(text.starts_with("GOOD ") && text.contains("# Poison"), "{text:?}");
    node.shutdown().await;

    let node = start(&docs.reopen(), &accounts.reopen(), |_| {}).await;
    let api = signed_in(&node, "ann").await;
    assert_eq!(api.markdown(&id).await.unwrap(), text, "the same text after a reload");
    let mut again = Client::new(node.http(), &id, api.credential());
    settle(&mut again, SETTLE).await;
    again.insert(crdt::len(&again.core.doc), " AFTER", 1).await.unwrap();
    settle(&mut again, SETTLE).await;
    assert!(api.markdown(&id).await.unwrap().ends_with(" AFTER"));
    node.shutdown().await;
}

/// I10: an update over `MAX_UPDATE` (4413) and a frame over the socket's limit close the socket, and inserts stop at
/// `MAX_STATE` with a `Full` notice while the socket stays open. (The unwritten limit, `Busy`, is a core test; a client that does
/// not read is `tests/stall.rs`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_limit_closes_with_its_code() {
    let (docs, accounts) = files();
    let node = start(&docs, &accounts, |_| {}).await;
    let api = user(&node, "ann").await;
    let id = api.create("# Limits\n").await.unwrap();

    let mut client = Client::new(node.http(), &id, api.credential());
    settle(&mut client, SETTLE).await;
    let update = crdt::insert(&client.core.doc, 0, &"y".repeat(MAX_UPDATE));
    client.send_raw(ClientFrame::Edit { seq: 1, offline: 0, update }.encode()).await.unwrap();
    assert_eq!(closed(&mut client, Duration::from_secs(1)).await, close::TOO_LARGE);

    let mut client = Client::new(node.http(), &id, api.credential());
    settle(&mut client, SETTLE).await;
    client.send_raw(vec![18; MAX_UPDATE + 8192]).await.unwrap();
    let code = closed(&mut client, Duration::from_secs(1)).await;
    assert!(code != 1000, "a frame over the limit: {code}");

    let mut client = Client::new(node.http(), &id, api.credential());
    settle(&mut client, SETTLE).await;
    let chunk = "z".repeat(60 << 10);
    for mark in 0.. {
        assert!(mark < 10, "never full");
        client.insert(0, &chunk, mark).await.unwrap();
        settle(&mut client, SETTLE).await;
        if client.core.full {
            assert!(!client.saved.contains(&mark), "the edit past the cap is not saved");
            break;
        }
    }
    let text = api.markdown(&id).await.unwrap();
    assert!(text.ends_with("# Limits\n") && text.len() <= MAX_STATE, "{}", text.len());
    let info = api.call(Method::GET, &format!("/api/docs/{id}"), None).await.unwrap();
    assert!(info["size"].as_u64().unwrap() <= info["max_size"].as_u64().unwrap());
    node.shutdown().await;
}

/// I10 the other way: a compaction of a busy document removes about a thousand keys in one batch, a frame each, more
/// than the outbox holds before it pauses. A client that reads takes it all and stays joined.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_reads_stays_joined_through_a_compaction() {
    let (docs, accounts) = files();
    let node = start(&docs, &accounts, |_| {}).await;
    let api = user(&node, "ann").await;
    let id = api.create("# Keys\n").await.unwrap();
    let mut reader = Client::new(node.http(), &id, api.credential());
    settle(&mut reader, SETTLE).await;
    let done = Arc::new(AtomicBool::new(false));
    let reading = tokio::spawn({
        let done = done.clone();
        async move {
            let mut stops = Vec::new();
            while !done.load(Ordering::Relaxed) {
                match reader.step(Instant::now() + Duration::from_millis(20)).await {
                    Ok(()) | Err(Stop::Timeout) => {}
                    Err(stop) => {
                        stops.push(stop);
                        reader.connect().await.expect("joins again");
                    }
                }
            }
            (reader, stops)
        }
    });
    // Eight writers, so that their edits share the writes to disk.
    let writers = (0..8).map(|_| {
        let mut writer = Client::new(node.http(), &id, api.credential());
        tokio::spawn(async move {
            settle(&mut writer, SETTLE).await;
            for mark in 0..STREAM_KEYS as u64 / 8 {
                writer.insert(0, "k", mark).await.unwrap();
                settle(&mut writer, SETTLE).await;
            }
        })
    });
    for writer in writers.collect::<Vec<_>>() {
        writer.await.unwrap();
    }
    let text = api.markdown(&id).await.unwrap();
    assert_eq!(text.len(), "# Keys\n".len() + STREAM_KEYS);
    done.store(true, Ordering::Relaxed);
    let (mut reader, stops) = reading.await.unwrap();
    assert_eq!(stops, [], "the reader was closed or had to join again");
    assert_eq!(reader.run_until(SETTLE, |c| c.text() == text).await, Ok(()));
    node.shutdown().await;
}

/// I14: the stream holds fewer keys than the document thinks, so a publish fails after its write. The document
/// restarts and publishes its state; every edit is saved, and a second client sees them all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edit_written_but_not_published_is_published_by_the_restart() {
    let (docs, accounts) = files();
    let tight = CompactLimits { max_keys: 4, max_bytes: STREAM_BYTES };
    let node = start(&docs, &accounts, |cfg| cfg.doc_limits = tight).await;
    let api = user(&node, "ann").await;
    let id = api.create("# Tight\n").await.unwrap();
    let mut writer = Client::new(node.http(), &id, api.credential());
    let mut reader = Client::new(node.http(), &id, api.credential());
    settle(&mut writer, SETTLE).await;
    settle(&mut reader, SETTLE).await;
    let mut want = "# Tight\n".to_string();
    for mark in 0..8u64 {
        let token = format!("<{mark}>");
        writer.insert(crdt::len(&writer.core.doc), &token, mark).await.unwrap();
        settle(&mut writer, SETTLE).await;
        assert!(writer.saved.contains(&mark), "{mark} saved");
        want.push_str(&token);
    }
    assert_eq!(api.markdown(&id).await.unwrap(), want);
    let seen = reader.run_until(SETTLE, |c| c.text() == want).await;
    assert_eq!(seen, Ok(()), "{:?}", reader.text());
    let generation = reader.core.position.map(|(generation, _)| generation);
    assert!(generation > Some(1), "the publish never failed: generation {generation:?}");
    node.shutdown().await;
}

/// I11: each start of a document is a higher generation than any before it, a start after a compaction too, so no
/// client's position from an earlier start passes the gap check.
#[tokio::test(flavor = "multi_thread")]
async fn each_start_is_a_higher_generation_across_compactions() {
    let (mut docs, mut accounts) = files();
    let mut node = start(&docs, &accounts, |_| {}).await;
    let api = user(&node, "ann").await;
    let id = api.create("# Gen\n").await.unwrap();
    let mut generations = Vec::new();
    for round in 0..3 {
        let api = signed_in(&node, "ann").await;
        let mut client = Client::new(node.http(), &id, api.credential());
        settle(&mut client, SETTLE).await;
        generations.push(client.core.welcome.as_ref().expect("joined").generation);
        // 48 KiB of edits: past the 32 KiB after which the document compacts into a checkpoint.
        for mark in 0..12 {
            let end = crdt::len(&client.core.doc);
            client.insert(end, &format!("r{round}m{mark}{}", "x".repeat(4 << 10)), mark).await.unwrap();
            settle(&mut client, SETTLE).await;
        }
        node.shutdown().await;
        (docs, accounts) = (docs.reopen(), accounts.reopen());
        node = start(&docs, &accounts, |_| {}).await;
    }
    assert!(generations.windows(2).all(|g| g[0] < g[1]), "generations {generations:?}");
    node.shutdown().await;
}

/// A home's restart starts every document its clients had open at once, and each asks the journal for its records:
/// none fails for the crowd. With the default 128 asks at once, 2,000 documents opened together restarted 245,000
/// times on one home in two minutes, each restart asking again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crowd_of_documents_starts_at_once_after_a_restart() {
    let (docs, accounts) = files();
    // `MemoryFiles` reads a whole file for each range, and a preallocated segment was 64 MiB: 400 reads of it took
    // longer than an ask may on a slower box. Growing segments keep the reads small.
    let small = |cfg: &mut mdpad::node::NodeConfig| cfg.docs_log.preallocate = false;
    let node = start(&docs, &accounts, small).await;
    let api = user(&node, "ann").await;
    let texts: Vec<String> = (0..400).map(|i| format!("# Crowd {i}\n")).collect();
    let ids: Vec<String> = futures_util::stream::iter(&texts)
        .map(|text| api.create(text))
        .buffered(32)
        .map(Result::unwrap)
        .collect()
        .await;
    node.shutdown().await;
    // Each start's load waits for its fence to be written: with slow writes the crowd's asks are all in the journal
    // at once, as on a disk under load.
    let docs = docs.reopen();
    docs.set_delay(Some(Duration::from_millis(200)));
    let node = start(&docs, &accounts.reopen(), small).await;
    let api = signed_in(&node, "ann").await;
    // As a client does: a 503 under the crowd is asked again.
    let started = Instant::now();
    let read = futures_util::future::join_all(ids.iter().map(|id| async {
        loop {
            match api.markdown(id).await {
                Err(e) if e.starts_with("503") && started.elapsed() < SETTLE => {
                    tokio::time::sleep(Duration::from_millis(100)).await
                }
                read => return read.unwrap(),
            }
        }
    }))
    .await;
    println!("{} documents read {:?} after the restart", ids.len(), started.elapsed());
    assert_eq!(read, texts);
    let hosted = node.cluster.hosted(0);
    let kind = hosted.kinds.iter().find(|k| k.kind == doc::KIND).expect("the document kind");
    assert_eq!(kind.count, ids.len());
    assert_eq!(kind.ended, [0; axon_cluster::KEYED_ENDS.len()], "ends by {:?}", axon_cluster::KEYED_ENDS);
    node.shutdown().await;
}
