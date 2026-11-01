//! The front end's bounds, on `kv stub` (no cluster): each is reached and refused, and what is refused is said.

use std::time::Duration;

use kv::Limits;
use kv::client::Client;
use kv::node;
use kv::resp::Value;

async fn stub(limits: Limits) -> std::net::SocketAddr {
    node::stub(([127, 0, 0, 1], 0).into(), limits).await.expect("the stub starts").0
}

#[tokio::test]
async fn the_stub_answers_what_a_ycsb_client_sends() {
    let addr = stub(Limits::default()).await;
    let mut c = Client::connect(addr).await.unwrap();
    assert_eq!(c.call(&[b"SET", b"user1", b"{}"]).await.unwrap(), Value::ok());
    assert_eq!(c.call(&[b"GET", b"user1"]).await.unwrap(), Value::Bulk(Some(node::stub_value())));
    let hello = c.call(&[b"HELLO", b"3"]).await.unwrap();
    assert!(matches!(&hello, Value::Error(e) if e.starts_with("ERR unknown command 'HELLO'")), "{hello:?}");
    assert_eq!(c.call(&[b"CLIENT", b"SETNAME", b"ycsb"]).await.unwrap(), Value::ok());
    assert_eq!(c.call(&[b"SELECT", b"0"]).await.unwrap(), Value::ok());
    let Value::Array(Some(ranges)) = c.call(&[b"CLUSTER", b"SLOTS"]).await.unwrap() else { panic!("slots") };
    let Value::Array(Some(range)) = &ranges[0] else { panic!("a range") };
    assert_eq!((ranges.len(), &range[0], &range[1]), (1, &Value::Int(0), &Value::Int(16383)));
    let Value::Bulk(Some(nodes)) = c.call(&[b"CLUSTER", b"NODES"]).await.unwrap() else { panic!("nodes") };
    assert_eq!(nodes, format!("stub {addr}@0 myself,master - 0 0 0 connected 0-16383\n").as_bytes());
    // redis-benchmark reads these two before it starts, and warns when they are missing.
    let appendonly = c.call(&[b"CONFIG", b"GET", b"appendonly"]).await.unwrap();
    assert_eq!(appendonly, Value::Array(Some(vec![Value::bulk("appendonly"), Value::bulk("no")])));
    assert_eq!(c.call(&[b"CONFIG", b"GET", b"maxmemory"]).await.unwrap(), Value::Array(Some(Vec::new())));
    // A cluster client finds each command's keys here: one entry for each command kv serves.
    let Value::Array(Some(table)) = c.call(&[b"COMMAND"]).await.unwrap() else { panic!("command") };
    assert_eq!(table.len(), kv::COMMANDS.len());
    let mset = table.iter().find(|e| matches!(e, Value::Array(Some(f)) if f[0] == Value::bulk("mset")));
    let Some(Value::Array(Some(mset))) = mset else { panic!("mset in {table:?}") };
    assert_eq!(mset[3..], [Value::Int(1), Value::Int(-1), Value::Int(2)]);
    assert_eq!(c.call(&[b"SET", b"k", b"v", b"EX", b"10"]).await.unwrap(), Value::err("ERR syntax error"));
    assert_eq!(c.call(&[b"GET"]).await.unwrap(), Value::err("ERR wrong number of arguments for 'get' command"));
}

#[tokio::test]
async fn connections_past_the_limit_are_told_and_closed() {
    let addr = stub(Limits { connections: 2, ..Limits::default() }).await;
    let mut held = Vec::new();
    for _ in 0..2 {
        let mut c = Client::connect(addr).await.unwrap();
        assert_eq!(c.call(&[b"PING"]).await.unwrap(), Value::Simple("PONG".into()));
        held.push(c);
    }
    let mut third = Client::connect(addr).await.unwrap();
    assert_eq!(third.read().await.unwrap(), Value::err("ERR max number of clients reached"));
    assert!(third.read().await.is_err(), "closed after");
    drop(held.pop());
    // The slot is given back once the connection's task sees it closed.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut c = Client::connect(addr).await.unwrap();
        if let Ok(Value::Simple(_)) = c.call(&[b"PING"]).await {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "a closed connection's slot came back");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_bulk_past_the_command_limit_is_a_protocol_error() {
    let addr = stub(Limits::default()).await;
    let mut c = Client::connect(addr).await.unwrap();
    c.raw(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$40000000\r\n").await.unwrap();
    assert_eq!(c.read().await.unwrap(), Value::err("ERR Protocol error: invalid bulk length"));
    assert!(c.read().await.is_err(), "closed after");
}

/// The node's input budget is shared: a second large command waits for it, then is told and closed, and the first
/// goes on.
#[tokio::test]
async fn large_commands_share_the_nodes_input_budget() {
    // A short wait, so the test does not sit through the shipped 30 s.
    let limits = Limits { input_budget: 1 << 20, input_wait: Duration::from_millis(300), ..Limits::default() };
    let addr = stub(limits).await;
    let head = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$900000\r\n";
    let mut first = Client::connect(addr).await.unwrap();
    first.raw(head).await.unwrap();
    first.raw(&[7; 1000]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut second = Client::connect(addr).await.unwrap();
    second.raw(head).await.unwrap();
    second.raw(&[7; 1000]).await.unwrap();
    assert_eq!(second.read().await.unwrap(), Value::err("ERR the node's input budget is used up"));
    assert!(second.read().await.is_err(), "closed after");
    first.raw(&[7; 899_000]).await.unwrap();
    first.raw(b"\r\n").await.unwrap();
    assert_eq!(first.read().await.unwrap(), Value::ok());
    // Its budget is back once the command is read: a third can send the same.
    let mut third = Client::connect(addr).await.unwrap();
    third.raw(head).await.unwrap();
    third.raw(&[7; 900_000]).await.unwrap();
    third.raw(b"\r\n").await.unwrap();
    assert_eq!(third.read().await.unwrap(), Value::ok());
}

#[tokio::test]
async fn a_value_past_32_mib_is_refused_and_the_connection_goes_on() {
    let addr = stub(Limits::default()).await;
    let mut c = Client::connect(addr).await.unwrap();
    assert_eq!(c.call(&[b"SET", b"k", &vec![1u8; 32 << 20]]).await.unwrap(), Value::ok());
    let over = vec![1u8; (32 << 20) + 1];
    assert_eq!(c.call(&[b"SET", b"k", &over]).await.unwrap(), Value::err("ERR value too large"));
    assert_eq!(c.call(&[b"MSET", b"k", b"1", b"j", &over]).await.unwrap(), Value::err("ERR value too large"));
    assert_eq!(c.call(&[b"PING"]).await.unwrap(), Value::Simple("PONG".into()));
}
