//! `api-gateway seed | backend | gateway | load`, one process per node. See the README.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api_gateway::gateway::{Gateway, GatewayConfig};
use api_gateway::service::{QuoteApi, Service, ServiceConfig};
use api_gateway::{SERVES, SERVICE, seed_timing, transport};
use axon::{ActorSystem, Histogram, MailboxLimits};
use axon_cluster::{AccessPolicy, Cluster, ClusterConfig, SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig};
use axon_obs::{Dashboard, DashboardConfig, MemoryMetrics, MetricsConfig, MetricsService, Telemetry, TelemetryConfig};

type Error = Box<dyn std::error::Error + Send + Sync>;

struct Args(HashMap<String, String>);

impl Args {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, Error> {
        let args: Vec<String> = args.collect();
        let mut map = HashMap::new();
        for pair in args.chunks(2) {
            let key = pair[0].strip_prefix("--").ok_or_else(|| format!("expected --flag, got {}", pair[0]))?;
            let value = pair.get(1).ok_or_else(|| format!("--{key} needs a value"))?;
            map.insert(key.to_owned(), value.clone());
        }
        Ok(Self(map))
    }
    fn get<T: std::str::FromStr>(&self, key: &str, default: T) -> Result<T, Error> {
        match self.0.get(key) {
            Some(v) => v.parse().map_err(|_| format!("bad --{key} {v}").into()),
            None => Ok(default),
        }
    }
    fn text(&self, key: &str, default: &str) -> String {
        self.0.get(key).cloned().unwrap_or_else(|| default.to_owned())
    }
    fn ms(&self, key: &str, default: u64) -> Result<Duration, Error> {
        Ok(Duration::from_millis(self.get(key, default)?))
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // No TLS between nodes: a node talks unencrypted only where `--plaintext` says it may.
    let (plaintext, argv): (Vec<String>, Vec<String>) = std::env::args().skip(1).partition(|a| a == "--plaintext");
    let mut argv = argv.into_iter();
    let mode = argv.next().unwrap_or_default();
    let args = Args::parse(argv)?;
    match mode.as_str() {
        "seed" | "backend" | "gateway" if plaintext.is_empty() => {
            Err("api-gateway has no TLS between nodes: add --plaintext to run on loopback or a private network only"
                .into())
        }
        "seed" => seed(&args).await,
        "backend" => backend(&args).await,
        "gateway" => gateway(&args).await,
        "load" => load(&args).await,
        _ => Err("usage: api-gateway seed|backend|gateway [--plaintext] | load [--flag value]..".into()),
    }
}

async fn terminated() -> Result<(), Error> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        r = tokio::signal::ctrl_c() => r?,
        _ = term.recv() => {}
    }
    Ok(())
}

async fn seed(args: &Args) -> Result<(), Error> {
    let bind: SocketAddr = args.get("bind", "127.0.0.1:0".parse()?)?;
    let server = SeedServer::bind(SeedServerConfig::new(bind, &transport(&args.text("id", "seed-1")))).await?;
    println!("seed {}", server.local_addr());
    terminated().await
}

async fn node(args: &Args, role: &str, system: &ActorSystem, advertise: Option<SocketAddr>) -> Result<Cluster, Error> {
    let id = args.text("id", role);
    let seeds: Vec<String> = args.text("seeds", "").split(',').map(str::to_owned).collect();
    let transport = transport(&id);
    let discovery = SeedDiscovery::start(SeedConfig::new(&seeds, &transport)).await?;
    let mut cfg = ClusterConfig::new(args.get("bind", "127.0.0.1:0".parse()?)?, transport, discovery);
    cfg.advertise = advertise;
    seed_timing(&mut cfg);
    cfg.labels.insert("role".into(), role.into());
    let cluster = Cluster::start(system, cfg).await?;
    let telemetry = TelemetryConfig { access: AccessPolicy::Cluster, ..Default::default() };
    Telemetry::start(system, &cluster, telemetry).await?;
    Ok(cluster)
}

async fn backend(args: &Args) -> Result<(), Error> {
    let limits = MailboxLimits { messages: args.get("mailbox", 1024)?, ..Default::default() };
    let system = ActorSystem::builder().mailbox(limits).timing_sample_every(64).build()?;
    // With --cuttable, peers dial a relay that SIGUSR1 cuts and SIGUSR2 heals: a partition between this node and
    // everyone who calls it, while its own links to the seed stay up.
    let relay = match args.get("cuttable", false)? {
        true => Some(tokio::net::TcpListener::bind("127.0.0.1:0").await?),
        false => None,
    };
    let advertise = relay.as_ref().map(|r| r.local_addr()).transpose()?;
    let cluster = node(args, "backend", &system, advertise).await?;
    if let Some(relay) = relay {
        let (cut, watch) = tokio::sync::watch::channel(false);
        tokio::spawn(cuttable(relay, cluster.local_addr(), watch));
        use tokio::signal::unix::{SignalKind, signal};
        let (mut on, mut off) = (signal(SignalKind::user_defined1())?, signal(SignalKind::user_defined2())?);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = on.recv() => cut.send_replace(true),
                    _ = off.recv() => cut.send_replace(false),
                };
                println!("cut {}", *cut.borrow());
            }
        });
    }
    let cfg = ServiceConfig {
        work: args.get("work", 200)?,
        delay: args.ms("delay-ms", 0)?,
        capacity: args.get("capacity", 1024)?,
    };
    let service = system.spawn(Service::new(cfg)).name(SERVICE).start()?;
    cluster.publish::<QuoteApi>(&service, SERVICE)?;
    cluster.set_metadata(SERVES, Some(SERVICE)).await?;
    println!("ready {}", cluster.identity().node_id);
    terminated().await?;
    system.shutdown().await?;
    Ok(())
}

async fn gateway(args: &Args) -> Result<(), Error> {
    let system = ActorSystem::builder().build()?;
    let cluster = node(args, "gateway", &system, None).await?;
    let cfg = GatewayConfig {
        deadline: args.ms("deadline-ms", 500)?,
        try_timeout: args.ms("try-ms", 150)?,
        max_in_flight: args.get("max-in-flight", 2048)?,
        ..Default::default()
    };
    let gateway = Gateway::start(&cluster, cfg);
    if let Some(bind) = args.0.get("dashboard") {
        // The console, and the history it charts, kept in memory on this gateway.
        let metrics = MetricsConfig { access: AccessPolicy::Cluster, ..Default::default() };
        MetricsService::start(&system, &cluster, Arc::new(MemoryMetrics::new(256 << 20)), metrics).await?;
        let dashboard =
            Dashboard::start(&system, &cluster, DashboardConfig { bind: bind.parse()?, ..Default::default() }).await?;
        println!("console http://{}", dashboard.address());
    }
    let wanted = args.get("wait-backends", 1)?;
    tokio::time::timeout(Duration::from_secs(30), gateway.wait_backends(|&n| n >= wanted))
        .await
        .map_err(|_| format!("fewer than {wanted} backends after 30 s"))?;
    let listener = tokio::net::TcpListener::bind(args.get("http", "127.0.0.1:0".parse::<SocketAddr>()?)?).await?;
    println!("listening http://{}", listener.local_addr()?);
    let serve = axum::serve(listener, gateway.router()).with_graceful_shutdown(async {
        let _ = terminated().await;
    });
    serve.await?;
    system.shutdown().await?;
    Ok(())
}

/// Relays each connection to `to`. While `cut` is true no byte moves either way and the sockets stay open, as
/// across a network partition; what was read meanwhile is delivered after the heal, as TCP would.
async fn cuttable(relay: tokio::net::TcpListener, to: SocketAddr, cut: tokio::sync::watch::Receiver<bool>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn pipe(
        mut from: tokio::net::tcp::OwnedReadHalf,
        mut into: tokio::net::tcp::OwnedWriteHalf,
        mut cut: tokio::sync::watch::Receiver<bool>,
    ) -> std::io::Result<()> {
        let mut buf = vec![0; 64 * 1024];
        loop {
            cut.wait_for(|c| !c).await.map_err(std::io::Error::other)?;
            let n = from.read(&mut buf).await?;
            if n == 0 {
                return Ok(());
            }
            cut.wait_for(|c| !c).await.map_err(std::io::Error::other)?;
            into.write_all(&buf[..n]).await?;
        }
    }
    loop {
        let inbound = match relay.accept().await {
            Ok((inbound, _)) => inbound,
            Err(e) => {
                eprintln!("relay accept: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await; // out of descriptors: do not spin
                continue;
            }
        };
        let cut = cut.clone();
        tokio::spawn(async move {
            let Ok(outbound) = tokio::net::TcpStream::connect(to).await else { return }; // the peer redials
            let ((ri, wi), (ro, wo)) = (inbound.into_split(), outbound.into_split());
            tokio::select! {
                _ = pipe(ri, wo, cut.clone()) => {}
                _ = pipe(ro, wi, cut) => {}
            }
        });
    }
}

#[derive(Clone, Copy, Default)]
struct Second {
    /// 2xx, 429, 503, 504, other status, no response in time, connection error.
    counts: [u64; 7],
    max_us: u64,
}

const COLUMNS: [&str; 7] = ["ok", "429", "503", "504", "other", "hung", "conn_err"];

struct Tally {
    latency: Histogram,
    seconds: Vec<Second>,
}

async fn load(args: &Args) -> Result<(), Error> {
    let urls: Vec<String> = args.text("url", "http://127.0.0.1:8080").split(',').map(str::to_owned).collect();
    let concurrency: usize = args.get("concurrency", 64)?;
    let secs: usize = args.get("secs", 10)?;
    let deadline: u64 = args.get("deadline-ms", 500)?;
    let orders_every: u64 = args.get("orders-every", 0)?;
    // A request still unanswered a second past its deadline counts as hung, and is abandoned.
    let client = reqwest::Client::builder().timeout(Duration::from_millis(deadline + 1000)).build()?;
    let started = Instant::now();
    let end = started + Duration::from_secs(secs as u64);
    let mut workers = tokio::task::JoinSet::new();
    let urls = Arc::new(urls);
    for w in 0..concurrency {
        let (client, urls) = (client.clone(), urls.clone());
        workers.spawn(async move {
            let mut tally = Tally { latency: Histogram::default(), seconds: vec![Second::default(); secs + 1] };
            let mut n = w as u64;
            while Instant::now() < end {
                n += concurrency as u64;
                let url = &urls[n as usize % urls.len()];
                let request = if orders_every > 0 && n.is_multiple_of(orders_every) {
                    client.post(format!("{url}/v1/orders/{n}"))
                } else {
                    client.get(format!("{url}/v1/quote/{n}"))
                };
                let sent = Instant::now();
                let column = match request.header("x-deadline-ms", deadline).send().await {
                    Ok(response) => {
                        let status = response.status().as_u16();
                        // Read the body: the request is not done until it is.
                        let whole = response.bytes().await.is_ok();
                        match status {
                            _ if !whole => 5,
                            200..=299 => 0,
                            429 => 1,
                            503 => 2,
                            504 => 3,
                            _ => 4,
                        }
                    }
                    Err(e) if e.is_timeout() => 5,
                    Err(_) => 6,
                };
                let took = sent.elapsed();
                tally.latency.record(took);
                let slot = &mut tally.seconds[((sent - started).as_secs() as usize).min(secs)];
                slot.counts[column] += 1;
                slot.max_us = slot.max_us.max(took.as_micros() as u64);
                if column == 6 {
                    // A refused connection returns at once; do not spin on it.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            tally
        });
    }
    let mut total = Tally { latency: Histogram::default(), seconds: vec![Second::default(); secs + 1] };
    while let Some(tally) = workers.join_next().await {
        let tally = tally?;
        total.latency.merge(&tally.latency);
        for (a, b) in total.seconds.iter_mut().zip(&tally.seconds) {
            a.counts.iter_mut().zip(b.counts).for_each(|(x, y)| *x += y);
            a.max_us = a.max_us.max(b.max_us);
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    for (t, s) in total.seconds.iter().enumerate() {
        let cols: Vec<String> = COLUMNS.iter().zip(s.counts).map(|(c, n)| format!("{c}={n}")).collect();
        println!("t={t:<3} {} max_ms={:.1}", cols.join(" "), s.max_us as f64 / 1000.0);
    }
    let mut sums = [0u64; 7];
    total.seconds.iter().for_each(|s| sums.iter_mut().zip(s.counts).for_each(|(a, b)| *a += b));
    let requests: u64 = sums.iter().sum();
    let max_us = total.seconds.iter().map(|s| s.max_us).max().unwrap_or(0);
    let cols: Vec<String> = COLUMNS.iter().zip(sums).map(|(c, n)| format!("\"{c}\":{n}")).collect();
    let ms = |p| total.latency.percentile(p).map_or(0.0, |d| d.as_secs_f64() * 1e3);
    println!(
        "summary {{\"secs\":{elapsed:.1},\"concurrency\":{concurrency},\"requests\":{requests},\"rps\":{:.0},{},\"p50_ms\":{:.2},\"p90_ms\":{:.2},\"p99_ms\":{:.2},\"p999_ms\":{:.2},\"max_ms\":{:.1}}}",
        requests as f64 / elapsed,
        cols.join(","),
        ms(0.5),
        ms(0.9),
        ms(0.99),
        ms(0.999),
        max_us as f64 / 1000.0
    );
    Ok(())
}
