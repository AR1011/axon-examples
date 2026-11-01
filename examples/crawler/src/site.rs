//! A generated web site to crawl: `domains` hosts named `d{i}.site`, each with `pages` pages. Every page links to a
//! few others, on its own host and on others, and now and then to one that does not exist. The server records every
//! request it answers, which is what the tests check the crawler against.

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

#[derive(Clone, Debug)]
pub struct SiteConfig {
    pub domains: usize,
    pub pages: usize,
    pub links: usize,
    /// Percent of links that point at another domain.
    pub cross_percent: u64,
    pub seed: u64,
    /// Each answer waits this long, as a real server would.
    pub latency: Duration,
    /// One page in this many answers 503 to its first request. 0: never.
    pub flaky_every: u64,
}

impl Default for SiteConfig {
    fn default() -> Self {
        Self {
            domains: 8,
            pages: 100,
            links: 6,
            cross_percent: 30,
            seed: 1,
            latency: Duration::from_millis(5),
            flaky_every: 16,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Hit {
    pub at: Instant,
    pub url: String,
    pub status: u16,
}

pub fn url(domain: usize, page: usize) -> String {
    format!("http://d{domain}.site/p{page}")
}

fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

impl SiteConfig {
    pub fn roots(&self) -> Vec<String> {
        (0..self.domains).map(|d| url(d, 0)).collect()
    }

    /// The links on page `page` of `domain`; about one in 25 points past the last page, a 404.
    pub fn links(&self, domain: usize, page: usize) -> Vec<(usize, usize)> {
        let mut state = mix(self.seed ^ ((domain as u64) << 32) ^ page as u64);
        (0..self.links)
            .map(|_| {
                state = mix(state);
                let d = if state % 100 >= self.cross_percent { domain } else { (state >> 8) as usize % self.domains };
                let p = (state >> 24) as usize % (self.pages + self.pages / 25);
                (d, p)
            })
            .collect()
    }

    fn flaky(&self, domain: usize, page: usize) -> bool {
        self.flaky_every > 0
            && mix(self.seed ^ 0xF1A7 ^ ((domain as u64) << 32) ^ page as u64).is_multiple_of(self.flaky_every)
    }

    /// Every URL a crawl from the roots can find, 404s included.
    pub fn reachable(&self) -> BTreeSet<String> {
        let mut seen: HashSet<(usize, usize)> = (0..self.domains).map(|d| (d, 0)).collect();
        let mut queue: VecDeque<_> = seen.iter().copied().collect();
        while let Some((d, p)) = queue.pop_front() {
            if p >= self.pages {
                continue;
            }
            for link in self.links(d, p) {
                if seen.insert(link) {
                    queue.push_back(link);
                }
            }
        }
        seen.into_iter().map(|(d, p)| url(d, p)).collect()
    }
}

/// The running site. Dropping it stops the server.
pub struct Site {
    pub addr: SocketAddr,
    pub cfg: SiteConfig,
    hits: Arc<Mutex<Vec<Hit>>>,
    count: watch::Receiver<usize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Site {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Shared {
    cfg: SiteConfig,
    // ponytail: grows with the crawl; it is the test's oracle, sized by the site.
    hits: Arc<Mutex<Vec<Hit>>>,
    flaked: Mutex<HashSet<(usize, usize)>>,
    count: watch::Sender<usize>,
}

impl Site {
    pub async fn start(cfg: SiteConfig) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let hits = Arc::new(Mutex::new(Vec::new()));
        let (count, rx) = watch::channel(0);
        let shared =
            Arc::new(Shared { cfg: cfg.clone(), hits: hits.clone(), flaked: Mutex::new(HashSet::new()), count });
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(stream, shared.clone()));
            }
        });
        Ok(Self { addr, cfg, hits, count: rx, task })
    }

    pub fn hits(&self) -> Vec<Hit> {
        self.hits.lock().expect("site log").clone()
    }

    /// Waits until the site has answered at least one request that was not a 503 for every URL in `urls`.
    pub async fn wait_all_served(&self, urls: &BTreeSet<String>) {
        let mut count = self.count.clone();
        let mut served = HashSet::new();
        let mut read = 0;
        loop {
            {
                let hits = self.hits.lock().expect("site log");
                for hit in &hits[read..] {
                    if hit.status != 503 && urls.contains(&hit.url) {
                        served.insert(hit.url.clone());
                    }
                }
                read = hits.len();
            }
            if served.len() == urls.len() || count.changed().await.is_err() {
                return;
            }
        }
    }

    /// Waits until the site has answered `n` requests.
    pub async fn wait_hits(&self, n: usize) {
        let _ = self.count.clone().wait_for(|c| *c >= n).await;
    }
}

async fn serve(mut stream: TcpStream, site: Arc<Shared>) {
    let mut head = Vec::with_capacity(512);
    let mut buf = [0u8; 1024];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) if head.len() + n > 8192 => return,
            Ok(n) => head.extend_from_slice(&buf[..n]),
        }
    }
    let arrived = Instant::now();
    let head = String::from_utf8_lossy(&head);
    let path = head.split(' ').nth(1).unwrap_or("/");
    let host =
        head.lines().find_map(|l| l.strip_prefix("Host: ").or_else(|| l.strip_prefix("host: "))).unwrap_or("").trim();
    let page = host
        .strip_prefix('d')
        .and_then(|h| h.strip_suffix(".site"))
        .and_then(|d| d.parse::<usize>().ok())
        .zip(path.strip_prefix("/p").and_then(|p| p.parse::<usize>().ok()));
    let cfg = &site.cfg;
    let (status, body) = match page {
        Some((d, p)) if d < cfg.domains && p < cfg.pages => {
            if cfg.flaky(d, p) && site.flaked.lock().expect("flaky set").insert((d, p)) {
                (503, String::new())
            } else {
                let links: String = cfg
                    .links(d, p)
                    .into_iter()
                    .map(|(ld, lp)| format!("<a href=\"{}\">page {lp}</a>\n", url(ld, lp)))
                    .collect();
                (200, format!("<html><body><h1>d{d} page {p}</h1>\n{links}</body></html>\n"))
            }
        }
        _ => (404, String::new()),
    };
    tokio::time::sleep(cfg.latency).await;
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        _ => "Service Unavailable",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    {
        let mut hits = site.hits.lock().expect("site log");
        hits.push(Hit { at: arrived, url: format!("http://{host}{path}"), status });
        site.count.send_replace(hits.len());
    }
    let _ = stream.write_all(response.as_bytes()).await;
}
