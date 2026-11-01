//! Runs a whole crawl: the site, a seed server, crawler nodes as child processes, and a control node that seeds
//! the crawl, can kill a node with SIGKILL half way, waits for the end and checks the site's log.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use axon_cluster::{Cluster, KeyedRef, Placement, SeedServer, SeedServerConfig};
use bytes::Bytes;
use tokio::process::{Child, Command};

use crate::crawler::{self, Answer, Call, DomainApi, KIND, NodeConfig, Status};
use crate::frontier::MAX_ATTEMPTS;
use crate::site::{Site, SiteConfig};

type Error = Box<dyn std::error::Error>;

#[derive(Clone, Debug)]
pub struct CrawlConfig {
    pub site: SiteConfig,
    pub nodes: usize,
    /// Requests per second to one domain.
    pub rate: f64,
    pub per_domain: usize,
    pub node_fetches: usize,
    /// Kill one crawler node with SIGKILL once the site has answered this share of the reachable URLs.
    pub kill_at: Option<f64>,
    pub data: PathBuf,
    /// The `crawler` binary.
    pub exe: PathBuf,
    pub deadline: Duration,
    /// Serve the console from the control node, and keep the cluster up this long after the crawl to look at it.
    pub console: Option<(std::net::SocketAddr, Duration)>,
}

#[derive(Debug)]
pub struct Report {
    pub reachable: usize,
    pub requests: usize,
    pub elapsed: Duration,
    pub pages_per_sec: f64,
    /// Reachable URLs the site never answered with anything but a 503.
    pub missing: Vec<String>,
    /// Answers other than 503 per URL, at most.
    pub max_answers: usize,
    pub duplicated_urls: usize,
    /// Requests per URL, 503s included, at most.
    pub max_requests: usize,
    /// The most requests one domain got in any one-second window.
    pub max_per_second: BTreeMap<String, usize>,
    /// The longest time one domain went without a request, such as while its owner was dead.
    pub longest_gap: BTreeMap<String, Duration>,
    pub killed: Option<(String, usize)>,
    pub statuses: BTreeMap<String, Status>,
}

impl Report {
    pub fn summary(&self) -> String {
        format!(
            "{} URLs, {} requests in {:.1?}: {:.0} pages/s; {} URLs answered twice; busiest domain second: {}; \
             longest gap: {:.1?}; killed: {:?}",
            self.reachable,
            self.requests,
            self.elapsed,
            self.pages_per_sec,
            self.duplicated_urls,
            self.max_per_second.values().max().unwrap_or(&0),
            self.longest_gap.values().max().copied().unwrap_or_default(),
            self.killed,
        )
    }

    /// The claim: every URL answered at least once, at most once more per kill, no domain over `rate` (one
    /// request of slack for network jitter), and nothing lost on the way.
    pub fn check(&self, cfg: &CrawlConfig) -> Result<(), String> {
        let kills = usize::from(self.killed.is_some());
        let rate_bound = cfg.rate.ceil() as usize + 1;
        let mut problems = Vec::new();
        if !self.missing.is_empty() {
            problems.push(format!("{} URLs never crawled, such as {}", self.missing.len(), self.missing[0]));
        }
        if self.max_answers > 1 + kills {
            problems.push(format!("a URL was answered {} times, bound {}", self.max_answers, 1 + kills));
        }
        if self.max_requests > MAX_ATTEMPTS as usize * (1 + kills) {
            problems.push(format!("a URL was requested {} times", self.max_requests));
        }
        for (domain, n) in &self.max_per_second {
            if *n > rate_bound {
                problems.push(format!("{domain} got {n} requests in one second, bound {rate_bound}"));
            }
        }
        for (domain, s) in &self.statuses {
            if s.lost_links > 0 || s.counts.over_cap > 0 || !s.idle() {
                problems.push(format!("{domain}: {s:?}"));
            }
        }
        let seen: u64 = self.statuses.values().map(|s| s.counts.seen).sum();
        if seen != self.reachable as u64 {
            problems.push(format!("the domains saw {seen} URLs, the site has {} reachable", self.reachable));
        }
        if self.killed.as_ref().is_some_and(|(_, owned)| *owned == 0) {
            problems.push("the killed node owned no domain".into());
        }
        if problems.is_empty() { Ok(()) } else { Err(problems.join("\n")) }
    }
}

struct Nodes(Vec<(String, Child)>);

impl Nodes {
    /// Closing a node's stdin asks it to leave cleanly.
    async fn stop(mut self) {
        for (_, child) in &mut self.0 {
            drop(child.stdin.take());
        }
        for (_, child) in &mut self.0 {
            if tokio::time::timeout(Duration::from_secs(15), child.wait()).await.is_err() {
                let _ = child.start_kill();
            }
        }
    }
}

fn refs(control: &Cluster, site: &SiteConfig) -> Vec<(String, KeyedRef<DomainApi>)> {
    (0..site.domains)
        .map(|d| format!("d{d}.site"))
        .map(|d| {
            let r = control.keyed::<DomainApi>(KIND, &d, Bytes::new(), &Placement::default());
            (d, r)
        })
        .collect()
}

async fn statuses(domains: &[(String, KeyedRef<DomainApi>)]) -> Result<BTreeMap<String, Status>, Error> {
    let mut out = BTreeMap::new();
    for (name, domain) in domains {
        let mut pause = Duration::from_millis(20);
        let status = loop {
            match domain.request(Call::Status).await {
                Ok(Answer::Status(s)) => break s,
                Ok(Answer::Added) => return Err("a status call answered Added".into()),
                // A read: safe to repeat whatever went wrong, while the cluster settles after a kill.
                Err(e) if pause < Duration::from_secs(2) => {
                    eprintln!("status of {name}: {e}");
                    tokio::time::sleep(pause).await;
                    pause *= 2;
                }
                Err(e) => return Err(e.into()),
            }
        };
        out.insert(name.clone(), status);
    }
    Ok(out)
}

pub async fn crawl(cfg: &CrawlConfig) -> Result<Report, Error> {
    std::fs::create_dir_all(&cfg.data)?;
    let site = Site::start(cfg.site.clone()).await?;
    let reachable = cfg.site.reachable();
    let seed = SeedServer::bind(SeedServerConfig::new("127.0.0.1:0".parse()?, &crawler::transport("seed"))).await?;
    let seeds = vec![seed.local_addr().to_string()];

    let mut nodes = Nodes(Vec::new());
    for i in 0..cfg.nodes {
        let id = format!("n{i}");
        let log = std::fs::File::create(cfg.data.join(format!("{id}.stderr")))?;
        let child = Command::new(&cfg.exe)
            .args(["node", "--id", &id, "--seeds", &seeds[0], "--site", &site.addr.to_string()])
            .args(["--data", &cfg.data.to_string_lossy(), "--rate", &cfg.rate.to_string()])
            .args(["--per-domain", &cfg.per_domain.to_string(), "--node-fetches", &cfg.node_fetches.to_string()])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(log)
            .kill_on_drop(true)
            .spawn()?;
        nodes.0.push((id, child));
    }
    let node = crawler::start_node(NodeConfig {
        id: "control".into(),
        seeds,
        site: site.addr,
        data: cfg.data.clone(),
        rate: cfg.rate,
        per_domain: cfg.per_domain,
        node_fetches: cfg.node_fetches,
        crawl: false,
        console: cfg.console.map(|(bind, _)| bind),
    })
    .await?;
    let (control_system, control) = (node.system, node.cluster);
    if let Some(console) = node.console {
        eprintln!("console: http://{console}/");
    }
    let crawlers = |view: &axon_cluster::View| view.members.iter().filter(|m| m.record.has_kind(KIND)).count();
    tokio::time::timeout(Duration::from_secs(30), control.wait_view(|v| crawlers(v) == cfg.nodes)).await??;

    let domains = refs(&control, &cfg.site);
    let started = Instant::now();
    for ((_, domain), root) in domains.iter().zip(cfg.site.roots()) {
        crawler::add(domain, vec![root], Duration::from_secs(10)).await?;
    }

    let result = async {
        let mut killed = None;
        if let Some(share) = cfg.kill_at {
            site.wait_hits((reachable.len() as f64 * share) as usize).await;
            let owners = statuses(&domains).await?;
            let owned = |node: &str| owners.values().filter(|s| s.node == node).count();
            let victim = nodes.0.iter().map(|(id, _)| id.clone()).max_by_key(|id| owned(id)).ok_or("no nodes")?;
            let owned = owned(&victim);
            let (_, child) = nodes.0.iter_mut().find(|(id, _)| *id == victim).ok_or("victim is not a child")?;
            child.start_kill()?; // SIGKILL
            child.wait().await?;
            eprintln!("killed {victim}, which owned {owned} domains, after {:?}", started.elapsed());
            killed = Some((victim, owned));
        }
        tokio::time::timeout(cfg.deadline, site.wait_all_served(&reachable)).await?;
        // ponytail: nothing tells the control node that every domain has gone quiet; ask until they all say so.
        let statuses = tokio::time::timeout(cfg.deadline, async {
            loop {
                let now = statuses(&domains).await?;
                let seen: u64 = now.values().map(|s| s.counts.seen).sum();
                if now.values().all(Status::idle) && seen >= reachable.len() as u64 {
                    return Ok::<_, Error>(now);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await??;
        Ok::<_, Error>((killed, statuses))
    }
    .await;
    if let Some((_, hold)) = cfg.console {
        eprintln!("crawl over; the console stays up for {hold:?}");
        tokio::time::sleep(hold).await;
    }
    control_system.shutdown().await?;
    nodes.stop().await;
    let (killed, statuses) = result?;
    Ok(report(&site, &reachable, killed, statuses))
}

fn report(
    site: &Site,
    reachable: &BTreeSet<String>,
    killed: Option<(String, usize)>,
    statuses: BTreeMap<String, Status>,
) -> Report {
    let hits = site.hits();
    let mut answers: HashMap<&str, usize> = HashMap::new();
    let mut requests: HashMap<&str, usize> = HashMap::new();
    let mut by_domain: BTreeMap<String, Vec<Instant>> = BTreeMap::new();
    for hit in &hits {
        *requests.entry(&hit.url).or_default() += 1;
        if hit.status != 503 {
            *answers.entry(&hit.url).or_default() += 1;
        }
        let host = crate::http::split(&hit.url).map_or("", |(h, _)| h);
        by_domain.entry(host.to_owned()).or_default().push(hit.at);
    }
    for times in by_domain.values_mut() {
        times.sort();
    }
    let longest_gap = by_domain
        .iter()
        .map(|(d, times)| (d.clone(), times.windows(2).map(|w| w[1] - w[0]).max().unwrap_or_default()))
        .collect();
    let max_per_second = by_domain
        .into_iter()
        .map(|(domain, times)| {
            let (mut start, mut max) = (0, 0);
            for end in 0..times.len() {
                while times[end] - times[start] >= Duration::from_secs(1) {
                    start += 1;
                }
                max = max.max(end - start + 1);
            }
            (domain, max)
        })
        .collect();
    let first = hits.iter().map(|h| h.at).min();
    let last = hits.iter().map(|h| h.at).max();
    let elapsed = first.zip(last).map_or(Duration::ZERO, |(f, l)| l - f);
    Report {
        reachable: reachable.len(),
        requests: hits.len(),
        elapsed,
        pages_per_sec: hits.len() as f64 / elapsed.as_secs_f64().max(1e-9),
        missing: reachable.iter().filter(|u| !answers.contains_key(u.as_str())).cloned().collect(),
        max_answers: answers.values().copied().max().unwrap_or(0),
        duplicated_urls: answers.values().filter(|n| **n > 1).count(),
        max_requests: requests.values().copied().max().unwrap_or(0),
        max_per_second,
        longest_gap,
        killed,
        statuses,
    }
}
