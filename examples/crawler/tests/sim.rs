//! The crawl protocol on the seeded network simulator: domain frontiers (the app's own `Frontier`) on three nodes,
//! links handed between them over a network that loses, duplicates and reorders messages, and one node crashing
//! part way. Every reachable URL must be fetched at least once and at most twice, and no domain faster than its
//! interval, in virtual time.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Duration;

use axon_testkit::{NodeId, SimConfig, Simulator, check_seeds};
use crawler::frontier::{Frontier, Next};
use crawler::http::split;
use crawler::site::SiteConfig;

const INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone)]
enum Wire {
    Add { id: u64, domain: String, urls: Vec<String> },
    Ack { id: u64 },
}

/// A domain's log: what survives a crash.
#[derive(Default)]
struct Log {
    discovered: Vec<String>,
    finished: HashSet<String>,
}

struct Page {
    pending: crawler::frontier::Pending,
    waiting: BTreeSet<u64>,
    same_domain: Vec<String>,
}

struct Handover {
    domain: String,
    urls: Vec<String>,
    page: String,
}

#[derive(Default)]
struct Node {
    domains: BTreeMap<String, Frontier>,
    handovers: BTreeMap<u64, Handover>,
    pages: BTreeMap<String, Page>,
}

struct World {
    net: Simulator<Wire>,
    site: SiteConfig,
    ids: Vec<NodeId>,
    alive: Vec<bool>,
    nodes: Vec<Node>,
    logs: BTreeMap<String, Log>,
    fetches: Vec<(Duration, String)>,
    next_id: u64,
}

fn score(domain: &str, node: usize) -> u64 {
    domain
        .bytes()
        .chain([0, node as u8])
        .fold(0xcbf2_9ce4_8422_2325, |h, b| (h ^ b as u64).wrapping_mul(0x0100_0000_01b3))
}

fn parse(url: &str) -> (String, usize) {
    let (host, path) = split(url).unwrap();
    (host.to_owned(), path.trim_start_matches("/p").parse().unwrap())
}

impl World {
    fn new(seed: u64, site: SiteConfig) -> Self {
        let net = Simulator::new(SimConfig {
            seed,
            jitter: Duration::from_millis(20),
            drop_every: 8,
            duplicate_every: 8,
            ordered: false,
            ..Default::default()
        });
        let mut world = Self {
            net,
            site,
            ids: Vec::new(),
            alive: Vec::new(),
            nodes: Vec::new(),
            logs: BTreeMap::new(),
            fetches: Vec::new(),
            next_id: 0,
        };
        for i in 0..3 {
            world.ids.push(world.net.add_node(&format!("n{i}")).unwrap());
            world.alive.push(true);
            world.nodes.push(Node::default());
        }
        for root in world.site.roots() {
            let (domain, _) = parse(&root);
            let owner = world.owner(&domain);
            world.add(owner, &domain, vec![root]);
        }
        world
    }

    fn owner(&self, domain: &str) -> usize {
        (0..self.nodes.len()).filter(|n| self.alive[*n]).max_by_key(|n| score(domain, *n)).unwrap()
    }

    /// The domain's frontier on `node`, read back from its log the first time this node runs it.
    fn frontier(&mut self, node: usize, domain: &str) -> &mut Frontier {
        let now = self.net.now();
        let log = self.logs.entry(domain.to_owned()).or_default();
        self.nodes[node].domains.entry(domain.to_owned()).or_insert_with(|| {
            let mut f = Frontier::new(INTERVAL, 2, 10_000);
            f.restore(log.discovered.clone(), &log.finished, now);
            f
        })
    }

    /// Logs the new URLs before queueing them, as the actor does before it acknowledges.
    fn add(&mut self, node: usize, domain: &str, urls: Vec<String>) {
        let new = self.frontier(node, domain).discover(urls);
        self.logs.get_mut(domain).unwrap().discovered.extend(new.iter().cloned());
        self.frontier(node, domain).logged(new);
    }

    fn pump(&mut self, node: usize) {
        let domains: Vec<String> = self.nodes[node].domains.keys().cloned().collect();
        for domain in domains {
            let now = self.net.now();
            while let Next::Fetch(pending) = self.frontier(node, &domain).next(now) {
                self.fetch(node, &domain, pending);
            }
        }
    }

    fn fetch(&mut self, node: usize, domain: &str, pending: crawler::frontier::Pending) {
        self.fetches.push((self.net.now(), pending.url.clone()));
        let (_, page) = parse(&pending.url);
        let mut by_domain: BTreeMap<String, Vec<String>> = BTreeMap::new();
        if page < self.site.pages {
            let d: usize = domain.trim_start_matches('d').trim_end_matches(".site").parse().unwrap();
            for (ld, lp) in self.site.links(d, page) {
                let link = crawler::site::url(ld, lp);
                by_domain.entry(parse(&link).0).or_default().push(link);
            }
        }
        let same_domain = by_domain.remove(domain).unwrap_or_default();
        let url = pending.url.clone();
        let mut waiting = BTreeSet::new();
        for (to, urls) in by_domain {
            self.next_id += 1;
            waiting.insert(self.next_id);
            self.nodes[node].handovers.insert(self.next_id, Handover { domain: to, urls, page: url.clone() });
            self.send_handover(node, self.next_id);
        }
        self.nodes[node].pages.insert(url.clone(), Page { pending, waiting, same_domain });
        self.maybe_finish(node, domain, &url);
    }

    fn send_handover(&mut self, node: usize, id: u64) {
        let h = &self.nodes[node].handovers[&id];
        let (domain, urls) = (h.domain.clone(), h.urls.clone());
        let to = self.ids[self.owner(&domain)];
        self.net.send(self.ids[node], to, Wire::Add { id, domain, urls }).unwrap();
    }

    /// A page is finished once every domain it links to has logged its links.
    fn maybe_finish(&mut self, node: usize, domain: &str, url: &str) {
        if !self.nodes[node].pages[url].waiting.is_empty() {
            return;
        }
        let page = self.nodes[node].pages.remove(url).unwrap();
        self.add(node, domain, page.same_domain);
        self.logs.get_mut(domain).unwrap().finished.insert(url.to_owned());
        let f = self.frontier(node, domain);
        assert!(f.returned(page.pending, false));
        f.finished();
    }

    fn deliver(&mut self, to: usize, from: usize, wire: Wire) {
        match wire {
            Wire::Add { id, domain, urls } => {
                if self.owner(&domain) != to {
                    return; // not ours in this view: no ack, so the sender tries the owner again
                }
                self.add(to, &domain, urls);
                self.net.send(self.ids[to], self.ids[from], Wire::Ack { id }).unwrap();
            }
            Wire::Ack { id } => {
                let Some(h) = self.nodes[to].handovers.remove(&id) else { return }; // a duplicate
                let (domain, _) = parse(&h.page);
                if let Some(page) = self.nodes[to].pages.get_mut(&h.page) {
                    page.waiting.remove(&id);
                    self.maybe_finish(to, &domain, &h.page);
                }
            }
        }
    }

    /// Loses everything the node held in memory. The survivors start the logged domains they now own.
    fn crash(&mut self, node: usize) {
        self.net.crash_node(self.ids[node]).unwrap();
        self.alive[node] = false;
        self.nodes[node] = Node::default();
        let domains: Vec<String> = self.logs.keys().cloned().collect();
        for domain in domains {
            let owner = self.owner(&domain);
            self.frontier(owner, &domain);
        }
    }

    fn run(&mut self, crash_after: usize, victim: usize) {
        for n in 0..self.nodes.len() {
            self.pump(n);
        }
        for _ in 0..1_000_000 {
            if self.alive[victim] && self.fetches.len() >= crash_after {
                self.crash(victim);
            }
            if let Some(d) = self.net.step() {
                let (to, from) = (
                    self.ids.iter().position(|i| *i == d.to).unwrap(),
                    self.ids.iter().position(|i| *i == d.from).unwrap(),
                );
                if self.alive[to] {
                    self.deliver(to, from, d.payload);
                    self.pump(to);
                }
                continue;
            }
            // Quiet network: hand-overs still unacknowledged were lost, so send them again.
            let unacked: Vec<(usize, u64)> = (0..self.nodes.len())
                .filter(|n| self.alive[*n])
                .flat_map(|n| self.nodes[n].handovers.keys().map(move |id| (n, *id)))
                .collect();
            if !unacked.is_empty() {
                for (n, id) in unacked {
                    self.send_handover(n, id);
                }
                continue;
            }
            let now = self.net.now();
            let wake = self.nodes.iter().flat_map(|n| n.domains.values()).filter_map(|f| f.wake_at()).min();
            match wake {
                Some(at) => {
                    self.net.advance_idle_to(at.max(now)).unwrap();
                    for n in 0..self.nodes.len() {
                        if self.alive[n] {
                            self.pump(n);
                        }
                    }
                }
                None => return,
            }
        }
        panic!("the crawl did not end");
    }
}

fn crawl(seed: u64) -> World {
    let site = SiteConfig { domains: 5, pages: 30, links: 4, seed, flaky_every: 0, ..SiteConfig::default() };
    let mut world = World::new(seed, site);
    let reachable = world.site.reachable().len();
    world.run(reachable / 5 + seed as usize % (reachable / 2), (seed % 3) as usize);
    world
}

#[test]
fn every_url_is_fetched_once_or_twice_across_a_crash_on_a_lossy_network() {
    let twice = std::sync::atomic::AtomicUsize::new(0);
    check_seeds(0..200, |seed| {
        let world = crawl(seed);
        assert!(!world.alive.iter().all(|a| *a), "a node crashed");
        let mut per_url: BTreeMap<&str, usize> = BTreeMap::new();
        let mut per_domain: BTreeMap<String, Vec<Duration>> = BTreeMap::new();
        for (at, url) in &world.fetches {
            *per_url.entry(url).or_default() += 1;
            per_domain.entry(parse(url).0).or_default().push(*at);
        }
        let reachable = world.site.reachable();
        for url in &reachable {
            let n = per_url.get(url.as_str()).copied().unwrap_or(0);
            assert!((1..=2).contains(&n), "{url} fetched {n} times");
            twice.fetch_add(usize::from(n == 2), std::sync::atomic::Ordering::Relaxed);
        }
        assert_eq!(per_url.len(), reachable.len(), "nothing unreachable was fetched");
        for (domain, times) in per_domain {
            for w in times.windows(2) {
                assert!(w[1] - w[0] >= INTERVAL, "{domain} fetched at {:?} and {:?}", w[0], w[1]);
            }
        }
        for (domain, log) in &world.logs {
            assert_eq!(log.discovered.len(), log.finished.len(), "{domain} ended with work left");
        }
    })
    .unwrap();
    assert!(twice.into_inner() > 0, "some crash caught a fetch in flight, or the bound is not exercised");
}

#[test]
fn the_same_seed_gives_the_same_crawl() {
    assert_eq!(crawl(7).fetches, crawl(7).fetches);
}
