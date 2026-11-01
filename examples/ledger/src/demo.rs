use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

use axon_cluster::{Cluster, MemoryDiscovery};
use tokio::task::JoinSet;

use crate::account::FENCED;
use crate::audit::{Audit, audit};
use crate::{Answer, Client, Command, KIND, Outcome, Result, Settings, store};

pub const HOSTS: [&str; 3] = ["ledger-a", "ledger-b", "ledger-c"];
const VICTIM: usize = 1;
const GHOST: &str = "acct-ghost";
const WORKERS: usize = 64;
const SEED: u64 = 1000;

#[derive(Debug)]
pub struct Report {
    pub accounts: usize,
    pub commands: usize,
    pub load: Duration,
    /// Commands answered per second over the whole load, the crash included, and before it.
    pub rate: f64,
    pub rate_before_crash: f64,
    /// The accounts that were on the crashed node, and how long after the crash each answered again.
    pub moved: usize,
    pub unavailable_median: Duration,
    pub unavailable_max: Duration,
    /// How long after the restart the last of them answered from the restarted node again.
    pub back_max: Duration,
    pub retries: u64,
    pub resent: u64,
    /// Writes refused because another owner had written since: in this process, over the whole run.
    pub fenced: u64,
    pub audit: Audit,
}

/// Random commands from `WORKERS` clients, each retried until answered, one in ten sent again once answered.
#[derive(Default)]
pub struct Load {
    next: AtomicUsize,
    pub done: AtomicUsize,
    pub retries: AtomicU64,
    pub resent: AtomicU64,
    deposits: AtomicU64,
    withdrawals: AtomicU64,
    settled: AtomicUsize,
    refunded: AtomicUsize,
}

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

pub fn account(i: usize) -> String {
    format!("acct-{i:04}")
}

impl Load {
    pub fn start(
        front: &Cluster,
        accounts: usize,
        commands: usize,
        patience: Duration,
    ) -> (Arc<Load>, JoinSet<Result<()>>) {
        let (load, mut workers) = (Arc::new(Load::default()), JoinSet::new());
        for w in 0..WORKERS as u64 {
            let (client, load) = (Client::new(front), load.clone());
            workers.spawn(load.work(client, commands, accounts as u64, 0x9E37_79B9_7F4A_7C15 ^ (w + 1), patience));
        }
        (load, workers)
    }

    async fn work(
        self: Arc<Self>,
        mut client: Client,
        n: usize,
        accounts: u64,
        mut rng: u64,
        patience: Duration,
    ) -> Result<()> {
        loop {
            let i = self.next.fetch_add(1, Relaxed);
            if i >= n {
                return Ok(());
            }
            let (r, from, to, amount) = (
                xorshift(&mut rng) % 100,
                xorshift(&mut rng) % accounts,
                xorshift(&mut rng),
                xorshift(&mut rng) % 50 + 1,
            );
            let to = account(((from + 1 + to % (accounts - 1)) % accounts) as usize);
            let (from, key) = (account(from as usize), format!("c{i}"));
            let command = match r {
                0..4 => Command::Deposit { key, amount },
                4..8 => Command::Withdraw { key, amount },
                8 => Command::Transfer { tid: key, to: GHOST.into(), amount },
                _ => Command::Transfer { tid: key, to, amount },
            };
            let (answer, _, attempts) = client.call(&from, command.clone(), patience).await?;
            self.retries.fetch_add(u64::from(attempts - 1), Relaxed);
            if r % 10 == 9 {
                // As if the answer had been lost: the same command again must change nothing.
                let (again, _, _) = client.call(&from, command.clone(), patience).await?;
                if again.outcome != answer.outcome {
                    return Err(format!("{command:?} answered {:?}, then {:?}", answer.outcome, again.outcome).into());
                }
                self.resent.fetch_add(1, Relaxed);
            }
            match (&command, answer.outcome) {
                (Command::Deposit { .. }, Outcome::Ok) => drop(self.deposits.fetch_add(amount, Relaxed)),
                (Command::Withdraw { .. }, Outcome::Ok) => drop(self.withdrawals.fetch_add(amount, Relaxed)),
                (Command::Transfer { .. }, Outcome::Ok) => drop(self.settled.fetch_add(1, Relaxed)),
                (Command::Transfer { .. }, Outcome::NoAccount) => drop(self.refunded.fetch_add(1, Relaxed)),
                (_, Outcome::Ok | Outcome::Overdraft) => {}
                (_, outcome) => return Err(format!("{command:?} answered {outcome:?}").into()),
            }
            self.done.fetch_add(1, Relaxed);
        }
    }

    /// The logs against what the clients were told, once every node has settled.
    pub fn check(&self, dir: &Path, accounts: usize, balances: &BTreeMap<String, i64>) -> Result<Audit> {
        let audit = audit(&store::cut(dir)?)?;
        let told = (
            accounts as u64 * SEED + self.deposits.load(Relaxed),
            self.withdrawals.load(Relaxed),
            self.settled.load(Relaxed),
            self.refunded.load(Relaxed),
        );
        let found = (audit.deposits, audit.withdrawals, audit.settled, audit.refunded);
        if found != told || audit.pending != 0 || audit.in_flight != 0 {
            return Err(format!("the logs say {found:?}, the clients were told {told:?}: {audit:?}").into());
        }
        if audit.balance != *balances || audit.accounts != accounts {
            return Err("the actors' balances differ from the logs'".into());
        }
        Ok(audit)
    }
}

/// Opens and funds every account; answers where each one runs.
pub async fn open(front: &Cluster, accounts: usize, patience: Duration) -> Result<Vec<(String, String)>> {
    let mut opening = JoinSet::new();
    for id in (0..accounts).map(account) {
        let mut client = Client::new(front);
        opening.spawn(async move {
            client.call(&id, Command::Open { key: "open".into() }, patience).await?;
            let seed = Command::Deposit { key: "seed".into(), amount: SEED };
            match client.call(&id, seed, patience).await? {
                (answer, node, _) if answer.outcome == Outcome::Ok => Ok::<_, crate::Error>((id, node)),
                (answer, ..) => Err(format!("{id}: seed deposit {:?}", answer.outcome).into()),
            }
        });
    }
    let mut placed = Vec::new();
    while let Some(opened) = opening.join_next().await {
        placed.push(opened??);
    }
    Ok(placed)
}

/// Touches every account until none owes a transfer: a started account sends what it still owes. Answers the
/// balances.
pub async fn settle(front: &Cluster, accounts: usize, patience: Duration) -> Result<BTreeMap<String, i64>> {
    let (mut client, mut balances) = (Client::new(front), BTreeMap::new());
    let deadline = Instant::now() + patience;
    for id in (0..accounts).map(account).chain([GHOST.to_owned()]) {
        let balance = balance_until(&mut client, &id, deadline, patience, |answer, _| match answer.pending {
            0 => Ok(answer.balance),
            owed => Err(format!("{id} still owes {owed} transfers")),
        })
        .await?;
        balances.insert(id, balance);
    }
    Ok(balances)
}

/// Asks `id` its balance every 20 ms until `ready` takes the answer and the node that gave it, or `deadline` has
/// passed: then fails with what `ready` said last.
async fn balance_until<T>(
    client: &mut Client,
    id: &str,
    deadline: Instant,
    patience: Duration,
    ready: impl Fn(&Answer, &str) -> Result<T, String>,
) -> Result<T> {
    loop {
        let (answer, node, _) = client.call(id, Command::Balance, patience).await?;
        match ready(&answer, &node) {
            Ok(done) => return Ok(done),
            Err(not_yet) if Instant::now() > deadline => return Err(not_yet.into()),
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
}

/// How long after `since` each account answers, from `on` if given.
async fn probe(
    front: &Cluster,
    ids: &[String],
    since: Instant,
    on: Option<&str>,
    patience: Duration,
) -> Result<Vec<Duration>> {
    let mut probes = JoinSet::new();
    for id in ids {
        let (mut client, on, id) = (Client::new(front), on.map(str::to_owned), id.clone());
        probes.spawn(async move {
            let deadline = Instant::now() + patience;
            balance_until(&mut client, &id, deadline, patience, |_, node| match &on {
                Some(on) if on != node => Err(format!("{id} still on {node}")),
                _ => Ok(since.elapsed()),
            })
            .await
        });
    }
    let mut times = Vec::new();
    while let Some(probed) = probes.join_next().await {
        times.push(probed??);
    }
    times.sort();
    Ok(times)
}

/// Three hosts and a client node in one process, over one directory. Accounts are opened and funded, then the load
/// runs; a host crashes a third of the way in and comes back once its accounts answer elsewhere. The logs are
/// audited right after the crash and at the end.
pub async fn demo(dir: &Path, accounts: usize, commands: usize, lease: Duration) -> Result<Report> {
    let started = Instant::now();
    let say = |what: String| println!("{:>6.2}s  {what}", started.elapsed().as_secs_f64());
    let settings = Settings { dir: dir.to_owned(), ..Settings::new(HOSTS.map(String::from).to_vec(), lease) };
    let discovery = MemoryDiscovery::new();
    let mut hosts = Vec::new();
    for id in HOSTS {
        hosts.push(Some(settings.start(settings.transport(id)?, discovery.clone()).await?));
    }
    let front = settings.start(settings.transport("client")?, discovery.clone()).await?;
    front.cluster.wait_view(|v| v.members.iter().filter(|m| m.record.has_kind(KIND)).count() >= HOSTS.len()).await?;
    say(format!("3 hosts and a client up, lease {lease:?}, logs in {}", dir.display()));

    let patience = lease * 5 + Duration::from_secs(10);
    let on_victim: Vec<String> = open(&front.cluster, accounts, patience)
        .await?
        .into_iter()
        .filter_map(|(id, node)| (node == HOSTS[VICTIM]).then_some(id))
        .collect();
    say(format!("{accounts} accounts open with {SEED} each, {} of them on {}", on_victim.len(), HOSTS[VICTIM]));

    let fenced = FENCED.load(Relaxed);
    let (load, mut workers) = Load::start(&front.cluster, accounts, commands, patience);
    let loading = Instant::now();
    while load.done.load(Relaxed) < commands / 3 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let rate_before_crash = load.done.load(Relaxed) as f64 / loading.elapsed().as_secs_f64();
    drop(hosts[VICTIM].take()); // no shutdown: a crash
    let crashed = Instant::now();
    say(format!("{} crashed after {} commands, {rate_before_crash:.0}/s", HOSTS[VICTIM], load.done.load(Relaxed)));

    let cut = audit(&store::cut(dir)?)?;
    say(format!(
        "logs hold together mid-crash: {} debited and not yet credited, {} in flight",
        cut.pending, cut.in_flight
    ));

    let away = probe(&front.cluster, &on_victim, crashed, None, patience).await?;
    let (unavailable_median, unavailable_max) = (away[away.len() / 2], away[away.len() - 1]);
    say(format!(
        "its {} accounts answer again elsewhere: median {unavailable_median:.1?}, max {unavailable_max:.1?}",
        away.len()
    ));

    let s = &settings;
    hosts[VICTIM] = Some(s.start(s.transport(HOSTS[VICTIM])?, discovery.clone()).await?);
    let restarted = Instant::now();
    say(format!("{} back, as a new incarnation", HOSTS[VICTIM]));
    let back = probe(&front.cluster, &on_victim, restarted, Some(HOSTS[VICTIM]), patience).await?;
    let back_max = back[back.len() - 1];
    say(format!("its accounts are back on it, the last after {back_max:.1?}"));

    while let Some(done) = workers.join_next().await {
        done??;
    }
    let elapsed = loading.elapsed();
    let rate = commands as f64 / elapsed.as_secs_f64();
    let (retries, resent) = (load.retries.load(Relaxed), load.resent.load(Relaxed));
    say(format!("{commands} commands answered in {elapsed:.1?}: {rate:.0}/s, {retries} retries, {resent} sent twice"));

    let balances = settle(&front.cluster, accounts, patience).await?;
    say("every transfer settled".into());
    for node in [Some(front)].into_iter().chain(hosts).flatten() {
        node.system.shutdown().await?;
    }
    let audit = load.check(dir, accounts, &balances)?;
    say(format!(
        "audit passed: {} records, {} owners, {} bytes fenced, {} settled, {} refunded, total {} = deposits {} - withdrawals {}",
        audit.records,
        audit.owners,
        audit.fenced_bytes,
        audit.settled,
        audit.refunded,
        audit.balances,
        audit.deposits,
        audit.withdrawals
    ));
    Ok(Report {
        accounts,
        commands,
        load: elapsed,
        rate,
        rate_before_crash,
        moved: on_victim.len(),
        unavailable_median,
        unavailable_max,
        back_max,
        retries,
        resent,
        fenced: FENCED.load(Relaxed) - fenced,
        audit,
    })
}
