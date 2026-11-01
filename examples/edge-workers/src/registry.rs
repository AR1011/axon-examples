//! The control plane's state: workers, their immutable versions and which of them take the traffic.
//!
//! [`State`] is the pure part: it checks a change, turns it into [`Event`]s and applies them. The [`Registry`] actor
//! writes each change's events to its log, and the bundle and source they name to files of their own, before it
//! applies them, publishes the worker's manifest and answers; so a restart replays the log into the same state.
//! [`Bundles`] hands the stored bundles out in pieces.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use axon::files::{FileOp, Files};
use axon::{Actor, Context, JobExecutor, MailboxConfig, MailboxLimits};
use axon_cluster::{CallError, Reply};
use axon_stream::{CompactLimits, DeliveryPolicy, StreamNode};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::engine::{Engine, Env, Failure, Limits, Objects, Request, guarded, submit};
use crate::store;
use crate::wire::{
    Answer, Call, Canary, Deployment, Fetch, Hash, MAX_BINDINGS, MAX_BUNDLE, MAX_ENV, MAX_NAME, MAX_PREVIEW_BODY,
    MAX_SOURCE, MAX_VERSIONS, MAX_WORKERS, Manifest, PIECE_HEADER, Piece, PreviewAsk, Previewed, Refused, Upload,
    VersionDetail, VersionInfo, VersionRef, WorkerInfo, answer, manifests,
};

const LOG: &str = "registry.log";
const REPUBLISH: Duration = Duration::from_secs(1);

/// What the manifest stream may hold: every worker's record at its longest.
pub fn compact_limits() -> CompactLimits {
    CompactLimits { max_keys: MAX_WORKERS, max_bytes: 800 << 10 }
}

fn blob(hash: &Hash) -> String {
    format!("blob.{}", hash.as_str())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRecord {
    pub info: VersionInfo,
    pub source: Hash,
    pub vars: Env,
    pub secrets: Env,
    pub objects: Objects,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[expect(clippy::large_enum_variant, reason = "an event lives for one write to the log, never in a collection")]
pub enum Event {
    /// Makes the worker too, when the name is new.
    Version {
        name: String,
        version: VersionRecord,
    },
    Deploy {
        name: String,
        deployment: Deployment,
    },
    Delete {
        name: String,
    },
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Worker {
    versions: BTreeMap<u32, VersionRecord>,
    latest: u32,
    deployment: Option<Deployment>,
}

/// An upload checked against the state, with what it keeps of the version it builds on filled in.
#[derive(Debug, PartialEq, Eq)]
pub struct Plan {
    /// The version it kept from, `None` for a new worker.
    pub base: Option<u32>,
    pub source: Source,
    pub vars: Env,
    pub secrets: Env,
    pub objects: Objects,
    pub limits: Limits,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Source {
    Given(String),
    /// The latest version's, to be read back from its file.
    Kept(Hash),
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct State {
    workers: BTreeMap<String, Worker>,
    /// The last deployment made, of any worker.
    seq: u64,
    /// Deleted and not made again: their removal is published again at every start, in case a crash came between
    /// the log and the stream.
    removed: BTreeSet<String>,
}

fn invalid<T>(why: impl Into<String>) -> Result<T, Refused> {
    Err(Refused::Invalid(why.into()))
}

fn valid_name(name: &str) -> Result<(), Refused> {
    let ok = (1..=MAX_NAME).contains(&name.len())
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-');
    if ok { Ok(()) } else { invalid(format!("a name is 1 to {MAX_NAME} of a-z, 0-9 and -, with no - at an end")) }
}

impl State {
    /// The version an upload keeps what it does not change from: the one that takes the traffic, so a change of
    /// one variable after a rollback does not bring back the source rolled back from. The latest, before any
    /// deployment.
    pub fn base(&self, name: &str) -> Option<&VersionRecord> {
        let worker = self.workers.get(name)?;
        worker.versions.get(&worker.deployment.as_ref().map_or(worker.latest, |d| d.stable))
    }

    pub fn plan(&self, upload: &Upload) -> Result<Plan, Refused> {
        valid_name(&upload.name)?;
        let latest = self.base(&upload.name);
        if latest.is_none() && self.workers.len() >= MAX_WORKERS {
            return invalid(format!("at most {MAX_WORKERS} workers"));
        }
        let source = match (&upload.source, latest) {
            (Some(source), _) if source.len() > MAX_SOURCE => {
                return invalid(format!("a source over {MAX_SOURCE} bytes"));
            }
            (Some(source), _) => Source::Given(source.clone()),
            (None, Some(latest)) => Source::Kept(latest.source.clone()),
            (None, None) => return Err(Refused::NotFound),
        };
        let mut vars = upload.vars.clone().or_else(|| latest.map(|v| v.vars.clone())).unwrap_or_default();
        let mut secrets = latest.map(|v| v.secrets.clone()).unwrap_or_default();
        let change = |to: &mut Env, from: &mut Env, changes: &BTreeMap<String, Option<String>>| {
            for (key, value) in changes {
                match value {
                    Some(value) => {
                        from.remove(key);
                        to.insert(key.clone(), value.clone());
                    }
                    None => drop(to.remove(key)),
                }
            }
        };
        change(&mut vars, &mut secrets, &upload.set);
        change(&mut secrets, &mut vars, &upload.secrets);
        if let Some(key) = vars.keys().find(|key| secrets.contains_key(*key)) {
            return invalid(format!("{key} is both a variable and a secret"));
        }
        if vars.keys().chain(secrets.keys()).any(String::is_empty) {
            return invalid("a variable with no name");
        }
        if vars.iter().chain(&secrets).map(|(k, v)| k.len() + v.len()).sum::<usize>() > MAX_ENV {
            return invalid(format!("variables and secrets over {MAX_ENV} bytes"));
        }
        let objects = upload.objects.clone().or_else(|| latest.map(|v| v.objects.clone())).unwrap_or_default();
        // A class's name is part of an object's key, where `/` separates.
        let word = |word: &str| {
            let rest = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '$';
            (1..=64).contains(&word.len()) && word.chars().all(rest) && !word.starts_with(|c: char| c.is_ascii_digit())
        };
        if objects.len() > MAX_BINDINGS || !objects.iter().all(|(name, class)| word(name) && word(class)) {
            return invalid(format!(
                "at most {MAX_BINDINGS} object bindings, each a name and a class of 64 characters"
            ));
        }
        if let Some(name) = objects.keys().find(|name| vars.contains_key(*name) || secrets.contains_key(*name)) {
            return invalid(format!("{name} is both an object binding and a variable"));
        }
        let limits = upload.limits.or_else(|| latest.map(|v| v.info.limits)).unwrap_or_default();
        Ok(Plan { base: latest.map(|v| v.info.id), source, vars, secrets, objects, limits })
    }

    /// Whether a plan made before its build still holds: the worker is as new, or builds on the same version,
    /// as when it was made. A removal, a deployment or a 513th worker may have come between.
    pub fn stands(&self, name: &str, plan: &Plan) -> bool {
        let base = self.base(name).map(|version| version.info.id);
        base == plan.base && (base.is_some() || self.workers.len() < MAX_WORKERS)
    }

    /// The id the worker's next version takes.
    pub fn next_version(&self, name: &str) -> u32 {
        self.workers.get(name).map_or(1, |f| f.latest + 1)
    }

    /// The seq the next deployment takes.
    pub fn next_seq(&self) -> u64 {
        self.seq + 1
    }

    pub fn deploy(&self, name: &str, stable: u32, canary: Option<(u32, u8)>) -> Result<Event, Refused> {
        let worker = self.workers.get(name).ok_or(Refused::NotFound)?;
        for id in [Some(stable), canary.map(|c| c.0)].into_iter().flatten() {
            if !worker.versions.contains_key(&id) {
                return Err(Refused::NoSuchVersion(id));
            }
        }
        match canary {
            Some((id, _)) if id == stable => return invalid("the canary is the stable version"),
            Some((_, percent)) if !(1..=99).contains(&percent) => return invalid("a canary takes 1 to 99 percent"),
            _ => {}
        }
        Ok(Event::Deploy { name: name.into(), deployment: Deployment { seq: self.next_seq(), stable, canary } })
    }

    pub fn delete(&self, name: &str) -> Result<Event, Refused> {
        self.workers.get(name).map(|_| Event::Delete { name: name.into() }).ok_or(Refused::NotFound)
    }

    pub fn apply(&mut self, event: &Event) {
        match event {
            Event::Version { name, version } => {
                self.removed.remove(name);
                let worker = self.workers.entry(name.clone()).or_default();
                worker.latest = worker.latest.max(version.info.id);
                worker.versions.insert(version.info.id, version.clone());
                let deployed = |f: &Worker, id| {
                    f.deployment.as_ref().is_some_and(|d| d.stable == id || d.canary.is_some_and(|c| c.0 == id))
                };
                while worker.versions.len() > MAX_VERSIONS {
                    let oldest = worker.versions.keys().copied().find(|id| !deployed(worker, *id));
                    match oldest {
                        Some(id) => drop(worker.versions.remove(&id)),
                        None => break,
                    }
                }
            }
            Event::Deploy { name, deployment } => {
                self.seq = self.seq.max(deployment.seq);
                if let Some(worker) = self.workers.get_mut(name) {
                    worker.deployment = Some(deployment.clone());
                }
            }
            Event::Delete { name } => {
                self.workers.remove(name);
                self.removed.insert(name.clone());
                // Removals are said again at every start; past this many names the lowest is forgotten.
                while self.removed.len() > MAX_WORKERS {
                    self.removed.pop_first();
                }
            }
        }
    }

    /// What the worker's nodes are to serve, once it has a deployment.
    pub fn manifest(&self, name: &str) -> Option<Manifest> {
        let worker = self.workers.get(name)?;
        let deployment = worker.deployment.as_ref()?;
        let version = |id: u32| {
            let info = &worker.versions.get(&id)?.info;
            Some(VersionRef { id, bundle: info.bundle.clone(), size: info.size, limits: info.limits })
        };
        let canary = match deployment.canary {
            Some((id, percent)) => Some(Canary { version: version(id)?, percent }),
            None => None,
        };
        Some(Manifest { name: name.into(), seq: deployment.seq, stable: version(deployment.stable)?, canary })
    }

    pub fn info(&self, name: &str) -> Option<WorkerInfo> {
        let worker = self.workers.get(name)?;
        Some(WorkerInfo { name: name.into(), deployment: worker.deployment.clone(), latest: worker.latest })
    }

    pub fn list(&self) -> Vec<WorkerInfo> {
        self.workers.keys().filter_map(|name| self.info(name)).collect()
    }

    /// Newest first.
    pub fn versions(&self, name: &str) -> Vec<VersionInfo> {
        self.workers.get(name).map_or_else(Vec::new, |f| f.versions.values().rev().map(|v| v.info.clone()).collect())
    }

    pub fn version(&self, name: &str, id: u32) -> Result<&VersionRecord, Refused> {
        self.workers.get(name).ok_or(Refused::NotFound)?.versions.get(&id).ok_or(Refused::NoSuchVersion(id))
    }

    /// Every name the stream should say something about: the workers, and the removed.
    fn published(&self) -> impl Iterator<Item = &String> {
        self.workers.keys().chain(&self.removed)
    }
}

type ReplyTo = Option<Reply<Answer>>;

pub enum Msg {
    Call(Call, ReplyTo),
    Loaded(Result<Box<Loaded>, String>),
    /// The kept source of an upload that brought none.
    Sourced(String, Result<String, String>),
    Built(String, Box<Result<Built, Failure>>),
    Committed(Result<(), String>),
    Detail(u64, Result<VersionDetail, String>),
    Previewed(u64, Box<Result<Previewed, Refused>>),
    Republish,
}

pub struct Loaded {
    state: State,
    /// Each file's length.
    files: BTreeMap<String, u64>,
    /// The log's length where a crash tore its tail.
    torn: Option<u64>,
}

pub struct Built {
    source: String,
    artifact: Bytes,
    bundle: Hash,
    source_hash: Hash,
}

/// An upload between its call and its commit.
struct Build {
    plan: Plan,
    message: String,
    deploy: bool,
    reply: ReplyTo,
}

enum Change {
    Version(String, Box<(Build, Built)>),
    Deploy { name: String, stable: u32, canary: Option<(u32, u8)>, reply: ReplyTo },
    Delete { name: String, reply: ReplyTo },
}

struct Commit {
    events: Vec<Event>,
    written: Vec<(String, u64)>,
    reply: ReplyTo,
    answer: Answer,
}

pub struct Registry<E> {
    engine: E,
    /// Where the engine compiles and previews: `InvokerConfig::runtime`'s.
    jobs: Option<JobExecutor>,
    files: Arc<dyn Files>,
    streams: StreamNode,
    state: State,
    stored: BTreeMap<String, u64>,
    torn: Option<u64>,
    /// One upload of a worker at a time: the next builds on what this one makes.
    builds: BTreeMap<String, Build>,
    /// The write out. The actor is paused for it, so a change, or a build that finished, waits in the mailbox.
    committing: Option<Commit>,
    details: BTreeMap<u64, ReplyTo>,
    next_detail: u64,
    /// Workers whose manifest the stream refused; tried again every second.
    unpublished: BTreeSet<String>,
    republishing: bool,
}

impl<E: Engine + Clone> Registry<E> {
    pub fn new(engine: E, runtime: Option<tokio::runtime::Handle>, files: Arc<dyn Files>, streams: StreamNode) -> Self {
        Self {
            engine,
            jobs: runtime.map(JobExecutor::tokio),
            files,
            streams,
            state: State::default(),
            stored: BTreeMap::new(),
            torn: None,
            builds: BTreeMap::new(),
            committing: None,
            details: BTreeMap::new(),
            next_detail: 0,
            unpublished: BTreeSet::new(),
            republishing: false,
        }
    }

    /// The mailbox this actor needs: a call weighed with the source it carries, so the calls that wait hold
    /// megabytes, not a source each.
    pub fn mailbox() -> MailboxConfig<Msg> {
        MailboxConfig::from(MailboxLimits::default()).with_weight(|msg: &Msg| {
            size_of::<Msg>()
                + match msg {
                    Msg::Call(Call::Upload(upload), _) => upload.source.as_ref().map_or(0, String::len),
                    Msg::Call(Call::Preview(ask), _) => ask.source.len() + ask.body.len(),
                    Msg::Sourced(_, Ok(source)) => source.len(),
                    Msg::Built(_, built) => match &**built {
                        Ok(built) => built.source.len() + built.artifact.len(),
                        Err(_) => 0,
                    },
                    _ => 0,
                }
        })
    }

    fn upload(&mut self, upload: Upload, reply: ReplyTo, cx: &mut Context<'_, Self>) {
        if self.builds.contains_key(&upload.name) {
            return answer(reply, Answer::Refused(Refused::Busy));
        }
        let mut plan = match self.state.plan(&upload) {
            Ok(plan) => plan,
            Err(refused) => return answer(reply, Answer::Refused(refused)),
        };
        let name = upload.name;
        let source = std::mem::replace(&mut plan.source, Source::Given(String::new()));
        self.builds.insert(name.clone(), Build { plan, message: upload.message, deploy: upload.deploy, reply });
        match source {
            Source::Given(source) => self.build(name, source, cx),
            Source::Kept(hash) => {
                let (files, job_name) = (self.files.clone(), name.clone());
                let read = async move { Msg::Sourced(job_name, read_source(&*files, &blob(&hash)).await) };
                if cx.submit_future(read).is_err() {
                    self.refuse(&name, Refused::Busy);
                }
            }
        }
    }

    fn refuse(&mut self, name: &str, refused: Refused) {
        if let Some(build) = self.builds.remove(name) {
            answer(build.reply, Answer::Refused(refused));
        }
    }

    /// The engine prepares the version off this thread: a second or more of compiling on the real one.
    fn build(&mut self, name: String, source: String, cx: &mut Context<'_, Self>) {
        let Some(build) = self.builds.get(&name) else { return };
        let mut env = build.plan.vars.clone();
        env.extend(build.plan.secrets.clone());
        let objects = build.plan.objects.clone();
        let (engine, job_name) = (self.engine.clone(), name.clone());
        let job = async move {
            let build = async move {
                let version = engine.deploy_bound(&source, &env, &objects).await?;
                let artifact = engine.artifact(&version);
                let (bundle, source_hash) = (Hash::of(&artifact), Hash::of(source.as_bytes()));
                Ok::<_, Failure>(Built { bundle, source_hash, artifact, source })
            };
            // A compiler that panics on a source fails that upload, not the registry.
            Msg::Built(job_name, Box::new(guarded(build).await.and_then(|built| built)))
        };
        if submit(cx, self.jobs.as_ref(), job).is_err() {
            self.refuse(&name, Refused::Busy);
        }
    }

    /// The file ops that store `bytes` under `name`, none when it is there already.
    fn keep(&self, name: String, bytes: Bytes, ops: &mut Vec<FileOp>, written: &mut Vec<(String, u64)>) {
        if self.stored.get(&name) != Some(&(bytes.len() as u64)) && !written.iter().any(|(n, _)| *n == name) {
            written.push((name.clone(), bytes.len() as u64));
            ops.push(FileOp::Replace(name, bytes));
        }
    }

    /// Starts the change's write. None is out: the actor is paused for one until its result is handled.
    fn commit(&mut self, change: Change, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (mut ops, mut written) = (Vec::new(), Vec::new());
        let (events, reply, done) = match change {
            Change::Version(name, parts) => {
                let (build, built) = *parts;
                // The plan is as old as the build: what it kept may be of a version that is gone.
                if !self.state.stands(&name, &build.plan) {
                    answer(build.reply, Answer::Refused(Refused::Busy));
                    return Ok(());
                }
                let id = self.state.next_version(&name);
                let info = VersionInfo {
                    id,
                    created_ms: axon::trace::unix_micros() / 1000,
                    message: build.message,
                    bundle: built.bundle.clone(),
                    size: built.artifact.len() as u64,
                    source_bytes: built.source.len() as u64,
                    limits: build.plan.limits,
                };
                self.keep(blob(&built.bundle), built.artifact, &mut ops, &mut written);
                self.keep(blob(&built.source_hash), built.source.into(), &mut ops, &mut written);
                let version = VersionRecord {
                    info,
                    source: built.source_hash,
                    vars: build.plan.vars,
                    secrets: build.plan.secrets,
                    objects: build.plan.objects,
                };
                let mut events = vec![Event::Version { name: name.clone(), version }];
                let seq = build.deploy.then(|| self.state.next_seq());
                if let Some(seq) = seq {
                    let deployment = Deployment { seq, stable: id, canary: None };
                    events.push(Event::Deploy { name, deployment });
                }
                (events, build.reply, Answer::Uploaded { version: id, seq })
            }
            Change::Deploy { name, stable, canary, reply } => match self.state.deploy(&name, stable, canary) {
                Ok(event) => (vec![event], reply, Answer::Deployed { seq: self.state.next_seq() }),
                Err(refused) => {
                    answer(reply, Answer::Refused(refused));
                    return Ok(());
                }
            },
            Change::Delete { name, reply } => match self.state.delete(&name) {
                Ok(event) => (vec![event], reply, Answer::Deleted),
                Err(refused) => {
                    answer(reply, Answer::Refused(refused));
                    return Ok(());
                }
            },
        };
        if let Some(good) = self.torn {
            ops.push(FileOp::Truncate(LOG.into(), good));
        }
        let records: Vec<Bytes> =
            events.iter().map(|e| store::frame(&serde_json::to_vec(e).expect("an event is plain data"))).collect();
        ops.push(FileOp::Append(LOG.into(), records.concat().into()));
        // Called in the job: a `Files` may write when `apply` is called, as `MemoryFiles` does, and a commit whose
        // job cannot start is answered `Busy`, so it must not land.
        let files = self.files.clone();
        match cx.submit_future(async move { Msg::Committed(files.apply(ops).await.map_err(|e| e.to_string())) }) {
            Ok(job) => {
                // Calls wait in the mailbox for this write, so each is checked against the state it changes.
                cx.pause_until(&job).map_err(|e| format!("pausing for a write: {e:?}"))?;
                self.committing = Some(Commit { events, written, reply, answer: done });
            }
            Err(_) => answer(reply, Answer::Refused(Refused::Busy)),
        }
        Ok(())
    }

    fn publish(&mut self, name: &str, cx: &mut Context<'_, Self>) {
        let record = match self.state.manifest(name) {
            Some(manifest) => manifest.record(),
            None if self.state.workers.contains_key(name) => return,
            None => Manifest::removal(name),
        };
        if self.streams.publish(&manifests(), vec![record]).is_err() {
            self.unpublished.insert(name.to_owned());
            if !self.republishing {
                self.republishing = cx.schedule_after(REPUBLISH, Msg::Republish).is_ok();
            }
        }
    }

    fn detail(&mut self, name: &str, id: u32, reply: ReplyTo, cx: &mut Context<'_, Self>) {
        let version = match self.state.version(name, id) {
            Ok(version) => version,
            Err(refused) => return answer(reply, Answer::Refused(refused)),
        };
        let mut detail = VersionDetail {
            info: version.info.clone(),
            source: String::new(),
            vars: version.vars.clone(),
            secrets: version.secrets.keys().cloned().collect(),
            objects: version.objects.clone(),
        };
        let (files, source, ticket) = (self.files.clone(), blob(&version.source), self.hold(reply));
        let read = async move {
            let read = read_source(&*files, &source).await.map(|source| {
                detail.source = source;
                detail
            });
            Msg::Detail(ticket, read)
        };
        if cx.submit_future(read).is_err() {
            answer(self.details.remove(&ticket).flatten(), Answer::Refused(Refused::Busy));
        }
    }

    /// Keeps `reply` for a job's answer, under the ticket the job carries.
    fn hold(&mut self, reply: ReplyTo) -> u64 {
        let ticket = self.next_detail;
        self.next_detail += 1;
        self.details.insert(ticket, reply);
        ticket
    }

    /// Prepares the source and runs the one request, both off this thread. Nothing of it is kept.
    fn preview(&mut self, ask: PreviewAsk, reply: ReplyTo, cx: &mut Context<'_, Self>) {
        if ask.source.len() > MAX_SOURCE || ask.body.len() > MAX_PREVIEW_BODY {
            let why = format!("a source over {MAX_SOURCE} bytes, or a body over {MAX_PREVIEW_BODY}");
            return answer(reply, Answer::Refused(Refused::Invalid(why)));
        }
        let base = self.state.base(&ask.name);
        let limits = base.map_or_else(Limits::default, |version| version.info.limits);
        // Not its secrets: the code is whatever the caller sent, and its answer goes back to them.
        let mut env: Env = base.map(|version| version.vars.clone()).unwrap_or_default();
        env.extend(ask.env);
        // Its classes are checked, and its bindings are there; with no host, a call of one is an error in the script.
        let objects = base.map(|version| version.objects.clone()).unwrap_or_default();
        let (engine, ticket) = (self.engine.clone(), self.hold(reply));
        let job = async move {
            let run = async move {
                let version = engine.deploy_bound(&ask.source, &env, &objects).await.map_err(refused)?;
                let request = Request { method: ask.method, url: ask.url, headers: ask.headers, body: ask.body.into() };
                let (status, headers, body, usage, error) = match engine.invoke(&version, request, limits).await {
                    Ok(ran) => (ran.status, ran.headers, ran.body, ran.usage, None),
                    Err(failed) => {
                        let why = failed.failure.to_string();
                        (failed.failure.status(), Vec::new(), Bytes::new(), failed.usage, Some(why))
                    }
                };
                let shown = &body[..body.len().min(MAX_PREVIEW_BODY)];
                Ok(Previewed {
                    status,
                    headers,
                    body: String::from_utf8_lossy(shown).into_owned(),
                    body_truncated: shown.len() < body.len(),
                    logs: usage.logs,
                    cpu_us: usage.cpu.as_micros() as u64,
                    wall_us: usage.wall.as_micros() as u64,
                    error,
                })
            };
            let ran = guarded(run).await.unwrap_or_else(|failure| Err(Refused::Load(failure.to_string())));
            Msg::Previewed(ticket, Box::new(ran))
        };
        if submit(cx, self.jobs.as_ref(), job).is_err() {
            answer(self.details.remove(&ticket).flatten(), Answer::Refused(Refused::Busy));
        }
    }
}

async fn read_source(files: &dyn Files, name: &str) -> Result<String, String> {
    let bytes = files.read(name).await.map_err(|e| e.to_string())?;
    String::from_utf8(bytes).map_err(|e| e.to_string())
}

fn refused(failure: Failure) -> Refused {
    match failure {
        Failure::Load(why) => Refused::Load(why),
        other => Refused::Load(other.to_string()),
    }
}

/// Reads the log and opens the manifest stream, both from files: a job, not the handler.
async fn load(files: Arc<dyn Files>, streams: StreamNode) -> Result<Box<Loaded>, String> {
    let listed: BTreeMap<String, u64> =
        files.list().await.map_err(|e| format!("listing the registry: {e}"))?.into_iter().collect();
    let log = store::read(&*files, LOG).await.map_err(|e| format!("reading {LOG}: {e}"))?.unwrap_or_default();
    let (records, good) = store::frames(&log);
    let mut state = State::default();
    for record in records {
        let event =
            serde_json::from_slice(record).map_err(|e| format!("{LOG} holds a record this build cannot read: {e}"))?;
        state.apply(&event);
    }
    // The generation the disk holds resumes the stream. A control node that lost that disk starts another, which
    // every follower takes from its first record: the one before would be ahead of this node's.
    let generation = streams.stored(&manifests()).map(|cursor| cursor.generation);
    let generation = generation.unwrap_or_else(|| axon::trace::unix_micros() / 1000);
    streams
        .open_stream(&manifests(), generation, DeliveryPolicy::Compacted(compact_limits()))
        .await
        .map_err(|e| format!("opening the manifest stream: {e}"))?;
    let torn = (good < log.len()).then_some(good as u64);
    Ok(Box::new(Loaded { state, files: listed, torn }))
}

impl<E: Engine + Clone> Actor for Registry<E> {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let load = load(self.files.clone(), self.streams.clone());
        let job = cx.submit_future(async move { Msg::Loaded(load.await) }).map_err(|e| format!("{:?}", e.reason))?;
        // Part of the start: `ready()` returns once the log is read and the stream open, or fails with why not.
        cx.pause_until(&job).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Loaded(loaded) => {
                let loaded = loaded?;
                (self.state, self.stored, self.torn) = (loaded.state, loaded.files, loaded.torn);
                // A crash between a change's write and its publish left the stream behind the log. Followers take
                // a manifest only over a lower seq, so saying everything again changes nothing for the others.
                for name in self.state.published().cloned().collect::<Vec<_>>() {
                    self.publish(&name, cx);
                }
            }
            Msg::Call(Call::Upload(upload), reply) => self.upload(upload, reply, cx),
            Msg::Call(Call::Deploy { name, stable, canary }, reply) => {
                self.commit(Change::Deploy { name, stable, canary, reply }, cx)?;
            }
            Msg::Call(Call::Delete { name }, reply) => self.commit(Change::Delete { name, reply }, cx)?,
            Msg::Call(Call::List, reply) => answer(reply, Answer::Workers(self.state.list())),
            Msg::Call(Call::Get { name }, reply) => answer(
                reply,
                match self.state.info(&name) {
                    Some(info) => Answer::Worker(info, self.state.versions(&name)),
                    None => Answer::Refused(Refused::NotFound),
                },
            ),
            Msg::Call(Call::Version { name, id }, reply) => self.detail(&name, id, reply, cx),
            Msg::Call(Call::Preview(ask), reply) => self.preview(ask, reply, cx),
            Msg::Previewed(ticket, previewed) => {
                let reply = self.details.remove(&ticket).flatten();
                answer(reply, previewed.map_or_else(Answer::Refused, Answer::Previewed));
            }
            Msg::Sourced(name, Ok(source)) => self.build(name, source, cx),
            Msg::Sourced(name, Err(e)) => self.refuse(&name, Refused::Storage(e)),
            Msg::Built(name, built) => match *built {
                Ok(built) if built.artifact.len() as u64 > MAX_BUNDLE => {
                    self.refuse(&name, Refused::Invalid(format!("a bundle over {MAX_BUNDLE} bytes")));
                }
                Ok(built) => {
                    if let Some(build) = self.builds.remove(&name) {
                        self.commit(Change::Version(name, Box::new((build, built))), cx)?;
                    }
                }
                Err(failure) => self.refuse(&name, refused(failure)),
            },
            Msg::Committed(result) => {
                let Some(commit) = self.committing.take() else { return Ok(()) };
                match result {
                    Ok(()) => {
                        self.torn = None;
                        self.stored.extend(commit.written);
                        let mut names = BTreeSet::new();
                        for event in &commit.events {
                            self.state.apply(event);
                            let (Event::Version { name, .. } | Event::Deploy { name, .. } | Event::Delete { name }) =
                                event;
                            names.insert(name.clone());
                        }
                        for name in names {
                            self.publish(&name, cx);
                        }
                        answer(commit.reply, commit.answer);
                    }
                    Err(e) => {
                        // A removal may have happened before the write failed: they are written again when next named.
                        for (name, _) in &commit.written {
                            self.stored.remove(name);
                        }
                        answer(commit.reply, Answer::Refused(Refused::Storage(e)));
                    }
                }
            }
            Msg::Detail(ticket, detail) => {
                let reply = self.details.remove(&ticket).flatten();
                answer(reply, detail.map_or_else(|e| Answer::Refused(Refused::Storage(e)), Answer::Version));
            }
            Msg::Republish => {
                self.republishing = false;
                for name in std::mem::take(&mut self.unpublished) {
                    self.publish(&name, cx);
                }
            }
        }
        Ok(())
    }
}

pub enum BundleMsg {
    Fetch(Fetch, Option<Reply<Piece>>),
    Sent(Result<(), CallError>),
}

/// The pieces of bundles, each read from the registry's files as the caller takes it.
pub struct Bundles {
    files: Arc<dyn Files>,
}

impl Bundles {
    pub fn new(files: Arc<dyn Files>) -> Self {
        Self { files }
    }
}

/// The bundle from `fetch.offset` on, in pieces of `size` bytes: `Missing` alone for one that is not here.
fn pieces(
    files: Arc<dyn Files>,
    fetch: Fetch,
    size: usize,
) -> impl futures_util::Stream<Item = Result<Piece, CallError>> + Send + 'static {
    let name = blob(&fetch.hash);
    futures_util::stream::unfold(Some((fetch.offset, None)), move |at| {
        let (files, name) = (files.clone(), name.clone());
        async move {
            let (offset, total) = at?;
            let total = match total {
                Some(total) => Some(total),
                None => files.list().await.ok()?.into_iter().find(|(file, _)| *file == name).map(|(_, len)| len),
            };
            let Some(total) = total else { return Some((Ok(Piece::Missing), None)) };
            if offset >= total {
                return None;
            }
            match files.read_range(&name, offset, size).await {
                Ok(bytes) if !bytes.is_empty() => {
                    let next = offset + bytes.len() as u64;
                    Some((Ok(Piece::Chunk { total, bytes: bytes.into() }), Some((next, Some(total)))))
                }
                _ => Some((Ok(Piece::Missing), None)),
            }
        }
    })
}

impl Actor for Bundles {
    type Message = BundleMsg;
    type Error = String;

    fn on_message(&mut self, msg: BundleMsg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            BundleMsg::Fetch(_, None) | BundleMsg::Sent(_) => {}
            BundleMsg::Fetch(fetch, Some(reply)) => {
                let size = reply.max_payload().saturating_sub(PIECE_HEADER).min(fetch.max as usize).max(1);
                let sending = reply.send_parts(pieces(self.files.clone(), fetch, size));
                // A reply that cannot start is dropped: the caller sees NoReply and fetches again later.
                drop(cx.submit_future(async move { BundleMsg::Sent(sending.await) }));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upload(name: &str, source: Option<&str>) -> Upload {
        Upload { name: name.into(), source: source.map(String::from), ..Upload::default() }
    }

    /// Plans the upload and applies the version it makes, as the actor does once the engine and the disk are done.
    fn add(state: &mut State, upload: &Upload) -> Result<Event, Refused> {
        let plan = state.plan(upload)?;
        let source = match plan.source {
            Source::Given(source) => Hash::of(source.as_bytes()),
            Source::Kept(hash) => hash,
        };
        let info = VersionInfo {
            id: state.next_version(&upload.name),
            created_ms: 0,
            message: upload.message.clone(),
            bundle: source.clone(),
            size: 1,
            source_bytes: 1,
            limits: plan.limits,
        };
        let version = VersionRecord { info, source, vars: plan.vars, secrets: plan.secrets, objects: plan.objects };
        let event = Event::Version { name: upload.name.clone(), version };
        state.apply(&event);
        Ok(event)
    }

    fn deploy(state: &mut State, name: &str, stable: u32, canary: Option<(u32, u8)>) -> Result<Event, Refused> {
        let event = state.deploy(name, stable, canary)?;
        state.apply(&event);
        Ok(event)
    }

    #[test]
    fn a_name_is_a_host_label_and_a_new_worker_needs_a_source() {
        let state = State::default();
        for bad in ["", "-a", "a-", "Hello", "a.b", "a/b", &"a".repeat(64)] {
            assert!(matches!(state.plan(&upload(bad, Some("x"))), Err(Refused::Invalid(_))), "{bad:?}");
        }
        for good in ["a", "hello-world-2", &"a".repeat(63)] {
            assert!(state.plan(&upload(good, Some("x"))).is_ok(), "{good:?}");
        }
        assert_eq!(state.plan(&upload("hello", None)), Err(Refused::NotFound), "nothing to keep a source from");
        let big = "x".repeat(MAX_SOURCE + 1);
        assert!(matches!(state.plan(&upload("hello", Some(&big))), Err(Refused::Invalid(_))));
    }

    #[test]
    fn a_version_keeps_what_the_upload_does_not_change() {
        let mut state = State::default();
        let limits = Limits { subrequests: 3, ..Limits::default() };
        let first = Upload {
            vars: Some(Env::from([("A".into(), "1".into())])),
            secrets: BTreeMap::from([("S".into(), Some("s".into())), ("T".into(), Some("t".into()))]),
            limits: Some(limits),
            ..upload("hello", Some("one"))
        };
        add(&mut state, &first).unwrap();
        // A change of one secret alone: the source, the variables, the other secret and the limits stay.
        let second = Upload {
            secrets: BTreeMap::from([("T".into(), None), ("U".into(), Some("u".into()))]),
            ..upload("hello", None)
        };
        let plan = state.plan(&second).unwrap();
        assert_eq!(plan.source, Source::Kept(Hash::of(b"one")));
        assert_eq!(plan.vars, Env::from([("A".into(), "1".into())]));
        assert_eq!(plan.secrets, Env::from([("S".into(), "s".into()), ("U".into(), "u".into())]));
        assert_eq!(plan.limits, limits);
        // New variables replace the old ones whole.
        let third = Upload { vars: Some(Env::new()), ..upload("hello", Some("two")) };
        assert_eq!(state.plan(&third).unwrap().vars, Env::new());

        let clash = Upload { vars: Some(Env::from([("S".into(), "x".into())])), ..upload("hello", None) };
        assert!(matches!(state.plan(&clash), Err(Refused::Invalid(why)) if why.contains("both")));
        // One variable set or removed leaves the others, and a name set is a variable or a secret from then on.
        let one = |key: &str, value: Option<&str>| BTreeMap::from([(key.to_owned(), value.map(String::from))]);
        add(&mut state, &Upload { set: one("B", Some("2")), ..upload("hello", None) }).unwrap();
        let plan = state.plan(&Upload { set: one("S", Some("plain")), ..upload("hello", None) }).unwrap();
        assert_eq!(plan.vars.keys().collect::<Vec<_>>(), ["A", "B", "S"]);
        assert_eq!(plan.secrets, Env::from([("T".into(), "t".into())]));
        let plan = state.plan(&Upload { secrets: one("A", Some("hidden")), ..upload("hello", None) }).unwrap();
        assert_eq!((plan.vars, plan.secrets.len()), (Env::from([("B".into(), "2".into())]), 3));
        assert_eq!(state.plan(&Upload { set: one("A", None), ..upload("hello", None) }).unwrap().vars.len(), 1);
        let heavy = Upload { vars: Some(Env::from([("A".into(), "x".repeat(MAX_ENV))])), ..upload("hello", None) };
        assert!(matches!(state.plan(&heavy), Err(Refused::Invalid(_))));
    }

    #[test]
    fn after_a_rollback_an_upload_builds_on_the_version_that_takes_the_traffic() {
        let mut state = State::default();
        let first = Upload { vars: Some(Env::from([("A".into(), "1".into())])), ..upload("hello", Some("one")) };
        add(&mut state, &first).unwrap();
        let second = Upload { vars: Some(Env::from([("A".into(), "2".into())])), ..upload("hello", Some("two")) };
        add(&mut state, &second).unwrap();
        assert_eq!(state.plan(&upload("hello", None)).unwrap().source, Source::Kept(Hash::of(b"two")), "the latest");
        deploy(&mut state, "hello", 2, None).unwrap();
        deploy(&mut state, "hello", 1, None).unwrap();
        let secret = Upload { secrets: BTreeMap::from([("S".into(), Some("s".into()))]), ..upload("hello", None) };
        let plan = state.plan(&secret).unwrap();
        assert_eq!((plan.source, plan.vars), (Source::Kept(Hash::of(b"one")), Env::from([("A".into(), "1".into())])));
    }

    #[test]
    fn a_plan_stands_only_while_the_worker_is_as_it_was_planned_on() {
        let mut state = State::default();
        let new = state.plan(&upload("hello", Some("one"))).unwrap();
        assert!(state.stands("hello", &new));
        add(&mut state, &upload("hello", Some("one"))).unwrap();
        assert!(!state.stands("hello", &new), "the name was made meanwhile");
        add(&mut state, &upload("hello", Some("two"))).unwrap();
        deploy(&mut state, "hello", 2, None).unwrap();

        let kept = state.plan(&upload("hello", None)).unwrap();
        assert!(state.stands("hello", &kept));
        deploy(&mut state, "hello", 1, None).unwrap();
        assert!(!state.stands("hello", &kept), "a rollback: the secrets and source it kept are not the ones served");
        let kept = state.plan(&upload("hello", None)).unwrap();
        let delete = state.delete("hello").unwrap();
        state.apply(&delete);
        assert!(!state.stands("hello", &kept), "a removal: the upload would make the worker again");

        for i in 0..MAX_WORKERS - 1 {
            add(&mut state, &upload(&format!("f{i}"), Some("x"))).unwrap();
        }
        let last = state.plan(&upload("last", Some("x"))).unwrap();
        add(&mut state, &upload("other", Some("x"))).unwrap();
        assert!(!state.stands("last", &last), "another new worker took the last place");
    }

    #[test]
    fn a_deployment_names_versions_that_exist_and_every_one_takes_the_next_seq() {
        let mut state = State::default();
        assert_eq!(state.deploy("hello", 1, None), Err(Refused::NotFound));
        add(&mut state, &upload("hello", Some("one"))).unwrap();
        add(&mut state, &upload("hello", Some("two"))).unwrap();
        add(&mut state, &upload("other", Some("three"))).unwrap();
        assert_eq!(state.manifest("hello"), None, "uploaded is not deployed");
        assert_eq!(state.deploy("hello", 3, None), Err(Refused::NoSuchVersion(3)));
        assert_eq!(state.deploy("hello", 1, Some((4, 10))), Err(Refused::NoSuchVersion(4)));
        for canary in [(2, 0), (2, 100), (1, 10)] {
            assert!(matches!(state.deploy("hello", 1, Some(canary)), Err(Refused::Invalid(_))), "{canary:?}");
        }

        deploy(&mut state, "hello", 2, None).unwrap();
        deploy(&mut state, "other", 1, None).unwrap();
        // A rollback is a later deployment of an older version, with a canary on the newer.
        deploy(&mut state, "hello", 1, Some((2, 25))).unwrap();
        let manifest = state.manifest("hello").unwrap();
        assert_eq!((manifest.seq, manifest.stable.id), (3, 1));
        assert_eq!(manifest.stable.bundle, Hash::of(b"one"));
        let canary = manifest.canary.unwrap();
        assert_eq!((canary.version.id, canary.percent, &canary.version.bundle), (2, 25, &Hash::of(b"two")));
        assert_eq!(state.manifest("other").unwrap().seq, 2);
        assert_eq!(state.info("hello").unwrap().latest, 2);
        assert_eq!(state.versions("hello").iter().map(|v| v.id).collect::<Vec<_>>(), vec![2, 1], "newest first");
    }

    #[test]
    fn the_log_replays_into_the_same_state() {
        let mut state = State::default();
        let mut log = vec![
            add(&mut state, &upload("hello", Some("one"))).unwrap(),
            deploy(&mut state, "hello", 1, None).unwrap(),
            add(&mut state, &upload("gone", Some("two"))).unwrap(),
            deploy(&mut state, "gone", 1, None).unwrap(),
        ];
        let delete = state.delete("gone").unwrap();
        state.apply(&delete);
        log.push(delete);
        log.push(add(&mut state, &upload("hello", Some("three"))).unwrap());
        log.push(deploy(&mut state, "hello", 2, Some((1, 5))).unwrap());

        // Through the bytes the actor writes, and with the last record torn off as a crash leaves it.
        let written: Vec<u8> =
            log.iter().flat_map(|e| store::frame(&serde_json::to_vec(e).unwrap()).to_vec()).collect();
        let replay = |data: &[u8]| {
            let mut state = State::default();
            for record in store::frames(data).0 {
                state.apply(&serde_json::from_slice(record).unwrap());
            }
            state
        };
        assert_eq!(replay(&written), state);
        assert_eq!(
            replay(&written[..written.len() - 1]).manifest("hello").unwrap().seq,
            1,
            "the torn deployment is not made"
        );

        assert_eq!(state.delete("gone"), Err(Refused::NotFound));
        assert_eq!(state.published().collect::<Vec<_>>(), vec!["hello", "gone"], "a removed name is still said");
        // A deployment after the removal goes on from the seq before it, and a name made again starts at version 1.
        assert_eq!(state.next_seq(), 4);
        add(&mut state, &upload("gone", Some("four"))).unwrap();
        assert_eq!((state.info("gone").unwrap().latest, state.published().count()), (1, 2));
    }

    #[test]
    fn old_versions_go_but_never_a_deployed_one() {
        let mut state = State::default();
        add(&mut state, &upload("hello", Some("0"))).unwrap();
        deploy(&mut state, "hello", 1, None).unwrap();
        for i in 1..MAX_VERSIONS + 20 {
            add(&mut state, &upload("hello", Some(&i.to_string()))).unwrap();
        }
        let kept = state.versions("hello");
        assert_eq!(kept.len(), MAX_VERSIONS);
        assert_eq!((kept[0].id, kept[kept.len() - 1].id), ((MAX_VERSIONS + 20) as u32, 1));
        assert_eq!(state.version("hello", 2).err(), Some(Refused::NoSuchVersion(2)));
        assert!(state.manifest("hello").is_some());
    }
}
