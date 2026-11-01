//! The services' and the sagas' databases: one SQLite file per service and one for the sagas, standing in for the
//! database each service would own. Every write is fenced by the writer's lease epoch: a write from an owner older
//! than the key's last is refused, so an actor that lost its key cannot overwrite its successor.
//!
//! Calls block; actors make them from `submit_io`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use crate::core::{Hold, Order, Phase, Pool, SERVICES};

pub const SAGAS: &str = "sagas";

#[derive(Debug)]
pub enum StoreError {
    /// A newer owner of the key has written.
    Fenced {
        key: String,
        mine: u64,
        current: u64,
    },
    Db(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fenced { key, mine, current } => write!(f, "{key}: epoch {mine} fenced by {current}"),
            Self::Db(e) => write!(f, "database: {e}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Db(e.to_string())
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        Self::Db(e.to_string())
    }
}

type Result<T> = std::result::Result<T, StoreError>;

/// A saga as its database holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SagaRow {
    pub order: Order,
    pub phase: Phase,
    /// How many times it was taken up again before it ended: by another node, or after a restart.
    pub moves: u64,
}

#[derive(Clone)]
pub struct Store {
    dir: Arc<Path>,
    databases: Arc<[Connections; 4]>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Store({})", self.dir.display())
    }
}

/// Jobs run on tokio's blocking pool, which grows to 512 threads: a connection per thread runs out of file
/// descriptors, so each database lends out at most this many and a job waits for one.
const MAX_CONNECTIONS: usize = 8;

#[derive(Default)]
struct Connections {
    /// Idle connections, and how many are open.
    idle: Mutex<(Vec<Connection>, usize)>,
    returned: Condvar,
}

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS fence (key TEXT PRIMARY KEY, epoch INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS pool (key TEXT PRIMARY KEY, available INTEGER NOT NULL, initial INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS hold (key TEXT NOT NULL, saga TEXT NOT NULL, state TEXT NOT NULL,
        amount INTEGER NOT NULL, PRIMARY KEY (key, saga));
    CREATE TABLE IF NOT EXISTS saga (id TEXT PRIMARY KEY, order_json TEXT NOT NULL, phase_json TEXT NOT NULL,
        moves INTEGER NOT NULL);
";

impl Store {
    pub fn open(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into().into(), databases: Arc::default() }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Creates the databases with every service's pools and their starting amounts, as `(service, key, amount)`.
    pub fn create(&self, pools: &[(&str, String, u64)]) -> Result<()> {
        std::fs::create_dir_all(&self.dir).map_err(|e| StoreError::Db(e.to_string()))?;
        for db in SERVICES.iter().chain([&SAGAS]) {
            self.with(db, |c| Ok(c.execute_batch(SCHEMA)?))?;
        }
        for service in SERVICES {
            self.with(service, |c| {
                let tx = c.transaction()?;
                for (_, key, amount) in pools.iter().filter(|p| p.0 == service) {
                    tx.execute("INSERT OR REPLACE INTO pool VALUES (?1, ?2, ?2)", params![key, *amount as i64])?;
                }
                Ok(tx.commit()?)
            })?;
        }
        Ok(())
    }

    fn with<R>(&self, db: &str, f: impl FnOnce(&mut Connection) -> Result<R>) -> Result<R> {
        let index = SERVICES.iter().chain([&SAGAS]).position(|d| *d == db).expect("a known database");
        let pool = &self.databases[index];
        let mut connection = {
            let mut idle = pool.idle.lock().expect("never poisoned");
            loop {
                if let Some(connection) = idle.0.pop() {
                    break connection;
                }
                if idle.1 < MAX_CONNECTIONS {
                    idle.1 += 1;
                    drop(idle);
                    return match self.connect(db) {
                        Ok(mut connection) => {
                            let out = f(&mut connection);
                            self.give_back(pool, connection);
                            out
                        }
                        Err(e) => {
                            pool.idle.lock().expect("never poisoned").1 -= 1;
                            Err(e)
                        }
                    };
                }
                idle = pool.returned.wait(idle).expect("never poisoned");
            }
        };
        let out = f(&mut connection);
        self.give_back(pool, connection);
        out
    }

    fn give_back(&self, pool: &Connections, connection: Connection) {
        pool.idle.lock().expect("never poisoned").0.push(connection);
        pool.returned.notify_one();
    }

    fn connect(&self, db: &str) -> Result<Connection> {
        let c = Connection::open(self.dir.join(format!("{db}.db")))?;
        c.busy_timeout(Duration::from_secs(10))?;
        // WAL with NORMAL sync survives a killed process; a lost machine may lose the last commits.
        c.pragma_update(None, "journal_mode", "WAL")?;
        c.pragma_update(None, "synchronous", "NORMAL")?;
        Ok(c)
    }

    fn write<R>(&self, db: &str, key: &str, epoch: u64, f: impl FnOnce(&Transaction<'_>) -> Result<R>) -> Result<R> {
        self.with(db, |c| {
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current: Option<i64> =
                tx.query_row("SELECT epoch FROM fence WHERE key = ?1", [key], |r| r.get(0)).optional()?;
            match current.map(|c| c as u64) {
                Some(current) if current > epoch => {
                    return Err(StoreError::Fenced { key: key.into(), mine: epoch, current });
                }
                Some(current) if current == epoch => {}
                _ => drop(tx.execute("INSERT OR REPLACE INTO fence VALUES (?1, ?2)", params![key, epoch as i64])?),
            }
            let out = f(&tx)?;
            tx.commit()?;
            Ok(out)
        })
    }

    /// Takes the pool for `epoch` and reads it. `None`: the service has no such pool.
    pub fn load_pool(&self, service: &str, key: &str, epoch: u64) -> Result<Option<Pool>> {
        self.write(service, key, epoch, |tx| {
            let available: Option<i64> =
                tx.query_row("SELECT available FROM pool WHERE key = ?1", [key], |r| r.get(0)).optional()?;
            let Some(available) = available else { return Ok(None) };
            let mut holds = BTreeMap::new();
            let mut rows = tx.prepare("SELECT saga, state, amount FROM hold WHERE key = ?1")?;
            let mut rows = rows.query([key])?;
            while let Some(row) = rows.next()? {
                holds.insert(row.get(0)?, hold_from(&row.get::<_, String>(1)?, row.get(2)?)?);
            }
            Ok(Some(Pool { available: available as u64, holds }))
        })
    }

    /// Writes the pool's amount and the holds that changed.
    pub fn commit_pool(
        &self,
        service: &str,
        key: &str,
        epoch: u64,
        available: u64,
        holds: &[(String, Hold)],
    ) -> Result<()> {
        self.write(service, key, epoch, |tx| {
            tx.execute("UPDATE pool SET available = ?2 WHERE key = ?1", params![key, available as i64])?;
            let mut put = tx.prepare_cached("INSERT OR REPLACE INTO hold VALUES (?1, ?2, ?3, ?4)")?;
            for (saga, hold) in holds {
                let (state, amount) = hold_to(*hold);
                put.execute(params![key, saga, state, amount as i64])?;
            }
            Ok(())
        })
    }

    /// Takes the saga for `epoch` and reads it. `None`: it was never submitted.
    pub fn load_saga(&self, id: &str, epoch: u64) -> Result<Option<SagaRow>> {
        self.write(SAGAS, id, epoch, |tx| {
            let found = tx
                .query_row("SELECT order_json, phase_json, moves FROM saga WHERE id = ?1", [id], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))
                })
                .optional()?;
            let Some((order, phase, moves)) = found else { return Ok(None) };
            let (phase, mut moves): (Phase, u64) = (serde_json::from_str(&phase)?, moves as u64);
            if !phase.terminal() {
                moves += 1;
                tx.execute("UPDATE saga SET moves = ?2 WHERE id = ?1", params![id, moves as i64])?;
            }
            Ok(Some(SagaRow { order: serde_json::from_str(&order)?, phase, moves }))
        })
    }

    /// Stores a submitted saga at its first phase.
    pub fn create_saga(&self, order: &Order, epoch: u64) -> Result<()> {
        self.write(SAGAS, &order.id, epoch, |tx| {
            let (json, phase) = (serde_json::to_string(order)?, serde_json::to_string(&Phase::Forward(0))?);
            tx.execute("INSERT INTO saga VALUES (?1, ?2, ?3, 0)", params![order.id, json, phase])?;
            Ok(())
        })
    }

    pub fn save_saga(&self, id: &str, epoch: u64, phase: &Phase) -> Result<()> {
        self.write(SAGAS, id, epoch, |tx| {
            let json = serde_json::to_string(phase)?;
            tx.execute("UPDATE saga SET phase_json = ?2 WHERE id = ?1", params![id, json])?;
            Ok(())
        })
    }

    pub fn sagas(&self) -> Result<BTreeMap<String, SagaRow>> {
        self.with(SAGAS, |c| {
            let mut query = c.prepare("SELECT order_json, phase_json, moves FROM saga")?;
            let mut rows = query.query([])?;
            let mut out = BTreeMap::new();
            while let Some(r) = rows.next()? {
                let order: Order = serde_json::from_str(&r.get::<_, String>(0)?)?;
                let phase = serde_json::from_str(&r.get::<_, String>(1)?)?;
                let row = SagaRow { order, phase, moves: r.get::<_, i64>(2)? as u64 };
                out.insert(row.order.id.clone(), row);
            }
            Ok(out)
        })
    }

    /// Every pool of `service`: its starting amount and what it holds now.
    pub fn pools(&self, service: &str) -> Result<BTreeMap<String, (u64, Pool)>> {
        self.with(service, |c| {
            let mut out = BTreeMap::new();
            let mut query = c.prepare("SELECT key, available, initial FROM pool")?;
            let mut rows = query.query([])?;
            while let Some(r) = rows.next()? {
                let pool = Pool { available: r.get::<_, i64>(1)? as u64, ..Default::default() };
                out.insert(r.get::<_, String>(0)?, (r.get::<_, i64>(2)? as u64, pool));
            }
            let mut query = c.prepare("SELECT key, saga, state, amount FROM hold")?;
            let mut rows = query.query([])?;
            while let Some(r) = rows.next()? {
                let key: String = r.get(0)?;
                let hold = hold_from(&r.get::<_, String>(2)?, r.get(3)?)?;
                let pool = out.get_mut(&key).ok_or_else(|| StoreError::Db(format!("a hold on unknown pool {key}")))?;
                pool.1.holds.insert(r.get(1)?, hold);
            }
            Ok(out)
        })
    }
}

fn hold_to(hold: Hold) -> (&'static str, u64) {
    match hold {
        Hold::Applied(amount) => ("applied", amount),
        Hold::Refused => ("refused", 0),
        Hold::Undone => ("undone", 0),
    }
}

fn hold_from(state: &str, amount: i64) -> Result<Hold> {
    match state {
        "applied" => Ok(Hold::Applied(amount as u64)),
        "refused" => Ok(Hold::Refused),
        "undone" => Ok(Hold::Undone),
        other => Err(StoreError::Db(format!("unknown hold state {other}"))),
    }
}
