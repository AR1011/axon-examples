//! Each owner of a symbol appends the commands it accepts to a log named by its lease epoch:
//! `<dir>/<symbol>/<epoch>.log`, one JSON command per line. A new owner seals every older log where it read it
//! (`<epoch>.seal` holds the length), before it writes, and every reader stops at a seal. An old owner that wakes
//! from a pause and writes on is therefore ignored, and since it acks only after its write, and is fenced before it
//! handles the write's result, nothing it acks after the seal reaches a client.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::book::Command;

pub struct Loaded {
    /// Every command of the older logs, in order.
    pub commands: Vec<Command>,
    pub log: Arc<File>,
}

fn epoch_of(path: &Path, ext: &str) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    name.strip_suffix(ext)?.strip_suffix('.')?.parse().ok()
}

/// The logs of `symbol` with an epoch below `below`, oldest first.
fn logs(dir: &Path, below: u64) -> std::io::Result<Vec<(u64, PathBuf)>> {
    let mut logs: Vec<_> = fs::read_dir(dir)?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter_map(|path| epoch_of(&path, "log").map(|epoch| (epoch, path)))
        .filter(|(epoch, _)| *epoch < below)
        .collect();
    logs.sort();
    Ok(logs)
}

fn parse(bytes: &[u8], into: &mut Vec<Command>) -> Result<(), String> {
    for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        into.push(serde_json::from_slice(line).map_err(|e| format!("journal line: {e}"))?);
    }
    Ok(())
}

/// Reads a log up to its seal, or to its last whole line when `seal` is set and it has none yet, sealing it there.
fn read_log(path: &Path, seal: bool, into: &mut Vec<Command>) -> Result<(), String> {
    let bytes = fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let seal_path = path.with_extension("seal");
    let end = match fs::read_to_string(&seal_path) {
        Ok(len) => len.trim().parse::<usize>().map_err(|e| format!("seal {}: {e}", seal_path.display()))?,
        Err(_) => {
            // A write cut short by a crash leaves a partial last line: it was never acked.
            let end = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
            if seal {
                let tmp = path.with_extension("seal.tmp");
                fs::write(&tmp, end.to_string())
                    .and_then(|()| fs::rename(&tmp, &seal_path))
                    .map_err(|e| e.to_string())?;
            }
            end
        }
    };
    parse(&bytes[..end.min(bytes.len())], into)
}

/// Seals and reads every older log of `symbol`, then opens this owner's own.
pub fn load(dir: &Path, symbol: &str, epoch: u64) -> Result<Loaded, String> {
    let dir = dir.join(symbol);
    fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    if logs(&dir, u64::MAX).map_err(|e| e.to_string())?.iter().any(|(e, _)| *e > epoch) {
        return Err(format!("a newer owner than epoch {epoch} has written {symbol}"));
    }
    let mut commands = Vec::new();
    for (_, path) in logs(&dir, epoch).map_err(|e| e.to_string())? {
        read_log(&path, true, &mut commands)?;
    }
    let path = dir.join(format!("{epoch}.log"));
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| format!("open {}: {e}", path.display()))?;
    Ok(Loaded { commands, log: Arc::new(log) })
}

/// Everything the journal of `symbol` holds, as a new owner would replay it plus the current owner's log. Read-only.
pub fn read_all(dir: &Path, symbol: &str) -> Result<Vec<Command>, String> {
    let mut commands = Vec::new();
    for (_, path) in logs(&dir.join(symbol), u64::MAX).map_err(|e| e.to_string())? {
        read_log(&path, false, &mut commands)?;
    }
    Ok(commands)
}

/// ponytail: no fsync, so an acked order survives a killed process, not a lost machine. `sync_data` here if it must.
pub fn append(log: &File, bytes: &[u8]) -> Result<(), String> {
    let mut log = log;
    log.write_all(bytes).map_err(|e| format!("journal write: {e}"))
}
