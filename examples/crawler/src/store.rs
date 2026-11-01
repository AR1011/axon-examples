//! Each domain's frontier as an append-only log in a directory every node can reach: `D url` when a URL is
//! discovered, `F url` when it is finished. A new owner reads it back. Records are idempotent, so two writers during
//! a hand-over cost a duplicate fetch, never a lost URL. The write goes to the page cache before the actor
//! acknowledges, which survives the process being killed; `fsync` would also cover the machine.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

pub enum Record {
    Discovered(String),
    Finished(String),
}

pub struct Loaded {
    pub file: File,
    pub discovered: Vec<String>,
    pub finished: HashSet<String>,
}

fn file_name(domain: &str) -> String {
    let safe: String =
        domain.chars().map(|c| if c.is_ascii_alphanumeric() || c == '.' || c == '-' { c } else { '_' }).collect();
    format!("{safe}.log")
}

/// Every domain with a log. Log names are the domain names, which `file_name` leaves alone for DNS names.
pub fn domains(dir: &Path) -> std::io::Result<Vec<String>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        if let Some(domain) = entry?.file_name().to_str().and_then(|n| n.strip_suffix(".log")) {
            out.push(domain.to_owned());
        }
    }
    Ok(out)
}

pub fn open(dir: &Path, domain: &str) -> std::io::Result<Loaded> {
    let mut file = OpenOptions::new().create(true).read(true).append(true).open(dir.join(file_name(domain)))?;
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    let (mut discovered, mut finished) = (Vec::new(), HashSet::new());
    // A line without its newline is a write the kill cut short; its URL was never acknowledged.
    for line in text.split_inclusive('\n').filter(|l| l.ends_with('\n')) {
        match line.trim_end().split_once(' ') {
            Some(("D", url)) => discovered.push(url.to_owned()),
            Some(("F", url)) => drop(finished.insert(url.to_owned())),
            _ => {}
        }
    }
    Ok(Loaded { file, discovered, finished })
}

/// One `write` for the whole batch.
pub fn append(mut file: &File, records: &[Record]) -> std::io::Result<()> {
    let mut out = String::new();
    for record in records {
        let (tag, url) = match record {
            Record::Discovered(url) => ("D", url),
            Record::Finished(url) => ("F", url),
        };
        out.push_str(tag);
        out.push(' ');
        out.push_str(url);
        out.push('\n');
    }
    file.write_all(out.as_bytes())
}
