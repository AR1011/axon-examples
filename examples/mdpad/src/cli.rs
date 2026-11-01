//! The commands a user runs against a node's HTTP API (design §4.4): `login`, then `docs`, `doc`, `export`, `import`,
//! `share`, `versions` and `restore`, and for an operator `cluster` and `doc chown`. The sign-in is kept in `~/.config/mdpad/credentials.json` (`MDPAD_CONFIG`);
//! `MDPAD_URL` and `MDPAD_TOKEN` override it. `--json` prints the API's answer as it is.

use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use reqwest::Method;
use serde_json::{Value, json};

pub const HELP: &str = "\
  mdpad login url=http://host:port name=N [password=P]   sign in; the token is kept in
                                                ~/.config/mdpad/credentials.json (MDPAD_CONFIG)
  mdpad docs [tab=mine|shared|trash]            your documents, newest edit first
  mdpad doc <id>                                a document's title, owner, your level and size
  mdpad doc copy <id>                           a fresh copy, shared with the same people
  mdpad doc chown <id> <user>                   give the document to another user (operators)
  mdpad export <id> [out=FILE]                  the Markdown, to stdout or FILE
  mdpad export --all <dir>                      the Markdown of every document you have, one file each
  mdpad import <file> [into=<id>]               a new document from a .md file (- for stdin), or the text
                                                of document <id> replaced, after a version
  mdpad share <id> off|view|edit                the share link's level, and the link
  mdpad share <id> publish|unpublish            the read-only page at /p/<id>, for anyone
  mdpad versions <id>                           a document's versions, newest first
  mdpad restore <id> <n>                        the text of version n again, as a new edit
  mdpad cluster                                 nodes, open documents per home, sockets, saves per second
                                                (operators)

Each takes --json (the API's answer as it is). MDPAD_URL and MDPAD_TOKEN override the kept sign-in.";

pub struct Cli {
    pub words: Vec<String>,
    pub args: BTreeMap<String, String>,
    pub json: bool,
    pub all: bool,
}

impl Cli {
    fn word(&self, at: usize, what: &str) -> Result<&str, String> {
        self.words.get(at).map(String::as_str).ok_or(format!("missing {what}: see `mdpad help`"))
    }
}

fn config_file() -> Result<PathBuf, String> {
    if let Ok(file) = std::env::var("MDPAD_CONFIG") {
        return Ok(file.into());
    }
    let home = std::env::var("HOME").map_err(|_| "no HOME to keep the sign-in in: set MDPAD_CONFIG")?;
    Ok(PathBuf::from(home).join(".config/mdpad/credentials.json"))
}

fn kept() -> Value {
    let read = config_file().ok().and_then(|file| std::fs::read(file).ok());
    read.and_then(|text| serde_json::from_slice(&text).ok()).unwrap_or(Value::Null)
}

struct Api {
    http: reqwest::Client,
    url: String,
    token: Option<String>,
}

impl Api {
    fn new(url: Option<&str>) -> Result<Self, String> {
        let kept = kept();
        let env = |name| std::env::var(name).ok();
        let url = url.map(String::from).or_else(|| env("MDPAD_URL")).or_else(|| kept["url"].as_str().map(String::from));
        let url = url.ok_or("no node: `mdpad login url=http://host:port name=N`, or set MDPAD_URL")?;
        let url = url.trim_end_matches('/').to_owned();
        // A token holds for the node that signed it.
        let same = kept["url"].as_str() == Some(url.as_str());
        let token = env("MDPAD_TOKEN").or_else(|| kept["token"].as_str().filter(|_| same).map(String::from));
        Ok(Self { http: reqwest::Client::new(), url, token })
    }

    async fn send(
        &self,
        method: Method,
        path: &str,
        body: Option<reqwest::Body>,
        json: bool,
    ) -> Result<String, String> {
        let mut request = self.http.request(method, format!("{}/api{path}", self.url));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            let kind = if json { "application/json" } else { "text/markdown; charset=utf-8" };
            request = request.header("content-type", kind).body(body);
        }
        let response = request.send().await.map_err(|e| format!("cannot reach {}: {e}", self.url))?;
        let status = response.status();
        let text = response.text().await.map_err(|e| format!("{}: {e}", self.url))?;
        if status.is_success() {
            return Ok(text);
        }
        if status == reqwest::StatusCode::UNAUTHORIZED && path != "/login" {
            return Err(format!("not signed in to {}: `mdpad login`", self.url));
        }
        let said: Value = serde_json::from_str(&text).unwrap_or(Value::String(text));
        let message = said["message"].as_str().map(String::from).unwrap_or_else(|| said.to_string());
        Err(format!("{message} ({status})"))
    }

    async fn call(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value, String> {
        let text = self.send(method, path, body.map(|b| b.to_string().into()), true).await?;
        Ok(serde_json::from_str(&text).unwrap_or(Value::String(text)))
    }

    async fn get(&self, path: &str) -> Result<Value, String> {
        self.call(Method::GET, path, None).await
    }
}

fn password(cli: &Cli) -> Result<String, String> {
    if let Some(p) = cli.args.get("password").cloned().or_else(|| std::env::var("MDPAD_PASSWORD").ok()) {
        return Ok(p);
    }
    if std::io::stdin().is_terminal() {
        eprint!("password: ");
    }
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).map_err(|e| format!("reading stdin: {e}"))?;
    Ok(line.trim_end_matches(['\n', '\r']).to_owned())
}

/// A document id goes in a path as it is: ids are letters, digits and `-`.
fn id(word: &str) -> Result<&str, String> {
    match !word.is_empty() && word.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
        true => Ok(word),
        false => Err(format!("{word:?} is not a document id")),
    }
}

fn when(ms: u64) -> String {
    let secs = ms / 1000;
    let (days, rest) = (secs / 86_400, secs % 86_400);
    // Days since 1970-01-01 to a date, by the civil calendar.
    let z = days as i64 + 719_468;
    let (era, doe) = (z.div_euclid(146_097), z.rem_euclid(146_097));
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let (d, m) = (doy - (153 * mp + 2) / 5 + 1, if mp < 10 { mp + 3 } else { mp - 9 });
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02} {:02}:{:02} UTC", rest / 3600, rest % 3600 / 60)
}

fn print(cli: &Cli, value: &Value, text: impl FnOnce() -> String) {
    match cli.json {
        true => println!("{value}"),
        false => println!("{}", text()),
    }
}

/// All the rows of a tab, page by page.
async fn rows(api: &Api, tab: &str) -> Result<Vec<Value>, String> {
    let (mut rows, mut after) = (Vec::new(), None::<String>);
    loop {
        let path = match &after {
            Some(after) => format!("/docs?tab={tab}&limit=200&after={after}"),
            None => format!("/docs?tab={tab}&limit=200"),
        };
        let page = api.get(&path).await?;
        rows.extend(page["docs"].as_array().cloned().unwrap_or_default());
        match page["next"].as_str() {
            Some(next) => after = Some(next.to_string()),
            None => return Ok(rows),
        }
    }
}

fn read_file(path: &str) -> Result<String, String> {
    match path {
        "-" => std::io::read_to_string(std::io::stdin()),
        path => std::fs::read_to_string(path),
    }
    .map_err(|e| format!("{path}: {e}"))
}

fn title(row: &Value) -> &str {
    row["title"].as_str().filter(|t| !t.is_empty()).unwrap_or("Untitled")
}

pub async fn run(command: &str, cli: Cli) -> Result<(), String> {
    if command == "login" {
        return login(&cli).await;
    }
    let api = Api::new(cli.args.get("url").map(String::as_str))?;
    match (command, cli.words.first().map(String::as_str)) {
        ("docs", _) => {
            let tab = cli.args.get("tab").map_or("mine", String::as_str);
            let rows = rows(&api, tab).await?;
            print(&cli, &json!({ "docs": rows }), || {
                let lines: Vec<String> = rows
                    .iter()
                    .map(|r| {
                        let edited = when(r["edited_ms"].as_u64().unwrap_or(0));
                        format!(
                            "{}  {}  {edited}  {} B  {}",
                            r["id"].as_str().unwrap_or("?"),
                            title(r),
                            r["size"],
                            r["level"].as_str().unwrap_or("")
                        )
                    })
                    .collect();
                if lines.is_empty() { "No documents.".into() } else { lines.join("\n") }
            });
        }
        ("doc", Some("copy")) => {
            let answer =
                api.call(Method::POST, &format!("/docs/{}/copy", id(cli.word(1, "the document id")?)?), None).await?;
            print(&cli, &answer, || answer["id"].as_str().unwrap_or("?").to_string());
        }
        ("doc", Some("chown")) => {
            let (doc, user) = (id(cli.word(1, "the document id")?)?, cli.word(2, "the new owner")?);
            let body = json!({ "user": user });
            let answer = api.call(Method::PUT, &format!("/admin/docs/{doc}/owner"), Some(body)).await?;
            print(&cli, &answer, || format!("{doc} is {user}'s"));
        }
        ("doc", Some(word)) => {
            let info = api.get(&format!("/docs/{}", id(word)?)).await?;
            print(&cli, &info, || {
                format!(
                    "{}\nowner {}, your level {}, {} of {} B",
                    title(&info),
                    info["owner"].as_str().unwrap_or("?"),
                    info["level"].as_str().unwrap_or("?"),
                    info["size"],
                    info["max_size"]
                )
            });
        }
        ("export", _) if cli.all => {
            let dir = Path::new(cli.word(0, "the directory")?);
            std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
            let mut written = Vec::new();
            for tab in ["mine", "shared"] {
                for row in rows(&api, tab).await? {
                    let doc = id(row["id"].as_str().unwrap_or(""))?.to_string();
                    let text = api.send(Method::GET, &format!("/docs/{doc}/markdown"), None, false).await?;
                    let file = dir.join(format!("{doc}.md"));
                    std::fs::write(&file, text).map_err(|e| format!("{}: {e}", file.display()))?;
                    written.push(file.display().to_string());
                }
            }
            let answer = json!({ "written": written });
            print(&cli, &answer, || format!("{} documents in {}", written.len(), dir.display()));
        }
        ("export", Some(word)) => {
            let text = api.send(Method::GET, &format!("/docs/{}/markdown", id(word)?), None, false).await?;
            match cli.args.get("out") {
                Some(out) => std::fs::write(out, &text).map_err(|e| format!("{out}: {e}"))?,
                None => print!("{text}"),
            }
        }
        ("import", Some(file)) => {
            let markdown = read_file(file)?;
            let answer = match cli.args.get("into") {
                Some(into) => {
                    let path = format!("/docs/{}/markdown", id(into)?);
                    let text = api.send(Method::PUT, &path, Some(markdown.into()), false).await?;
                    let answer: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    json!({ "id": into, "before": answer["before"] })
                }
                None => api.call(Method::POST, "/docs", Some(json!({ "markdown": markdown }))).await?,
            };
            print(&cli, &answer, || answer["id"].as_str().unwrap_or("?").to_string());
        }
        ("share", Some(word)) if matches!(cli.words.get(1).map(String::as_str), Some("publish" | "unpublish")) => {
            let doc = id(word)?;
            let on = cli.words[1] == "publish";
            api.call(Method::PUT, &format!("/docs/{doc}/share"), Some(json!({ "published": on }))).await?;
            let url = format!("{}/p/{doc}", api.url);
            let answer = json!({ "id": doc, "published": on, "url": on.then_some(&url) });
            print(&cli, &answer, || if on { url.clone() } else { "Not published.".into() });
        }
        ("share", Some(word)) => {
            let doc = id(word)?;
            let level = cli.word(1, "off, view or edit")?;
            api.call(Method::PUT, &format!("/docs/{doc}/share"), Some(json!({ "link": level }))).await?;
            let share = api.get(&format!("/docs/{doc}/share")).await?;
            let link = share["token"].as_str().filter(|_| level != "off").map(|t| format!("{}/s/{t}", api.url));
            let answer = json!({ "id": doc, "link": share["link"], "url": link });
            print(&cli, &answer, || link.unwrap_or_else(|| "The link is off.".into()));
        }
        ("versions", Some(word)) => {
            let answer = api.get(&format!("/docs/{}/versions", id(word)?)).await?;
            print(&cli, &answer, || {
                let list = answer["versions"].as_array().cloned().unwrap_or_default();
                let lines: Vec<String> = list
                    .iter()
                    .map(|v| {
                        let authors: Vec<&str> =
                            v["authors"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
                        let name = v["name"]
                            .as_str()
                            .filter(|n| !n.is_empty())
                            .map(|n| format!(" \"{n}\""))
                            .unwrap_or_default();
                        format!(
                            "{}  {}  {}{name}  {}  {} B",
                            v["n"],
                            when(v["at_ms"].as_u64().unwrap_or(0)),
                            v["kind"].as_str().unwrap_or("?"),
                            authors.join(", "),
                            v["bytes"]
                        )
                    })
                    .collect();
                if lines.is_empty() { "No versions yet.".into() } else { lines.join("\n") }
            });
        }
        ("restore", Some(word)) => {
            let n: u64 = cli.word(1, "the version number")?.parse().map_err(|_| "the version is a number")?;
            let answer = api.call(Method::POST, &format!("/docs/{}/versions/{n}/restore", id(word)?), None).await?;
            print(&cli, &answer, || {
                format!("restored version {n}; the text before it is version {}", answer["before"]["n"])
            });
        }
        ("cluster", _) => cluster(&cli, &api).await?,
        _ => return Err(format!("missing the document: see `mdpad help`\n{HELP}")),
    }
    Ok(())
}

/// `mdpad cluster`: the admin page's homes, asked twice a second apart for the saves per second.
async fn cluster(cli: &Cli, api: &Api) -> Result<(), String> {
    let first = api.get("/admin/homes").await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let mut now = api.get("/admin/homes").await?;
    let saved = |answer: &Value, home: &Value| {
        let homes = answer["homes"].as_array().cloned().unwrap_or_default();
        let same = homes.iter().find(|h| h["home"] == home["home"]).cloned().unwrap_or_default();
        same["saved"].as_u64()
    };
    if let Some(homes) = now["homes"].as_array_mut() {
        for home in homes {
            let rate = saved(&first, home).zip(home["saved"].as_u64()).map(|(a, b)| b.saturating_sub(a));
            home["saved_per_sec"] = json!(rate);
        }
    }
    print(cli, &now, || {
        let mut lines = Vec::new();
        for node in now["nodes"].as_array().into_iter().flatten() {
            let roles: Vec<&str> = node["roles"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
            let state = if node["available"] == true { "" } else { "  unreachable" };
            lines.push(format!("node {}  {}{state}", node["id"].as_str().unwrap_or("?"), roles.join(",")));
        }
        for home in now["homes"].as_array().into_iter().flatten() {
            let name = home["home"].as_str().unwrap_or("?");
            match home["error"].as_str() {
                Some(e) => lines.push(format!("home {name}  no answer: {e}")),
                None => lines.push(format!(
                    "home {name}  {} open  {} stored  {} saves/s  log {} KiB  {} streams",
                    home["open"],
                    home["stored"],
                    home["saved_per_sec"],
                    home["log_bytes"].as_u64().unwrap_or(0) / 1024,
                    home["streams"]
                )),
            }
        }
        for web in now["webs"].as_array().into_iter().flatten() {
            let name = web["node"].as_str().unwrap_or("?");
            match web["error"].as_str() {
                Some(e) => lines.push(format!("web {name}  no answer: {e}")),
                None => lines.push(format!(
                    "web {name}  {} sockets  {} users  {} streams",
                    web["sessions"], web["users"], web["streams"]
                )),
            }
        }
        lines.join("\n")
    });
    Ok(())
}

async fn login(cli: &Cli) -> Result<(), String> {
    let mut api = Api::new(cli.args.get("url").map(String::as_str))?;
    api.token = None;
    let name = cli.args.get("name").ok_or("missing name=")?;
    let body = json!({ "name": name, "password": password(cli)?, "token": true });
    let answer = api.call(Method::POST, "/login", Some(body)).await?;
    let token = answer["token"].as_str().ok_or("no token in the answer")?;
    let file = config_file()?;
    if let Some(dir) = file.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    let kept = json!({ "url": api.url, "user": name, "token": token });
    // Who reads it acts as the user: the owner only.
    let mut out = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&file)
        .map_err(|e| format!("{}: {e}", file.display()))?;
    std::io::Write::write_all(&mut out, kept.to_string().as_bytes()).map_err(|e| e.to_string())?;
    let answer = json!({ "url": api.url, "user": name });
    print(cli, &answer, || format!("signed in to {} as {name}", api.url));
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn dates_are_utc() {
        assert_eq!(super::when(0), "1970-01-01 00:00 UTC");
        assert_eq!(super::when(1_760_000_000_000), "2025-10-09 08:53 UTC");
    }
}
