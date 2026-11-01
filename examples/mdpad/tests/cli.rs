//! `mdpad` run as a process against a node: sign in, import, export, share, history, restore and a fresh copy, as an
//! operator's scripts would call them.

mod support;

use std::path::{Path, PathBuf};

use serde_json::Value;
use support::{CutFiles, start, user};

struct Mdpad {
    config: PathBuf,
    url: String,
}

impl Mdpad {
    /// Runs `mdpad` with `args`: its stdout, or its stderr as the error.
    async fn run(&self, args: &[&str]) -> Result<String, String> {
        let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_mdpad"))
            .args(args)
            .env("MDPAD_CONFIG", &self.config)
            .env_remove("MDPAD_URL")
            .env_remove("MDPAD_TOKEN")
            .output()
            .await
            .expect("mdpad runs");
        let text = |b: Vec<u8>| String::from_utf8(b).expect("UTF-8");
        match out.status.success() {
            true => Ok(text(out.stdout)),
            false => Err(text(out.stderr)),
        }
    }

    async fn json(&self, args: &[&str]) -> Value {
        let out = self.run(&[args, &["--json"]].concat()).await.unwrap_or_else(|e| panic!("{args:?}: {e}"));
        serde_json::from_str(&out).unwrap_or_else(|e| panic!("{args:?}: {e}: {out}"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_cli_signs_in_imports_restores_and_exports() {
    let (docs, accounts) = (CutFiles::new(None), CutFiles::new(None));
    let node = start(&docs, &accounts, |_| {}).await;
    user(&node, "ann").await;
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("mdpad-cli-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mdpad = Mdpad { config: dir.join("credentials.json"), url: format!("http://{}", node.http()) };

    let url = format!("url={}", mdpad.url);
    let refused = mdpad.run(&["login", &url, "name=ann", "password=wrong"]).await.unwrap_err();
    assert!(refused.starts_with("mdpad: "), "{refused}");
    assert!(mdpad.run(&["docs"]).await.is_err(), "no sign-in kept");
    let signed = mdpad.run(&["login", &url, "name=ann", "password=pw-ann"]).await.unwrap();
    assert_eq!(signed.trim(), format!("signed in to {} as ann", mdpad.url));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&mdpad.config).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "the token is the owner's to read");
    }
    assert_eq!(mdpad.run(&["docs"]).await.unwrap().trim(), "No documents.");

    let plan = dir.join("plan.md");
    std::fs::write(&plan, "# Plan\n\nIntro.\n").unwrap();
    let id = mdpad.run(&["import", plan.to_str().unwrap()]).await.unwrap().trim().to_string();
    let docs = mdpad.json(&["docs"]).await;
    assert_eq!(docs["docs"][0]["id"], id.as_str());
    assert_eq!(docs["docs"][0]["title"], "Plan");

    std::fs::write(&plan, "# Plan\n\nIntro.\n## Draft\nOld idea.\n").unwrap();
    let into = format!("into={id}");
    let imported = mdpad.json(&["import", plan.to_str().unwrap(), &into]).await;
    assert_eq!(imported["before"]["kind"], "before_import");
    let first = imported["before"]["n"].as_u64().unwrap().to_string();
    assert_eq!(mdpad.run(&["export", &id]).await.unwrap(), "# Plan\n\nIntro.\n## Draft\nOld idea.\n");

    let restored = mdpad.run(&["restore", &id, &first]).await.unwrap();
    assert_eq!(restored.trim(), format!("restored version {first}; the text before it is version 2"));
    let out = dir.join("out.md");
    mdpad.run(&["export", &id, &format!("out={}", out.display())]).await.unwrap();
    assert_eq!(std::fs::read_to_string(&out).unwrap(), "# Plan\n\nIntro.\n");
    let versions = mdpad.json(&["versions", &id]).await;
    let kinds: Vec<&str> =
        versions["versions"].as_array().unwrap().iter().map(|v| v["kind"].as_str().unwrap()).collect();
    assert_eq!(kinds, ["before_restore", "before_import"], "newest first");
    let lines = mdpad.run(&["versions", &id]).await.unwrap();
    assert!(lines.lines().next().unwrap().contains("before_restore  ann (import)"), "{lines}");

    let link = mdpad.run(&["share", &id, "view"]).await.unwrap();
    assert!(link.trim().starts_with(&format!("{}/s/", mdpad.url)), "{link}");
    assert_eq!(mdpad.run(&["share", &id, "off"]).await.unwrap().trim(), "The link is off.");

    let copy = mdpad.run(&["doc", "copy", &id]).await.unwrap().trim().to_string();
    assert_ne!(copy, id);
    let all = dir.join("all");
    let written = mdpad.run(&["export", "--all", all.to_str().unwrap()]).await.unwrap();
    assert_eq!(written.trim(), format!("2 documents in {}", all.display()));
    for doc in [&id, &copy] {
        assert_eq!(std::fs::read_to_string(all.join(format!("{doc}.md"))).unwrap(), "# Plan\n\nIntro.\n");
    }
    let unknown = mdpad.run(&["doc", "h1-nope"]).await.unwrap_err();
    assert!(unknown.starts_with("mdpad: "), "{unknown}");

    let page = mdpad.run(&["share", &id, "publish"]).await.unwrap();
    assert_eq!(page.trim(), format!("{}/p/{id}", mdpad.url));
    assert_eq!(mdpad.run(&["share", &id, "unpublish"]).await.unwrap().trim(), "Not published.");
    let refused = mdpad.run(&["cluster"]).await.unwrap_err();
    assert!(refused.contains("Operators only"), "{refused}");
    std::fs::remove_dir_all(&dir).unwrap();
    node.shutdown().await;
}

/// An operator's `mdpad cluster` and `mdpad doc chown`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_operator_sees_the_cluster_and_gives_a_document_a_new_owner() {
    use mdpad::{accounts::Request, core::accounts::Event, keeper::Msg};
    let (docs, accounts) = (CutFiles::new(None), CutFiles::new(None));
    let node = start(&docs, &accounts, |_| {}).await;
    let (ann, _) = (user(&node, "ann").await, user(&node, "ben").await);
    user(&node, "dev").await;
    let event = Event::Operator { name: "dev".into(), operator: true };
    let recorded = node.accounts().ask(|reply| Msg::Request(Request::Record { event: event.clone(), reply })).await;
    recorded.unwrap().unwrap();
    let id = ann.create("# Given\n").await.unwrap();
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("mdpad-cli-op-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mdpad = Mdpad { config: dir.join("credentials.json"), url: format!("http://{}", node.http()) };
    mdpad.run(&["login", &format!("url={}", mdpad.url), "name=dev", "password=pw-dev"]).await.unwrap();

    let given = mdpad.run(&["doc", "chown", &id, "ben"]).await.unwrap();
    assert_eq!(given.trim(), format!("{id} is ben's"));
    assert!(mdpad.run(&["doc", "chown", &id, "zed"]).await.unwrap_err().contains("No such user"));
    let lines = mdpad.run(&["cluster"]).await.unwrap();
    assert!(lines.contains("node h1  home,web,accounts"), "{lines}");
    assert!(lines.contains("home h1  1 open  1 stored"), "{lines}");
    assert!(lines.contains("web h1  ") && lines.contains(" sockets  "), "{lines}");
    let json = mdpad.json(&["cluster"]).await;
    assert_eq!(json["homes"][0]["docs"][0]["owner"], "ben", "{json}");
    assert!(json["homes"][0]["saved_per_sec"].is_u64(), "{json}");
    std::fs::remove_dir_all(&dir).unwrap();
    node.shutdown().await;
}
