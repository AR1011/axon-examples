//! Whole crawls against the generated site, with crawler nodes as child processes.

use std::path::PathBuf;
use std::time::Duration;

use crawler::harness::{CrawlConfig, crawl};
use crawler::site::SiteConfig;

fn config(name: &str, kill_at: Option<f64>) -> CrawlConfig {
    let data = std::env::temp_dir().join(format!("crawler-test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data);
    CrawlConfig {
        site: SiteConfig { domains: 8, pages: 80, latency: Duration::from_millis(30), ..SiteConfig::default() },
        nodes: 3,
        rate: 20.0,
        per_domain: 2,
        node_fetches: 32,
        kill_at,
        data,
        exe: PathBuf::from(env!("CARGO_BIN_EXE_crawler")),
        deadline: Duration::from_secs(30),
        console: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crawl_fetches_every_reachable_url_once_within_the_rate() {
    let cfg = config("plain", None);
    let report = crawl(&cfg).await.unwrap();
    eprintln!("{report:#?}\n{}", report.summary());
    report.check(&cfg).unwrap();
    assert_eq!(report.max_answers, 1, "no crash, so no URL twice");
    std::fs::remove_dir_all(&cfg.data).unwrap();
}

async fn killed_mid_crawl(name: &str, cross_percent: u64) {
    let mut cfg = config(name, Some(0.4));
    cfg.site.cross_percent = cross_percent;
    let report = crawl(&cfg).await.unwrap();
    eprintln!("{report:#?}\n{}", report.summary());
    report.check(&cfg).unwrap();
    let (victim, owned) = report.killed.clone().unwrap();
    assert!(owned > 0);
    assert!(report.statuses.values().all(|s| s.node != victim), "its domains moved to the survivors");
    std::fs::remove_dir_all(&cfg.data).unwrap();
}

/// Pages hand links to domains on the dead node while it is dead: those calls fail over to the new owner.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_killed_mid_crawl_loses_no_url_and_repeats_each_at_most_once() {
    killed_mid_crawl("kill", 30).await;
}

/// No links cross domains, so nothing calls a dead node's domains again: the survivors must start them by themselves.
#[tokio::test(flavor = "multi_thread")]
async fn the_domains_of_a_killed_node_resume_on_the_survivors_without_being_called() {
    killed_mid_crawl("resume", 0).await;
}
