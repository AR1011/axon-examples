//! The engine's own series, and the Prometheus text format they go out in on `GET /metrics`. An engine counts what
//! its runs do ([`EngineMetrics`]); the run counts come from a history replica's index, through the observer.

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use axon::{AtomicHistogram, Histogram, HistogramSnapshot};

/// What the runs of one engine node did since it started.
#[derive(Default)]
pub struct EngineMetrics {
    /// From a message that made journal entries to those entries on a majority.
    pub step: AtomicHistogram,
    /// A run's handler, per message: the workflow code it polls and the bookkeeping around it.
    pub poll: AtomicHistogram,
    /// From a run's open to its journal read and replayed.
    pub open: AtomicHistogram,
    /// From entries handed to the journal's writer to their ack from a majority.
    pub append: AtomicHistogram,
    /// How late a durable timer fired, past its time, on the firing node's wall clock.
    pub lateness: AtomicHistogram,
    /// Opens and writes refused by a newer owner's fence.
    pub fenced: AtomicU64,
}

impl EngineMetrics {
    pub fn fenced(&self) -> u64 {
        self.fenced.load(Ordering::Relaxed)
    }

    pub fn write(&self, out: &mut Text) {
        let series = [
            ("wf_step_seconds", "From a run's input to its journal entries on a majority.", &self.step),
            ("wf_poll_seconds", "A run's handler, per message.", &self.poll),
            ("wf_open_seconds", "From a run's open to its journal replayed.", &self.open),
            ("wf_journal_append_seconds", "From entries handed to the writer to a majority's ack.", &self.append),
            ("wf_timer_lateness_seconds", "How late a durable timer fired.", &self.lateness),
        ];
        for (name, help, histogram) in series {
            out.histogram(name, help, &histogram.snapshot());
        }
        out.family("wf_fenced_behind_total", "Opens and writes refused by a newer fence.", "counter");
        out.sample("wf_fenced_behind_total", &[], self.fenced() as f64);
    }
}

/// The text exposition format, one family after another.
#[derive(Default)]
pub struct Text(pub String);

fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

impl Text {
    pub fn family(&mut self, name: &str, help: &str, kind: &str) {
        let _ = writeln!(self.0, "# HELP {name} {help}\n# TYPE {name} {kind}");
    }

    pub fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: f64) {
        let labels: Vec<String> = labels.iter().map(|(k, v)| format!("{k}=\"{}\"", escape(v))).collect();
        let labels = if labels.is_empty() { String::new() } else { format!("{{{}}}", labels.join(",")) };
        let _ = writeln!(self.0, "{name}{labels} {value}");
    }

    /// A histogram with a bucket for each of axon's buckets that holds a count, by its upper bound.
    pub fn histogram(&mut self, name: &str, help: &str, snapshot: &HistogramSnapshot) {
        self.family(name, help, "histogram");
        let mut seen = 0;
        for (i, n) in snapshot.counts().iter().enumerate() {
            seen += n;
            let bound = Histogram::upper_bound_ns(snapshot.first() + i);
            if *n > 0 && bound < u64::MAX {
                let le = (bound as f64 / 1e9).to_string();
                self.sample(&format!("{name}_bucket"), &[("le", &le)], seen as f64);
            }
        }
        self.sample(&format!("{name}_bucket"), &[("le", "+Inf")], snapshot.count() as f64);
        self.sample(&format!("{name}_sum"), &[], snapshot.sum_ns() as f64 / 1e9);
        self.sample(&format!("{name}_count"), &[], snapshot.count() as f64);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_histogram_reads_as_cumulative_buckets() {
        let metrics = EngineMetrics::default();
        for ms in [1, 1, 3] {
            metrics.step.record(Duration::from_millis(ms));
        }
        metrics.fenced.fetch_add(2, Ordering::Relaxed);
        let mut text = Text::default();
        metrics.write(&mut text);
        let lines: Vec<&str> = text.0.lines().filter(|l| l.starts_with("wf_step_seconds")).collect();
        assert_eq!(lines.len(), 5, "{lines:?}");
        assert!(lines[0].starts_with("wf_step_seconds_bucket{le=\"0.001") && lines[0].ends_with(" 2"), "{lines:?}");
        assert!(lines[1].ends_with(" 3") && lines[2] == "wf_step_seconds_bucket{le=\"+Inf\"} 3", "{lines:?}");
        assert_eq!(lines[3], "wf_step_seconds_sum 0.005");
        assert!(text.0.contains("# TYPE wf_open_seconds histogram\nwf_open_seconds_bucket{le=\"+Inf\"} 0\n"));
        assert!(text.0.contains("\nwf_fenced_behind_total 2\n"));
        let mut labelled = Text::default();
        labelled.sample("wf_runs", &[("state", "a\"b")], 1.0);
        assert_eq!(labelled.0, "wf_runs{state=\"a\\\"b\"} 1\n");
    }
}
