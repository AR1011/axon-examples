//! The HTTP API from Rust: what `wf` and the tests call. Each call returns the API's JSON, or its refusal as one line:
//! `message (status, code)`.

use serde_json::{Value, json};

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    url: String,
    token: Option<String>,
}

/// A start as the API takes it. `input` is any JSON the workflow's input type reads.
#[derive(Clone, Debug, Default)]
pub struct StartRun {
    pub workflow: String,
    pub version: Option<u32>,
    pub id: Option<String>,
    pub input: Value,
    pub tags: Vec<(String, String)>,
    /// `30s`, `5m`, `2h` or `7d`.
    pub run_timeout: Option<String>,
}

/// A path segment with every byte outside `A-Z a-z 0-9 - . _ ~` percent-encoded: a run id may hold `/` and `:`.
pub fn segment(text: &str) -> String {
    text.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

impl Client {
    /// `url` is the api node's, as `dev` prints it; `token` from [`Client::login`].
    pub fn new(url: &str, token: Option<String>) -> Client {
        Client { http: reqwest::Client::new(), url: url.trim_end_matches('/').to_owned(), token }
    }

    /// A token for a command line, and when it expires in Unix ms.
    pub async fn login(&self, password: &str) -> Result<(String, u64), String> {
        let said = self.post("/login", json!({ "password": password, "token": true })).await?;
        let token = said["token"].as_str().ok_or("the login answered no token")?;
        Ok((token.to_owned(), said["expires_ms"].as_u64().unwrap_or(0)))
    }

    /// `{id, version, created}`.
    pub async fn start(&self, start: &StartRun) -> Result<Value, String> {
        let mut body = json!({ "workflow": start.workflow, "input": start.input, "tags": start.tags });
        if let Some(version) = start.version {
            body["version"] = json!(version);
        }
        if let Some(id) = &start.id {
            body["id"] = json!(id);
        }
        if let Some(timeout) = &start.run_timeout {
            body["run_timeout"] = json!(timeout);
        }
        self.post("/runs", body).await
    }

    /// `{signal_id, duplicate}`.
    pub async fn signal(&self, id: &str, name: &str, signal_id: Option<&str>, payload: Value) -> Result<Value, String> {
        let mut body = json!({ "payload": payload });
        if let Some(signal_id) = signal_id {
            body["signal_id"] = json!(signal_id);
        }
        self.post(&format!("/runs/{}/signals/{}", segment(id), segment(name)), body).await
    }

    pub async fn get(&self, id: &str) -> Result<Value, String> {
        self.call(reqwest::Method::GET, &format!("/runs/{}", segment(id)), None).await
    }

    /// `{queues: [{name, backlog, oldest_ms, pollers, workers, ..}]}`.
    pub async fn queues(&self) -> Result<Value, String> {
        self.call(reqwest::Method::GET, "/queues", None).await
    }

    /// `{runs: [{id, workflow, version, status, started_ms, updated_ms, ..}], next}`: `query` is the filters as a
    /// query string, `status=failed&workflow=checkout`.
    pub async fn list(&self, query: &str) -> Result<Value, String> {
        let query = if query.is_empty() { String::new() } else { format!("?{query}") };
        self.call(reqwest::Method::GET, &format!("/runs{query}"), None).await
    }

    /// `{entries, next}`: the journal from entry `after` on.
    pub async fn history(&self, id: &str, after: u64, limit: u32) -> Result<Value, String> {
        self.call(reqwest::Method::GET, &format!("/runs/{}/history?after={after}&limit={limit}", segment(id)), None)
            .await
    }

    /// `retry`, `resume` or `pause`.
    pub async fn act(&self, id: &str, action: &str) -> Result<Value, String> {
        self.post(&format!("/runs/{}/{action}", segment(id)), json!({})).await
    }

    pub async fn delete(&self, id: &str) -> Result<Value, String> {
        self.call(reqwest::Method::DELETE, &format!("/runs/{}", segment(id)), None).await
    }

    /// One of the console's pages: `workflows`, `queues`, `workers`, `metrics` or `cluster`.
    pub async fn page(&self, name: &str) -> Result<Value, String> {
        self.call(reqwest::Method::GET, &format!("/{name}"), None).await
    }

    pub async fn logout(&self) -> Result<Value, String> {
        self.post("/logout", json!({})).await
    }

    pub async fn cancel(&self, id: &str, reason: &str) -> Result<Value, String> {
        self.post(&format!("/runs/{}/cancel", segment(id)), json!({ "reason": reason })).await
    }

    pub async fn terminate(&self, id: &str, reason: &str) -> Result<Value, String> {
        self.post(&format!("/runs/{}/terminate", segment(id)), json!({ "reason": reason })).await
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value, String> {
        self.call(reqwest::Method::POST, path, Some(body)).await
    }

    async fn call(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> Result<Value, String> {
        let (status, said) = self.send(method, path, body).await?;
        if status.is_success() {
            return Ok(said);
        }
        let message = said["message"].as_str().map(String::from).unwrap_or_else(|| said.to_string());
        let code = said["error"].as_str().map_or(status.to_string(), |code| format!("{status}, {code}"));
        Err(format!("{message} ({code})"))
    }

    /// The status and the JSON of any answer; `Err` only where none came.
    pub async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<(reqwest::StatusCode, Value), String> {
        let mut request = self.http.request(method, format!("{}/api{path}", self.url));
        if let Some(token) = &self.token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        if let Some(body) = body {
            request = request.header("content-type", "application/json").body(body.to_string());
        }
        let response = request.send().await.map_err(|e| format!("cannot reach {}: {e}", self.url))?;
        let status = response.status();
        let text = response.bytes().await.map_err(|e| format!("{}: {e}", self.url))?;
        let said: Value =
            serde_json::from_slice(&text).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&text).into()));
        Ok((status, said))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_id_is_one_path_segment() {
        assert_eq!(segment("order-42"), "order-42");
        assert_eq!(segment("a/b:c d"), "a%2Fb%3Ac%20d");
    }
}
