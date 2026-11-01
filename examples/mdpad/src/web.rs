//! The HTTP API (design §4.2) and the websocket upgrade. Every document route asks the document, which decides by the
//! permission table; the routes only say who asks. `/api/p/<id>` asks as anyone, and the admin routes as an operator.
//!
//! The accounts and each home's catalog are on other nodes, asked by name with 2 s each, and once more within those
//! 2 s where the name's node had restarted or left (`Retry::Resolve`, nothing delivered). While the accounts cannot
//! be reached a sign-in cookie is taken on its signature alone, and counted in [`FAIL_OPEN`]: a disable or a new
//! password made meanwhile reaches the sessions through each web node's [`Gate`](crate::gate) once it is recorded.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axon::{ActorHandle, ActorSystem};
use axon_cluster::{Cluster, ClusterError, NamedRef, Retry, WireProtocol};
use axon_stream::StreamNode;
use axum::Json;
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use axum::extract::{Path, Query, State, WebSocketUpgrade};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::auth::{Auth, Guest, Invite, SignedIn, hash_password, verify_password};
use crate::catalog::{Ids, valid_id};
use crate::core::accounts::{Event as AccountsEvent, User, valid_name};
use crate::core::acl::{Change, Level, Principal};
use crate::core::catalog::{Row, RowState, Tab, order};
use crate::core::doc::MAX_UPDATE;
use crate::doc;
use crate::follow::SessionCore;
use crate::gate;
use crate::remote::{
    AccountsAnswer, AccountsP, AccountsReq, CatalogAnswer, CatalogP, CatalogReq, GATE, GateP, GateReq,
};
use crate::session::{In, Out, Reader, Session, Writer};
use crate::wire::{DocAnswer, DocInfo, DocReq, Refusal, close};

/// A sign-in lasts this long.
pub const SESSION: Duration = Duration::from_secs(12 * 3600);
/// A guest token lasts this long.
pub const GUEST: Duration = Duration::from_secs(24 * 3600);
/// An invite from the admin page lasts this long, as `mdpad user invite`'s does.
pub const INVITE: Duration = Duration::from_secs(7 * 24 * 3600);
const CALL_WITHIN: Duration = Duration::from_secs(5);
/// The accounts and each catalog answer within this, or count as not there.
pub const ASK_WITHIN: Duration = Duration::from_secs(2);
pub const COOKIE: &str = "mdpad";

/// Sign-ins taken on their signature alone, as the accounts did not answer.
pub static FAIL_OPEN: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct Web {
    pub system: ActorSystem,
    pub cluster: Cluster,
    pub streams: StreamNode,
    pub auth: Arc<Auth>,
    pub accounts: NamedRef<AccountsP>,
    /// Each home's catalog, by home name: the homes this node was given.
    pub catalogs: Arc<BTreeMap<String, NamedRef<CatalogP>>>,
    pub gate: ActorHandle<gate::Msg>,
}

pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    /// A `forbidden` for a document in the trash, so the page shows the trash and not a refusal.
    trashed: bool,
}

pub(crate) fn error(status: StatusCode, code: &'static str, message: impl Into<String>) -> ApiError {
    ApiError { status, code, message: message.into(), trashed: false }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = json!({ "error": self.code, "message": self.message });
        if self.trashed {
            body["trashed"] = Value::Bool(true);
        }
        (self.status, Json(body)).into_response()
    }
}

type Api<T = Json<Value>> = Result<T, ApiError>;

fn unavailable(what: impl std::fmt::Debug) -> ApiError {
    error(StatusCode::SERVICE_UNAVAILABLE, "unavailable", format!("Try again. {what:?}"))
}

fn unauthorized() -> ApiError {
    error(StatusCode::UNAUTHORIZED, "unauthorized", "Sign in.")
}

fn not_found() -> ApiError {
    error(StatusCode::NOT_FOUND, "not_found", "No such document.")
}

/// `req` to `named` within `ASK_WITHIN`, asked again where it was not delivered because the reference was out of date
/// (`Retry::Resolve`, as after its node restarted): the second call looks the name up again.
pub async fn ask<P: WireProtocol>(named: &NamedRef<P>, req: P::Message) -> Result<P::Reply, ClusterError>
where
    P::Message: Clone,
{
    let deadline = tokio::time::Instant::now() + ASK_WITHIN;
    match named.request_within(req.clone(), ASK_WITHIN).await {
        Err(e) if e.retry() == Retry::Resolve => {
            named.request_within(req, deadline.saturating_duration_since(tokio::time::Instant::now())).await
        }
        answer => answer,
    }
}

fn now_ms() -> u64 {
    axon::trace::unix_micros() / 1000
}

pub fn router(web: Web) -> axum::Router {
    axum::Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/me", get(me))
        .route("/api/me/password", post(set_password))
        .route("/api/invite/{token}", get(read_invite).post(accept_invite))
        .route("/api/guest", post(guest))
        .route("/api/docs", get(list).post(create))
        .route("/api/docs/{id}", get(info).delete(trash))
        .route("/api/docs/{id}/markdown", get(markdown).put(import))
        .route("/api/docs/{id}/copy", post(copy))
        .route("/api/docs/{id}/versions", get(versions).post(name_version))
        .route("/api/docs/{id}/versions/{n}", get(version))
        .route("/api/docs/{id}/versions/{n}/restore", post(restore))
        .route("/api/docs/{id}/untrash", post(untrash))
        .route("/api/docs/{id}/hide", post(hide))
        .route("/api/docs/{id}/share", get(share).put(set_share))
        .route("/api/docs/{id}/share/reset", post(reset_link))
        .route("/api/docs/{id}/share/remove-link-members", post(remove_link_members))
        .route("/api/docs/{id}/members/{user}", put(set_member).delete(remove_member))
        .route("/api/docs/{id}/ws", get(upgrade))
        .route("/api/s/{token}", get(read_link))
        .route("/api/s/{token}/join", post(join_link))
        .route("/api/p/{id}", get(published))
        .route("/api/admin/users", get(admin_users).post(admin_invite))
        .route("/api/admin/users/{name}", put(admin_set_user))
        .route("/api/admin/docs/{id}/owner", put(admin_owner))
        .route("/api/admin/homes", get(admin_homes))
        .fallback(crate::page::serve)
        .with_state(web)
}

/// The bearer or cookie token in a request.
fn token(headers: &HeaderMap) -> Option<String> {
    if let Some(bearer) = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()) {
        return bearer.strip_prefix("Bearer ").map(str::to_string);
    }
    let cookies = headers.get_all(header::COOKIE).into_iter().filter_map(|v| v.to_str().ok());
    cookies.flat_map(|c| c.split(';')).find_map(|c| c.trim().strip_prefix("mdpad=").map(str::to_string))
}

impl Web {
    async fn account(&self, name: &str) -> Api<Option<User>> {
        match ask(&self.accounts, AccountsReq::Get { name: name.to_string() }).await {
            Ok(AccountsAnswer::User { user }) => Ok(user),
            Ok(other) => Err(unavailable(other)),
            Err(e) => Err(unavailable(e)),
        }
    }

    /// The signed-in user, at their account's current version and not disabled; on the signature alone while the
    /// accounts do not answer.
    async fn signed_in(&self, token: &str) -> Api<SignedIn> {
        let signed = self.auth.signed_in(token, now_ms()).ok_or_else(unauthorized)?;
        match self.account(&signed.user).await {
            Ok(Some(user)) if !user.disabled && user.version == signed.version => Ok(signed),
            Ok(_) => Err(unauthorized()),
            Err(_) => {
                FAIL_OPEN.fetch_add(1, Ordering::Relaxed);
                Ok(signed)
            }
        }
    }

    async fn user(&self, headers: &HeaderMap) -> Api<String> {
        let token = token(headers).ok_or_else(unauthorized)?;
        self.signed_in(&token).await.map(|s| s.user)
    }

    /// A signed-in user, with the version they signed in at, or a guest of this document's link.
    async fn signed_principal(
        &self,
        headers: &HeaderMap,
        token: Option<&str>,
        guest: Option<&str>,
        id: &str,
    ) -> Api<(Principal, Option<u64>)> {
        if let Some(guest) = guest.or_else(|| {
            headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Guest "))
        }) {
            return match self.auth.open_guest(guest, now_ms()) {
                Some(g) if g.doc == id => Ok((g.principal(), None)),
                _ => Err(error(StatusCode::UNAUTHORIZED, "unauthorized", "The link is not valid.")),
            };
        }
        let token = token.map(str::to_string).or_else(|| self::token(headers)).ok_or_else(unauthorized)?;
        let signed = self.signed_in(&token).await?;
        Ok((Principal::User(signed.user), Some(signed.version)))
    }

    /// The signed-in user, when the accounts say they are an operator: never on the signature alone.
    async fn operator(&self, headers: &HeaderMap) -> Api<String> {
        let name = self.user(headers).await?;
        match self.account(&name).await? {
            Some(user) if user.operator && !user.disabled => Ok(name),
            _ => Err(error(StatusCode::FORBIDDEN, "forbidden", "Operators only.")),
        }
    }

    async fn principal(&self, headers: &HeaderMap, guest: Option<&str>, id: &str) -> Api<Principal> {
        self.signed_principal(headers, None, guest, id).await.map(|(who, _)| who)
    }

    /// An id this example could have made, of one of the homes.
    fn known(&self, id: &str) -> Api<()> {
        match valid_id(id) && self.catalogs.contains_key(doc::home_of(id)) {
            true => Ok(()),
            false => Err(not_found()),
        }
    }

    async fn catalog(&self, home: &str, req: CatalogReq) -> Api<CatalogAnswer> {
        let catalog = self.catalogs.get(home).ok_or_else(not_found)?;
        match ask(catalog, req).await {
            Ok(CatalogAnswer::Unavailable { why }) => Err(unavailable(why)),
            Ok(answer) => Ok(answer),
            Err(e) => Err(unavailable(e)),
        }
    }

    /// A session of document `id` for `who` over `io`, known to the gate when `who` signed in at `version`.
    pub fn open_session(
        &self,
        id: &str,
        who: Principal,
        version: Option<u64>,
        io: (Reader, Writer),
    ) -> Result<(), String> {
        let colour = colour(who.name());
        let session = axon::trace::random_u64().max(1);
        let user = who.user().map(str::to_string);
        let core = SessionCore::new(session, who, colour);
        let handle = self
            .system
            .spawn(Session::new(core, io, &self.cluster, self.streams.clone(), id))
            .start()
            .map_err(|e| e.to_string())?;
        if let (Some(user), Some(version)) = (user, version) {
            let lifecycle = handle.lifecycle.clone();
            // A session the gate does not know could not be revoked: a full gate ends it, and its client joins again.
            if let Err(e) = self.gate.tell(gate::Msg::Open { user, version, session, handle }) {
                lifecycle.stop();
                return Err(format!("the gate: {:?}", e.reason));
            }
        }
        Ok(())
    }

    async fn ask_doc(&self, id: &str, req: DocReq) -> Api<DocAnswer> {
        self.known(id)?;
        match doc::keyed(&self.cluster, id).request_within(req, CALL_WITHIN).await {
            Ok(DocAnswer::Refused(why)) => Err(refused(why)),
            Ok(DocAnswer::Busy { retry_after_ms }) => {
                Err(error(StatusCode::SERVICE_UNAVAILABLE, "busy", format!("Busy. Try again in {retry_after_ms} ms.")))
            }
            Ok(answer) => Ok(answer),
            Err(e) if doc::unknown(&e) => Err(not_found()),
            Err(e) => Err(unavailable(e)),
        }
    }

    async fn info(&self, id: &str, who: Principal) -> Api<Box<DocInfo>> {
        match self.ask_doc(id, DocReq::Info { who }).await? {
            DocAnswer::Info(info) => Ok(info),
            _ => Err(unavailable("not an info answer")),
        }
    }
}

fn refused(why: Refusal) -> ApiError {
    match why {
        Refusal::Forbidden => error(StatusCode::FORBIDDEN, "forbidden", "You may not do this."),
        Refusal::Trashed => {
            ApiError { trashed: true, ..error(StatusCode::FORBIDDEN, "forbidden", "The document is in the trash.") }
        }
        Refusal::NotFound => not_found(),
        Refusal::Full => error(StatusCode::PAYLOAD_TOO_LARGE, "too_large", "The document is full."),
        Refusal::TooLarge => error(StatusCode::PAYLOAD_TOO_LARGE, "too_large", "Too large."),
        Refusal::Resync => error(StatusCode::CONFLICT, "invalid", "Not a valid change."),
    }
}

#[derive(Deserialize)]
struct Login {
    name: String,
    password: String,
    #[serde(default)]
    token: bool,
}

async fn login(State(web): State<Web>, Json(login): Json<Login>) -> Api<Response> {
    let wrong = || error(StatusCode::UNAUTHORIZED, "unauthorized", "Wrong name or password.");
    let user = web.account(&login.name).await?;
    // A name that is not there, or is disabled, takes as long as a wrong password: the time does not tell which.
    let hash = user.as_ref().map(|u| u.hash.clone());
    let verify = move || {
        static NOBODY: OnceLock<String> = OnceLock::new();
        let nobody = || hash_password("").expect("a hash of nothing");
        verify_password(hash.as_deref().unwrap_or_else(|| NOBODY.get_or_init(nobody)), &login.password)
    };
    let ok = tokio::task::spawn_blocking(verify).await.unwrap_or(false);
    let Some(user) = user.filter(|u| ok && !u.disabled) else { return Err(wrong()) };
    Ok(signed_in(&web, &login.name, user.version, login.token))
}

/// A new sign-in of `name` at `version`: the cookie, and the token too when asked.
fn signed_in(web: &Web, name: &str, version: u64, with_token: bool) -> Response {
    let expires_ms = now_ms() + SESSION.as_millis() as u64;
    let token = web.auth.sign_in(&SignedIn { user: name.into(), version, expires_ms });
    let cookie = format!("{COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}", SESSION.as_secs());
    let body = match with_token {
        true => json!({ "user": name, "token": token }),
        false => json!({ "user": name }),
    };
    let mut response = Json(body).into_response();
    response.headers_mut().insert(header::SET_COOKIE, HeaderValue::from_str(&cookie).expect("a cookie is ASCII"));
    response
}

/// The shortest password an account takes.
const MIN_PASSWORD: usize = 8;

/// A new password's hash, made off the runtime's threads; refused when the password is too short.
async fn new_hash(password: String) -> Api<String> {
    if password.chars().count() < MIN_PASSWORD {
        return Err(error(StatusCode::BAD_REQUEST, "invalid", format!("Use {MIN_PASSWORD} characters or more.")));
    }
    let hash = tokio::task::spawn_blocking(move || hash_password(&password)).await;
    hash.map_err(unavailable)?.map_err(unavailable)
}

impl Web {
    /// Records an accounts event: answered once it is durable, with why it may not be applied as `invalid`.
    async fn record(&self, event: AccountsEvent) -> Api<()> {
        match ask(&self.accounts, AccountsReq::Record { event }).await {
            Ok(AccountsAnswer::Recorded { result: Ok(()) }) => Ok(()),
            Ok(AccountsAnswer::Recorded { result: Err(why) }) => Err(error(StatusCode::CONFLICT, "invalid", why)),
            Ok(other) => Err(unavailable(other)),
            Err(e) => Err(unavailable(e)),
        }
    }

    /// The account after a change of password: its new version signs the caller in again.
    async fn set_hash(&self, name: &str, password: String, with_token: bool) -> Api<Response> {
        let hash = new_hash(password).await?;
        self.record(AccountsEvent::Password { name: name.into(), hash }).await?;
        let user = self.account(name).await?.ok_or_else(unauthorized)?;
        Ok(signed_in(self, name, user.version, with_token))
    }
}

#[derive(Deserialize)]
struct NewPassword {
    old: String,
    new: String,
    #[serde(default)]
    token: bool,
}

/// A new password ends every other sign-in of the user; this one goes on with a new cookie.
async fn set_password(State(web): State<Web>, headers: HeaderMap, Json(p): Json<NewPassword>) -> Api<Response> {
    let name = web.user(&headers).await?;
    let hash = web.account(&name).await?.map(|u| u.hash).ok_or_else(unauthorized)?;
    let old = p.old;
    let ok = tokio::task::spawn_blocking(move || verify_password(&hash, &old)).await.unwrap_or(false);
    if !ok {
        return Err(error(StatusCode::FORBIDDEN, "forbidden", "The current password is wrong."));
    }
    web.set_hash(&name, p.new, p.token).await
}

fn used_invite() -> ApiError {
    error(StatusCode::NOT_FOUND, "not_found", "This invite is not valid. Ask your operator for a new one.")
}

/// An invite still to be used.
async fn open_invite(web: &Web, token: &str) -> Api<Invite> {
    let invite = web.auth.open_invite(token, now_ms()).ok_or_else(used_invite)?;
    let user = web.account(&invite.name).await?;
    match (invite.version, user) {
        (0, None) => Ok(invite),
        (version, Some(user)) if version == user.version && !user.disabled => Ok(invite),
        _ => Err(used_invite()),
    }
}

async fn read_invite(State(web): State<Web>, Path(token): Path<String>) -> Api {
    let invite = open_invite(&web, &token).await?;
    Ok(Json(json!({ "user": invite.name, "new": invite.version == 0 })))
}

#[derive(Deserialize)]
struct InvitePassword {
    password: String,
    #[serde(default)]
    token: bool,
}

/// Sets the password an invite asks for, and signs the user in.
async fn accept_invite(
    State(web): State<Web>,
    Path(token): Path<String>,
    Json(p): Json<InvitePassword>,
) -> Api<Response> {
    let invite = open_invite(&web, &token).await?;
    if invite.version > 0 {
        return web.set_hash(&invite.name, p.password, p.token).await;
    }
    let hash = new_hash(p.password).await?;
    // A second use finds the account there: refused.
    web.record(AccountsEvent::Add { name: invite.name.clone(), hash }).await.map_err(|_| used_invite())?;
    Ok(signed_in(&web, &invite.name, 1, p.token))
}

async fn logout() -> Response {
    let mut response = Json(json!({})).into_response();
    let cookie = format!("{COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0");
    response.headers_mut().insert(header::SET_COOKIE, HeaderValue::from_str(&cookie).expect("a cookie is ASCII"));
    response
}

/// Who is signed in. A request with no sign-in at all is answered `user: null`, not 401: the page asks before it knows,
/// as its cookie is `HttpOnly`, and a refused call would show in the browser's console.
async fn me(State(web): State<Web>, headers: HeaderMap) -> Api {
    if token(&headers).is_none() {
        return Ok(Json(json!({ "user": null, "operator": false })));
    }
    let user = web.user(&headers).await?;
    // While the accounts do not answer, no one is an operator.
    let operator = web.account(&user).await.ok().flatten().is_some_and(|u| u.operator);
    Ok(Json(json!({ "user": user, "operator": operator })))
}

#[derive(Deserialize)]
struct GuestAsk {
    link: String,
    name: String,
}

/// A guest token for a view link: anyone with the link may read, under a name they give.
async fn guest(State(web): State<Web>, Json(ask): Json<GuestAsk>) -> Api {
    let name = ask.name.trim().to_string();
    if name.is_empty() || name.chars().count() > 40 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid", "Give a name of 1 to 40 characters."));
    }
    let (id, link_version) = web.auth.open_link(&ask.link).ok_or_else(not_found)?;
    let expires_ms = now_ms() + GUEST.as_millis() as u64;
    let guest = Guest { doc: id.clone(), link_version, name, expires_ms };
    // The document checks the link's level: a guest of an edit link or a link that is off gets nothing.
    match web.info(&id, guest.principal()).await?.level {
        Level::View => Ok(Json(json!({ "id": id, "token": web.auth.guest(&guest) }))),
        _ => Err(error(StatusCode::FORBIDDEN, "forbidden", "This link does not let guests in.")),
    }
}

#[derive(Deserialize)]
struct ListQuery {
    tab: Option<Tab>,
    limit: Option<usize>,
    /// The `next` of the page before: `<edited_ms>.<id>`.
    after: Option<String>,
}

fn row_json(id: &str, row: &Row, user: &str) -> Value {
    let level = if row.owner == user { "owner" } else { row.members.get(user).map_or("none", |l| l.name()) };
    json!({
        "id": id, "title": row.title, "owner": row.owner, "edited_ms": row.edited_ms, "size": row.size,
        "level": level, "trashed_ms": row.trashed_ms, "edited_by": row.edited_by,
    })
}

/// Each home's page, merged: newest edit first. `partial` when a home did not answer in 2 s.
async fn list(State(web): State<Web>, headers: HeaderMap, Query(q): Query<ListQuery>) -> Api {
    let user = web.user(&headers).await?;
    let (tab, limit) = (q.tab.unwrap_or(Tab::Mine), q.limit.unwrap_or(50).clamp(1, 200));
    let after = q.after.as_deref().map(|a| {
        let after = a.split_once('.').and_then(|(ms, id)| Some((ms.parse::<u64>().ok()?, id.to_string())));
        after.ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid", "after is the next of the page before."))
    });
    let after = after.transpose()?;
    let pages = futures_util::future::join_all(web.catalogs.keys().map(|home| {
        let req = CatalogReq::List { user: user.clone(), tab, after: after.clone(), limit };
        web.catalog(home, req)
    }))
    .await;
    let mut partial = false;
    let mut rows: Vec<(String, Row)> = Vec::new();
    for page in pages {
        match page {
            Ok(CatalogAnswer::Rows { rows: page }) => rows.extend(page),
            _ => partial = true,
        }
    }
    rows.sort_by(|(a, ra), (b, rb)| order(ra.edited_ms, a).cmp(&order(rb.edited_ms, b)));
    rows.truncate(limit);
    let next = rows.last().filter(|_| rows.len() == limit).map(|(id, row)| format!("{}.{id}", row.edited_ms));
    let docs: Vec<Value> = rows.iter().map(|(id, row)| row_json(id, row, &user)).collect();
    Ok(Json(json!({ "docs": docs, "partial": partial, "next": next })))
}

#[derive(Deserialize, Default)]
struct CreateAsk {
    #[serde(default)]
    markdown: String,
}

async fn create(State(web): State<Web>, headers: HeaderMap, body: Option<Json<CreateAsk>>) -> Api {
    let owner = web.user(&headers).await?;
    let markdown = body.map(|Json(b)| b.markdown).unwrap_or_default();
    Ok(Json(json!({ "id": new_doc(&web, &owner, markdown).await? })))
}

/// A new document of `owner`'s with `markdown`: its id.
async fn new_doc(web: &Web, owner: &str, markdown: String) -> Api<String> {
    let owner = owner.to_string();
    // A home picked at random, and the next one while a home does not answer.
    let homes: Vec<&String> = web.catalogs.keys().collect();
    let first = axon::trace::random_u64() as usize % homes.len().max(1);
    let mut failed = None;
    for home in homes.iter().cycle().skip(first).take(homes.len()) {
        match web.catalog(home, CatalogReq::Create { owner: owner.clone() }).await {
            Ok(CatalogAnswer::Created { id }) => {
                web.ask_doc(&id, DocReq::Create { owner, markdown }).await?;
                return Ok(id);
            }
            Ok(other) => failed = Some(unavailable(other)),
            Err(e) => failed = Some(e),
        }
    }
    Err(failed.unwrap_or_else(|| unavailable("no home")))
}

#[derive(Deserialize, Default)]
struct AsGuest {
    guest: Option<String>,
}

async fn info(State(web): State<Web>, headers: HeaderMap, Path(id): Path<String>, Query(q): Query<AsGuest>) -> Api {
    let who = web.principal(&headers, q.guest.as_deref(), &id).await?;
    let info = web.info(&id, who.clone()).await?;
    let owner = info.acl.owner == who.name() && who.user().is_some();
    Ok(Json(json!({
        "id": id, "title": info.title, "owner": info.acl.owner, "level": if owner { "owner" } else { info.level.name() },
        "size": info.size, "max_size": info.max_size, "trashed_ms": info.acl.trashed_ms,
    })))
}

async fn markdown(
    State(web): State<Web>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<AsGuest>,
) -> Api<Response> {
    let who = web.principal(&headers, q.guest.as_deref(), &id).await?;
    let DocAnswer::Text(text) = web.ask_doc(&id, DocReq::Text { who }).await? else {
        return Err(unavailable("not a text answer"));
    };
    let disposition = format!("attachment; filename=\"{id}.md\"");
    Ok((
        [
            (header::CONTENT_TYPE, "text/markdown; charset=utf-8".to_string()),
            (header::CONTENT_DISPOSITION, disposition),
        ],
        text,
    )
        .into_response())
}

/// Import into the document: the body's Markdown replaces the text, as a forward edit after a "before import"
/// version.
async fn import(State(web): State<Web>, headers: HeaderMap, Path(id): Path<String>, markdown: String) -> Api {
    let who = Principal::User(web.user(&headers).await?);
    let answer = web.ask_doc(&id, DocReq::Replace { who, markdown }).await?;
    Ok(Json(json!({ "before": version_json(answer)? })))
}

/// A fresh copy: the text as it stands in a new document of the caller's, shared with the same people.
async fn copy(State(web): State<Web>, headers: HeaderMap, Path(id): Path<String>) -> Api {
    let user = web.user(&headers).await?;
    let who = Principal::User(user.clone());
    let DocAnswer::Text(markdown) = web.ask_doc(&id, DocReq::Text { who: who.clone() }).await? else {
        return Err(unavailable("not a text answer"));
    };
    let acl = web.info(&id, who.clone()).await?.acl;
    let copied = new_doc(&web, &user, markdown).await?;
    let mut people: Vec<(String, Level)> = acl.members.iter().map(|(name, m)| (name.clone(), m.level)).collect();
    people.push((acl.owner.clone(), Level::Edit));
    for (name, level) in people.into_iter().filter(|(name, _)| *name != user) {
        let change = Change::Member(name, level);
        web.ask_doc(&copied, DocReq::Share { who: who.clone(), change }).await?;
    }
    Ok(Json(json!({ "id": copied })))
}

fn version_json(answer: DocAnswer) -> Api<Value> {
    match answer {
        DocAnswer::Version(version, _) => Ok(json!(*version)),
        other => Err(unavailable(other)),
    }
}

async fn versions(State(web): State<Web>, headers: HeaderMap, Path(id): Path<String>, Query(q): Query<AsGuest>) -> Api {
    let who = web.principal(&headers, q.guest.as_deref(), &id).await?;
    match web.ask_doc(&id, DocReq::Versions { who }).await? {
        // Newest first.
        DocAnswer::Versions(list) => Ok(Json(json!({ "versions": list.iter().rev().collect::<Vec<_>>() }))),
        other => Err(unavailable(other)),
    }
}

async fn version(
    State(web): State<Web>,
    headers: HeaderMap,
    Path((id, n)): Path<(String, u64)>,
    Query(q): Query<AsGuest>,
) -> Api {
    let who = web.principal(&headers, q.guest.as_deref(), &id).await?;
    match web.ask_doc(&id, DocReq::Version { who, n }).await? {
        DocAnswer::Version(version, markdown) => Ok(Json(json!({ "version": *version, "markdown": markdown }))),
        other => Err(unavailable(other)),
    }
}

#[derive(Deserialize)]
struct NameAsk {
    name: String,
}

async fn name_version(
    State(web): State<Web>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(a): Json<NameAsk>,
) -> Api {
    if a.name.trim().is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "invalid", "Give the version a name."));
    }
    let who = Principal::User(web.user(&headers).await?);
    let answer = web.ask_doc(&id, DocReq::NameVersion { who, name: a.name }).await?;
    Ok(Json(json!({ "version": version_json(answer)? })))
}

/// Restores version `n` as a forward edit: the answer is the "before restore" version saved first.
async fn restore(State(web): State<Web>, headers: HeaderMap, Path((id, n)): Path<(String, u64)>) -> Api {
    let who = Principal::User(web.user(&headers).await?);
    let answer = web.ask_doc(&id, DocReq::Restore { who, n }).await?;
    Ok(Json(json!({ "before": version_json(answer)? })))
}

async fn change(web: &Web, headers: &HeaderMap, id: &str, change: Change) -> Api {
    let who = Principal::User(web.user(headers).await?);
    web.ask_doc(id, DocReq::Share { who, change }).await?;
    Ok(Json(json!({})))
}

#[derive(Deserialize)]
struct Purge {
    purge: Option<u8>,
}

async fn trash(State(web): State<Web>, headers: HeaderMap, Path(id): Path<String>, Query(q): Query<Purge>) -> Api {
    if q.purge == Some(1) {
        let who = Principal::User(web.user(&headers).await?);
        web.ask_doc(&id, DocReq::Purge { who }).await?;
        return Ok(Json(json!({})));
    }
    change(&web, &headers, &id, Change::Trash(now_ms())).await
}

async fn untrash(State(web): State<Web>, headers: HeaderMap, Path(id): Path<String>) -> Api {
    change(&web, &headers, &id, Change::Untrash).await
}

async fn hide(State(web): State<Web>, headers: HeaderMap, Path(id): Path<String>) -> Api {
    let user = web.user(&headers).await?;
    web.known(&id)?;
    let hidden = web.catalog(doc::home_of(&id), CatalogReq::Hide { id: id.clone(), user }).await?;
    match hidden {
        CatalogAnswer::Hidden { hidden: true } => Ok(Json(json!({}))),
        _ => Err(error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "Only a document shared with you can be removed from your list.",
        )),
    }
}

/// The sharing, for the owner and the members; the link itself for the owner only.
async fn share(State(web): State<Web>, headers: HeaderMap, Path(id): Path<String>) -> Api {
    let who = Principal::User(web.user(&headers).await?);
    let info = web.info(&id, who.clone()).await?;
    let acl = &info.acl;
    let owner = acl.owner == who.name();
    if !owner && !acl.members.contains_key(who.name()) {
        return Err(error(StatusCode::FORBIDDEN, "forbidden", "You may not do this."));
    }
    let members: Vec<Value> =
        acl.members.iter().map(|(name, m)| json!({ "user": name, "level": m.level.name(), "via": m.via })).collect();
    let token = owner.then(|| web.auth.link(&id, acl.link_version));
    Ok(Json(json!({
        "owner": acl.owner, "link": acl.link.name(), "token": token, "members": members, "published": acl.published,
    })))
}

#[derive(Deserialize)]
struct SetShare {
    link: Option<String>,
    published: Option<bool>,
}

async fn set_share(State(web): State<Web>, headers: HeaderMap, Path(id): Path<String>, Json(s): Json<SetShare>) -> Api {
    let invalid = || error(StatusCode::BAD_REQUEST, "invalid", "Give link (off, view or edit) or published.");
    let link = s.link.map(|l| Level::parse(&l).ok_or_else(invalid)).transpose()?;
    if link.is_none() && s.published.is_none() {
        return Err(invalid());
    }
    if let Some(level) = link {
        let _set = change(&web, &headers, &id, Change::Link(level)).await?;
    }
    match s.published {
        Some(published) => change(&web, &headers, &id, Change::Publish(published)).await,
        None => Ok(Json(json!({}))),
    }
}

/// The published page's text, for anyone. Not published, in the trash, deleted or never made: the same 404.
async fn published(State(web): State<Web>, Path(id): Path<String>) -> Api<Response> {
    let not_published = || error(StatusCode::NOT_FOUND, "not_found", "This page is not published.");
    let text = match web.ask_doc(&id, DocReq::Text { who: Principal::Reader }).await {
        Ok(DocAnswer::Text(text)) => text,
        Ok(other) => return Err(unavailable(other)),
        Err(e) if matches!(e.status, StatusCode::FORBIDDEN | StatusCode::NOT_FOUND) => return Err(not_published()),
        Err(e) => return Err(e),
    };
    let headers = [
        (header::CONTENT_TYPE, "text/markdown; charset=utf-8"),
        // Not shared caches: an unpublish reaches every reader within 10 s.
        (header::CACHE_CONTROL, "private, max-age=10"),
        // Anyone's text, opened at this address: never taken as HTML.
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
    ];
    Ok((headers, text).into_response())
}

async fn reset_link(State(web): State<Web>, headers: HeaderMap, Path(id): Path<String>) -> Api {
    change(&web, &headers, &id, Change::ResetLink).await
}

async fn remove_link_members(State(web): State<Web>, headers: HeaderMap, Path(id): Path<String>) -> Api {
    change(&web, &headers, &id, Change::RemoveLinkMembers).await
}

#[derive(Deserialize)]
struct SetMember {
    level: String,
}

async fn set_member(
    State(web): State<Web>,
    headers: HeaderMap,
    Path((id, user)): Path<(String, String)>,
    Json(m): Json<SetMember>,
) -> Api {
    let level = Level::parse(&m.level)
        .filter(|l| *l != Level::None)
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid", "level is view or edit."))?;
    if web.account(&user).await?.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "not_found", "No such user."));
    }
    change(&web, &headers, &id, Change::Member(user, level)).await
}

async fn remove_member(State(web): State<Web>, headers: HeaderMap, Path((id, user)): Path<(String, String)>) -> Api {
    change(&web, &headers, &id, Change::RemoveMember(user)).await
}

async fn admin_users(State(web): State<Web>, headers: HeaderMap) -> Api {
    web.operator(&headers).await?;
    match ask(&web.accounts, AccountsReq::List).await {
        Ok(AccountsAnswer::Users { users }) => Ok(Json(json!({ "users": users }))),
        Ok(other) => Err(unavailable(other)),
        Err(e) => Err(unavailable(e)),
    }
}

#[derive(Deserialize)]
struct NewUser {
    name: String,
}

/// An invite link that makes the account, as `mdpad user invite` prints, for a name not taken.
async fn admin_invite(State(web): State<Web>, headers: HeaderMap, Json(u): Json<NewUser>) -> Api {
    web.operator(&headers).await?;
    if !valid_name(&u.name) {
        return Err(error(StatusCode::BAD_REQUEST, "invalid", "A name is 1 to 32 of a-z, 0-9, - and _."));
    }
    if web.account(&u.name).await?.is_some() {
        return Err(error(StatusCode::CONFLICT, "invalid", format!("{} exists.", u.name)));
    }
    let expires_ms = now_ms() + INVITE.as_millis() as u64;
    let path = format!("/i/{}", web.auth.invite(&Invite { name: u.name.clone(), version: 0, expires_ms }));
    let host = headers.get(header::HOST).and_then(|h| h.to_str().ok());
    let link = host.map_or_else(|| path.clone(), |host| format!("http://{host}{path}"));
    Ok(Json(json!({ "name": u.name, "path": path, "link": link, "expires_ms": expires_ms })))
}

#[derive(Deserialize)]
struct SetUser {
    disabled: Option<bool>,
    operator: Option<bool>,
}

/// Disables or enables a user, or makes them an operator or not. A disable ends their sockets and sign-ins. Not the
/// operator's own account: one click would lock the last operator out of this page.
async fn admin_set_user(
    State(web): State<Web>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(s): Json<SetUser>,
) -> Api {
    if web.operator(&headers).await? == name {
        return Err(error(StatusCode::CONFLICT, "invalid", "Another operator must change your account."));
    }
    if web.account(&name).await?.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "not_found", "No such user."));
    }
    if let Some(disabled) = s.disabled {
        let name = name.clone();
        web.record(if disabled { AccountsEvent::Disable { name } } else { AccountsEvent::Enable { name } }).await?;
    }
    if let Some(operator) = s.operator {
        web.record(AccountsEvent::Operator { name, operator }).await?;
    }
    Ok(Json(json!({})))
}

#[derive(Deserialize)]
struct NewOwner {
    user: String,
}

/// Gives a document to another user: the owner before stays as an edit member.
async fn admin_owner(
    State(web): State<Web>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(o): Json<NewOwner>,
) -> Api {
    let operator = web.operator(&headers).await?;
    if web.account(&o.user).await?.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "not_found", "No such user."));
    }
    web.ask_doc(&id, DocReq::Share { who: Principal::Operator(operator), change: Change::Owner(o.user) }).await?;
    Ok(Json(json!({})))
}

#[derive(Deserialize, Default)]
struct HostedQuery {
    /// The last id of the page before, per home.
    from: Option<String>,
}

/// The nodes in this node's view, what each home runs, and each web node's sockets. A home or a web node that does
/// not answer in 2 s is listed with its error.
async fn admin_homes(State(web): State<Web>, headers: HeaderMap, Query(q): Query<HostedQuery>) -> Api {
    web.operator(&headers).await?;
    let view = web.cluster.view();
    let nodes: Vec<Value> = view
        .members
        .iter()
        .map(|m| {
            let roles: Vec<&str> =
                m.record.label("role").into_iter().flat_map(|r| r.split(',')).filter(|r| !r.is_empty()).collect();
            json!({ "id": m.record.id, "roles": roles, "available": m.available(), "address": m.record.address })
        })
        .collect();
    let (web, q) = (&web, &q);
    let homes = futures_util::future::join_all(web.catalogs.keys().map(|home| async move {
        let from = q.from.clone().filter(|from| doc::home_of(from) == home);
        match web.catalog(home, CatalogReq::Hosted { from, limit: 200 }).await {
            Ok(CatalogAnswer::Hosted(stats)) => {
                let mut stats = serde_json::to_value(stats).unwrap_or_default();
                stats["home"] = json!(home);
                stats
            }
            Ok(other) => json!({ "home": home, "error": format!("{other:?}") }),
            Err(e) => json!({ "home": home, "error": e.message }),
        }
    }))
    .await;
    let (gates, failed) = web.cluster.resolve_all_among::<GateP>(GATE, &doc::web()).await;
    let mut webs = futures_util::future::join_all(gates.iter().map(|gate| async move {
        match gate.request_within(GateReq::Stats, ASK_WITHIN).await {
            Ok(stats) => {
                let mut stats = serde_json::to_value(stats).unwrap_or_default();
                stats["node"] = json!(gate.node());
                stats
            }
            Err(e) => json!({ "node": gate.node(), "error": e.to_string() }),
        }
    }))
    .await;
    webs.extend(failed.into_iter().map(|(node, e)| json!({ "node": node, "error": e.to_string() })));
    webs.sort_by(|a, b| a["node"].as_str().cmp(&b["node"].as_str()));
    Ok(Json(json!({ "nodes": nodes, "homes": homes, "webs": webs })))
}

/// What a link opens: its document and level. Changes nothing.
async fn read_link(State(web): State<Web>, Path(token): Path<String>) -> Api {
    let (id, link_version) = web.auth.open_link(&token).ok_or_else(not_found)?;
    let probe = Principal::Guest { name: String::new(), link_version };
    match web.ask_doc(&id, DocReq::Info { who: probe }).await {
        Ok(DocAnswer::Info(info)) => Ok(Json(json!({ "id": id, "level": info.acl.link.name() }))),
        // A link of an older version, or one that is off, opens nothing.
        _ => Err(not_found()),
    }
}

async fn join_link(State(web): State<Web>, headers: HeaderMap, Path(token): Path<String>) -> Api {
    let (id, link_version) = web.auth.open_link(&token).ok_or_else(not_found)?;
    let _joined = change(&web, &headers, &id, Change::JoinLink(link_version)).await?;
    Ok(Json(json!({ "id": id })))
}

#[derive(Deserialize, Default)]
struct WsQuery {
    token: Option<String>,
    guest: Option<String>,
}

/// Upgrades first, so a refusal reaches the client as a close code it acts on (design §4.3).
async fn upgrade(
    State(web): State<Web>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    let who = match (&q.token, &q.guest) {
        // A page of another origin may open this socket with the browser's cookie: only this origin's own may.
        (None, None) if !headers.contains_key(header::AUTHORIZATION) && cross_origin(&headers) => {
            Err(error(StatusCode::FORBIDDEN, "forbidden", "Another origin."))
        }
        _ => web.signed_principal(&headers, q.token.as_deref(), q.guest.as_deref(), &id).await,
    };
    let who = match (who, web.known(&id)) {
        (Err(e), _) if e.status == StatusCode::SERVICE_UNAVAILABLE => Err(close::UNAVAILABLE),
        (Err(e), _) if e.status == StatusCode::FORBIDDEN => Err(close::FORBIDDEN),
        (Err(_), _) => Err(close::UNAUTHORIZED),
        (_, Err(_)) => Err(close::NOT_FOUND),
        (Ok(who), Ok(())) => Ok(who),
    };
    ws.max_message_size(MAX_UPDATE + 4096).on_upgrade(move |mut socket: WebSocket| async move {
        let (who, version) = match who {
            Ok(who) => who,
            Err(code) => {
                let _ = socket.send(Message::Close(Some(CloseFrame { code, reason: "".into() }))).await;
                return;
            }
        };
        // Refused at the runtime's actor limit or a full gate: the socket closes with the session.
        if let Err(e) = web.open_session(&id, who, version, halves(socket)) {
            eprintln!("mdpad: session refused: {e}");
        }
    })
}

/// A websocket as a session's reader and writer.
fn halves(socket: WebSocket) -> (Reader, Writer) {
    let (write, read) = socket.split();
    let read = read.map(|m| match m {
        Ok(Message::Binary(bytes)) => In::Frame(bytes),
        Ok(Message::Close(_)) | Err(_) => In::Closed,
        Ok(_) => In::Other,
    });
    let write = write
        .with(|out: Out| {
            std::future::ready(Ok::<_, axum::Error>(match out {
                Out::Frame(bytes) => Message::Binary(bytes),
                Out::Close(code) => Message::Close(Some(CloseFrame { code, reason: "".into() })),
            }))
        })
        .sink_map_err(|e| e.to_string());
    (Box::pin(read), Box::pin(write))
}

/// The request names an `Origin` whose host is not the one it was sent to.
fn cross_origin(headers: &HeaderMap) -> bool {
    let value = |name| headers.get(name).and_then(|v: &HeaderValue| v.to_str().ok());
    match (value(header::ORIGIN), value(header::HOST)) {
        (Some(origin), Some(host)) => origin.split_once("://").map(|(_, rest)| rest) != Some(host),
        (Some(_), None) => true,
        (None, _) => false,
    }
}

/// A colour per name, the same on every node.
pub fn colour(name: &str) -> String {
    const PALETTE: [&str; 8] = ["#e0b252", "#5b8def", "#e06c75", "#56b6c2", "#98c379", "#c678dd", "#d19a66", "#61afef"];
    let hash = name.bytes().fold(0u32, |h, b| h.wrapping_mul(31).wrapping_add(b as u32));
    PALETTE[hash as usize % PALETTE.len()].to_string()
}

/// Is the row ready? The CLI's and tests' view of the catalog's id set.
pub fn ready(ids: &Ids, id: &str) -> bool {
    ids.lock().expect("the id set").get(id) == Some(&RowState::Ready)
}
