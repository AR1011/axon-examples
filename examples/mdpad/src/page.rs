//! The browser page, as `npm run build` in `ui/` left it (`build.rs` refuses a build without it): its files, and the
//! page itself for every other path, so the page's router reads the path. A path under `/api` that is no route, and a
//! file the page does not have, is a 404, so a mistyped asset is not answered with the page.

use axum::extract::Request;
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use include_dir::{Dir, include_dir};

use crate::web::error;

static UI: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/ui/dist");

/// Scripts, styles and fonts from this origin only, and no other site may frame the page. Images may come from
/// anywhere, as a document shows them by URL.
const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: https: \
                   http:; font-src 'self'; connect-src 'self'; worker-src 'self'; frame-ancestors 'none'; \
                   base-uri 'self'; form-action 'self'; object-src 'none'";

pub async fn serve(request: Request) -> Response {
    let whole = request.uri().path();
    let path = whole.trim_start_matches('/');
    let not_found = || error(StatusCode::NOT_FOUND, "not_found", format!("Nothing at {whole}.")).into_response();
    if whole == "/api" || whole.starts_with("/api/") || !matches!(*request.method(), Method::GET | Method::HEAD) {
        return not_found();
    }
    let secured = |mut response: Response, cache: &'static str| {
        let headers = response.headers_mut();
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
        headers.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
        headers.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static(CSP));
        headers.insert(header::REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
        response
    };
    if let Some(file) = UI.get_file(path).filter(|_| path != "index.html") {
        let mime = match path.rsplit('.').next() {
            Some("js") => "text/javascript",
            Some("css") => "text/css",
            Some("svg") => "image/svg+xml",
            Some("woff2") => "font/woff2",
            Some("json") => "application/json",
            Some("png") => "image/png",
            _ => "application/octet-stream",
        };
        // A built file's name holds the hash of what is in it; the service worker's does not, and must be fresh.
        let cache = if path.starts_with("assets/") { "public, max-age=31536000, immutable" } else { "no-cache" };
        return secured(([(header::CONTENT_TYPE, mime)], file.contents()).into_response(), cache);
    }
    match UI.get_file("index.html").filter(|_| is_page(path)) {
        Some(index) => {
            let response = ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], index.contents()).into_response();
            secured(response, "no-cache")
        }
        None => not_found(),
    }
}

/// Whether a path the page has no file for is the page's: a route, not a file of a kind the build makes. A route's
/// last part may hold dots, as the signed tokens of `/i/`, `/s/` and `/g/` do.
fn is_page(path: &str) -> bool {
    let last = path.rsplit('/').next().unwrap_or(path);
    let kind = last.rsplit_once('.').map(|(_, kind)| kind);
    !matches!(kind, Some("js" | "css" | "svg" | "woff2" | "json" | "png" | "ico" | "map" | "webmanifest" | "txt"))
}

#[cfg(test)]
mod tests {
    use super::is_page;

    #[test]
    fn routes_with_dots_are_the_page_and_missing_files_are_not() {
        for route in
            ["", "index.html", "login", "d/h1-abc", "i/ann.0.1792280908236.96f1", "s/h2-e7v2.0.eadb", "g/h2-e7v2.0.ea"]
        {
            assert!(is_page(route), "{route}");
        }
        for file in ["assets/index-x.js", "assets/gone.css", "favicon.ico", "sw.js", "manifest.webmanifest"] {
            assert!(!is_page(file), "{file}");
        }
    }
}
