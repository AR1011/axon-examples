# The console of edge-workers

A web console for the operator's API of the control node (`../src/admin.rs`). React 19, Vite 8, TypeScript, Tailwind 4,
TanStack Query, react-router 8, echarts 6 and CodeMirror 6, with the components of [Kumo](https://kumo-ui.com/)
(`@cloudflare/kumo`, Cloudflare's component library on Base UI; icons from `@phosphor-icons/react`). The look is in the
style of the Cloudflare dashboard: Kumo's tokens for both themes (light first; `data-mode="dark"` on `<html>`), its
breadcrumbs, tabs, tables, dialogs, banners and toasts, and its `TimeseriesChart` for the charts. It is an
example product with a name and a mark of its own, and no Cloudflare logo.

**Sidebar.** `src/shell/Shell.tsx`, made with Kumo's tokens: Kumo's `Sidebar` does not open above the page. It is a
rail of icons. The pointer or the keyboard in it opens it above the page, which does not move. The button at its
bottom left, at the same place in every state, keeps it open beside the page, and the browser remembers that
(`localStorage`, `edge-sidebar`).

**Names.** A **Worker** is what you deploy and a **runner** is a node that runs Workers, in the console as in the
API. `src/api.ts` holds the API's paths and the role's name in one place (`WORKERS`, `workerPath()`, `RUNNER`); the
pages are `workers/..`.

```bash
npm install
npm run dev:mock     # the console on http://127.0.0.1:5173 with the mock API, password: edge
EDGE_API=http://127.0.0.1:7703 npm run dev    # the console against a control node (admin=addr)
npm run build        # dist/, which is committed: the control node embeds it (../build.rs, admin.rs)
npm run lint
npm run e2e          # Playwright against dist and the mock API; build first
npm run mock         # dist and the mock API on http://127.0.0.1:7799
npm run e2e:live     # Playwright against a real cluster: see below
npm run shots:live   # the same, and it writes screenshots/*.png
```

## The live tests

`tests-live/console.spec.ts` drives the console that a control node serves, on a cluster of `../scripts/run.sh`
(one control node, two gateways, three runners). The tests run in order in one browser and make the Workers
`hello`, `counter` and `draft`; they delete those first, so a run can follow a run.

| Variable | Is |
|---|---|
| `EDGE_URL` | the admin address of the control node, for example `http://127.0.0.1:7703` |
| `EDGE_GATEWAY` | gateway `g1`, for example `http://127.0.0.1:7701`. It must be the URL that `g1` gives the cluster (its `http=` address, or `public=`): the tests look for it in the pages |
| `EDGE_PASSWORD` | the admin password (`edge-dev` from `run.sh`) |
| `EDGE_LOAD_CMD` | a command that sends load to `hello` and ends, for example `edge load --gateway .. --mix hello --rps 150 --duration 30`. Without it the tests send a smaller load from the browser's machine |
| `EDGE_KILL_CMD`, `EDGE_START_CMD` | commands that kill runner `r3` with `kill -9` and start it again (`run.sh start r3`). Without them the test of a killed runner is skipped |

With the cluster on another machine, forward the two ports (`ssh -N -L 7703:127.0.0.1:7703 -L 7701:127.0.0.1:7701
host`) and give the two commands as `ssh host '..'`.

## What the server does to serve `dist`

`admin.rs` does this (`console`, and `base=` for the base path); `mock/server.mjs` does the same for the mock.

| | |
|---|---|
| Base path | Any. Every asset path in `dist` is relative (`./assets/..`), and `index.html` holds `<base href="/" />`. A server that mounts the console at another path replaces that tag, for example with `<base href="/ops/edge/" />` (with the `/` at the end), as `crates/axon-obs/src/dashboard/api.rs` does. At `/` it sends the file as it is. |
| Pages | Every path under the base that is not a file of `dist` answers `index.html` (the pages are `login`, `workers`, `workers/{name}/{tab}`, `cluster`). |
| API prefix | `/api/`, absolute, as `Admin::router` nests it. It is not below the base path. `index.html` holds `<meta name="edge-api" content="/api/" />`; a server that has the API at another prefix replaces the `content`. |
| Session | The `edge_session` cookie of `POST /api/login` (`Path=/`, HttpOnly, SameSite=Strict). The console and the API must have the same origin. The console sends `{password}` without `token`. |
| 401 | Any 401 from the API, on any page, sends the browser to `login?next=<page>`. The login page asks `GET /api/session` first and goes on if the answer is 200. |
| Errors | The console reads `{error, message}` and shows `<status> <error>: <message>` as it is. |

`mock/server.mjs [port] [base]` is such a server; the e2e tests run the same `dist` at `/` and at `/ops/edge/`.

## Pages

| Page | Shows | API |
|---|---|---|
| Login | the password; wrong password; a refusal as the API says it | `GET /session`, `POST /login`, `POST /logout` |
| Workers | each Worker: its URL at the first gateway (a link, a copy button, and how many more gateways serve it), active version, requests, errors, p50 and p99 of CPU and wall time; create from a template | `GET /workers`, `GET /templates`, `POST /workers {name, template, message, deploy}` |
| Worker, header | the name, the deployment, and the URL at each gateway (`urls`): a link and a copy button | `GET /workers/{name}` |
| Worker, Overview | totals, charts of the last hour (requests and errors per second, CPU time, wall time), the numbers by runner, the version each runner serves | `GET .../metrics`, `GET /cluster` |
| Worker, Code | CodeMirror editor of a version's source (the active one first), save as a new version, save and deploy, deploy the saved version; a preview of the unsaved code with a request builder; a call of the deployed version, and the URL of that request at the first gateway | `GET .../versions/{id}`, `POST .../versions {source, message, deploy}`, `POST .../deploy {version}`, `POST .../preview {source, request}`, `POST .../invoke` |
| Worker, Versions | the deployment, roll back, canary (start, change, promote, stop), the versions, a version's source and its changes from the active version | `GET /workers/{name}`, `POST .../rollback {}`, `POST .../deploy {version, canary}` |
| Worker, Settings | variables and secrets (a secret shows as Hidden; set, replace, remove), CPU and memory limits, object bindings, delete the Worker | `GET .../vars`, `PUT`/`DELETE .../vars/{name}`, `POST .../versions {cpu_ms, memory_mb, message}`, `DELETE /workers/{name}` |
| Worker, Logs | live tail (pause, clear, filter by status and text), the recent invocations filtered by the API, an invocation by its trace id | `GET .../tail` (SSE), `GET .../invocations?status=&q=&limit=200`, `GET /invocations/{trace}` |
| Worker, Durable Objects | each Durable Object: class, id, runner, epoch, storage; its keys and values; delete the storage after a question in the page | `GET .../objects`, `GET`/`DELETE .../objects/{class}/{id}` |
| Cluster | each node: roles, address, the URL of a gateway, state, in flight, logs dropped, the versions each runner serves | `GET /cluster` |

Every view has a loading, an empty and an error state. A refusal of the API and a warning show in the page (a Kumo
banner); a change that went well shows as a toast. A question before a change that loses something is a Kumo alert
dialog. No page uses `window.confirm` or `window.alert`.

## The mock API

`mock/api.mjs` answers every route of `admin.rs` with the same field names and types. `npm run dev:mock` puts it in the
Vite dev server; `mock/server.mjs` serves it with `dist`. Each sign-in gets a state of its own (four workers, six
nodes, a steady made-up load), so parallel tests do not meet. A preview and an invoke run the JavaScript they are given
in the node process, with `Request` and `Response` of node: use the mock on your own machine only.

## What the console assumes of the API

Read from `admin.rs`, `wire.rs`, `observer.rs` and `gateway.rs`; the Rust side must keep these, or the console changes
with it.

- A worker has `urls`, a list of `{gateway, url}` in the order of the gateways' ids: one for each gateway that is up
  and gave the cluster its URL. `url` ends with `/w/<name>/`. A node of `GET /cluster` has `url`, null but for a gateway.
- `headers` of the request of a preview and of an invoke are a map, `{"name": "value"}`. `headers` of the response are
  a list of pairs, `[["name", "value"], ..]`.
- A canary is `{version, percent}` or null, in `deployment.canary` of a worker and in `serving` of `GET /cluster`.
  A runner's `serving` entry is `{stable, canary, seq, warm}`; `warm` is true while the runner has the version loaded.
- `GET .../metrics` has one `series` for the worker over all runners (a `Point` per `sample_ms`), not one per runner.
  So the charts are of all runners, and "by node" is the table of `by_node` counters. `requests` and `errors` of a
  point are counts in one sample; the console divides by `sample_ms / 1000` to draw a rate.
- The counters of `GET /workers` and of `total` and `by_node` count from the start of each runner, not over a window.
  The console says so below the tables. It has no sparkline per worker, because the list has no series.
- Times are microseconds (`cpu_us`, `wall_us`, `cpu_p50_us`, ..) and milliseconds since the epoch (`at_ms`,
  `created_ms`). A percentile is null before the first request.
- `POST .../versions` and `POST /workers` answer 201 with `seq: null` when `deploy` is false.
- `POST .../rollback` takes `{}` (the console sends no `version`) and answers `{seq, version, from}`.
- `PUT .../vars/{name}` takes `{type: "plain" | "secret", value}` and always deploys the new version.
- A `LogRecord` is `{at_ms, worker, version, node, trace, method, url, status, outcome, cpu_us, wall_us, logs,
  truncated}`; `trace` can be null. `url` is absolute for a request through a gateway and a path for the
  console's own invoke; the console shows the path. The tail sends events named `invocation` (a `LogRecord`)
  and `missed` (a count).
- An object's `value` in `GET .../objects/{class}/{id}` is any JSON; the console shows it as JSON text.
- An object's id is one path segment, percent-encoded by the console.

Checked on a real cluster (the live tests hold most of these):

- **A preview of code that throws** answers 200 with `status: 500`, `error: "threw: <message>"`, no headers and an
  empty body. Its `logs` end with `Error while running request handler: <message>`, `Stack:` and the frames. A preview
  past its CPU limit has `status: 503` and an `error` that says so.
- **The message of a 422 `load`** has more than one line: `the source did not load: ..`, then the engine's
  `SyntaxError` with the line and a caret. The console keeps the line breaks.
- **A body that is not JSON, or lacks a field** is axum's own rejection: 400, 415 or 422 with a text body. The console
  shows the status and that text.
- **`invoke` when the worker is not deployed** answers 200 with `status: 503`, `error: "unavailable"` and
  `retry_after_ms: 1000`. An invoke of code that throws has `status: 500`, `error: "exception"` and the body
  `the worker threw: ..`. A gateway answers 429 to a new account in its first second.
- **A runner that stops.** For about a second it is in `silent` of `GET .../metrics` and `reachable: false` in
  `GET /cluster`; then it is not in `nodes` at all. The console keeps a node that it saw in this page and shows it as
  "Not in the cluster", with the time, until the node is back. A runner that starts again has a new address, and its
  counters start at zero.
- **`epoch`** is the number of the object's lease, 16 digits; `GET .../objects/{class}/{id}` says 0 for an object that
  never wrote. An object whose storage was deleted stays in the list while it runs, with storage Empty.
- **A worker's requests go to one runner first** (the gateway's order of runners for the worker), so with a small
  load "By runner" shows one runner with all the requests. The others have 0, or no row before they have counters for
  the worker. They still show as Loaded in "Deployment", because each runner loads a version when it is deployed.
- **The size of a version** is `source_bytes`, the length of its source. `build_bytes` is the length of the
  component the engine built from it (about 11.5 MiB for any source): the console shows it as Build where it shows
  one version.
- **`cold_starts`** counts a load after a runner put an idle worker away, not the first load.
- **The tail without a session** answers 401 with the JSON body. A browser cannot read the status of a failed
  EventSource, so on an error the console asks `GET /api/session`; a 401 there sends the browser to the login page.
  Not checked in a browser with a session that ends while the tail is open.

## Not done

- Triggers (routes, cron) and the daily budget views of the plan: the API has no route for them.
- A sparkline per worker in the list, charts by node or by version, and a time range other than the last hour: the
  API has one series per worker.
- Object bindings cannot be changed in the console (the upload takes `objects`; the Settings tab only shows them).
- The tail filters in the page, so a change of filter does not open the stream again. The recent list filters in the
  API. Neither has a filter by node.
- The URL of a Worker by host name (`domain=`): the API gives the path form only.
- No link to axon's own console, and no warning when you leave the Code tab with changes that are not saved.
- No unit tests (the house `vitest` setup is left out); the e2e tests cover the pages.
- The mock has no runner that stops; only the live tests cover that.
