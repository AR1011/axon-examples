# The console of workflows

A web console for the HTTP API of the api nodes (`../src/api.rs`). React 19, Vite 8, TypeScript, Tailwind 4, TanStack
Query, react-router 8 and echarts 6, with the components of [Kumo](https://kumo-ui.com/) (`@cloudflare/kumo`; icons
from `@phosphor-icons/react`), in the style of the console of `../../edge-workers/ui`: Kumo's tokens for both themes,
its breadcrumbs, tabs, tables, dialogs, banners and toasts. It is an example product with a mark of its own.

Each page asks the API again every 2 s (`POLL` in `src/queries.ts`). When an answer does not come, the page keeps the
data it has and says from when it is. Explanations are in the help dialog of each page (the `?` button in its header).

```bash
npm install
npm run dev:mock     # the console on http://127.0.0.1:5173 with the mock API, password: wf
WF_API=http://127.0.0.1:7704 npm run dev    # the console against an api node of ../scripts/run.sh (e1, the default)
npm run build        # dist/, which is committed: the api node embeds it (../build.rs, ../src/api.rs)
npm run lint
npm run e2e          # Playwright against dist and the mock API, at / and at /ops/wf/; build first
npm run mock         # dist and the mock API on http://127.0.0.1:7789
npm run e2e:live     # Playwright against a real cluster: see below
npm run shots:live   # the same, and it writes screenshots/*.png
```

## The live tests

`tests-live/console.spec.ts` drives the console that an api node serves, on a cluster of `../scripts/run.sh` (three
history nodes h1 to h3 and two engines e1 and e2, each an api node too, and workers w1 and w2). The tests run in
order in one browser. Each run of the suite starts runs with new ids (`live-<time>-..`), so a run can follow a run.

1. Sign in through a deep link, with a wrong password first.
2. A checkout waits for its `approve` signal. The signal dialog sends it, and the run completes.
3. A declined card fails the run. Retry runs the failed step again, and it fails again.
4. A sleeper shows in the runs list with its owner and its wake time. Cancel ends it.
5. Kill the engine that owns a waiting checkout. Within 60 s the timeline shows "moved to" the other engine. The
   signal then completes the run, with the charge done once.
6. The overview, workflows, queues and cluster pages, in both themes.
7. No page moves as it polls (layout shift under 0.01 in 4.5 s), at a desktop's width and at a phone's (390 px),
   and no page scrolls sideways at a phone's width.

| Variable | Is |
|---|---|
| `WF_URL` | the api address of a node that the kill does not stop, for example `http://127.0.0.1:7701` (h1). Default: that |
| `WF_PASSWORD` | the admin password (`wf-dev` from `run.sh`) |
| `WF_KILL_CMD`, `WF_START_CMD` | commands that kill a node with `kill -9` and start it again, with `{node}` for its name: `../scripts/run.sh kill {node}` and `../scripts/run.sh start {node}`. Without them the test of the move is skipped |

With the cluster on another machine, forward the port (`ssh -N -L 7701:127.0.0.1:7701 host`) and give the two
commands as `ssh host '..'`.

## What the server does to serve `dist`

`console` in `../src/api.rs` does this (with `base=` for the base path); `mock/server.mjs` does the same for the mock.
`tests/api.rs::the_console_is_served_beside_the_api_under_its_base_path` holds the Rust side.

| | |
|---|---|
| Base path | Any. Every asset path in `dist` is relative (`./assets/..`), and `index.html` holds `<base href="/" />`. The api node replaces that tag with its `base=` (with a `/` at each end). It takes a path whole or with the base cut, so a proxy can do either. |
| Pages | Every path outside `assets/` answers `index.html`: `login`, `overview`, `runs`, `runs/{id}` (an id may hold a `/` or a `.`), `workflows`, `queues`, `cluster`. A file under `assets/` that `dist` does not have is a 404. |
| Headers | Files under `assets/` are immutable for a year (their names hold a hash); the page is `no-store`. Each has a CSP that allows only this origin, `nosniff` and `no-referrer`. |
| API prefix | `/api/`, absolute, not below the base path. `index.html` holds `<meta name="wf-api" content="/api/" />`; a server that has the API at another prefix replaces the `content`. `GET /ready` is beside it, with no credentials. |
| Session | The `wf_session` cookie of `POST /api/login` (`Path=/`, HttpOnly, SameSite=Strict). The console and the API must have the same origin. |
| 401 | Any 401 from the API sends the browser to `login?next=<page>`. The login page asks `GET /api/session` first and goes on if a name comes back. |
| Errors | The console reads `{error, message}` and shows the message as it is. |

## Pages

| Page | Shows | API |
|---|---|---|
| Login | the password; wrong password | `GET /session`, `POST /login`, `POST /logout` |
| Overview | runs started, completed and failed per minute; the counts (running, sleeping, paused, parked, stuck, failed in the last hour, waiting over 7 days), each a link to the runs it counts | `GET /metrics` |
| Runs | the runs with their status, owner, what they wait for and their next timer, filtered by workflow, status, tag, parent, id prefix and start time, a page of 50 at a time, held between polls with a button for the changes; start a run (workflow, version, id, input, tags, run timeout) with its `curl` | `GET /runs?workflow=&status=&tag=&parent=&prefix=&since=&limit=&cursor=`, `GET /workflows`, `POST /runs` |
| Run, Timeline | the steps and what the run went through, in journal order: opened, moved to another engine, signals, pauses, retries, its end. A step opens to its input, output, attempts and last error. 200 rows first, then more | `GET /runs/{id}` (`steps`, `events`) |
| Run, header | status, owner and epoch, actions: signal, pause, resume, retry, cancel, terminate, delete; a parked run's reason; its children | `POST /runs/{id}/signals/{name}`, `/pause`, `/resume`, `/retry`, `/cancel`, `/terminate`, `DELETE /runs/{id}`, `GET /runs?parent=` |
| Run, History | the journal entries as JSON, a page at a time | `GET /runs/{id}/history?after=` |
| Workflows | each workflow, the versions this build has, its runs by status, runs on an old version and on a version that is missing (parked) | `GET /workflows` |
| Queues and workers | each queue's backlog (tasks and bytes), its oldest task, pollers, slots in use of the total, attempts failed in the last minute and refused; each worker, its state, slots in use per queue and its last poll | `GET /queues`, `GET /workers` |
| Cluster | the history nodes up of all, quorum, warming, isolated; each member and its roles | `GET /cluster` |

Every view has a loading, an empty and an error state. Below its bar each page shows a banner when the journal has no
quorum (from `/ready`; the actions are then off, with the reason), and one when its data is old. A refusal of the API
shows in the page; a change that went well shows as a toast. A question before a change that loses something is a Kumo
alert dialog. No page uses `window.confirm` or `window.alert`.

## The mock API

`mock/api.mjs` answers every route of `api.rs` that the console calls, with the same field names and types.
`npm run dev:mock` puts it in the Vite dev server; `mock/server.mjs [port] [base]` serves it with `dist`. Each sign-in
gets a world of its own, so parallel tests do not meet: a checkout that moved from e1 to e2 and waits for `approve`,
failed, parked, sleeping and paused runs, a batch with 12 children, a run of 10,000 steps, 140 more runs, two queues
(one with no pollers), two workers (one draining) and a cluster with h3 down. A signal, an action and a start change
that world.

## What the console assumes of the API

- Times of a run's steps and events are microseconds of the wall clock of the node that journaled them (`started_us`,
  `ended_us`, `at_us`); times of the list and the metrics are milliseconds (`started_ms`, `updated_ms`, `at_ms`).
  `oldest_ms` of a queue is an age, not a time, as is a worker's `last_poll_ms`, which is 0 while a poll is out.
- A step and an event carry `seq`, the index of their first journal entry. The timeline sorts by it.
- An event's `kind` is one of the `RunEvent` kinds in `src/api.ts`; `text` is the node, the signal's name, the reason
  or the error.
- `cut` of `GET /runs` is true when the replica stopped at 10,000 index rows; the page then says the list is not all.
- An action answers 202 `{}`; a signal answers 202 `{signal_id, duplicate}`; a start answers 201 or 200
  `{id, version, created}`; a deleted run answers 404 `not_found`.
- `GET /ready` answers 200 or 503, both with `{journal, warming, isolated, quotas_starting}`.

## Not done

- The overview's sleeping, stuck and old counts link to the running runs, as the list has no filter for them.
- A runs row says "a call" for a run that waits for a signal or a result, not the signal's name.
- A step shows its number of attempts and the last error, not each attempt.
- History leaves out the `Own` entries (an owner's mark at an open); the timeline shows them as opened and moved.
- No unit tests; the e2e tests cover the pages.
