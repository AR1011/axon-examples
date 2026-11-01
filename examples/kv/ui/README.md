# The console of kv

A web page for `kv console` (`../src/console.rs`): where each of the 256 shards runs, the operations per second of
each node, and the bench runs in `../results`. React 19, Vite 8, TypeScript, Tailwind 4, TanStack Query, react-router 8
and echarts 6, with the components of [Kumo](https://kumo-ui.com/) (`@cloudflare/kumo`; icons from
`@phosphor-icons/react`), in the style of [edge-workers' console](../../edge-workers/ui): the same sidebar, page frame
and components, a mark of its own. Light first; `data-mode="dark"` on `<html>`, kept in `localStorage` (`kv-theme`).

```bash
npm install
npm run dev:mock     # the page on http://127.0.0.1:5173 with the mock API, no cluster
KV_CONSOLE=http://127.0.0.1:7380 npm run dev   # the page against a `kv console`
npm run build        # dist/, which is committed: `kv console` embeds it (../build.rs)
npm run lint
npm run e2e          # Playwright against dist and the mock API, at 1280 and at 375 px wide; build first
npm run shots        # the same tests, and screenshots/<width>-<page>.png
npm run e2e:live     # Playwright against a real cluster: see below
```

## The pages

| Page | Shows | From |
|---|---|---|
| Overview | the nodes that answer, the shards placed, operations and failures per second; a chart of each node's operations and one of reads, writes and failures, over 5 minutes or an hour; a row per node, and its `INFO` in a sheet | `/api/cluster` every 2 s, `/api/ops?minutes=5\|60` |
| Shards | each node's count of shards; a map of the 256 shards in the colour of their node, which a node filter dims and a click opens (its 64 slots, its lease epoch); the same as a list | `/api/cluster` |
| Runs | each run in the results directory, newest first: the box, the kind, rounds, rows, rows with errors, and the fault phases and the soak that passed | `/api/runs` |
| A run | the saturation rows as bars (the median of the rounds, per target and YCSB workload, for each table), `summary.md` as tables, the fault phases, `env.txt` | `/api/runs/{name}` |

Each page has a help dialog (the `?` at the top) that says how to read it.

## The API

`kv console` asks each node `INFO all` and `INFO kvhosted` on a new connection once a second, so a node that is
stopped, paused or cut off shows as not answering at the next sample. It counts only the commands that kv's command
table flags `readonly` or `write`: its own `INFO` is not load.

| Route | Answers |
|---|---|
| `GET /api/cluster` | `{at_ms, every_ms, slots_per_shard, nodes: [{addr, id, up, error, seen_ms, down_ms, mode, reads, pid, uptime_s, clients, shards_here, rate: {reads, writes, failed} \| null, info}], shards: [{hosts: [{node, epoch}]}]}`; a shard's `hosts` come from the nodes that answer, so it is empty while it moves |
| `GET /api/ops?minutes=n` | `{every_ms, nodes: [{addr, id, points: [[at_ms, reads, writes, failed]]}]}` over the last n minutes (1 to 60), at most 360 points a node; nulls where the node did not answer |
| `GET /api/runs` | `{dir, runs: [{name, date, host, kind, rounds, rows, error_rows, targets, faults: {phases, passed, soak} \| null, summary}]}` |
| `GET /api/runs/{name}` | the same fields, and `env`, `summary` (the text), `columns`, `rows` (`results.csv` as strings) and `faults` (the lines of `fault/faults.jsonl`) |
| anything else under `/api/` | 404 `{error: "not_found", message}` |

Every other path answers `index.html`, except a missing file with an extension (404). No sign-in: `kv console`
binds to loopback unless it is given `--public`.

## The mock

`mock/api.mjs` answers the same routes for three nodes with load that changes in time, and reads the runs from
`../tests/runs`. Each browser has a cluster of its own, by a cookie, so the tests can run at once. Two routes only the
mock has: `GET /api/_mock/down?node=n3` stops a node (its shards are on no node for 3 s, then on the others) and
`GET /api/_mock/up?node=n3` starts it again (its own shards come back after 3 s).

## The live tests

`tests-live/console.spec.ts` drives a real `kv console` in front of `../scripts/run.sh up durable 3`, started with
`--results examples/kv/tests/runs`. The tests run
in order in one browser.

| Variable | Is |
|---|---|
| `KV_URL` | the console, default `http://127.0.0.1:7380` (`run.sh console`) |
| `KV_LOAD_CMD` | a command that sends load and ends, for example `kv check --addrs $(run.sh addrs) --secs 20 --prefix live$(date +%s)`. Without it that test is skipped. Each run of the suite needs a new cluster or a new `--prefix`: a second check of the same keys reads numbers the first one wrote, which it did not send, and fails |
| `KV_KILL_CMD`, `KV_START_CMD` | commands that kill n3 with `kill -9` and start it again (`run.sh kill n3`, `run.sh start n3`). Without them that test is skipped |
| `SHOTS=1` | writes `screenshots/live-desktop-*.png` and `live-phone-*.png` |
