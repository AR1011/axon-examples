// The README's screenshots: node tools/shots/shot.mjs examples/mdpad/docs/shots.mjs --base http://127.0.0.1:<h1's web>/
// Against a fresh cluster of scripts/run.sh (its users ann, ben and dev), with `mdpad load` running on it, from the
// repository root on the same machine, with MDPAD_PORT, MDPAD_DIR and MDPAD_BIN as the cluster was started with: the
// shot of the banner kills a home and starts it again. The console is on MDPAD_PORT + 100.
const RUN = "examples/mdpad/scripts/run.sh";
const PORT = Number(process.env.MDPAD_PORT ?? 7300);
const CONSOLE = `http://127.0.0.1:${PORT + 100}/`;

const PLAN = `# Release plan 0.4

The release ships the search index and the new import. Ben owns the import, Ann the index.

## Dates

| Step | Who | When |
|---|---|---|
| Feature freeze | Ann | 14 October |
| Release candidate | Ben | 21 October |
| Release | Ann | 28 October |

## Open questions

- Do we keep the old import for one more release?
- Who writes the upgrade notes?

## Checks before the release

1. Every test passes on the release branch.
2. The upgrade from 0.3 keeps every document.
3. The notes name each change a user sees.
`;
const OTHERS = [
  "# Meeting notes, 6 October\n\n- The index is ready for review.\n- Import: two bugs left.\n",
  "# Onboarding checklist\n\n1. Get an account from an operator.\n2. Read the release plan.\n",
  "# Design: search index\n\nOne index a home, rebuilt from the log at start.\n",
  "# Reading list\n\n- Designing Data-Intensive Applications\n",
];

let id; // the release plan, on h2 or h3, so h1's web stays up when its home is killed
let ben; // Ben's page, open in a browser of its own: his caret and avatar show to Ann

const ok = async (answer, what) => {
  if (!answer.ok()) throw new Error(`${what}: ${answer.status()} ${await answer.text()}`);
  return answer.json();
};
const newDoc = async (request, markdown, homes) => {
  for (let i = 0; i < 30; i++) {
    const { id } = await ok(await request.post("api/docs", { data: { markdown } }), "create");
    if (homes.includes(id.split("-")[0])) return id;
    // On another home: into the trash and deleted for good, so the list has one of it.
    await ok(await request.delete(`api/docs/${id}`), "trash");
    await ok(await request.delete(`api/docs/${id}?purge=1`), "purge");
  }
  throw new Error(`no document on ${homes}`);
};
const badge = (page) => page.getByTestId("badge");
const editor = (page) => page.getByRole("textbox", { name: "Markdown text" });
const open = async (page, doc) => {
  await page.goto(`d/${doc}`);
  await badge(page).filter({ hasText: /Saved|Read-only/ }).waitFor({ timeout: 30_000 });
};
const typeAt = async (page, line, text) => {
  await editor(page).locator(".cm-line").filter({ hasText: line }).first().click();
  await page.keyboard.press("End");
  await page.keyboard.insertText(text);
};

export default {
  async login(page) {
    await ok(await page.request.post("api/login", { data: { name: "ann", password: "ann-pw" } }), "login");
  },
  shots: [
    {
      name: "editor",
      async steps(page, ctx) {
        id = await newDoc(page.request, PLAN, ["h2", "h3"]);
        for (const text of OTHERS) await newDoc(page.request, text, ["h1", "h2", "h3"]);
        await ok(await page.request.put(`api/docs/${id}/members/ben`, { data: { level: "edit" } }), "share");
        const context = await page.context().browser().newContext({ baseURL: ctx.base, viewport: { width: 1280, height: 800 } });
        await ok(await context.request.post("api/login", { data: { name: "ben", password: "ben-pw" } }), "ben");
        const theirs = await newDoc(context.request, "# Import: test files\n\n- A file of 120 KiB.\n", ["h1", "h2", "h3"]);
        await ok(await context.request.put(`api/docs/${theirs}/members/ann`, { data: { level: "view" } }), "share back");
        await ok(await page.request.post(`api/docs/${id}/versions`, { data: { name: "First draft" } }), "version");
        ben = await context.newPage();
        await open(ben, id);
        await open(page, id);
        await typeAt(page, "Who writes the upgrade notes?", "\n- Ann: a draft by Friday.");
        await badge(page).filter({ hasText: "Saved" }).waitFor();
        await page.waitForTimeout(2000);
        // Ben types last: his caret's name shows for 1.5 s after it moves.
        await typeAt(ben, "Ben owns the import", " He also writes the upgrade notes.");
        await page.locator(".md-caret-recent").waitFor({ timeout: 5_000 });
      },
      settle: 200,
    },
    {
      name: "docs",
      path: "",
      wait: "table",
      settle: 1500,
    },
    {
      name: "share",
      async steps(page) {
        await open(page, id);
        await ok(await page.request.put(`api/docs/${id}/share`, { data: { link: "view" } }), "link");
        await page.getByRole("button", { name: "Share" }).click();
        await page.getByRole("dialog").getByRole("row").filter({ hasText: "ben" }).waitFor();
      },
    },
    {
      name: "history",
      async steps(page) {
        await page.keyboard.press("Escape");
        await ok(await page.request.post(`api/docs/${id}/versions`, { data: { name: "Before the review" } }), "version");
        await typeAt(page, "The notes name each change a user sees.", "\n4. The console shows no new errors.");
        await badge(page).filter({ hasText: "Saved" }).waitFor();
        await page.getByRole("button", { name: "History" }).click();
        const history = page.getByTestId("history");
        await history.getByRole("button", { name: /First draft/ }).click();
        const version = page.getByTestId("version");
        await version.getByRole("tab", { name: "Changes" }).click();
        await version.getByTestId("changes").waitFor();
      },
    },
    {
      // The home of the document killed: the banner, and typing goes on. The next shot starts it again.
      name: "unreachable",
      async steps(page, ctx) {
        await open(page, id);
        ctx.sh(`${RUN} kill ${id.split("-")[0]}`);
        await page.locator('[data-notice="unreachable"]').waitFor({ timeout: 40_000 });
        await typeAt(page, "Ann: a draft by Friday.", "\n- Ben: test the import on a 120 KiB file.");
        await badge(page).filter({ hasText: "Server unreachable" }).waitFor();
      },
    },
    {
      name: "published",
      async steps(page, ctx) {
        ctx.sh(`${RUN} start ${id.split("-")[0]}`);
        await badge(page).filter({ hasText: "Saved" }).waitFor({ timeout: 60_000 });
        await ok(await page.request.put(`api/docs/${id}/share`, { data: { published: true } }), "publish");
        await page.goto(`p/${id}`);
        await page.getByRole("heading", { name: "Release plan 0.4" }).waitFor();
      },
    },
    {
      name: "admin",
      async steps(page) {
        await ok(await page.request.post("api/login", { data: { name: "dev", password: "dev-pw" } }), "dev");
        await page.goto("admin");
        const open = page.getByText("Open documents", { exact: true });
        await open.waitFor();
        await page.getByRole("table").nth(3).waitFor();
        await open.evaluate((el) => window.scrollTo(0, el.getBoundingClientRect().top + window.scrollY - 24));
      },
      settle: 2000,
    },
    {
      name: "console",
      async steps(page) {
        await page.goto(CONSOLE);
      },
      settle: 4000,
    },
  ],
};
