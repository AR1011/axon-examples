import { expect, test, type Page } from "@playwright/test";
import { shots } from "./shots";

/** axon's own console on n1 (`KV_AXON_CONSOLE` to scripts/run.sh), not this page: its placement page shows the shards. */
const { KV_AXON_URL } = process.env;

const card = (page: Page, node: string) => page.locator(`[aria-label^="Placement board"] [data-node="${node}"]`);

test("axon's console on n1 places the 256 shards on the three nodes", async ({ page }) => {
  test.skip(!KV_AXON_URL, "KV_AXON_URL is not set");
  await page.goto(`${KV_AXON_URL}/placement`);
  const counts: number[] = [];
  for (const node of ["n1", "n2", "n3"]) {
    await expect(card(page, node)).toBeVisible();
    // The card's first line: the node, then its keys of every kind; kv runs one kind, `shard`.
    const line = card(page, node).locator(":scope > div").first();
    await expect(line).toHaveText(/\d+\s*$/);
    counts.push(Number((await line.innerText()).match(/(\d+)\s*$/)![1]));
  }
  expect(counts.every((n) => n > 0)).toBe(true);
  expect(counts.reduce((a, n) => a + n, 0)).toBe(256);
  await shots(page, "axon-placement");
  await page.goto(`${KV_AXON_URL}/placement?node=n1`);
  const keys = page.locator('[aria-label="Node"] [aria-label="Keys"] tbody tr');
  // A shard's key is its number in three digits, as `kv::slot::shard_key` makes it.
  await expect(keys.first().locator("td").first()).toHaveText(/^\d{3}$/);
  await shots(page, "axon-placement-n1");
});
