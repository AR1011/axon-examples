import { expect, type Page } from "@playwright/test";

/** The mock's password (mock/api.mjs). Each sign-in gets a world of its own, so tests do not meet. */
export const PASSWORD = "edge";

export async function signIn(page: Page, to = "/") {
  await page.goto(to);
  await page.getByLabel("Password").fill(PASSWORD);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page).not.toHaveURL(/\/login/);
}

/** Replaces the editor's text. `insertText` types it as one input, so the editor closes no brackets of its own. */
export async function type(page: Page, source: string) {
  await page.getByRole("textbox", { name: "Source" }).click();
  await page.keyboard.press("ControlOrMeta+A");
  await page.keyboard.insertText(source);
}

export const answer = (text: string) => `export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    console.log("asked " + request.method + " " + url.pathname);
    return new Response("${text} " + (env.GREETING ?? "none") + " " + (env.API_KEY ?? "no-secret"), { status: 202, headers: { "x-draft": "yes" } });
  },
};
`;

/** Picks an option of a select of the console. It is a listbox, not the browser's own select. */
export async function choose(page: Page, label: string, option: string | RegExp) {
  await page.getByRole("combobox", { name: label }).click();
  await page.getByRole("option", { name: option, exact: true }).click();
}
