import { expect, type Browser, type Page } from "@playwright/test";

/** A user name no other test takes: the mock signs in any name with the password "pw-<name>". */
export const someone = (prefix: string) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`;

/** Signs in as `name` on the login page the page shows. */
export async function fillLogin(page: Page, name: string) {
  await page.getByLabel("Name").fill(name);
  await page.getByLabel("Password").fill(`pw-${name}`);
  await page.getByRole("button", { name: "Sign in" }).click();
}

/** Goes to `to` through the login page as `name`. */
export async function signIn(page: Page, name: string, to = "/") {
  await page.goto(to);
  await expect(page).toHaveURL(/\/login\?next=/);
  await fillLogin(page, name);
  await expect(page).not.toHaveURL(/\/login/);
}

/** A page of a browser of its own, as another person, signed in as `name` when one is given. */
export async function another(browser: Browser, baseURL: string | undefined, name?: string, to = "/") {
  const context = await browser.newContext({ baseURL });
  const page = await context.newPage();
  if (name) await signIn(page, name, to);
  return page;
}

/** A call of the mock API with the page's cookie. */
export async function api<T = Record<string, unknown>>(page: Page, method: string, path: string, data?: unknown): Promise<T> {
  const response = await page.request.fetch(`/api/${path}`, { method, data });
  expect(response.ok(), `${method} ${path}: ${await response.text()}`).toBe(true);
  return (await response.json()) as T;
}

export const newDoc = async (page: Page, markdown: string) => (await api<{ id: string }>(page, "POST", "docs", { markdown })).id;

/** Sets what the mock server does for a document: close, notice, refuse, hold, type, max, version. */
export async function mock(page: Page, id: string, what: string, body: unknown = {}) {
  const response = await page.request.post(`/mock/docs/${id}/${what}`, { data: body });
  expect(response.ok()).toBe(true);
  return ((await response.json()) as { text: string }).text;
}

export const editor = (page: Page) => page.getByRole("textbox", { name: "Markdown text" });
export const badge = (page: Page) => page.getByTestId("badge");

/** Opens the document's editor and waits until it is joined. */
export async function open(page: Page, id: string) {
  await page.goto(`/d/${id}`);
  await expect(badge(page)).toContainText(/Saved|Read-only/);
}

/** Types at the end of the text. */
export async function typeAtEnd(page: Page, text: string) {
  await editor(page).click();
  await page.keyboard.press("ControlOrMeta+End");
  await page.keyboard.insertText(text);
}

/** Picks an option of a select of the page. It is a listbox, not the browser's own select. */
export async function choose(page: Page, label: string, option: string) {
  await page.getByRole("combobox", { name: label }).click();
  await page.getByRole("option", { name: option, exact: true }).click();
}
