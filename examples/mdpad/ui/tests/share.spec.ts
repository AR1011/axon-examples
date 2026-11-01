import { expect, test } from "@playwright/test";
import { another, api, badge, choose, editor, fillLogin, newDoc, open, signIn, someone, typeAtEnd } from "./helpers";

test("journey 2: an edit link, sign-in with next, Join, and the member listed as via link", async ({ page, browser, baseURL }) => {
  const [ana, ben] = [someone("ana"), someone("ben")];
  await signIn(page, ana);
  const id = await newDoc(page, "# Plan\n\nIntro.\n");
  await open(page, id);
  await page.getByRole("button", { name: "Share" }).click();
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByRole("radio", { name: "Off" })).toBeChecked();
  await dialog.getByRole("radio", { name: "Can edit" }).click();
  await expect(dialog.getByRole("button", { name: "Reset link" })).toBeVisible();
  const { token } = await api<{ token: string }>(page, "GET", `docs/${id}/share`);
  await expect(dialog).toContainText(`/s/${token}`);

  await dialog.getByRole("button", { name: "Close" }).click();

  const other = await another(browser, baseURL);
  await other.goto(`/s/${token}`);
  await expect(other).toHaveURL(/\/login\?next=/);
  await expect(other.getByText("You need an account. Ask your operator.")).toBeVisible();
  await fillLogin(other, ben);
  await expect(other.getByRole("heading", { name: "Join a shared document" })).toBeVisible();
  await expect(other.getByText("The link lets you edit it.")).toBeVisible();
  await other.getByRole("button", { name: "Join" }).click();
  await expect(other).toHaveURL(new RegExp(`/d/${id}$`));
  await expect(badge(other)).toContainText("Saved");
  await typeAtEnd(other, "Ben was here.");
  await expect(editor(page)).toContainText("Ben was here.");
  await expect(page.getByTestId("avatars")).toBeVisible();

  await page.getByRole("button", { name: "Share" }).click();
  const row = page.getByRole("dialog").getByRole("row").filter({ hasText: ben });
  await expect(row).toContainText("via link");
  await expect(row.getByRole("combobox", { name: `Access of ${ben}` })).toContainText("Can edit");

  await other.getByRole("link", { name: "Documents" }).click();
  await other.getByRole("tab", { name: "Shared with me" }).click();
  await expect(other.getByRole("row").filter({ hasText: "Plan" })).toContainText("Edit");
});

test("the owner invites by name, changes a level and removes a person, whose editor closes", async ({ page, browser, baseURL }) => {
  const [ana, ben] = [someone("ana"), someone("ben")];
  await signIn(page, ana);
  const id = await newDoc(page, "# Team\n");
  await open(page, id);
  await page.getByRole("button", { name: "Share" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("Invite a user").fill("nobody-here");
  await dialog.getByRole("button", { name: "Invite" }).click();
  await expect(dialog.getByRole("alert")).toContainText("No such user.");
  await dialog.getByLabel("Invite a user").fill(ben);
  await dialog.getByRole("button", { name: "Invite" }).click();
  const row = dialog.getByRole("row").filter({ hasText: ben });
  await expect(row).toContainText("invited");
  await expect(row.getByRole("combobox")).toContainText("Can edit");

  const other = await another(browser, baseURL, ben, `/d/${id}`);
  await expect(badge(other)).toContainText("Saved");
  await choose(page, `Access of ${ben}`, "Can view");
  await expect(row.getByRole("combobox")).toContainText("Can view");
  // An open editor learns it at its next edit, which the server refuses; a new one joins read-only.
  await other.reload();
  await expect(badge(other)).toContainText("Read-only");
  await expect(other.getByTestId("notices")).toContainText("You can only read this document.");

  await row.getByRole("button", { name: `Remove ${ben}` }).click();
  await expect(page.getByText(`${ben} removed`)).toBeVisible();
  await expect(row).toHaveCount(0);
  await expect(other.getByTestId("notices")).toContainText("Access removed.");
});

test("a member sees the people and the link read-only", async ({ page, browser, baseURL }) => {
  const [ana, ben] = [someone("ana"), someone("ben")];
  await signIn(page, ana);
  const id = await newDoc(page, "# Read only share\n");
  await api(page, "PUT", `docs/${id}/members/${ben}`, { level: "edit" });
  await api(page, "PUT", `docs/${id}/share`, { link: "view" });
  const other = await another(browser, baseURL, ben, `/d/${id}`);
  await other.getByRole("button", { name: "Share" }).click();
  const dialog = other.getByRole("dialog");
  await expect(dialog.getByText(`Only ${ana} can change who has access.`)).toBeVisible();
  await expect(dialog.getByRole("radio", { name: "Can view" })).toBeChecked();
  await expect(dialog.getByRole("radio", { name: "Off" })).toBeDisabled();
  await expect(dialog.getByRole("button", { name: "Reset link" })).toHaveCount(0);
  await expect(dialog.getByLabel("Invite a user")).toHaveCount(0);
  await expect(dialog.getByRole("row").filter({ hasText: ben })).toContainText("Can edit");
  await expect(dialog.getByRole("row").filter({ hasText: ana })).toContainText("Owner");
});

test("journeys 4 and 7: a view-link guest reads live, cannot type, and is closed when the link goes off", async ({ page, browser, baseURL }) => {
  const ana = someone("ana");
  await signIn(page, ana);
  const id = await newDoc(page, "# Public plan\n\nIntro.\n");
  await api(page, "PUT", `docs/${id}/share`, { link: "view" });
  const { token } = await api<{ token: string }>(page, "GET", `docs/${id}/share`);
  await open(page, id);

  const guest = await another(browser, baseURL);
  await guest.goto(`/s/${token}`);
  await expect(guest).toHaveURL(new RegExp(`/g/${token}$`));
  await guest.getByLabel("Your name").fill("Cleo");
  await guest.getByRole("button", { name: "Open" }).click();
  await expect(guest).toHaveURL(new RegExp(`/d/${id}$`));
  await expect(badge(guest)).toContainText("Read-only");
  await expect(guest.getByRole("button", { name: "History" })).toHaveCount(0);
  await expect(guest.getByRole("button", { name: "Share" })).toHaveCount(0);
  await expect(editor(guest)).toHaveAttribute("contenteditable", "false");
  await typeAtEnd(page, "Live line.");
  await expect(editor(guest)).toContainText("Live line.");

  await page.getByRole("button", { name: "Share" }).click();
  await page.getByRole("dialog").getByRole("radio", { name: "Off" }).click();
  await expect(guest.getByTestId("notices")).toContainText("Access removed.");
  await expect(badge(guest)).toContainText("No access");
  // It does not join again.
  await guest.waitForTimeout(1500);
  await expect(badge(guest)).toContainText("No access");
});

test("Reset link: the old link opens nothing, and Remove all via link removes the members who joined by it", async ({ page, browser, baseURL }) => {
  const [ana, ben] = [someone("ana"), someone("ben")];
  await signIn(page, ana);
  const id = await newDoc(page, "# Reset\n");
  await api(page, "PUT", `docs/${id}/share`, { link: "edit" });
  const { token } = await api<{ token: string }>(page, "GET", `docs/${id}/share`);
  const other = await another(browser, baseURL, ben);
  await api(other, "POST", `s/${token}/join`);

  await open(page, id);
  await page.getByRole("button", { name: "Share" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByRole("button", { name: "Reset link" }).click();
  await expect(page.getByText("New link made")).toBeVisible();
  await other.goto(`/s/${token}`);
  await expect(other.getByRole("heading", { name: "This link opens nothing" })).toBeVisible();

  await expect(dialog.getByRole("row").filter({ hasText: ben })).toContainText("via link");
  await dialog.getByRole("button", { name: "Remove all via link" }).click();
  await expect(dialog.getByRole("row").filter({ hasText: ben })).toHaveCount(0);
});

test("an operator's invite sets a password and signs in; a used invite is refused", async ({ page }) => {
  const cid = someone("cid");
  await page.goto(`/i/inv-${cid}`);
  await expect(page.getByRole("heading", { name: "Set your password" })).toBeVisible();
  await expect(page.getByText(`You are ${cid}.`)).toBeVisible();
  await page.getByLabel("Password", { exact: true }).fill("cids-secret");
  await page.getByLabel("Password again").fill("other-thing");
  await page.getByRole("button", { name: "Set password" }).click();
  await expect(page.getByRole("alert")).toContainText("The passwords are not the same.");
  await page.getByLabel("Password again").fill("cids-secret");
  await page.getByRole("button", { name: "Set password" }).click();
  await expect(page.getByRole("heading", { name: "Documents", exact: true })).toBeVisible();
  await page.goto(`/i/inv-${cid}`);
  await expect(page.getByRole("heading", { name: "This invite is not valid" })).toBeVisible();
  await expect(page.getByText("Ask your operator for a new one.")).toBeVisible();
});
