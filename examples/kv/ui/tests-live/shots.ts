import type { Page } from "@playwright/test";

/** With SHOTS=1: screenshots/live-{desktop,phone}-<name>.png, at desktop and at phone width. */
export async function shots(page: Page, name: string) {
  if (!process.env.SHOTS) return;
  await page.waitForTimeout(1200); // the charts draw their lines in about a second
  await page.screenshot({ path: `screenshots/live-desktop-${name}.png` });
  await page.setViewportSize({ width: 375, height: 812 });
  await page.waitForTimeout(800);
  await page.screenshot({ path: `screenshots/live-phone-${name}.png` });
  await page.setViewportSize({ width: 1280, height: 760 });
}
