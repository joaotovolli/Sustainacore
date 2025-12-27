import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright";

const args = process.argv.slice(2);
const getArg = (name, fallback) => {
  const idx = args.indexOf(name);
  if (idx === -1) return fallback;
  return args[idx + 1] || fallback;
};

const mode = getArg("--mode", process.env.CONSTITUENTS_SCREENSHOT_MODE || "after");
const baseUrl = getArg("--base-url", "http://127.0.0.1:8002");
const screenshotDir = process.env.CONSTITUENTS_SCREENSHOT_DIR || "constituents";
const timeoutMs = Number(process.env.CONSTITUENTS_SCREENSHOT_TIMEOUT_MS || "90000");

const cwd = process.cwd();
const rootDir = cwd.endsWith("website_django") ? path.resolve(cwd, "..") : cwd;
const outDir = path.resolve(rootDir, "docs", "screenshots", screenshotDir, mode);
fs.mkdirSync(outDir, { recursive: true });

const buildToken = (email) => {
  const payload = Buffer.from(JSON.stringify({ email })).toString("base64url");
  return `header.${payload}.sig`;
};

const run = async () => {
  const browser = await chromium.launch();
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const page = await context.newPage();

  await page.goto(`${baseUrl}/tech100/?dg_debug=1`, { waitUntil: "networkidle", timeout: timeoutMs });
  await page.waitForFunction(
    () => document.documentElement.dataset.downloadGate === "ready",
    { timeout: 5000 }
  );
  await page.screenshot({ path: path.join(outDir, "constituents_preview.png"), fullPage: true, timeout: timeoutMs });

  await page.click("[data-constituents-locked]");
  await page.waitForSelector("#download-auth-modal.modal--open", { timeout: timeoutMs });
  await page.screenshot({ path: path.join(outDir, "constituents_modal.png"), fullPage: true, timeout: timeoutMs });

  await context.close();

  const loggedInContext = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const cookieDomain = new URL(baseUrl).hostname;
  await loggedInContext.addCookies([
    {
      name: "sc_session",
      value: buildToken("demo@example.com"),
      domain: cookieDomain,
      path: "/",
      httpOnly: true,
    },
  ]);
  const loggedInPage = await loggedInContext.newPage();
  await loggedInPage.goto(`${baseUrl}/tech100/`, { waitUntil: "networkidle", timeout: timeoutMs });
  await loggedInPage.screenshot({ path: path.join(outDir, "constituents_full.png"), fullPage: true, timeout: timeoutMs });
  await loggedInContext.close();

  await browser.close();
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
