import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright";

const args = process.argv.slice(2);
const getArg = (name, fallback) => {
  const idx = args.indexOf(name);
  if (idx === -1) return fallback;
  return args[idx + 1] || fallback;
};

const mode = getArg("--mode", process.env.DOWNLOAD_SCREENSHOT_MODE || "after");
const baseUrl = getArg("--base-url", "http://127.0.0.1:8002");
const screenshotDir = process.env.DOWNLOAD_SCREENSHOT_DIR || "downloads";
const timeoutMs = Number(process.env.DOWNLOAD_SCREENSHOT_TIMEOUT_MS || "90000");

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
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    acceptDownloads: true,
  });
  const page = await context.newPage();

  await page.addStyleTag({
    content: "* { transition: none !important; animation: none !important; }",
  });

  await page.goto(`${baseUrl}/tech100/performance/`, { waitUntil: "networkidle", timeout: timeoutMs });
  await page.waitForFunction(
    () => document.documentElement.dataset.downloadGate === "ready",
    { timeout: timeoutMs }
  );
  await page.evaluate(() => {
    sessionStorage.removeItem("sc_pending_download");
    sessionStorage.removeItem("sc_pending_next");
    const modal = document.querySelector("#download-auth-modal");
    if (!modal) return;
    modal.classList.remove("modal--open", "is-visible");
    modal.setAttribute("aria-hidden", "true");
    modal.hidden = true;
  });
  await page.evaluate(() => {
    const link = document.querySelector("a[href*=\"/tech100/performance/export/\"]");
    if (link) link.click();
  });
  await page.waitForSelector(".modal--open", { timeout: timeoutMs });
  await page.screenshot({ path: path.join(outDir, "download_modal_email.png"), fullPage: true, timeout: timeoutMs });

  await page.evaluate(() => {
    const modal = document.querySelector("[data-download-modal]");
    if (!modal) return;
    const emailStep = modal.querySelector("[data-step='email']");
    const codeStep = modal.querySelector("[data-step='code']");
    const emailDisplay = modal.querySelector("[data-email-display]");
    const emailInput = modal.querySelector("[data-email-input]");
    if (emailInput) emailInput.value = "demo@example.com";
    if (emailDisplay) emailDisplay.textContent = "demo@example.com";
    if (emailStep) emailStep.hidden = true;
    if (codeStep) codeStep.hidden = false;
  });
  await page.screenshot({ path: path.join(outDir, "download_modal_code.png"), fullPage: true, timeout: timeoutMs });

  const cookieDomain = new URL(baseUrl).hostname;
  await context.addCookies([
    {
      name: "sc_session",
      value: buildToken("demo@example.com"),
      domain: cookieDomain,
      path: "/",
      httpOnly: true,
    },
  ]);
  await page.goto(`${baseUrl}/tech100/performance/`, { waitUntil: "networkidle", timeout: timeoutMs });
  const downloadPromise = page.waitForEvent("download", { timeout: 15000 }).catch(() => null);
  await page.evaluate(() => {
    const link = document.querySelector("a[href*=\"/tech100/performance/export/\"]");
    if (link) link.click();
  });
  await downloadPromise;
  await page.screenshot({ path: path.join(outDir, "download_success.png"), fullPage: true, timeout: timeoutMs });

  await context.close();
  await browser.close();
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
