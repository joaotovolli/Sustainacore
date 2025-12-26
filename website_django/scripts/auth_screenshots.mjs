import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright";

const args = process.argv.slice(2);
const getArg = (name, fallback) => {
  const idx = args.indexOf(name);
  if (idx === -1) return fallback;
  return args[idx + 1] || fallback;
};

const mode = getArg("--mode", process.env.AUTH_SCREENSHOT_MODE || "after");
const baseUrl = getArg("--base-url", "http://127.0.0.1:8002");
const screenshotDir = process.env.AUTH_SCREENSHOT_DIR || "auth";
const timeoutMs = Number(process.env.AUTH_SCREENSHOT_TIMEOUT_MS || "60000");

const loginSession = process.env.AUTH_LOGIN_SESSION || "";
const accountSession = process.env.AUTH_ACCOUNT_SESSION || "";
const accountToken = process.env.AUTH_ACCOUNT_TOKEN || "demo-token";

const cwd = process.cwd();
const rootDir = cwd.endsWith("website_django") ? path.resolve(cwd, "..") : cwd;
const outDir = path.resolve(rootDir, "docs", "screenshots", screenshotDir, mode);
fs.mkdirSync(outDir, { recursive: true });

const cookieDomain = new URL(baseUrl).hostname;

const cookieFor = (name, value) => ({
  name,
  value,
  domain: cookieDomain,
  path: "/",
  httpOnly: name === "sc_session",
});

const run = async () => {
  const browser = await chromium.launch();
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
  });
  const page = await context.newPage();

  await page.addStyleTag({
    content: "* { transition: none !important; animation: none !important; }",
  });

  await page.goto(`${baseUrl}/login/`, { waitUntil: "networkidle", timeout: timeoutMs });
  await page.screenshot({ path: path.join(outDir, "login_email.png"), fullPage: true, timeout: timeoutMs });

  if (loginSession) {
    await context.addCookies([cookieFor("sessionid", loginSession)]);
  }
  await page.goto(`${baseUrl}/login/code/`, { waitUntil: "networkidle", timeout: timeoutMs });
  await page.screenshot({ path: path.join(outDir, "login_code.png"), fullPage: true, timeout: timeoutMs });

  if (accountSession) {
    await context.addCookies([
      cookieFor("sessionid", accountSession),
      cookieFor("sc_session", accountToken),
    ]);
  }
  await page.goto(`${baseUrl}/account/`, { waitUntil: "networkidle", timeout: timeoutMs });
  await page.screenshot({ path: path.join(outDir, "account.png"), fullPage: true, timeout: timeoutMs });

  await page.goto(`${baseUrl}/login/`, { waitUntil: "networkidle", timeout: timeoutMs });
  await page.screenshot({ path: path.join(outDir, "header_logged_in.png"), fullPage: true, timeout: timeoutMs });

  await context.close();
  await browser.close();
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
