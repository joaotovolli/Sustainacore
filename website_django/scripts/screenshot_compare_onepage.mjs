import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright";
import pixelmatch from "pixelmatch";
import { PNG } from "pngjs";

const beforeUrl = process.env.BEFORE_URL || "https://sustainacore.org/";
const afterUrl = process.env.AFTER_URL || "https://preview.sustainacore.org/";
const authUser = process.env.SCREENSHOT_BASIC_AUTH_USER || "";
const authPass = process.env.SCREENSHOT_BASIC_AUTH_PASS || "";
const timeoutMs = Number(process.env.TIMEOUT_MS || "60000");
const ignoreHttpsErrors = ["1", "true"].includes((process.env.SCREENSHOT_IGNORE_HTTPS_ERRORS || "").toLowerCase());
const outDir = process.env.OUT_DIR || "/tmp/ui_shots";
const viewport = { width: 1440, height: 900 };

const heartbeatIntervalMs = 1500;
let lastProgress = "init";
const progress = (message) => {
  lastProgress = message;
  process.stdout.write(`${message}\n`);
};
const heartbeat = setInterval(() => {
  process.stdout.write(`[compare] heartbeat ${lastProgress}\n`);
}, heartbeatIntervalMs);

const withTimeout = (promise, ms, label) => {
  let timeoutId = null;
  const timeout = new Promise((_, reject) => {
    timeoutId = setTimeout(() => {
      reject(new Error(`Timeout after ${ms}ms (${label})`));
    }, ms);
  });
  return Promise.race([promise, timeout]).finally(() => {
    if (timeoutId) clearTimeout(timeoutId);
  });
};

const readPng = (filePath) => PNG.sync.read(fs.readFileSync(filePath));

const writeDiff = (beforePath, afterPath, diffPath) => {
  const before = readPng(beforePath);
  const after = readPng(afterPath);
  const width = Math.min(before.width, after.width);
  const height = Math.min(before.height, after.height);
  const diff = new PNG({ width, height });
  pixelmatch(before.data, after.data, diff.data, width, height, { threshold: 0.1 });
  fs.writeFileSync(diffPath, PNG.sync.write(diff));
};

const capture = async (label, url, auth) => {
  progress(`[compare] launch start ${label}`);
  const browser = await withTimeout(
    chromium.launch({
      headless: true,
      args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    }),
    timeoutMs,
    "chromium.launch"
  );
  progress(`[compare] launch done ${label}`);
  const context = await withTimeout(
    browser.newContext({
      viewport,
      httpCredentials: auth ? { username: authUser, password: authPass } : undefined,
      ignoreHTTPSErrors: auth ? ignoreHttpsErrors : false,
    }),
    timeoutMs,
    "browser.newContext"
  );
  const page = await withTimeout(context.newPage(), timeoutMs, "context.newPage");
  page.setDefaultTimeout(timeoutMs);
  page.setDefaultNavigationTimeout(timeoutMs);
  try {
    progress(`[compare] goto start ${label} ${url}`);
    await withTimeout(
      page.goto(url, { waitUntil: "domcontentloaded", timeout: timeoutMs }),
      timeoutMs + 1000,
      `page.goto ${label}`
    );
    progress(`[compare] goto done ${label}`);
    await withTimeout(page.waitForTimeout(250), timeoutMs, "page.waitForTimeout");
    const outPath = path.join(outDir, `${label}.png`);
    progress(`[compare] screenshot start ${outPath}`);
    await withTimeout(
      page.screenshot({ path: outPath, fullPage: false, timeout: timeoutMs }),
      timeoutMs + 1000,
      "page.screenshot"
    );
    progress(`[compare] screenshot done ${outPath}`);
    return outPath;
  } finally {
    await withTimeout(page.close(), 5000, "page.close").catch(() => {});
    await withTimeout(context.close(), 5000, "context.close").catch(() => {});
    await withTimeout(browser.close(), 5000, "browser.close").catch(() => {});
  }
};

const run = async () => {
  fs.mkdirSync(outDir, { recursive: true });
  progress(`[compare] before url ${beforeUrl}`);
  progress(`[compare] after url ${afterUrl}`);

  const beforePath = await capture("before", beforeUrl, false);
  const afterPath = await capture("after", afterUrl, true);
  const diffPath = path.join(outDir, "diff.png");
  progress(`[compare] diff start ${diffPath}`);
  writeDiff(beforePath, afterPath, diffPath);
  progress(`[compare] diff done ${diffPath}`);
};

run()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => {
    clearInterval(heartbeat);
  });
