import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright";
import pixelmatch from "pixelmatch";
import { PNG } from "pngjs";

const prodBaseUrl = process.env.PROD_BASE_URL || "https://sustainacore.org";
const previewBaseUrl = process.env.PREVIEW_BASE_URL || "https://preview.sustainacore.org";
const authUser = process.env.BASIC_AUTH_USER || "";
const authPass = process.env.BASIC_AUTH_PASS || "";
const timeoutMs = Number(process.env.TIMEOUT_MS || "60000");
const outRoot = process.env.OUTPUT_DIR || path.resolve("artifacts", "ui_home");
const viewport = { width: 1440, height: 900 };

const progress = (message) => {
  process.stdout.write(`${message}\n`);
};

let lastBeat = "init";
const heartbeat = setInterval(() => {
  process.stdout.write(`[home-compare] heartbeat ${lastBeat}\n`);
}, 2000);

const withTimeout = (promise, ms, label) => {
  let timeoutId = null;
  const timeout = new Promise((_, reject) => {
    timeoutId = setTimeout(() => reject(new Error(`Timeout after ${ms}ms (${label})`)), ms);
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

const capture = async ({ label, url, useAuth }) => {
  lastBeat = `launch ${label}`;
  progress(`[home-compare] launch start ${label}`);
  const browser = await withTimeout(
    chromium.launch({
      headless: true,
      args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    }),
    timeoutMs,
    "chromium.launch"
  );
  progress(`[home-compare] launch done ${label}`);
  const context = await withTimeout(
    browser.newContext({
      viewport,
      httpCredentials: useAuth ? { username: authUser, password: authPass } : undefined,
    }),
    timeoutMs,
    "browser.newContext"
  );
  const page = await withTimeout(context.newPage(), timeoutMs, "context.newPage");
  page.setDefaultTimeout(timeoutMs);
  page.setDefaultNavigationTimeout(timeoutMs);
  try {
    lastBeat = `goto ${label}`;
    progress(`[home-compare] goto start ${label} ${url}`);
    await withTimeout(
      page.goto(url, { waitUntil: "domcontentloaded", timeout: timeoutMs }),
      timeoutMs + 1000,
      `page.goto ${label}`
    );
    progress(`[home-compare] goto done ${label}`);
    await withTimeout(page.waitForTimeout(250), timeoutMs, "page.waitForTimeout");
    const outPath = path.join(outRoot, label, "home.png");
    progress(`[home-compare] screenshot start ${outPath}`);
    await withTimeout(
      page.screenshot({ path: outPath, fullPage: false, timeout: timeoutMs }),
      timeoutMs + 1000,
      "page.screenshot"
    );
    progress(`[home-compare] screenshot done ${outPath}`);
    return outPath;
  } finally {
    await withTimeout(page.close(), 5000, "page.close").catch(() => {});
    await withTimeout(context.close(), 5000, "context.close").catch(() => {});
    await withTimeout(browser.close(), 5000, "browser.close").catch(() => {});
  }
};

const run = async () => {
  fs.mkdirSync(path.join(outRoot, "before"), { recursive: true });
  fs.mkdirSync(path.join(outRoot, "after"), { recursive: true });
  fs.mkdirSync(path.join(outRoot, "diff"), { recursive: true });

  progress(`[home-compare] prod url ${prodBaseUrl}`);
  progress(`[home-compare] preview url ${previewBaseUrl}`);

  const beforePath = await capture({ label: "before", url: `${prodBaseUrl}/`, useAuth: false });
  const afterPath = await capture({ label: "after", url: `${previewBaseUrl}/`, useAuth: true });
  const diffPath = path.join(outRoot, "diff", "home_diff.png");
  lastBeat = "diff";
  progress(`[home-compare] diff start ${diffPath}`);
  writeDiff(beforePath, afterPath, diffPath);
  progress(`[home-compare] diff done ${diffPath}`);
};

run()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => {
    clearInterval(heartbeat);
  });
