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
const maxDiffPixels = Number(process.env.DIFF_MAX_PIXELS || "100");
const outRoot = process.env.OUTPUT_DIR || path.resolve("artifacts", "ui_home");
const reportDir = path.join(outRoot, "report");
const viewport = { width: 1440, height: 900 };
const tmpReportPath = "/tmp/ui_home_report.json";

const progress = (message) => {
  process.stdout.write(`${message}\n`);
};

let lastBeat = "init";
const heartbeat = setInterval(() => {
  process.stdout.write(`[home-compare] heartbeat ${lastBeat}\n`);
}, 1500);

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
  const mismatchPixels = pixelmatch(before.data, after.data, diff.data, width, height, {
    threshold: 0.1,
  });
  fs.writeFileSync(diffPath, PNG.sync.write(diff));
  const mismatchPercent = Number(((mismatchPixels / (width * height)) * 100).toFixed(4));
  return { mismatchPixels, mismatchPercent, width, height };
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
  lastBeat = `context ${label}`;
  const context = await withTimeout(
    browser.newContext({
      viewport,
      httpCredentials: useAuth ? { username: authUser, password: authPass } : undefined,
    }),
    timeoutMs,
    "browser.newContext"
  );
  progress(`[home-compare] context done ${label}`);
  lastBeat = `page ${label}`;
  const page = await withTimeout(context.newPage(), timeoutMs, "context.newPage");
  progress(`[home-compare] page done ${label}`);
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
    lastBeat = `metrics ${label}`;
    progress(`[home-compare] metrics start ${label}`);
    const layoutMetrics = await withTimeout(
      page.evaluate(() => {
        const viewportWidth = window.innerWidth;
        const viewportHeight = window.innerHeight;
        const docEl = document.documentElement;
        const docScrollWidth = docEl.scrollWidth;
        const docClientWidth = docEl.clientWidth;
        const horizontalOverflow = docScrollWidth > viewportWidth + 1;
        const offenders = [];
        const elements = Array.from(document.body.querySelectorAll("*"));
        for (const el of elements) {
          const rect = el.getBoundingClientRect();
          const over =
            rect.right > viewportWidth + 1 || rect.left < -1 || rect.width > viewportWidth + 1;
          if (!over) continue;
          const tag = el.tagName.toLowerCase();
          const id = el.id || "";
          const className = (el.className || "").toString().trim();
          const selectorLike =
            tag +
            (id ? `#${id}` : "") +
            (className ? `.${className.split(/\s+/).join(".")}` : "");
          offenders.push({
            selectorLike: selectorLike.slice(0, 160),
            tag,
            id,
            class: className.slice(0, 160),
            scrollWidth: el.scrollWidth || 0,
            clientWidth: el.clientWidth || 0,
            bbox: {
              left: Math.round(rect.left),
              right: Math.round(rect.right),
              width: Math.round(rect.width),
            },
            text: (el.textContent || "").trim().slice(0, 60),
          });
        }
        offenders.sort((a, b) => (b.bbox.right - viewportWidth) - (a.bbox.right - viewportWidth));
        return {
          viewportWidth,
          viewportHeight,
          docScrollWidth,
          docClientWidth,
          horizontalOverflow,
          overflowOffendersTop15: offenders.slice(0, 15),
        };
      }),
      timeoutMs,
      `page.evaluate metrics ${label}`
    );
    progress(`[home-compare] metrics done ${label}`);
    const outPath = path.join(outRoot, label, "home.png");
    progress(`[home-compare] screenshot start ${outPath}`);
    await withTimeout(
      page.screenshot({ path: outPath, fullPage: false, timeout: timeoutMs }),
      timeoutMs + 1000,
      "page.screenshot"
    );
    progress(`[home-compare] screenshot done ${outPath}`);
    return { outPath, layoutMetrics };
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
  fs.mkdirSync(reportDir, { recursive: true });

  progress(`[home-compare] prod url ${prodBaseUrl}`);
  progress(`[home-compare] preview url ${previewBaseUrl}`);

  const before = await capture({ label: "before", url: `${prodBaseUrl}/`, useAuth: false });
  const after = await capture({ label: "after", url: `${previewBaseUrl}/`, useAuth: true });
  const diffPath = path.join(outRoot, "diff", "home_diff.png");
  lastBeat = "diff";
  progress(`[home-compare] diff start ${diffPath}`);
  const diffStats = writeDiff(before.outPath, after.outPath, diffPath);
  progress(`[home-compare] diff done ${diffPath}`);
  lastBeat = "report";
  progress("[home-compare] report start");
  const report = {
    urls: {
      prod: `${prodBaseUrl}/`,
      preview: `${previewBaseUrl}/`,
    },
    viewport,
    diff: diffStats,
    layout: {
      before: before.layoutMetrics,
      after: after.layoutMetrics,
    },
  };
  const reportPath = path.join(reportDir, "ui_compare_report.json");
  fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(tmpReportPath, `${JSON.stringify(report, null, 2)}\n`);
  const summaryPath = path.join(reportDir, "ui_compare_summary.txt");
  const offenderLines = after.layoutMetrics.overflowOffendersTop15
    .map((item, index) => {
      return `${index + 1}. ${item.selectorLike} (right=${item.bbox.right}, width=${item.bbox.width})`;
    })
    .join("\n");
  const summary = [
    "UI Compare Summary (home)",
    `Prod: ${prodBaseUrl}/`,
    `Preview: ${previewBaseUrl}/`,
    `Mismatch pixels: ${diffStats.mismatchPixels}`,
    `Mismatch percent: ${diffStats.mismatchPercent}%`,
    `Horizontal overflow (preview): ${after.layoutMetrics.horizontalOverflow}`,
    `Document scrollWidth: ${after.layoutMetrics.docScrollWidth}`,
    `Viewport width: ${after.layoutMetrics.viewportWidth}`,
    "Overflow offenders (preview):",
    offenderLines || "None",
  ].join("\n");
  fs.writeFileSync(summaryPath, `${summary}\n`);
  progress(`[home-compare] report done ${summaryPath}`);
  if (Number.isFinite(maxDiffPixels) && diffStats.mismatchPixels > maxDiffPixels) {
    throw new Error(
      `Diff exceeds threshold: ${diffStats.mismatchPixels} > ${maxDiffPixels}`
    );
  }
};

run()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => {
    clearInterval(heartbeat);
  });
