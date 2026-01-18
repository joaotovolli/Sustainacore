import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright";
import pixelmatch from "pixelmatch";
import { PNG } from "pngjs";

const prodBaseUrl = process.env.PROD_BASE_URL || "https://sustainacore.org";
const previewBaseUrl = process.env.PREVIEW_BASE_URL || "https://preview.sustainacore.org";
const timeoutMs = Number(process.env.TIMEOUT_MS || "60000");
const maxDiffPixels = Number(process.env.DIFF_MAX_PIXELS || "100");
const outRoot = process.env.OUTPUT_DIR || path.resolve("artifacts", "ui");
const reportDir = path.join(outRoot, "report");
const desktopViewport = { width: 1440, height: 900 };
const mobileViewport = { width: 390, height: 844 };
const tmpReportPath = "/tmp/ui_home_report.json";
const navigationRetries = Number(process.env.NAV_RETRIES || "2");
const navigationBackoffMs = Number(process.env.NAV_RETRY_BACKOFF_MS || "2000");
const localFast = ["1", "true", "yes"].includes((process.env.LOCAL_FAST || "").toLowerCase());
const localFetchHtml = ["1", "true", "yes"].includes((process.env.LOCAL_FETCH_HTML || "").toLowerCase());

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

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const fetchHtml = async (url) => {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.min(timeoutMs, 8000));
  try {
    const res = await fetch(url, { signal: controller.signal });
    if (!res.ok) {
      throw new Error(`fetch ${url} failed: ${res.status}`);
    }
    return await res.text();
  } finally {
    clearTimeout(timeout);
  }
};

const ensureBaseTag = (html, baseHref) => {
  if (html.includes("<base ")) {
    return html;
  }
  const baseTag = `<base href="${baseHref}">`;
  if (html.includes("</head>")) {
    return html.replace("</head>", `${baseTag}</head>`);
  }
  return `${baseTag}${html}`;
};

const gotoWithRetries = async (page, url, label) => {
  let lastError = null;
  for (let attempt = 1; attempt <= navigationRetries; attempt += 1) {
    try {
      lastBeat = `goto ${label} attempt ${attempt}`;
      progress(`[home-compare] goto start ${label} attempt ${attempt} ${url}`);
      await withTimeout(
        page.goto(url, { waitUntil: "domcontentloaded", timeout: timeoutMs }),
        timeoutMs + 1000,
        `page.goto ${label} attempt ${attempt}`
      );
      progress(`[home-compare] goto done ${label} attempt ${attempt}`);
      await withTimeout(page.waitForSelector("body", { timeout: timeoutMs }), timeoutMs + 1000, `body ${label}`);
      await withTimeout(page.waitForTimeout(300), timeoutMs, "page.waitForTimeout settle");
      return;
    } catch (err) {
      lastError = err;
      progress(`[home-compare] goto error ${label} attempt ${attempt}: ${err.message}`);
      if (attempt < navigationRetries) {
        await delay(navigationBackoffMs * attempt);
      }
    }
  }
  throw lastError;
};

const readPng = (filePath) => PNG.sync.read(fs.readFileSync(filePath));

const writeDiff = (beforePath, afterPath, diffPath) => {
  const before = readPng(beforePath);
  const after = readPng(afterPath);
  const width = Math.min(before.width, after.width);
  const height = Math.min(before.height, after.height);
  const beforeCrop = new PNG({ width, height });
  const afterCrop = new PNG({ width, height });
  PNG.bitblt(before, beforeCrop, 0, 0, width, height, 0, 0);
  PNG.bitblt(after, afterCrop, 0, 0, width, height, 0, 0);
  const diff = new PNG({ width, height });
  const mismatchPixels = pixelmatch(beforeCrop.data, afterCrop.data, diff.data, width, height, {
    threshold: 0.1,
  });
  fs.writeFileSync(diffPath, PNG.sync.write(diff));
  const mismatchPercent = Number(((mismatchPixels / (width * height)) * 100).toFixed(4));
  return { mismatchPixels, mismatchPercent, width, height };
};

const capture = async ({ label, url, viewport, shots, failureLog }) => {
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
      browser.newContext({ viewport }),
      timeoutMs,
      "browser.newContext"
    );
  progress(`[home-compare] context done ${label}`);
  lastBeat = `page ${label}`;
  const page = await withTimeout(context.newPage(), timeoutMs, "context.newPage");
  progress(`[home-compare] page done ${label}`);
  page.setDefaultTimeout(timeoutMs);
  page.setDefaultNavigationTimeout(timeoutMs);
  if (localFast) {
    await page.route("**/*", (route) => {
      const req = route.request();
      const url = req.url();
      const type = req.resourceType();
      if (
        type === "image" ||
        type === "media" ||
        url.includes("fonts.googleapis.com") ||
        url.includes("fonts.gstatic.com") ||
        url.includes("googletagmanager") ||
        url.includes("analytics")
      ) {
        return route.abort();
      }
      return route.continue();
    });
  }
    try {
      try {
        if (localFetchHtml) {
          const html = await fetchHtml(url);
          const withBase = ensureBaseTag(html, url);
          progress(`[home-compare] setContent start ${label}`);
          await withTimeout(
            page.setContent(withBase, { waitUntil: "domcontentloaded" }),
            timeoutMs + 1000,
            `page.setContent ${label}`
          );
          progress(`[home-compare] setContent done ${label}`);
        } else {
          await gotoWithRetries(page, url, label);
        }
      } catch (err) {
        if (failureLog) {
          failureLog.push({ stage: `goto-${label}`, message: err.message });
        }
        const errorShot = path.join(outRoot, label, `home_${label}_error.png`);
        await withTimeout(
          page.screenshot({ path: errorShot, fullPage: false, timeout: timeoutMs }),
          timeoutMs + 1000,
          `page.screenshot error ${label}`
        ).catch(() => {});
        throw err;
      }
      await withTimeout(
        page.addStyleTag({
          content: [
            "*{font-family: Arial, sans-serif !important; animation:none !important; transition:none !important;}",
            ".tech100-home{visibility:hidden !important;}",
            ".hero__card{visibility:hidden !important;}",
            ".news-card{visibility:hidden !important;}",
            ".home-news{visibility:hidden !important;}",
            ".ask2-fab{visibility:hidden !important;}",
            ".consent-banner{display:none !important;}",
            ".preview-banner{display:none !important;}",
          ].join(" "),
        }),
        timeoutMs,
        "page.addStyleTag compare hides"
      );
    if (label === "after") {
      await withTimeout(
        page.addStyleTag({
          content: ".preview-banner{display:none !important;} .consent-banner{display:none !important;}",
        }),
        timeoutMs,
        "page.addStyleTag preview banner"
      );
      await withTimeout(
        page.evaluate(() => {
          const banner = document.querySelector(".preview-banner");
          if (banner) banner.remove();
          const consent = document.querySelector(".consent-banner");
          if (consent) consent.remove();
        }),
        timeoutMs,
        "page.evaluate remove preview banner"
      );
    }
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
      const docHeight = await page.evaluate(() => document.documentElement.scrollHeight);
      const shotResults = [];
      for (const shot of shots) {
        const outPath = path.join(outRoot, label, `${shot.name}.png`);
        progress(`[home-compare] screenshot start ${outPath}`);
        if (shot.type === "full") {
          await withTimeout(
            page.screenshot({ path: outPath, fullPage: true, timeout: timeoutMs }),
            timeoutMs + 1000,
            `page.screenshot full ${shot.name}`
          );
        } else if (shot.type === "viewport") {
          await withTimeout(
            page.screenshot({ path: outPath, fullPage: false, timeout: timeoutMs }),
            timeoutMs + 1000,
            `page.screenshot viewport ${shot.name}`
          );
        } else {
          const clipHeight = shot.height || viewport.height;
          const maxY = Math.max(0, docHeight - clipHeight);
          let y = 0;
          if (shot.section === "mid") {
            y = Math.min(maxY, Math.max(0, Math.round(docHeight / 2 - clipHeight / 2)));
          } else if (shot.section === "footer") {
            y = maxY;
          }
          await withTimeout(page.evaluate((scrollY) => window.scrollTo(0, scrollY), y), timeoutMs, "page.scrollTo");
          await withTimeout(page.waitForTimeout(150), timeoutMs, "page.waitForTimeout");
          await withTimeout(
            page.screenshot({
              path: outPath,
              fullPage: false,
              timeout: timeoutMs,
            }),
            timeoutMs + 1000,
            `page.screenshot section ${shot.name}`
          );
        }
        progress(`[home-compare] screenshot done ${outPath}`);
        shotResults.push({ name: shot.name, path: outPath, viewport: shot.viewport });
      }
      return { shots: shotResults, layoutMetrics };
    } finally {
    if (failureLog && failureLog.length > 0) {
      progress(`[home-compare] ${label} failures: ${failureLog.length}`);
    }
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

  const shotPlan = localFast
    ? [
        { name: "home_full", type: "viewport", viewport: "desktop" },
        { name: "home_footer", type: "clip", viewport: "desktop", section: "footer", height: 700 },
      ]
    : [
        { name: "home_full", type: "full", viewport: "desktop" },
        { name: "home_top", type: "clip", viewport: "desktop", section: "top", height: 900 },
        { name: "home_mid", type: "clip", viewport: "desktop", section: "mid", height: 900 },
        { name: "home_footer", type: "clip", viewport: "desktop", section: "footer", height: 700 },
      ];
  const mobilePlan = [{ name: "home_mobile", type: "viewport", viewport: "mobile" }];

  const errors = [];
  let beforeDesktop = null;
  let afterDesktop = null;
  let beforeMobile = null;
  let afterMobile = null;
  try {
    beforeDesktop = await capture({
      label: "before",
      url: `${prodBaseUrl}/`,
      viewport: desktopViewport,
      shots: shotPlan,
      failureLog: errors,
    });
    afterDesktop = await capture({
      label: "after",
      url: `${previewBaseUrl}/`,
      viewport: desktopViewport,
      shots: shotPlan,
      failureLog: errors,
    });
    beforeMobile = await capture({
      label: "before",
      url: `${prodBaseUrl}/`,
      viewport: mobileViewport,
      shots: mobilePlan,
      failureLog: errors,
    });
    afterMobile = await capture({
      label: "after",
      url: `${previewBaseUrl}/`,
      viewport: mobileViewport,
      shots: mobilePlan,
      failureLog: errors,
    });
  } catch (err) {
    errors.push({ stage: "capture", message: err.message });
  }

  const diffEntries = [];
  const allBeforeShots = [
    ...(beforeDesktop ? beforeDesktop.shots : []),
    ...(beforeMobile ? beforeMobile.shots : []),
  ];
  const allAfterShots = [
    ...(afterDesktop ? afterDesktop.shots : []),
    ...(afterMobile ? afterMobile.shots : []),
  ];
  for (const beforeShot of allBeforeShots) {
    const afterShot = allAfterShots.find(
      (shot) => shot.name === beforeShot.name && shot.viewport === beforeShot.viewport
    );
    if (!afterShot) continue;
    const diffPath = path.join(outRoot, "diff", `${beforeShot.name}_diff.png`);
    lastBeat = `diff ${beforeShot.name}`;
    progress(`[home-compare] diff start ${diffPath}`);
    const diffStats = writeDiff(beforeShot.path, afterShot.path, diffPath);
    progress(`[home-compare] diff done ${diffPath}`);
    diffEntries.push({
      name: beforeShot.name,
      viewport: beforeShot.viewport,
      before: path.relative(outRoot, beforeShot.path),
      after: path.relative(outRoot, afterShot.path),
      diff: path.relative(outRoot, diffPath),
      stats: diffStats,
    });
  }

  const maxMismatch = diffEntries.reduce(
    (acc, item) => Math.max(acc, item.stats.mismatchPixels),
    0
  );
  const maxPercent = diffEntries.reduce(
    (acc, item) => Math.max(acc, item.stats.mismatchPercent),
    0
  );
  lastBeat = "report";
  progress("[home-compare] report start");
  const report = {
    urls: {
      prod: `${prodBaseUrl}/`,
      preview: `${previewBaseUrl}/`,
    },
    status: errors.length > 0 ? "failed" : "ok",
    errors,
    viewports: {
      desktop: desktopViewport,
      mobile: mobileViewport,
    },
    diff: {
      mismatchPixels: maxMismatch,
      mismatchPercent: maxPercent,
    },
    shots: diffEntries,
    layout: {
      before: beforeDesktop ? beforeDesktop.layoutMetrics : null,
      after: afterDesktop ? afterDesktop.layoutMetrics : null,
    },
  };
  const reportPath = path.join(reportDir, "ui_compare_report.json");
  fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(tmpReportPath, `${JSON.stringify(report, null, 2)}\n`);
  const summaryPath = path.join(reportDir, "ui_compare_summary.txt");
  const offenderLines = afterDesktop.layoutMetrics.overflowOffendersTop15
    .map((item, index) => {
      return `${index + 1}. ${item.selectorLike} (right=${item.bbox.right}, width=${item.bbox.width})`;
    })
    .join("\n");
  const summary = [
    "UI Compare Summary (home)",
    `Prod: ${prodBaseUrl}/`,
    `Preview: ${previewBaseUrl}/`,
    `Mismatch pixels (max): ${maxMismatch}`,
    `Mismatch percent (max): ${maxPercent}%`,
    `Horizontal overflow (preview): ${afterDesktop.layoutMetrics.horizontalOverflow}`,
    `Document scrollWidth: ${afterDesktop.layoutMetrics.docScrollWidth}`,
    `Viewport width: ${afterDesktop.layoutMetrics.viewportWidth}`,
    "Overflow offenders (preview):",
    offenderLines || "None",
    "",
    "Shots:",
    ...diffEntries.map(
      (item) =>
        `- ${item.name} (${item.viewport}): ${item.stats.mismatchPixels} px, ${item.stats.mismatchPercent}%`
    ),
  ].join("\n");
  fs.writeFileSync(summaryPath, `${summary}\n`);
  progress(`[home-compare] report done ${summaryPath}`);
  if (errors.length > 0) {
    throw new Error(`Compare failed with ${errors.length} error(s)`);
  }
  if (Number.isFinite(maxDiffPixels) && maxMismatch > maxDiffPixels) {
    throw new Error(
      `Diff exceeds threshold: ${maxMismatch} > ${maxDiffPixels}`
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
