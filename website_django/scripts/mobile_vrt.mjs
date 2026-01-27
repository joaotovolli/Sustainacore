import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright";

const args = process.argv.slice(2);
const getArg = (name, fallback) => {
  const idx = args.indexOf(name);
  if (idx === -1) return fallback;
  return args[idx + 1] || fallback;
};

const mode = getArg("--mode", process.env.VRT_MODE || "current");
const baseUrl = getArg("--base-url", process.env.VRT_BASE_URL || "http://127.0.0.1:8001");
const outRoot = process.env.VRT_DIR || path.resolve(process.cwd(), "..", "artifacts", "vrt");
const timeoutMs = Number(process.env.VRT_TIMEOUT_MS || "60000");
const pageTimeoutMs = Number(process.env.VRT_PAGE_TIMEOUT_MS || "15000");
const screenshotTimeoutMs = Number(process.env.VRT_SCREENSHOT_TIMEOUT_MS || "15000");
const waitAfterLoadMs = Number(process.env.VRT_WAIT_MS || "350");
const smokeMode = ["1", "true"].includes((process.env.VRT_SMOKE || "").toLowerCase());
const runChecks = mode !== "baseline";
const watchdogMs = Number(process.env.VRT_WATCHDOG_MS || "10000");

let lastProgressAt = Date.now();
let lastProgressLabel = "init";
const log = (message) => {
  lastProgressAt = Date.now();
  lastProgressLabel = message;
  process.stdout.write(`${message}\n`);
};

const parseForcedViewports = () => {
  const raw = (process.env.VRT_FORCE_VIEWPORT || "").trim();
  if (!raw) return null;
  const entries = raw.split(",").map((value) => value.trim()).filter(Boolean);
  const parsed = [];
  for (const entry of entries) {
    const match = entry.match(/^(\d+)\s*x\s*(\d+)$/i);
    if (!match) continue;
    const width = Number(match[1]);
    const height = Number(match[2]);
    if (!Number.isFinite(width) || !Number.isFinite(height)) continue;
    parsed.push({
      label: `forced_${width}x${height}`,
      width,
      height,
      mobile: width <= 820,
    });
  }
  return parsed.length ? parsed : null;
};

const parseForcedPages = () => {
  const raw = (process.env.VRT_FORCE_PAGES || "").trim();
  if (!raw) return null;
  const names = raw.split(",").map((value) => value.trim()).filter(Boolean);
  return names.length ? names : null;
};

const allViewports = [
  { label: "desktop_1920x1080", width: 1920, height: 1080, mobile: false },
  { label: "desktop_1536x864", width: 1536, height: 864, mobile: false },
  { label: "desktop_1366x768", width: 1366, height: 768, mobile: false },
  { label: "tablet_1024x768", width: 1024, height: 768, mobile: false },
  { label: "tablet_768x1024", width: 768, height: 1024, mobile: true },
  { label: "mobile_390x844", width: 390, height: 844, mobile: true },
  { label: "mobile_360x800", width: 360, height: 800, mobile: true },
];

const allPages = [
  { name: "home", path: "/" },
  { name: "privacy", path: "/privacy/" },
  { name: "terms", path: "/terms/" },
  { name: "news", path: "/news/" },
  { name: "news_detail_one", path: "/news/NEWS_ITEMS:99/" },
  { name: "news_detail_two", path: "/news/NEWS_ITEMS:44/" },
  { name: "tech100_performance", path: "/tech100/performance/" },
  { name: "tech100_index", path: "/tech100/index/" },
  { name: "tech100_constituents", path: "/tech100/constituents/" },
];

const forcedViewports = parseForcedViewports();
const viewports = forcedViewports
  ? forcedViewports
  : smokeMode
    ? [{ label: "mobile_390x844", width: 390, height: 844, mobile: true }]
    : allViewports;

const pages = smokeMode
  ? [
      { name: "home", path: "/" },
      { name: "privacy", path: "/privacy/" },
    ]
  : allPages;

const filterList = (process.env.VRT_PAGES || "")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);
const forcedPages = parseForcedPages();
const filterNames = forcedPages || filterList;
const filteredPages = filterNames.length
  ? pages.filter((page) => filterNames.includes(page.name))
  : pages;

const sanitizeName = (name) => name.replace(/[^a-z0-9_-]+/gi, "_");
const statusMap = {};

const checkMobileLayout = async (page, label, url) => {
  const result = await page.evaluate(() => {
    const viewportWidth = window.innerWidth;
    const bodyScrollWidth = document.body ? document.body.scrollWidth : 0;
    const docScrollWidth = document.documentElement ? document.documentElement.scrollWidth : 0;
    const maxScrollWidth = Math.max(bodyScrollWidth, docScrollWidth);
    const overflow = maxScrollWidth - viewportWidth;

    const offenders = [];
    const hasScrollableAncestor = (node) => {
      let current = node.parentElement;
      while (current) {
        const style = window.getComputedStyle(current);
        if ((style.overflowX === "auto" || style.overflowX === "scroll") && current.scrollWidth > current.clientWidth) {
          return true;
        }
        current = current.parentElement;
      }
      return false;
    };
    const elements = document.querySelectorAll("body *");
    for (const el of elements) {
      const style = window.getComputedStyle(el);
      if (style.display === "none" || style.position === "fixed" || style.position === "sticky") {
        continue;
      }
      if (hasScrollableAncestor(el)) {
        continue;
      }
      const rect = el.getBoundingClientRect();
      if (rect.width <= 0 || rect.height <= 0) continue;
      if (rect.width > viewportWidth + 1 || rect.right > viewportWidth + 1 || rect.left < -1) {
        offenders.push({
          tag: el.tagName.toLowerCase(),
          id: el.id || "",
          className: String(el.className || "").slice(0, 120),
          text: (el.textContent || "").replace(/\s+/g, " ").trim().slice(0, 60),
          left: Math.round(rect.left),
          right: Math.round(rect.right),
          width: Math.round(rect.width),
          delta: Math.round(rect.right - viewportWidth),
        });
      }
    }

    offenders.sort((a, b) => b.delta - a.delta);
    const topOffenders = offenders.slice(0, 15);

    const header = document.querySelector("header");
    const main = document.querySelector("main");
    let headerOverlap = 0;
    if (header && main) {
      const headerRect = header.getBoundingClientRect();
      const mainRect = main.getBoundingClientRect();
      headerOverlap = Math.max(0, headerRect.bottom - mainRect.top);
    }

    return {
      viewportWidth,
      maxScrollWidth,
      overflow,
      offenders: topOffenders,
      headerOverlap,
    };
  });

  if (result.overflow > 2) {
    throw new Error(
      `[${label}] Horizontal overflow on ${url}: viewport=${result.viewportWidth} scrollWidth=${result.maxScrollWidth} offenders=${JSON.stringify(result.offenders)}`
    );
  }

  if (result.offenders.length) {
    throw new Error(
      `[${label}] Elements overflow viewport on ${url}: ${JSON.stringify(result.offenders)}`
    );
  }

  if (result.headerOverlap > 2) {
    throw new Error(
      `[${label}] Header overlaps main content on ${url}: overlap=${result.headerOverlap}`
    );
  }
};

const checkMobileNav = async (page, label, url) => {
  const toggle = await page.$("[data-nav-toggle]");
  const panel = await page.$("[data-nav-panel]");
  if (!toggle || !panel) {
    throw new Error(`[${label}] Mobile nav toggle missing on ${url}`);
  }

  await toggle.click();
  await page.waitForTimeout(150);

  const panelVisible = await page.evaluate(() => {
    const el = document.querySelector("[data-nav-panel]");
    if (!el) return false;
    const style = window.getComputedStyle(el);
    return style.display !== "none" && style.visibility !== "hidden" && el.offsetHeight > 0;
  });
  if (!panelVisible) {
    throw new Error(`[${label}] Mobile nav did not open on ${url}`);
  }

  const hasHomeLink = await page.evaluate(() => {
    const link = document.querySelector('nav[aria-label="Primary"] a[href="/"]');
    return Boolean(link);
  });
  if (!hasHomeLink) {
    throw new Error(`[${label}] Mobile nav missing expected links on ${url}`);
  }

  await toggle.click();
  await page.waitForTimeout(100);
};

const run = async () => {
  const startLabel = `[VRT] start mode=${mode} baseUrl=${baseUrl} pages=${filteredPages.length} viewports=${viewports.length}`;
  log(startLabel);
  const progressWatchdog = setInterval(() => {
    if (Date.now() - lastProgressAt > watchdogMs) {
      process.stderr.write(
        `[VRT] watchdog: no progress within ${watchdogMs}ms (last=${lastProgressLabel})\n`
      );
      process.exit(1);
    }
  }, 1000);

  log("[VRT] launching browser");
  const browser = await chromium.launch();
  log("[VRT] browser ready");
  const failures = [];

  for (const viewport of viewports) {
    log(`[VRT] viewport ${viewport.label} ${viewport.width}x${viewport.height}`);
    log("[VRT] context create start");
    const context = await browser.newContext({
      viewport: { width: viewport.width, height: viewport.height },
      deviceScaleFactor: 1,
      locale: "en-US",
      timezoneId: "UTC",
      reducedMotion: "reduce",
    });
    log("[VRT] context create done");
    await context.addInitScript(() => {
      window.__VRT__ = true;
      const fixed = new Date("2025-01-01T00:00:00Z").getTime();
      const NativeDate = Date;
      class MockDate extends NativeDate {
        constructor(...args) {
          if (args.length === 0) {
            return new NativeDate(fixed);
          }
          return new NativeDate(...args);
        }
        static now() {
          return fixed;
        }
      }
      MockDate.parse = NativeDate.parse;
      MockDate.UTC = NativeDate.UTC;
      MockDate.prototype = NativeDate.prototype;
      window.Date = MockDate;
      Math.random = () => 0.42;
    });
    log("[VRT] page create start");
    const page = await context.newPage();
    log("[VRT] page ready");
    let pageStartWatchdog = null;
    pageStartWatchdog = setTimeout(() => {
      process.stderr.write(
        `[VRT] watchdog: no page start within ${watchdogMs}ms after page ready (last=${lastProgressLabel})\n`
      );
      process.exit(1);
    }, watchdogMs);
    await page.emulateMedia({ reducedMotion: "reduce" });
    log("[VRT] route setup");
    await page.route(/https:\/\/fonts\.googleapis\.com\/.*/i, (route) => route.abort());
    await page.route(/https:\/\/fonts\.gstatic\.com\/.*/i, (route) => route.abort());
    log("[VRT] route setup done");
    await page.addStyleTag({
      content: [
        "* { transition: none !important; animation: none !important; caret-color: transparent !important; }",
      ].join("\n"),
    });
    statusMap[viewport.label] = {};
    let currentCapture = { console: [], pageErrors: [], requestFailures: [] };

    page.on("console", (msg) => {
      if (["error", "warning"].includes(msg.type())) {
        currentCapture.console.push(`${msg.type()}: ${msg.text()}`);
      }
    });
    page.on("pageerror", (error) => {
      currentCapture.pageErrors.push(error.message);
    });
    page.on("requestfailed", (request) => {
      const failure = request.failure();
      currentCapture.requestFailures.push({
        url: request.url(),
        errorText: failure ? failure.errorText : "unknown",
      });
    });

    for (const entry of filteredPages) {
      currentCapture = { console: [], pageErrors: [], requestFailures: [] };
      const url = `${baseUrl}${entry.path}`;
      let response = null;
      try {
        if (pageStartWatchdog) {
          clearTimeout(pageStartWatchdog);
          pageStartWatchdog = null;
        }
        log(`[VRT] navigate start ${viewport.label} ${entry.name} ${url}`);
        response = await page.goto(url, { waitUntil: "commit", timeout: pageTimeoutMs });
        await page.evaluate(async () => {
          if (document.fonts && document.fonts.ready) {
            await document.fonts.ready;
          }
        });
        statusMap[viewport.label][entry.name] = response ? response.status() : 0;
        await page.waitForTimeout(waitAfterLoadMs);
        log(`[VRT] navigate done ${viewport.label} ${entry.name}`);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        failures.push({
          viewport: viewport.label,
          page: entry.name,
          url,
          error: message,
          console: currentCapture.console,
          pageErrors: currentCapture.pageErrors,
          requestFailures: currentCapture.requestFailures,
        });
        statusMap[viewport.label][entry.name] = 0;
        continue;
      }

      if (runChecks && viewport.mobile) {
        try {
          await checkMobileLayout(page, viewport.label, url);
          if (entry.name === "home") {
            await checkMobileNav(page, viewport.label, url);
          }
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          failures.push({
            viewport: viewport.label,
            page: entry.name,
            url,
            error: message,
            console: currentCapture.console,
            pageErrors: currentCapture.pageErrors,
            requestFailures: currentCapture.requestFailures,
          });
        }
      }

      const outDir = path.join(outRoot, mode, viewport.label);
      fs.mkdirSync(outDir, { recursive: true });
      const fileName = `${sanitizeName(entry.name)}.png`;
      const outPath = path.join(outDir, fileName);
      try {
        log(`[VRT] screenshot start ${outPath}`);
        await page.screenshot({ path: outPath, fullPage: true, timeout: screenshotTimeoutMs });
        log(`[VRT] saved ${outPath}`);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        failures.push({
          viewport: viewport.label,
          page: entry.name,
          url,
          error: `Screenshot failed: ${message}`,
          console: currentCapture.console,
          pageErrors: currentCapture.pageErrors,
          requestFailures: currentCapture.requestFailures,
        });
      }
    }

    if (pageStartWatchdog) {
      clearTimeout(pageStartWatchdog);
      pageStartWatchdog = null;
    }
    await context.close();
  }

  await browser.close();
  clearInterval(progressWatchdog);

  const statusPath = path.join(outRoot, mode, "status.json");
  fs.mkdirSync(path.dirname(statusPath), { recursive: true });
  fs.writeFileSync(
    statusPath,
    JSON.stringify({ baseUrl, mode, capturedAt: new Date().toISOString(), status: statusMap }, null, 2)
  );

  if (failures.length) {
    throw new Error(`VRT captured ${failures.length} failures: ${JSON.stringify(failures, null, 2)}`);
  }
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
