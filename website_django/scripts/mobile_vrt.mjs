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
const runChecks = mode !== "baseline";

const viewports = [
  { label: "desktop_1440x900", width: 1440, height: 900, mobile: false },
  { label: "desktop_1920x1080", width: 1920, height: 1080, mobile: false },
  { label: "mobile_390x844", width: 390, height: 844, mobile: true },
  { label: "mobile_844x390", width: 844, height: 390, mobile: true },
];

const pages = [
  { name: "home", path: "/" },
  { name: "news", path: "/news/" },
  { name: "news_detail_one", path: "/news/NEWS_ITEMS:99/" },
  { name: "news_detail_two", path: "/news/NEWS_ITEMS:44/" },
  { name: "tech100_performance", path: "/tech100/performance/" },
  { name: "tech100_index", path: "/tech100/index/" },
  { name: "tech100_constituents", path: "/tech100/constituents/" },
];

const filterList = (process.env.VRT_PAGES || "")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);
const filteredPages = filterList.length
  ? pages.filter((page) => filterList.includes(page.name))
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
      if (rect.right > viewportWidth + 2 || rect.width > viewportWidth + 2) {
        offenders.push({
          tag: el.tagName.toLowerCase(),
          className: el.className || "",
          right: Math.round(rect.right),
          width: Math.round(rect.width),
        });
        if (offenders.length >= 8) break;
      }
    }

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
      offenders,
      headerOverlap,
    };
  });

  if (result.overflow > 2) {
    throw new Error(
      `[${label}] Horizontal overflow on ${url}: viewport=${result.viewportWidth} scrollWidth=${result.maxScrollWidth}`
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

const run = async () => {
  const browser = await chromium.launch();

  for (const viewport of viewports) {
    const context = await browser.newContext({
      viewport: { width: viewport.width, height: viewport.height },
      deviceScaleFactor: 1,
      locale: "en-US",
      timezoneId: "UTC",
      reducedMotion: "reduce",
    });
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
    const page = await context.newPage();
    await page.emulateMedia({ reducedMotion: "reduce" });
    await page.addStyleTag({
      content: [
        "* { transition: none !important; animation: none !important; caret-color: transparent !important; }",
        ".footer__links a[href=\"/corrections/\"] { display: none !important; }",
      ].join("\n"),
    });
    statusMap[viewport.label] = {};

    for (const entry of filteredPages) {
      const url = `${baseUrl}${entry.path}`;
      const response = await page.goto(url, { waitUntil: "networkidle", timeout: timeoutMs });
      await page.evaluate(async () => {
        if (document.fonts && document.fonts.ready) {
          await document.fonts.ready;
        }
      });
      statusMap[viewport.label][entry.name] = response ? response.status() : 0;
      await page.waitForTimeout(500);

      if (runChecks && viewport.mobile) {
        await checkMobileLayout(page, viewport.label, url);
      }

      const outDir = path.join(outRoot, mode, viewport.label);
      fs.mkdirSync(outDir, { recursive: true });
      const fileName = `${sanitizeName(entry.name)}.png`;
      const outPath = path.join(outDir, fileName);
      await page.screenshot({ path: outPath, fullPage: true, timeout: timeoutMs });
      process.stdout.write(`saved ${outPath}\n`);
    }

    await context.close();
  }

  await browser.close();

  const statusPath = path.join(outRoot, mode, "status.json");
  fs.mkdirSync(path.dirname(statusPath), { recursive: true });
  fs.writeFileSync(
    statusPath,
    JSON.stringify({ baseUrl, mode, capturedAt: new Date().toISOString(), status: statusMap }, null, 2)
  );
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
