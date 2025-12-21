import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright";

const args = process.argv.slice(2);
const getArg = (name, fallback) => {
  const idx = args.indexOf(name);
  if (idx === -1) return fallback;
  return args[idx + 1] || fallback;
};

const mode = getArg("--mode", process.env.NEWS_SCREENSHOT_MODE || "after");
const baseUrl = getArg("--base-url", process.env.NEWS_SCREENSHOT_BASE_URL || "http://127.0.0.1:8001");
const detailPathOverride = getArg("--detail-path", process.env.NEWS_DETAIL_PATH || "");
const screenshotDir = process.env.NEWS_SCREENSHOT_DIR || "news";
const authUser =
  process.env.NEWS_BASIC_AUTH_USER || process.env.TECH100_BASIC_AUTH_USER || process.env.PREVIEW_BASIC_AUTH_USER || "";
const authPass =
  process.env.NEWS_BASIC_AUTH_PASS || process.env.TECH100_BASIC_AUTH_PASS || process.env.PREVIEW_BASIC_AUTH_PASS || "";
const ignoreHttpsErrors = process.env.NEWS_IGNORE_HTTPS_ERRORS === "1";

const outDir = path.resolve(process.cwd(), "..", "docs", "screenshots", screenshotDir, mode);
fs.mkdirSync(outDir, { recursive: true });

const resolveDetailPath = async (page) => {
  if (detailPathOverride) return detailPathOverride;
  const baseOrigin = new URL(baseUrl).origin;
  const links = await page.$$("[data-news-link]");
  for (const link of links) {
    const href = await link.getAttribute("href");
    if (!href) continue;
    if (href.startsWith("/news/")) return href;
    if (href.startsWith("http")) {
      const url = new URL(href);
      if (url.origin === baseOrigin && url.pathname.startsWith("/news/")) {
        return url.pathname;
      }
    }
  }
  const fallback = await page.$(".news-card__title a");
  if (!fallback) return "/news/placeholder/";
  const href = await fallback.getAttribute("href");
  if (!href) return "/news/placeholder/";
  return href.startsWith("/") ? href : "/news/placeholder/";
};

const run = async () => {
  const browser = await chromium.launch();
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    ignoreHTTPSErrors: ignoreHttpsErrors,
    httpCredentials: authUser && authPass ? { username: authUser, password: authPass } : undefined,
  });
  const page = await context.newPage();

  await page.addStyleTag({
    content: "* { transition: none !important; animation: none !important; }",
  });

  const listUrl = `${baseUrl}/news/`;
  await page.goto(listUrl, { waitUntil: "networkidle" });
  await page.waitForTimeout(500);
  const listPath = path.join(outDir, "news_list.png");
  await page.screenshot({ path: listPath, fullPage: true, timeout: 60000 });
  process.stdout.write(`saved ${listPath}\n`);

  const detailPath = await resolveDetailPath(page);
  const detailUrl = `${baseUrl}${detailPath}`;
  await page.goto(detailUrl, { waitUntil: "networkidle" });
  if (mode === "after") {
    await page.waitForSelector("[data-news-title]", { timeout: 15000 });
  }
  await page.waitForTimeout(300);
  const detailShot = path.join(outDir, "news_detail.png");
  await page.screenshot({ path: detailShot, fullPage: true, timeout: 60000 });
  process.stdout.write(`saved ${detailShot}\n`);

  await context.close();
  await browser.close();
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
