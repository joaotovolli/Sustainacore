import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright";

const baseUrl = process.env.HEADER_LAYOUT_URL || "http://127.0.0.1:8128/";
const outDir = process.env.HEADER_LAYOUT_OUT || path.resolve("docs", "screenshots", "ui", "header-overlap-after-pr535");
const viewports = [
  { name: "2048x768", width: 2048, height: 768, desktop: true },
  { name: "1536x864", width: 1536, height: 864, desktop: true },
  { name: "1440x900", width: 1440, height: 900, desktop: true },
  { name: "1366x768", width: 1366, height: 768, desktop: true },
  { name: "1280x800", width: 1280, height: 800, desktop: true },
  { name: "1024x768", width: 1024, height: 768, desktop: false },
  { name: "390x844", width: 390, height: 844, desktop: false },
];

const gap = 1;

const box = async (page, selector) => {
  const locator = page.locator(selector).first();
  const count = await locator.count();
  if (!count) throw new Error(`Missing selector: ${selector}`);
  const bounds = await locator.boundingBox();
  if (!bounds) throw new Error(`Selector has no bounding box: ${selector}`);
  return bounds;
};

const visible = async (page, selector) => page.locator(selector).first().isVisible();

const right = (bounds) => bounds.x + bounds.width;
const bottom = (bounds) => bounds.y + bounds.height;

const assertBefore = (leftBox, rightBox, label) => {
  if (right(leftBox) > rightBox.x - gap) {
    throw new Error(`${label}: ${right(leftBox).toFixed(1)} overlaps ${rightBox.x.toFixed(1)}`);
  }
};

fs.mkdirSync(outDir, { recursive: true });

const browser = await chromium.launch({
  headless: true,
  args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
});

try {
  for (const viewport of viewports) {
    console.log(`checking ${viewport.name}`);
    const page = await browser.newPage({ viewport: { width: viewport.width, height: viewport.height } });
    page.setDefaultTimeout(8000);
    await page.route("https://fonts.googleapis.com/**", (route) => route.abort());
    await page.route("https://fonts.gstatic.com/**", (route) => route.abort());
    await page.goto(baseUrl, { waitUntil: "domcontentloaded", timeout: 15000 });
    await page.waitForSelector(".topbar", { timeout: 10000 });
    await page.waitForTimeout(150);

    const header = await box(page, ".topbar");
    const brand = await box(page, ".brand");
    const toggleVisible = await visible(page, ".nav-toggle");
    const navVisible = await visible(page, ".topbar__nav");

    if (viewport.desktop) {
      if (toggleVisible) throw new Error(`${viewport.name}: hamburger toggle is visible on desktop`);
      if (!navVisible) throw new Error(`${viewport.name}: desktop nav is hidden`);

      const firstNav = await box(page, ".nav--primary .nav__item, .nav--primary > .nav__link");
      const lastPrimary = await box(page, ".nav--primary > .nav__link:last-child");
      const actions = await box(page, ".topbar__right");
      const github = await box(page, ".nav--actions .nav__icon-link:first-child");
      const linkedin = await box(page, ".nav--actions .nav__icon-link:nth-child(2)");
      const login = await box(page, ".nav--actions .nav__link--pill, .auth-pill");

      assertBefore(brand, firstNav, `${viewport.name}: brand/nav`);
      assertBefore(lastPrimary, actions, `${viewport.name}: nav/actions`);
      assertBefore(github, linkedin, `${viewport.name}: GitHub/LinkedIn`);
      assertBefore(linkedin, login, `${viewport.name}: LinkedIn/login`);

      for (const current of [brand, firstNav, lastPrimary, actions]) {
        if (current.y < header.y - gap || bottom(current) > bottom(header) + gap) {
          throw new Error(`${viewport.name}: header child is outside topbar vertical bounds`);
        }
      }
    } else {
      if (!toggleVisible) throw new Error(`${viewport.name}: hamburger toggle is hidden`);
      if (navVisible) throw new Error(`${viewport.name}: collapsed nav should stay hidden until opened`);
      const toggle = await box(page, ".nav-toggle");
      assertBefore(brand, toggle, `${viewport.name}: brand/hamburger`);
    }

    await page.screenshot({ path: path.join(outDir, `${viewport.name}.png`), fullPage: false, timeout: 10000 });
    await page.close();
    console.log(`ok ${viewport.name}`);
  }
} finally {
  await browser.close();
}
