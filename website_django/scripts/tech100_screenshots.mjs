import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright";

const args = process.argv.slice(2);
const getArg = (name, fallback) => {
  const idx = args.indexOf(name);
  if (idx === -1) return fallback;
  return args[idx + 1] || fallback;
};

const mode = getArg("--mode", process.env.TECH100_SCREENSHOT_MODE || "after");
const baseUrl = getArg("--base-url", "http://127.0.0.1:8001");
const tech100Path = getArg("--tech100-path", process.env.TECH100_SCREENSHOT_PATH || "/tech100/");

const outDir = path.resolve(process.cwd(), "..", "docs", "screenshots", "tech100", mode);
fs.mkdirSync(outDir, { recursive: true });

const dedupeTargets = (items) => {
  const seen = new Set();
  return items.filter((item) => {
    if (seen.has(item.path)) return false;
    seen.add(item.path);
    return true;
  });
};

const targets = dedupeTargets(
  mode === "before"
    ? [
        { path: "/", name: "home.png" },
        { path: tech100Path, name: "tech100.png" },
      ]
    : [
        { path: "/", name: "home.png" },
        { path: tech100Path, name: "tech100.png" },
        { path: "/tech100/index/", name: "index_overview.png" },
        { path: "/tech100/constituents/", name: "constituents.png" },
        { path: "/tech100/attribution/", name: "attribution.png" },
        { path: "/tech100/stats/", name: "stats.png" },
      ]
);

const validateTech100Data = async (page) => {
  const emptyState = await page.$("[data-tech100-empty-state]");
  if (emptyState) {
    throw new Error("TECH100 empty-state banner detected");
  }
  const status = await page.$("#tech100-data-status");
  if (!status) {
    throw new Error("TECH100 data status element not found");
  }
  const levelCount = Number(await status.getAttribute("data-level-count") || 0);
  const constituentCount = Number(await status.getAttribute("data-constituent-count") || 0);
  if (levelCount < 10) {
    throw new Error(`TECH100 chart has too few points (${levelCount})`);
  }
  if (constituentCount < 5) {
    throw new Error(`TECH100 constituents count too low (${constituentCount})`);
  }
  const rows = await page.$$("#tech100-constituents-body tr");
  if (rows.length < 5) {
    throw new Error(`TECH100 table rows missing (${rows.length})`);
  }
};

const run = async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({
    viewport: { width: 1440, height: 900 },
  });

  await page.addStyleTag({
    content: "* { transition: none !important; animation: none !important; }",
  });

  for (const target of targets) {
    const url = `${baseUrl}${target.path}`;
    const resp = await page.goto(url, { waitUntil: "networkidle" });
    if (resp) {
      const status = resp.status();
      if (status >= 500) {
        throw new Error(`Screenshot target failed with ${status}: ${target.path}`);
      }
      if (status === 404) {
        process.stdout.write(`skipping ${target.path} (404)\n`);
        continue;
      }
    }
    if (mode === "after" && target.path === tech100Path) {
      await page.waitForSelector("#tech100-data-status", { timeout: 15000, state: "attached" });
      await page.waitForSelector("#tech100-level-chart", { timeout: 15000 });
      await validateTech100Data(page);
    }
    if (target.path === tech100Path) {
      const chart = await page.$("#tech100-level-chart");
      if (chart) {
        await page.waitForTimeout(500);
        const chartPath = path.join(outDir, "index_overview_chart.png");
        await chart.screenshot({ path: chartPath });
        process.stdout.write(`saved ${chartPath}\n`);
      } else {
        process.stdout.write("chart selector not found; skipped\n");
      }
    }
    if (target.path.includes("tech100")) {
      await page.waitForTimeout(300);
    }
    const outPath = path.join(outDir, target.name);
    await page.screenshot({ path: outPath, fullPage: true });
    process.stdout.write(`saved ${outPath}\n`);
  }

  await browser.close();
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
