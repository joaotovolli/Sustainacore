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
    if (target.path === "/tech100/index/") {
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
