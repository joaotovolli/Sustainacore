import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright";

const args = process.argv.slice(2);
const getArg = (name, fallback) => {
  const idx = args.indexOf(name);
  if (idx === -1) return fallback;
  return args[idx + 1] || fallback;
};

const mode = getArg("--mode", "after");
const baseUrl = getArg("--base-url", "http://127.0.0.1:8001");

const outDir = path.resolve(process.cwd(), "..", "docs", "screenshots");
fs.mkdirSync(outDir, { recursive: true });

const targets =
  mode === "before"
    ? [
        { path: "/", name: "before_home.png" },
        { path: "/tech100/", name: "before_tech100_existing.png" },
      ]
    : [
        { path: "/", name: "after_home.png" },
        { path: "/tech100/", name: "after_tech100_existing.png" },
        { path: "/tech100/index/", name: "after_index_overview.png" },
        { path: "/tech100/constituents/", name: "after_constituents.png" },
        { path: "/tech100/attribution/", name: "after_attribution.png" },
        { path: "/tech100/stats/", name: "after_stats.png" },
      ];

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
    await page.goto(url, { waitUntil: "networkidle" });
    if (target.path === "/tech100/index/") {
      await page.waitForSelector("#tech100-level-chart");
      await page.waitForTimeout(500);
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
