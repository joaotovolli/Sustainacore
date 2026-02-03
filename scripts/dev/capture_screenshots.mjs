import fs from "fs";
import path from "path";
import { chromium } from "playwright";

const urlListPath = process.argv[2];
const outputDir = process.argv[3];

if (!urlListPath || !outputDir) {
  console.error("Usage: node scripts/dev/capture_screenshots.mjs <url_list.json> <output_dir>");
  process.exit(1);
}

const urls = JSON.parse(fs.readFileSync(urlListPath, "utf-8"));
fs.mkdirSync(outputDir, { recursive: true });

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1280, height: 720 } });

for (const entry of urls) {
  const localPath = path.join(outputDir, `local_${entry.slug}.png`);
  const prodPath = path.join(outputDir, `prod_${entry.slug}.png`);

  try {
    await page.goto(entry.local_url, { waitUntil: "networkidle", timeout: 30000 });
    await page.screenshot({ path: localPath, fullPage: true });
  } catch (err) {
    console.error(`Local screenshot failed for ${entry.local_url}: ${err}`);
  }

  try {
    await page.goto(entry.prod_url, { waitUntil: "networkidle", timeout: 30000 });
    await page.screenshot({ path: prodPath, fullPage: true });
  } catch (err) {
    console.error(`Prod screenshot failed for ${entry.prod_url}: ${err}`);
  }
}

await browser.close();
