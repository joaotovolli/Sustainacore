import fs from "node:fs";
import path from "node:path";
import pixelmatch from "pixelmatch";
import { PNG } from "pngjs";

const rootDir = process.cwd();
const outRoot = process.env.VRT_DIR || path.resolve(rootDir, "..", "artifacts", "vrt");
const beforeDir = path.join(outRoot, "baseline");
const afterDir = path.join(outRoot, "current");
const diffDir = path.join(outRoot, "diff");
const statusBeforePath = path.join(beforeDir, "status.json");
const statusAfterPath = path.join(afterDir, "status.json");

const viewports = [
  // Desktop remains strict; allow 0.25% to avoid failing on minor footer/link shifts.
  { label: "desktop_1440x900", desktop: true, maxMismatch: 0.0025 },
  { label: "desktop_1920x1080", desktop: true, maxMismatch: 0.0025 },
  // Mobile is advisory unless the mismatch exceeds 1% to avoid minor reflow noise.
  { label: "mobile_390x844", desktop: false, maxMismatch: 0.01 },
  { label: "mobile_844x390", desktop: false, maxMismatch: 0.01 },
];

const pages = [
  "home",
  "news",
  "news_detail_one",
  "news_detail_two",
  "tech100_performance",
  "tech100_index",
  "tech100_constituents",
];

const loadStatus = (filePath) => {
  if (!fs.existsSync(filePath)) return {};
  try {
    const raw = fs.readFileSync(filePath, "utf-8");
    const parsed = JSON.parse(raw);
    return parsed.status || {};
  } catch (err) {
    return {};
  }
};

const statusBefore = loadStatus(statusBeforePath);
const statusAfter = loadStatus(statusAfterPath);

const readPng = (filePath) => PNG.sync.read(fs.readFileSync(filePath));

const assertNotBlank = (filePath) => {
  const image = readPng(filePath);
  const { data } = image;
  let total = 0;
  for (let i = 0; i < data.length; i += 4) {
    total += data[i] + data[i + 1] + data[i + 2];
  }
  const avg = total / (data.length / 4);
  if (avg > 750) {
    throw new Error(`Screenshot appears blank: ${filePath}`);
  }
};

const cropPng = (image, width, height) => {
  const cropped = new PNG({ width, height });
  for (let y = 0; y < height; y += 1) {
    const rowStart = y * image.width * 4;
    const rowEnd = rowStart + width * 4;
    const targetStart = y * width * 4;
    image.data.copy(cropped.data, targetStart, rowStart, rowEnd);
  }
  return cropped;
};

const diffPair = (beforePath, afterPath, diffPath, maxMismatch) => {
  if (!fs.existsSync(beforePath) || !fs.existsSync(afterPath)) {
    throw new Error(`Missing screenshot: ${beforePath} or ${afterPath}`);
  }
  const before = readPng(beforePath);
  const after = readPng(afterPath);
  const width = Math.min(before.width, after.width);
  const height = Math.min(before.height, after.height);
  const beforeCrop = cropPng(before, width, height);
  const afterCrop = cropPng(after, width, height);
  const diff = new PNG({ width, height });
  const mismatch = pixelmatch(beforeCrop.data, afterCrop.data, diff.data, width, height, {
    threshold: 0.1,
  });
  const totalPixels = width * height;
  const ratio = mismatch / totalPixels;
  fs.mkdirSync(path.dirname(diffPath), { recursive: true });
  fs.writeFileSync(diffPath, PNG.sync.write(diff));
  return ratio;
};

let failed = false;
fs.mkdirSync(diffDir, { recursive: true });
const summary = {};

for (const viewport of viewports) {
  summary[viewport.label] = {};
  for (const page of pages) {
    const beforePath = path.join(beforeDir, viewport.label, `${page}.png`);
    const afterPath = path.join(afterDir, viewport.label, `${page}.png`);
    const beforeStatus = statusBefore?.[viewport.label]?.[page];
    const afterStatus = statusAfter?.[viewport.label]?.[page];

    if (afterStatus && afterStatus >= 500) {
      console.error(`Current render failed for ${viewport.label} ${page}: status ${afterStatus}`);
      failed = true;
      continue;
    }

    if (beforeStatus && beforeStatus >= 500) {
      process.stdout.write(`skip diff for ${viewport.label} ${page} (baseline status ${beforeStatus})\n`);
      continue;
    }
    assertNotBlank(afterPath);
    const diffPath = path.join(diffDir, viewport.label, `${page}.png`);
    const ratio = diffPair(beforePath, afterPath, diffPath, viewport.maxMismatch);
    const ratioText = (ratio * 100).toFixed(2);
    process.stdout.write(`${viewport.label} ${page} mismatch ${ratioText}%\n`);
    summary[viewport.label][page] = {
      mismatchRatio: ratio,
      mismatchPercent: Number(ratioText),
      baselineStatus: beforeStatus ?? null,
      currentStatus: afterStatus ?? null,
    };
    if (viewport.desktop && ratio > viewport.maxMismatch) {
      console.error(`Desktop diff too large for ${viewport.label} ${page}: ${ratioText}%`);
      failed = true;
    }
    if (!viewport.desktop && ratio > viewport.maxMismatch) {
      console.error(`Mobile diff too large for ${viewport.label} ${page}: ${ratioText}%`);
      failed = true;
    }
  }
}

const summaryPath = path.join(diffDir, "summary.json");
fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));

if (failed) {
  process.exit(1);
}
