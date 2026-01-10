import fs from "node:fs";
import path from "node:path";
import pixelmatch from "pixelmatch";
import { PNG } from "pngjs";

const rootDir = process.cwd();
const screenshotDir = path.resolve(
  rootDir,
  "..",
  "docs",
  "screenshots",
  process.env.AI_REG_SCREENSHOT_DIR || "ai_reg"
);
const beforeDir = path.join(screenshotDir, "before");
const afterDir = path.join(screenshotDir, "after");
const diffDir = path.join(screenshotDir, "diff");
const MAX_DIFF_HEIGHT = 1200;

fs.mkdirSync(diffDir, { recursive: true });

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

const diffPair = (beforeName, afterName, diffName) => {
  const beforePath = path.join(beforeDir, beforeName);
  const afterPath = path.join(afterDir, afterName);
  const diffPath = path.join(diffDir, diffName);
  if (!fs.existsSync(beforePath) || !fs.existsSync(afterPath)) {
    console.warn(`Skipping diff for ${beforeName} (missing file)`);
    return;
  }
  const before = readPng(beforePath);
  const after = readPng(afterPath);
  const width = Math.min(before.width, after.width);
  const height = Math.min(before.height, after.height, MAX_DIFF_HEIGHT);
  if (width <= 0 || height <= 0) {
    console.warn(`Skipping diff for ${beforeName} (invalid dimensions)`);
    return;
  }
  const beforeCrop = cropPng(before, width, height);
  const afterCrop = cropPng(after, width, height);
  const diff = new PNG({ width, height });
  const mismatch = pixelmatch(beforeCrop.data, afterCrop.data, diff.data, width, height, {
    threshold: 0.1,
  });
  fs.writeFileSync(diffPath, PNG.sync.write(diff));
  process.stdout.write(`diff ${diffPath} (mismatch: ${mismatch})\n`);
};

const requiredAfter = ["ai_regulation.png"];

for (const name of requiredAfter) {
  const filePath = path.join(afterDir, name);
  if (!fs.existsSync(filePath)) {
    console.warn(`Missing screenshot: ${name}`);
  } else {
    assertNotBlank(filePath);
  }
}

diffPair("ai_regulation.png", "ai_regulation.png", "diff_ai_regulation.png");
