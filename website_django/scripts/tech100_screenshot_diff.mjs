import fs from "node:fs";
import path from "node:path";
import pixelmatch from "pixelmatch";
import { PNG } from "pngjs";

const rootDir = process.cwd();
const screenshotDir = path.resolve(rootDir, "..", "docs", "screenshots", "tech100");
const beforeDir = path.join(screenshotDir, "before");
const afterDir = path.join(screenshotDir, "after");
const diffDir = path.join(screenshotDir, "diff");

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
  if (before.width !== after.width || before.height !== after.height) {
    console.warn(`Skipping diff for ${beforeName} (size mismatch)`);
    return;
  }
  const { width, height } = before;
  const diff = new PNG({ width, height });
  const mismatch = pixelmatch(before.data, after.data, diff.data, width, height, {
    threshold: 0.1,
  });
  fs.writeFileSync(diffPath, PNG.sync.write(diff));
  process.stdout.write(`diff ${diffPath} (mismatch: ${mismatch})\n`);
};

const requiredAfter = ["tech100.png", "constituents.png", "attribution.png", "stats.png"];

for (const name of requiredAfter) {
  const filePath = path.join(afterDir, name);
  if (!fs.existsSync(filePath)) {
    console.warn(`Missing screenshot: ${name}`);
  } else {
    assertNotBlank(filePath);
  }
}

diffPair("home.png", "home.png", "diff_home.png");
diffPair("tech100.png", "tech100.png", "diff_tech100.png");
