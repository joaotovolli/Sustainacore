import fs from "node:fs";
import path from "node:path";
import pixelmatch from "pixelmatch";
import { PNG } from "pngjs";
import { chromium } from "playwright";

const prodBase = process.env.PROD_BASE_URL || "https://sustainacore.org";
const previewBase = process.env.PREVIEW_BASE_URL || "https://preview.sustainacore.org";
const outputDir = process.env.OUTPUT_DIR || path.resolve(process.cwd(), "..", "artifacts", "ui_ask2");
const timeoutMs = Number(process.env.TIMEOUT_MS || "60000");

const beforeDir = path.join(outputDir, "before");
const afterDir = path.join(outputDir, "after");
const diffDir = path.join(outputDir, "diff");
const reportDir = path.join(outputDir, "report");

for (const dir of [beforeDir, afterDir, diffDir, reportDir]) {
  fs.mkdirSync(dir, { recursive: true });
}

const buildLongAnswer = (label) => {
  const longParagraph = Array.from({ length: 80 }, () => "Long answer text").join(" ");
  return [
    `${label}`,
    "",
    "Here is a quick overview with a markdown link to [SustainaCore](https://sustainacore.org) and a citation [1].",
    "",
    "- Bullet one",
    "- Bullet two",
    "",
    "```",
    "const answer = 'test';",
    "console.log(answer);",
    "```",
    "",
    longParagraph,
  ].join("\n");
};

const mockPayload = (label) => ({
  reply: buildLongAnswer(label),
  sources: [
    {
      title: "TECH100 methodology",
      url: "local://TECH100_AI_Governance_Methodology_v1.0.pdf",
      snippet:
        "Methodology overview with additional detail on eligibility thresholds and total-return construction that ends midwor",
    },
  ],
});

const renderAsk2 = async (page) => {
  await page.waitForSelector("[data-ask2-messages]", { timeout: timeoutMs });
  await page.evaluate((payloads) => {
    const api = window.SCAsk2?.renderMock;
    if (api) {
      payloads.forEach((payload) => {
        api(payload.data, payload.prompt);
      });
      return;
    }
    const messages = document.querySelector("[data-ask2-messages]");
    if (!messages) {
      throw new Error("Ask2 message container missing");
    }
    const appendBubble = (role, text) => {
      const bubble = document.createElement("div");
      bubble.className = `bubble bubble--${role}`;
      bubble.textContent = text;
      messages.appendChild(bubble);
      return bubble;
    };
    payloads.forEach((payload) => {
      appendBubble("user", payload.prompt);
      const bubble = appendBubble("assistant", payload.data.reply || "");
      bubble.classList.add("bubble--muted");
    });
  }, [
    { prompt: "Is Microsoft in the TECH100 index?", data: mockPayload("Note. See sources.") },
    { prompt: "How is the TECH100 index built and rebalanced?", data: mockPayload("**Answer summary**") },
  ]);
  await page.waitForSelector(".bubble--assistant:last-of-type", { timeout: timeoutMs });
};

const capture = async (baseUrl, outPath) => {
  process.stdout.write(`[ask2-compare] capture ${baseUrl}\n`);
  const browser = await chromium.launch({ args: ["--no-sandbox", "--disable-dev-shm-usage"] });
  const context = await browser.newContext({ viewport: { width: 1280, height: 1808 } });
  const page = await context.newPage();
  await page.goto(`${baseUrl}/ask2/`, { waitUntil: "domcontentloaded", timeout: timeoutMs });
  await renderAsk2(page);
  await page.screenshot({ path: outPath, fullPage: true });
  await context.close();
  await browser.close();
  process.stdout.write(`[ask2-compare] wrote ${outPath}\n`);
};

const readPng = (filePath) => PNG.sync.read(fs.readFileSync(filePath));

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

const diffImages = (beforePath, afterPath, diffPath, reportPath) => {
  const before = readPng(beforePath);
  const after = readPng(afterPath);
  const width = Math.min(before.width, after.width);
  const height = Math.min(before.height, after.height);
  const beforeCrop = cropPng(before, width, height);
  const afterCrop = cropPng(after, width, height);
  const diff = new PNG({ width, height });
  const mismatch = pixelmatch(beforeCrop.data, afterCrop.data, diff.data, width, height, { threshold: 0.1 });
  fs.writeFileSync(diffPath, PNG.sync.write(diff));
  const report = {
    before: path.basename(beforePath),
    after: path.basename(afterPath),
    width,
    height,
    mismatchPixels: mismatch,
    mismatchPercent: (mismatch / (width * height)) * 100,
  };
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
};

const run = async () => {
  const beforePath = path.join(beforeDir, "ask2_page.png");
  const afterPath = path.join(afterDir, "ask2_page.png");
  const diffPath = path.join(diffDir, "ask2_page_diff.png");
  const reportPath = path.join(reportDir, "ask2_page_diff_report.json");

  process.stdout.write("[ask2-compare] capture prod\n");
  await capture(prodBase, beforePath);
  process.stdout.write("[ask2-compare] capture preview\n");
  await capture(previewBase, afterPath);
  diffImages(beforePath, afterPath, diffPath, reportPath);
  process.stdout.write(`[ask2-compare] wrote diff ${diffPath}\n`);
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
