import { chromium } from "playwright";

const args = process.argv.slice(2);
const getArg = (name, fallback) => {
  const idx = args.indexOf(name);
  if (idx === -1) return fallback;
  return args[idx + 1] || fallback;
};

const baseUrl = getArg("--base-url", "http://127.0.0.1:8012");
const timeoutMs = Number(process.env.ASK2_SMOKE_TIMEOUT_MS || "30000");

const expect = (condition, message) => {
  if (!condition) {
    throw new Error(message);
  }
};

const buildLongAnswer = () => {
  const longParagraph = Array.from({ length: 120 }, () => "Long answer text").join(" ");
  return [
    "Note. See sources.",
    "**Answer summary**",
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

const mockPayload = {
  reply: buildLongAnswer(),
  sources: [
    {
      title: "TECH100 methodology",
      url: "local://TECH100_AI_Governance_Methodology_v1.0.pdf",
      snippet:
        "Methodology overview with additional detail on eligibility thresholds and total-return construction that ends midwor",
    },
  ],
};

process.stdout.write(`[ask2-smoke] mock_reply_len=${mockPayload.reply.length}\n`);

const run = async () => {
  const browser = await chromium.launch({
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });
  const context = await browser.newContext();
  const page = await context.newPage();
  page.setDefaultTimeout(8000);
  page.setDefaultNavigationTimeout(8000);
  page.on("console", (msg) => {
    process.stdout.write(`[ask2-console] ${msg.type()}: ${msg.text()}\n`);
  });
  page.on("pageerror", (err) => {
    process.stdout.write(`[ask2-pageerror] ${err}\n`);
  });
  page.on("requestfailed", (req) => {
    process.stdout.write(`[ask2-requestfailed] ${req.url()} ${req.failure()?.errorText || ""}\n`);
  });
  process.stdout.write("[ask2-smoke] open page\n");
  await page.goto(`${baseUrl}/ask2/`, { waitUntil: "domcontentloaded", timeout: timeoutMs });
  await page.waitForSelector("[data-ask2-input]");
  await page.waitForFunction(
    () => document.documentElement.dataset.ask2Ready === "true",
    { timeout: timeoutMs }
  );
  process.stdout.write("[ask2-smoke] submit prompt\n");
  await page.evaluate((payload) => {
    const api = window.SCAsk2?.renderMock;
    if (!api) {
      throw new Error("Ask2 render hook missing");
    }
    api(payload, "Is Microsoft in the TECH100 index?");
  }, mockPayload);

  process.stdout.write("[ask2-smoke] wait for response\n");
  await page.waitForSelector(".bubble--assistant .ask2-bubble", { timeout: timeoutMs });

  const assistantLength = await page.evaluate(() => {
    const bubbles = document.querySelectorAll(".bubble--assistant");
    const last = bubbles[bubbles.length - 1];
    return last?.textContent?.length || 0;
  });
  process.stdout.write(`[ask2-smoke] assistant_text_len=${assistantLength}\n`);

  const toggleEl = await page.$(".bubble--assistant:last-of-type .ask2-bubble__toggle");
  expect(toggleEl, "Expected Show more toggle for long answer");
  const toggleText = await page.textContent(".bubble--assistant:last-of-type .ask2-bubble__toggle");
  expect(toggleText?.includes("Show more"), "Expected Show more toggle for long answer");

  const hasList = await page.evaluate(() => {
    const bubbles = document.querySelectorAll(".bubble--assistant");
    const last = bubbles[bubbles.length - 1];
    return Boolean(last?.querySelector("ul li"));
  });
  expect(hasList, "Expected markdown list to render as <ul><li>");

  const linkCount = await page.evaluate(() => {
    const bubbles = document.querySelectorAll(".bubble--assistant");
    const last = bubbles[bubbles.length - 1];
    return last ? last.querySelectorAll(".ask2-sources a").length : 0;
  });
  expect(linkCount >= 1, "Expected at least one clickable source link");

  await page.click(".bubble--assistant:last-of-type .ask2-bubble__toggle");

  const overflowHidden = await page.evaluate(() => {
    const bubbles = document.querySelectorAll(".bubble--assistant");
    const last = bubbles[bubbles.length - 1];
    const content = last?.querySelector(".ask2-bubble__content");
    if (!content) return true;
    const style = window.getComputedStyle(content);
    return style.overflow === "hidden" || style.overflowY === "hidden" || style.maxHeight !== "none";
  });
  expect(!overflowHidden, "Expected expanded content to avoid overflow clipping");

  const renderedText = await page.evaluate(() => {
    const bubbles = document.querySelectorAll(".bubble--assistant");
    const last = bubbles[bubbles.length - 1];
    const content = last?.querySelector(".ask2-bubble__content");
    return content?.textContent || "";
  });
  const normalize = (value) => value.replace(/\s+/g, " ").trim();
  const normalizeReply = (value) =>
    normalize(
      value
        .replace(/```/g, "")
        .replace(/\*\*([^*]+)\*\*/g, "$1")
        .replace(/\*([^*]+)\*/g, "$1")
        .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
        .replace(/^\s*[-*]\s+/gm, "")
        .replace(/^\s*\d+\.\s+/gm, "")
    );
  const normalizedRendered = normalize(renderedText);
  const normalizedReply = normalizeReply(mockPayload.reply);
  process.stdout.write(`[ask2-smoke] reply_head=${normalizedReply.slice(0, 120)}\n`);
  process.stdout.write(`[ask2-smoke] render_head=${normalizedRendered.slice(0, 120)}\n`);
  expect(
    normalizedRendered.includes("Note. See sources."),
    "Expected rendered answer to include the leading sentence"
  );
  expect(
    normalizedRendered.includes("Answer summary"),
    "Expected rendered answer to include the answer summary heading"
  );
  expect(
    normalizedRendered.includes("Long answer text"),
    "Expected rendered answer to include the long answer body"
  );
  expect(
    !/\\bte\\. See sources\\./i.test(normalizedRendered),
    "Unexpected 'te. See sources.' artifact in assistant text"
  );

  const snippetCheck = await page.evaluate(() => {
    const bubbles = document.querySelectorAll(".bubble--assistant");
    const last = bubbles[bubbles.length - 1];
    const snippet = last?.querySelector(".ask2-source__snippet");
    const link = last?.querySelector(".ask2-sources a");
    return {
      snippetText: snippet?.textContent || "",
      href: link?.getAttribute("href") || "",
    };
  });
  expect(
    snippetCheck.snippetText.endsWith("…"),
    "Expected truncated snippet to end with an ellipsis"
  );
  expect(
    !snippetCheck.snippetText.toLowerCase().endsWith("midwor…"),
    "Expected snippet to avoid mid-word truncation"
  );
  expect(
    snippetCheck.href.startsWith("http") || snippetCheck.href.startsWith("/"),
    "Expected source href to be a real URL"
  );

  try {
    await context.close();
  } catch (_) {
    // Best-effort cleanup; do not block CI.
  }
  try {
    browser.close();
  } catch (_) {
    // ignore
  }
  setTimeout(() => process.exit(0), 250);
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
