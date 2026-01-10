import fs from "node:fs";
import path from "node:path";

process.env.PLAYWRIGHT_BROWSERS_PATH = process.env.PLAYWRIGHT_BROWSERS_PATH || "/home/ubuntu/.cache/ms-playwright";
process.env.PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = process.env.PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD || "1";

import { chromium } from "playwright";
import { buildAiRegCandidates } from "./ai_reg_url_candidates.mjs";

const args = process.argv.slice(2);
const getArg = (name, fallback) => {
  const idx = args.indexOf(name);
  if (idx === -1) return fallback;
  return args[idx + 1] || fallback;
};

const mode = getArg("--mode", process.env.AI_REG_SCREENSHOT_MODE || "after");
const baseUrl = getArg("--base-url", process.env.AI_REG_BASE_URL || "http://127.0.0.1:8001");
const overridePath = getArg("--path", process.env.AI_REG_SCREENSHOT_PATH || "");
const screenshotDir = process.env.AI_REG_SCREENSHOT_DIR || "ai_reg";
const timeoutMs = Number(process.env.AI_REG_SCREENSHOT_TIMEOUT_MS || "15000");
const force2d = process.env.AI_REG_FORCE_2D === "1";
const authUser = process.env.AI_REG_BASIC_AUTH_USER || "";
const authPass = process.env.AI_REG_BASIC_AUTH_PASS || "";
const hostHeader = process.env.AI_REG_SCREENSHOT_HOST_HEADER || "";
const ignoreHttpsErrors = process.env.AI_REG_IGNORE_HTTPS_ERRORS === "1";
const hostResolve = process.env.AI_REG_HOST_RESOLVE || "";

const outDir = path.resolve(process.cwd(), "..", "docs", "screenshots", screenshotDir, mode);
fs.mkdirSync(outDir, { recursive: true });

const targets = buildAiRegCandidates({ override: overridePath }).map((urlPath) => ({
  path: urlPath,
  name: "ai_regulation.png",
}));

const assertLoadingCleared = async (page) => {
  await page.waitForFunction(
    () => {
      const loading = document.querySelector("[data-map-loading]");
      if (!loading) return true;
      const style = window.getComputedStyle(loading);
      return loading.hidden || style.display === "none" || style.visibility === "hidden" || style.opacity === "0";
    },
    { timeout: timeoutMs }
  );
};

const assertSummaryPopulated = async (page) => {
  await page.waitForFunction(
    () => {
      const jurisdictions = document.querySelector("[data-ai-reg-summary-jurisdictions]");
      const instruments = document.querySelector("[data-ai-reg-summary-instruments]");
      const jText = jurisdictions?.textContent?.trim() || "";
      const iText = instruments?.textContent?.trim() || "";
      return /\d/.test(jText) && /\d/.test(iText);
    },
    { timeout: timeoutMs }
  );
};

const assertMapRendered = async (page) => {
  await page.waitForFunction(
    () => {
      const globeCanvas = document.querySelector("[data-globe] canvas");
      if (globeCanvas) return true;
      const paths = document.querySelectorAll("[data-fallback] svg path");
      return paths.length > 100;
    },
    { timeout: timeoutMs }
  );
};

const clickCountry = async (page) => {
  const fallbackPaths = await page.$$('[data-fallback] svg path[data-iso2]');
  if (fallbackPaths.length > 0) {
    await fallbackPaths[0].evaluate((el) => {
      el.dispatchEvent(
        new MouseEvent("click", { bubbles: true, cancelable: true, view: window })
      );
    });
  } else {
    const canvas = await page.$('[data-globe] canvas');
    if (canvas) {
      const box = await canvas.boundingBox();
      if (box) {
        await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
      }
    }
  }
  await page.waitForFunction(
    () => {
      const panel = document.querySelector('[data-drilldown-body]');
      const text = panel?.textContent || "";
      return !text.includes('Waiting for a jurisdiction selection');
    },
    { timeout: timeoutMs }
  );
};

const run = async () => {
  process.stdout.write("Launching browser...\\n");
  const hostResolverRule = hostResolve
    ? (() => {
        const [host, target] = hostResolve.split(/[:=]/).map((part) => part.trim());
        if (!host || !target) return "";
        return `MAP ${host} ${target}`;
      })()
    : "";
  const browser = await chromium.launch({
    args: [
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--single-process",
      "--no-zygote",
      "--disable-gpu",
      ...(hostResolverRule ? [`--host-resolver-rules=${hostResolverRule}`] : []),
    ],
  });
  process.stdout.write("Browser launched.\\n");
  const context = await browser.newContext({
    viewport: { width: 1200, height: 800 },
    ignoreHTTPSErrors: ignoreHttpsErrors,
    httpCredentials: authUser && authPass ? { username: authUser, password: authPass } : undefined,
    extraHTTPHeaders: hostHeader ? { Host: hostHeader } : undefined,
  });
  const page = await context.newPage();
  process.stdout.write("Page created.\\n");
  page.setDefaultTimeout(timeoutMs);
  page.setDefaultNavigationTimeout(timeoutMs * 2);

  await page.addStyleTag({ content: "* { transition: none !important; animation: none !important; }" });
  if (force2d) {
    await page.addInitScript(() => {
      window.__AI_REG_FORCE_2D = true;
    });
  }

  const withTimeout = async (promise, ms, label) => {
    let timer;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    });
    try {
      return await Promise.race([promise, timeout]);
    } finally {
      clearTimeout(timer);
    }
  };

  for (const target of targets) {
    const url = `${baseUrl}${target.path}`;
    process.stdout.write(`Loading ${url}\\n`);
    const response = await withTimeout(
      page.goto(url, { waitUntil: "commit", timeout: timeoutMs * 2 }),
      timeoutMs * 3,
      "Navigation"
    );
    await page.waitForTimeout(1000);
    if (response && response.status() >= 500) {
      throw new Error(`Screenshot target failed with ${response.status()}: ${target.path}`);
    }

    const failures = [];
    try {
      await assertLoadingCleared(page);
    } catch (error) {
      failures.push(`loading: ${error.message}`);
    }
    try {
      await assertSummaryPopulated(page);
    } catch (error) {
      failures.push(`summary: ${error.message}`);
    }
    try {
      await assertMapRendered(page);
    } catch (error) {
      failures.push(`map: ${error.message}`);
    }
    try {
      await clickCountry(page);
    } catch (error) {
      failures.push(`click: ${error.message}`);
    }

    const outPath = path.join(outDir, target.name);
    await page.screenshot({ path: outPath, fullPage: true, timeout: timeoutMs * 2 });
    process.stdout.write(`saved ${outPath}\n`);

    if (failures.length) {
      const failurePath = path.join(outDir, "ai_regulation_failures.txt");
      fs.writeFileSync(failurePath, failures.join("\n") + "\n");
      throw new Error(`Assertions failed:\n${failures.join("\n")}`);
    }
  }

  const closeWithTimeout = async (label, action, fallback) => {
    let timer;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out`)), 15000);
    });
    try {
      await Promise.race([action(), timeout]);
    } catch (error) {
      if (fallback) {
        try {
          fallback();
        } catch (fallbackError) {
          console.warn(fallbackError);
        }
      }
    } finally {
      clearTimeout(timer);
    }
  };

  await closeWithTimeout("Context close", () => context.close());
  await closeWithTimeout("Browser close", () => browser.close(), () => browser.process()?.kill("SIGKILL"));
};

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
