import { chromium } from "playwright";

const args = process.argv.slice(2);
const getArg = (name, fallback) => {
  const idx = args.indexOf(name);
  if (idx === -1) return fallback;
  return args[idx + 1] || fallback;
};

const baseUrl = getArg("--base-url", "http://127.0.0.1:8002");
const timeoutMs = Number(process.env.DOWNLOAD_SMOKE_TIMEOUT_MS || "60000");

const buildToken = (email) => {
  const payload = Buffer.from(JSON.stringify({ email })).toString("base64url");
  return `header.${payload}.sig`;
};

const expect = (condition, message) => {
  if (!condition) {
    throw new Error(message);
  }
};

const run = async () => {
  const browser = await chromium.launch();
  const context = await browser.newContext({ acceptDownloads: true });
  const page = await context.newPage();

  const pagesToCheck = [
    "/tech100/performance/",
    "/tech100/index/",
    "/tech100/",
  ];

  const collectTargets = async (activePage) => {
    return await activePage.evaluate(() => {
      const elements = Array.from(document.querySelectorAll("a, button, [data-download-url]"));
      return elements
        .filter((el) => {
          const text = (el.textContent || "").toLowerCase();
          const href = el.getAttribute?.("href") || "";
          const dataUrl = el.getAttribute?.("data-download-url") || "";
          return (
            text.includes("download") ||
            href.includes("/export/") ||
            href.includes("format=csv") ||
            href.endsWith(".csv") ||
            dataUrl
          );
        })
        .map((el) => {
          const href = el.getAttribute?.("href") || "";
          const dataUrl = el.getAttribute?.("data-download-url") || "";
          return {
            tag: el.tagName.toLowerCase(),
            href,
            dataUrl,
            text: (el.textContent || "").trim().slice(0, 80),
          };
        });
    });
  };

  const assertLoggedOut = async (path) => {
    await page.goto(`${baseUrl}${path}?dg_debug=1`, {
      waitUntil: "networkidle",
      timeout: timeoutMs,
    });
    const targets = await collectTargets(page);
    for (let i = 0; i < targets.length; i += 1) {
      const target = targets[i];
      const selector = target.href
        ? `a[href="${target.href}"]`
        : target.dataUrl
          ? `[data-download-url="${target.dataUrl}"]`
          : `${target.tag}:has-text("${target.text}")`;
      const locator = page.locator(selector).first();
      await locator.scrollIntoViewIfNeeded();
      await locator.click();
      await page.waitForTimeout(600);
      const modalOpen = await page.evaluate(
        () => document.querySelector("[data-download-modal]")?.classList.contains("modal--open")
      );
      const urlChanged = page.url().includes("/export/") || page.url().includes("download_login=1");
      if (!modalOpen && !urlChanged) {
        await page.screenshot({ path: `download_gate_dead_click_${path.replace(/\//g, "_")}_${i}.png` });
        throw new Error(`Dead click detected on ${path}: ${target.text || target.href || target.dataUrl}`);
      }
    }
  };

  await assertLoggedOut(pagesToCheck[0]);
  await assertLoggedOut(pagesToCheck[1]);
  await assertLoggedOut(pagesToCheck[2]);

  const cookieDomain = new URL(baseUrl).hostname;
  await context.addCookies([
    {
      name: "sc_session",
      value: buildToken("demo@example.com"),
      domain: cookieDomain,
      path: "/",
      httpOnly: true,
    },
  ]);

  const loggedInPage = await context.newPage();
  for (const path of pagesToCheck) {
    await loggedInPage.goto(`${baseUrl}${path}`, {
      waitUntil: "networkidle",
      timeout: timeoutMs,
    });
    const targets = await collectTargets(loggedInPage);
    for (let i = 0; i < targets.length; i += 1) {
      const target = targets[i];
      const selector = target.href
        ? `a[href="${target.href}"]`
        : target.dataUrl
          ? `[data-download-url="${target.dataUrl}"]`
          : `${target.tag}:has-text("${target.text}")`;
      const locator = loggedInPage.locator(selector).first();
      await locator.scrollIntoViewIfNeeded();
      const downloadPromise = loggedInPage.waitForEvent("download", { timeout: 8000 }).catch(() => null);
      await locator.click();
      const download = await downloadPromise;
      const nav = loggedInPage.url().includes("/export/");
      if (!download && !nav) {
        await loggedInPage.screenshot({ path: `download_gate_logged_in_dead_${path.replace(/\//g, "_")}_${i}.png` });
        throw new Error(`Logged-in click did not download or navigate: ${target.text || target.href || target.dataUrl}`);
      }
    }
  }

  await loggedInPage.close();
  await browser.close();
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
