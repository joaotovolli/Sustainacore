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

  // Logged-out: modal should open.
  await page.goto(`${baseUrl}/tech100/performance/?dg_debug=1`, {
    waitUntil: "domcontentloaded",
    timeout: timeoutMs,
  });
  await page.click('a[href*="/tech100/performance/export/"]');
  await page.waitForTimeout(800);
  const modalOpen = await page.evaluate(
    () => document.querySelector(".modal")?.classList.contains("modal--open")
  );
  expect(modalOpen, "Expected modal to open for logged-out download click.");

  // Non-download link should navigate (no dead click).
  await page.click('a[href="/"]');
  await page.waitForURL(/\/$/, { timeout: timeoutMs });

  // Logged-in: download should start.
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
  await loggedInPage.goto(`${baseUrl}/tech100/performance/`, {
    waitUntil: "domcontentloaded",
    timeout: timeoutMs,
  });
  const downloadPromise = loggedInPage.waitForEvent("download", { timeout: timeoutMs });
  await loggedInPage.click('a[href*="/tech100/performance/export/"]');
  await downloadPromise;
  await loggedInPage.close();

  await browser.close();
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
