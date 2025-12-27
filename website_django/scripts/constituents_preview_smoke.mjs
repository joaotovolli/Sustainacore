import { chromium } from "playwright";

const args = process.argv.slice(2);
const getArg = (name, fallback) => {
  const idx = args.indexOf(name);
  if (idx === -1) return fallback;
  return args[idx + 1] || fallback;
};

const baseUrl = getArg("--base-url", "http://127.0.0.1:8002");
const timeoutMs = Number(process.env.CONSTITUENTS_SMOKE_TIMEOUT_MS || "60000");

const buildToken = (email) => {
  const payload = Buffer.from(JSON.stringify({ email })).toString("base64url");
  return `header.${payload}.sig`;
};

const expect = (condition, message) => {
  if (!condition) throw new Error(message);
};

const run = async () => {
  const browser = await chromium.launch();

  const context = await browser.newContext();
  const page = await context.newPage();

  await page.goto(`${baseUrl}/tech100/?dg_debug=1`, {
    waitUntil: "domcontentloaded",
    timeout: timeoutMs,
  });
  await page.waitForFunction(
    () => document.documentElement.dataset.downloadGate === "ready",
    { timeout: 5000 }
  );

  await page.click("[data-constituents-locked]");
  await page.waitForSelector("#download-auth-modal.modal--open", { timeout: 1000 });
  const modalOpen = await page.evaluate(
    () => document.querySelector("#download-auth-modal")?.classList.contains("modal--open")
  );
  expect(modalOpen, "Expected download modal to open from locked preview");
  await page.click("#download-auth-modal [data-modal-close]");

  await page.close();
  await context.close();

  const loggedInContext = await browser.newContext();
  const cookieDomain = new URL(baseUrl).hostname;
  await loggedInContext.addCookies([
    {
      name: "sc_session",
      value: buildToken("demo@example.com"),
      domain: cookieDomain,
      path: "/",
      httpOnly: true,
    },
  ]);
  const loggedInPage = await loggedInContext.newPage();
  await loggedInPage.goto(`${baseUrl}/tech100/`, {
    waitUntil: "domcontentloaded",
    timeout: timeoutMs,
  });
  const lockedExists = await loggedInPage.evaluate(
    () => Boolean(document.querySelector("[data-constituents-locked]"))
  );
  expect(!lockedExists, "Locked preview should not render for logged-in users");
  const summaryText = await loggedInPage.textContent("#constituents-summary");
  expect(!summaryText?.includes("preview"), "Logged-in summary should not show preview text");

  await loggedInPage.close();
  await loggedInContext.close();
  await browser.close();
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
