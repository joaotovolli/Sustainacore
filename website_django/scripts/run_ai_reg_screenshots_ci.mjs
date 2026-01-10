import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, spawnSync } from "node:child_process";
import http from "node:http";
import https from "node:https";
import { buildAiRegCandidates } from "./ai_reg_url_candidates.mjs";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(scriptDir, "..");
const envFiles = ["/etc/sustainacore.env", "/etc/sustainacore/db.env"];

const ensureNodeDeps = () => {
  const lockfilePath = path.join(rootDir, "package-lock.json");
  const playwrightPath = path.join(rootDir, "node_modules", "playwright");
  const playwrightBin = path.join(rootDir, "node_modules", ".bin", "playwright");
  if (fs.existsSync(playwrightPath) || fs.existsSync(playwrightBin)) {
    return;
  }
  const hasLockfile = fs.existsSync(lockfilePath);
  process.stdout.write(
    `Playwright deps missing; running ${hasLockfile ? "npm ci" : "npm install --no-fund --no-audit"}...\n`
  );
  const npmArgs = hasLockfile ? ["ci"] : ["install", "--no-fund", "--no-audit"];
  const npmInstall = spawnSync("npm", npmArgs, { cwd: rootDir, stdio: "inherit" });
  if (npmInstall.status !== 0) {
    throw new Error("Dependency install failed.");
  }
  process.stdout.write("Ensuring Playwright browsers...\n");
  const install = spawnSync("npx", ["playwright", "install", "chromium"], { cwd: rootDir, stdio: "inherit" });
  if (install.status !== 0) {
    throw new Error("Playwright chromium install failed.");
  }
};

const resolvePython = () => {
  const preferred = [
    "/home/ubuntu/.venvs/sustainacore_vm2/bin/python",
    "/opt/code/Sustainacore/website_django/venv/bin/python",
    "/opt/sustainacore/website_django/venv/bin/python",
  ];
  for (const candidate of preferred) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return "python3";
};

const fetchText = (url, hostHeader, timeoutMs, rejectUnauthorized) =>
  new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const client = parsed.protocol === "https:" ? https : http;
    const req = client.request(
      {
        protocol: parsed.protocol,
        hostname: parsed.hostname,
        port: parsed.port,
        path: parsed.pathname + parsed.search,
        method: "GET",
        headers: hostHeader ? { Host: hostHeader } : {},
        ...(parsed.protocol === "https:" ? { rejectUnauthorized } : {}),
        timeout: timeoutMs,
      },
      (res) => {
        let data = "";
        res.on("data", (chunk) => {
          data += chunk.toString();
        });
        res.on("end", () => {
          resolve({ status: res.statusCode || 0, text: data });
        });
      }
    );
    req.on("timeout", () => {
      req.destroy(new Error("request timeout"));
    });
    req.on("error", reject);
    req.end();
  });

const waitFor = async (url, selector, timeoutMs = 90000, hostHeader, rejectUnauthorized) => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const { status, text } = await fetchText(url, hostHeader, 8000, rejectUnauthorized);
      if (status >= 200 && status < 400) {
        if (!selector || text.includes(selector)) {
          return text;
        }
      }
    } catch (error) {
      // ignore
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  throw new Error(`Timeout waiting for ${url}`);
};

const run = async () => {
  process.stdout.write("Preparing Playwright...\n");
  ensureNodeDeps();
  const requestedMode = process.env.AI_REG_SCREENSHOT_MODE || "after";
  const useExistingServer = process.env.AI_REG_USE_EXISTING_SERVER === "1";
  const externalBaseUrl = process.env.AI_REG_BASE_URL || "";
  const localHostHeader = "sustainacore.org";
  let hostHeader = process.env.AI_REG_SCREENSHOT_HOST_HEADER || "";
  const ignoreHttpsErrors = process.env.AI_REG_IGNORE_HTTPS_ERRORS === "1";
  let hostResolve = "";

  let port = "";
  let baseUrl = externalBaseUrl || "";
  let screenshotBaseUrl = baseUrl;
  if (!baseUrl) {
    baseUrl = "http://127.0.0.1";
  }

  if (baseUrl === "http://127.0.0.1" || baseUrl === "http://localhost") {
    if (!useExistingServer) {
      const portScript = path.join(rootDir, "scripts", "find_free_port.mjs");
      const portProc = spawn("node", [portScript], { stdio: ["ignore", "pipe", "pipe"] });
      port = await new Promise((resolve, reject) => {
        let out = "";
        let err = "";
        portProc.stdout.on("data", (data) => {
          out += data.toString();
        });
        portProc.stderr.on("data", (data) => {
          err += data.toString();
        });
        portProc.on("close", (code) => {
          if (code !== 0) {
            reject(new Error(err || `port script exited ${code}`));
          } else {
            resolve(out.trim());
          }
        });
      });
      baseUrl = `http://127.0.0.1:${port}`;
      screenshotBaseUrl = baseUrl;
      process.stdout.write(`Using port ${port}\n`);
    } else {
      baseUrl = "https://127.0.0.1";
      hostResolve = hostHeader || localHostHeader;
      screenshotBaseUrl = `https://${hostResolve}`;
      process.stdout.write("Using existing server at https://127.0.0.1\n");
    }
    hostHeader = useExistingServer ? (hostHeader || localHostHeader) : "";
  }
  if (!screenshotBaseUrl) {
    screenshotBaseUrl = baseUrl;
  }

  const pythonBin = resolvePython();
  const runId = Date.now();
  const unitName = port ? `vm2-ai-reg-runserver-${port}-${runId}` : "";
  const logPath = port ? `/tmp/ai_reg_runserver_${port}.log` : `/tmp/ai_reg_runserver_local.log`;
  const overrideEnvPath = port ? `/tmp/ai_reg_env_override_${port}.env` : `/tmp/ai_reg_env_override_local.env`;

  fs.writeFileSync(logPath, "");
  fs.writeFileSync(
    overrideEnvPath,
    [
      "DJANGO_ALLOWED_HOSTS=127.0.0.1,localhost,sustainacore.org,preview.sustainacore.org",
      "DJANGO_DEBUG=1",
      "PYTHONUNBUFFERED=1",
    ].join("\n") + "\n"
  );

  let journalProc = null;
  let logStream = null;
  if (port) {
    spawn("sudo", ["systemctl", "stop", unitName]);
    spawn("sudo", ["systemctl", "reset-failed", unitName]);
    journalProc = spawn("sudo", ["journalctl", "-u", unitName, "-f", "--no-pager"]);
    logStream = fs.createWriteStream(logPath, { flags: "a" });
    journalProc.stdout.pipe(logStream);
    journalProc.stderr.pipe(logStream);

    spawn("sudo", [
      "systemd-run",
      "--collect",
      "--unit",
      unitName,
      "--property",
      `EnvironmentFile=${envFiles[0]}`,
      "--property",
      `EnvironmentFile=${envFiles[1]}`,
      "--property",
      "EnvironmentFile=-/etc/sysconfig/sustainacore-django.env",
      "--property",
      `EnvironmentFile=${overrideEnvPath}`,
      "--property",
      "WorkingDirectory=/opt/code/Sustainacore/website_django",
      pythonBin,
      "manage.py",
      "runserver",
      `127.0.0.1:${port}`,
      "--noreload",
    ]);
  }

  try {
    const candidates = buildAiRegCandidates();
    const targetPath = candidates[0] || "/ai-regulation/";
    process.stdout.write(`Waiting for ${targetPath} on ${baseUrl}...\n`);
    const readinessTimeout = Number(process.env.AI_REG_READINESS_TIMEOUT_MS || "90000");
    await waitFor(
      `${baseUrl}${targetPath}`,
      "data-ai-reg-root",
      readinessTimeout,
      hostHeader,
      !ignoreHttpsErrors
    );

    process.stdout.write("Capturing screenshots...\n");
    const env = {
      ...process.env,
      AI_REG_SCREENSHOT_MODE: requestedMode,
      AI_REG_BASE_URL: screenshotBaseUrl,
      AI_REG_FORCE_2D: process.env.AI_REG_FORCE_2D || "1",
      AI_REG_SCREENSHOT_HOST_HEADER: hostResolve ? "" : hostHeader,
      AI_REG_IGNORE_HTTPS_ERRORS: process.env.AI_REG_IGNORE_HTTPS_ERRORS || "1",
      AI_REG_HOST_RESOLVE: hostResolve ? `${hostResolve}:127.0.0.1` : "",
      PLAYWRIGHT_BROWSERS_PATH: process.env.PLAYWRIGHT_BROWSERS_PATH || "/home/ubuntu/.cache/ms-playwright",
      PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD: process.env.PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD || "1",
    };

  const screenshotProc = spawnSync(
      "node",
      ["scripts/ai_reg_screenshots.mjs", "--mode", requestedMode, "--base-url", baseUrl],
      { cwd: rootDir, stdio: "inherit", env }
    );
    if (screenshotProc.status !== 0) {
      throw new Error("Screenshot capture failed.");
    }
  } finally {
    if (unitName) {
      spawn("sudo", ["systemctl", "stop", unitName]);
      spawn("sudo", ["systemctl", "reset-failed", unitName]);
    }
    if (journalProc && !journalProc.killed) {
      journalProc.kill("SIGTERM");
    }
    if (logStream) {
      logStream.end();
    }
    if (fs.existsSync(overrideEnvPath)) {
      fs.unlinkSync(overrideEnvPath);
    }
  }
};

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
