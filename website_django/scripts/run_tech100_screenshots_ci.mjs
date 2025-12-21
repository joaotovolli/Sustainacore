import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, spawnSync } from "node:child_process";
import { buildTech100Candidates } from "./tech100_url_candidates.mjs";

const redactSecrets = (value) => {
  if (value === undefined || value === null) return value;
  let output = String(value);
  const secrets = [
    process.env.TECH100_BASIC_AUTH_USER,
    process.env.TECH100_BASIC_AUTH_PASS,
  ].filter(Boolean);
  for (const secret of secrets) {
    output = output.split(secret).join("[REDACTED]");
  }
  return output.replace(/Basic\s+[A-Za-z0-9+/=]+/g, "Basic [REDACTED]");
};

const logError = (...args) => {
  console.error(...args.map(redactSecrets));
};

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(scriptDir, "..");
const envFiles = ["/etc/sustainacore.env", "/etc/sustainacore/db.env"];

const resolvePython = () => {
  const preferred = [
    "/opt/code/Sustainacore/website_django/venv/bin/python",
    "/opt/sustainacore/website_django/venv/bin/python",
  ];
  for (const candidate of preferred) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return "python3";
};

const ensureNodeDeps = (rootDir) => {
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
  const npmCheck = spawnSync("npm", ["--version"], { stdio: "ignore" });
  if (npmCheck.status !== 0) {
    logError("npm is required. Run: cd website_django && npm ci");
    process.exit(1);
  }
  const npmArgs = hasLockfile ? ["ci"] : ["install", "--no-fund", "--no-audit"];
  const npmInstall = spawnSync("npm", npmArgs, { cwd: rootDir, stdio: "inherit" });
  if (npmInstall.status !== 0) {
    logError("Dependency install failed. Run: cd website_django && npm install");
    process.exit(1);
  }
  process.stdout.write("Ensuring Playwright browsers...\n");
  const versionCheck = spawnSync("npx", ["playwright", "--version"], { cwd: rootDir, stdio: "inherit" });
  if (versionCheck.status !== 0) {
    logError("Playwright CLI not available. Run: cd website_django && npm install");
    process.exit(1);
  }
  const browserCheck = spawnSync(
    "node",
    ["-e", "import('playwright').then(async(p)=>{const b=await p.chromium.launch();await b.close();})"],
    { cwd: rootDir, stdio: "ignore" }
  );
  if (browserCheck.status !== 0) {
    const install = spawnSync("npx", ["playwright", "install", "chromium"], {
      cwd: rootDir,
      stdio: "inherit",
    });
    if (install.status !== 0) {
      logError("Playwright chromium install failed. Run: cd website_django && npx playwright install chromium");
      process.exit(1);
    }
  }
};

const run = async () => {
  ensureNodeDeps(rootDir);
  const externalBaseUrl = process.env.TECH100_BASE_URL || "";
  const localBaseEnv = process.env.TECH100_LOCAL_BASE_URL || "";
  const previewBaseUrl = process.env.TECH100_PREVIEW_BASE_URL || "https://preview.sustainacore.org";
  const requestedMode = process.env.TECH100_SCREENSHOT_MODE;
  const useExistingServer = process.env.TECH100_USE_EXISTING_SERVER === "1";
  const authUser = process.env.TECH100_BASIC_AUTH_USER || "";
  const authPass = process.env.TECH100_BASIC_AUTH_PASS || "";
  const previewAuthHeader =
    authUser && authPass
      ? `Basic ${Buffer.from(`${authUser}:${authPass}`).toString("base64")}`
      : "";
  const requestedPreview = externalBaseUrl.includes("preview.sustainacore.org");
  const fetchWithHeaders = (url, hostHeader, authHeader) =>
    fetch(url, {
      headers: {
        ...(hostHeader ? { Host: hostHeader } : {}),
        ...(authHeader ? { Authorization: authHeader } : {}),
      },
    });

  let port = "";
  let baseUrl = localBaseEnv || "";
  let existingServer = useExistingServer;
  const localHostHeader = "sustainacore.org";
  const localAuthHeader = "";
  const validatePreview = process.env.TECH100_VALIDATE_PREVIEW === "1";

  if (!baseUrl) {
    baseUrl = "http://127.0.0.1";
  }
  if (!localBaseEnv && externalBaseUrl) {
    baseUrl = externalBaseUrl;
  }
  if (requestedPreview && !previewAuthHeader) {
    process.stdout.write("Preview base requested but Basic Auth missing; using local runserver.\n");
    baseUrl = "";
    existingServer = false;
  }
  if (baseUrl === "http://127.0.0.1" || baseUrl === "http://localhost") {
    if (useExistingServer) {
      existingServer = true;
    } else {
      baseUrl = "";
      existingServer = false;
    }
  }

  if (!baseUrl) {
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
    process.stdout.write(`Using port ${port}\n`);
  } else {
    process.stdout.write(`Using base URL ${baseUrl}\n`);
    if (existingServer) {
      process.stdout.write("Using existing server (no runserver spawn)\n");
    }
  }

  const pythonBin = resolvePython();
  const runId = Date.now();
  const targetLabel = port
    ? port
    : (baseUrl.includes("127.0.0.1") || baseUrl.includes("localhost"))
      ? "local"
      : "custom";
  const logPath = port ? `/tmp/tech100_runserver_${port}.log` : `/tmp/tech100_runserver_${targetLabel}.log`;
  const unitName = port ? `vm2-tech100-runserver-${port}-${runId}` : "";
  const overrideEnvPath = port ? `/tmp/tech100_env_override_${port}.env` : `/tmp/tech100_env_override_${targetLabel}.env`;
  const dataMode = process.env.TECH100_UI_DATA_MODE
    || (requestedMode === "after" ? "fixture" : "");

  fs.writeFileSync(logPath, "");
  fs.writeFileSync(
    overrideEnvPath,
    [
      "DJANGO_ALLOWED_HOSTS=127.0.0.1,localhost,sustainacore.org,preview.sustainacore.org",
      "DJANGO_DEBUG=1",
      "PYTHONUNBUFFERED=1",
      ...(dataMode ? [`TECH100_UI_DATA_MODE=${dataMode}`] : []),
    ].join("\n") + "\n"
  );

  let journalProc = null;
  let logStream = null;
  if (port && !existingServer) {
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

  const cleanup = async () => {
    if (unitName && !existingServer) {
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
  };

  process.on("SIGINT", cleanup);
  process.on("SIGTERM", cleanup);

  const waitFor = async (url, selector, timeoutMs = 60000, hostHeader, authHeader) => {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      try {
        const resp = await fetchWithHeaders(url, hostHeader, authHeader);
        if (resp.ok) {
          const text = await resp.text();
          if (!selector || text.includes(selector)) {
            return text;
          }
        } else if (resp.status === 401 || resp.status === 403) {
          const body = await resp.text();
          const bodyPath = `/tmp/tech100_401_body_${targetLabel}.html`;
          fs.writeFileSync(bodyPath, body);
          const err = new Error(`Server returned ${resp.status} for ${url} (body saved to ${bodyPath})`);
          err.fatal = true;
          throw err;
        } else if (resp.status === 404) {
          const body = await resp.text();
          const bodyPath = `/tmp/tech100_readiness_body_${targetLabel}.html`;
          fs.writeFileSync(bodyPath, body);
          const err = new Error(`Server returned 404 for ${url} (body saved to ${bodyPath})`);
          err.fatal = true;
          throw err;
        } else if (resp.status >= 500) {
          const body = await resp.text();
          const bodyPath = `/tmp/tech100_500_body_${targetLabel}.html`;
          fs.writeFileSync(bodyPath, body);
          const err = new Error(`Server returned ${resp.status} for ${url} (body saved to ${bodyPath})`);
          err.fatal = true;
          throw err;
        }
      } catch (err) {
        if (err.fatal) {
          throw err;
        }
      }
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
    throw new Error(`Timeout waiting for ${url}`);
  };

  const dumpDiagnostics = (smokePath) => {
    const tail = fs.existsSync(logPath)
      ? fs.readFileSync(logPath, "utf8").split("\n").slice(-200).join("\n")
      : "";
    const smokeTail = smokePath && fs.existsSync(smokePath)
      ? fs.readFileSync(smokePath, "utf8").split("\n").slice(-20).join("\n")
      : "";
    console.error(`Diagnostics: port=${port} unit=${unitName}`);
    console.error(`Runserver log: ${logPath}\n${tail}`);
    if (smokeTail) {
      console.error(`Oracle smoke log: ${smokePath}\n${smokeTail}`);
    }
  };

  const smokeLogPath = `/tmp/tech100_oracle_smoke_${targetLabel}.log`;
  if (!process.env.TECH100_SKIP_SMOKE) {
    const smokeUnit = `vm2-tech100-smoke-${port || "preview"}-${Date.now()}`;
    const smokeProc = spawn("sudo", [
      "systemd-run",
      "--collect",
      "--wait",
      "--pipe",
      "--unit",
      smokeUnit,
      "--property",
      `EnvironmentFile=${envFiles[0]}`,
      "--property",
      `EnvironmentFile=${envFiles[1]}`,
      "--property",
      `EnvironmentFile=${overrideEnvPath}`,
      "--property",
      `WorkingDirectory=${rootDir}`,
      pythonBin,
      "manage.py",
      "shell",
      "-c",
      [
        "from core.oracle_db import get_connection",
        "import traceback",
        "try:",
        "    conn = get_connection()",
        "    cur = conn.cursor()",
        "    cur.execute('select user from dual')",
        "    print('ORACLE_SMOKE_OK', cur.fetchone())",
        "    conn.close()",
        "except Exception:",
        "    traceback.print_exc()",
        "    raise",
      ].join("\n"),
    ]);
    const smokeOut = [];
    smokeProc.stdout.on("data", (data) => smokeOut.push(data.toString()));
    smokeProc.stderr.on("data", (data) => smokeOut.push(data.toString()));
    const smokeExit = await new Promise((resolve) => smokeProc.on("close", resolve));
    fs.writeFileSync(smokeLogPath, smokeOut.join(""));
    if (smokeExit !== 0) {
      await cleanup();
    logError("Oracle smoke test failed.");
    dumpDiagnostics(smokeLogPath);
    process.exit(1);
  }
  }

  const resolveTech100Path = async (hostHeader, authHeader) => {
    const overridePath = process.env.TECH100_SCREENSHOT_PATH;
    const candidates = buildTech100Candidates({
      override: overridePath,
      discovered: ["/tech100/"],
    });
    let lastBody = null;
    const overrideCandidate = overridePath ? candidates[0] : null;
    for (const candidate of candidates) {
      const url = `${baseUrl}${candidate}`;
      const resp = await fetchWithHeaders(url, hostHeader, authHeader);
      if (resp.ok) {
        const resolved = new URL(resp.url);
        return resolved.pathname.endsWith("/") ? resolved.pathname : `${resolved.pathname}`;
      }
      if (resp.status === 401 || resp.status === 403) {
        const body = await resp.text();
        const bodyPath = `/tmp/tech100_readiness_body_${targetLabel}.txt`;
        fs.writeFileSync(bodyPath, body);
        throw new Error(`Tech100 readiness failed with ${resp.status} for ${candidate} (body saved to ${bodyPath})`);
      }
      const body = await resp.text();
      if (resp.status >= 500) {
        const bodyPath = `/tmp/tech100_readiness_body_${targetLabel}.txt`;
        fs.writeFileSync(bodyPath, body);
        throw new Error(`Tech100 readiness failed with ${resp.status} for ${candidate} (body saved to ${bodyPath})`);
      }
      if (resp.status === 404) {
        lastBody = body;
        if (overrideCandidate && candidate === overrideCandidate) {
          continue;
        }
      }
    }
    if (lastBody !== null) {
      const bodyPath = `/tmp/tech100_readiness_body_${targetLabel}.txt`;
      fs.writeFileSync(bodyPath, lastBody);
    }
    throw new Error("Tech100 readiness failed (no candidate path returned 200)");
  };

  const baseAuthHeader = baseUrl.includes("preview.sustainacore.org") ? previewAuthHeader : localAuthHeader;
  const baseHostHeader = existingServer ? localHostHeader : "";

  let tech100Path = "/tech100/";
  try {
    await waitFor(`${baseUrl}/`, null, 60000, baseHostHeader, baseAuthHeader);
    tech100Path = await resolveTech100Path(baseHostHeader, baseAuthHeader);
    process.stdout.write(`Using Tech100 path ${tech100Path}\n`);
    await waitFor(`${baseUrl}${tech100Path}`, null, 60000, baseHostHeader, baseAuthHeader);
  } catch (err) {
    await cleanup();
    logError(`Readiness failed: ${err.message}`);
    logError(`Readiness URL: ${baseUrl}/`);
    if (fs.existsSync(logPath)) {
      const logTail = fs.readFileSync(logPath, "utf8").split("\n").slice(-60).join("\n");
      logError(`Runserver log tail (${logPath}):\n${logTail}`);
    }
    dumpDiagnostics(smokeLogPath);
    process.exit(1);
  }

  const screenshotScript = path.join(rootDir, "scripts", "tech100_screenshots.mjs");
  const runScreenshots = (mode) =>
    new Promise((resolve, reject) => {
      const proc = spawn(
        "node",
        [screenshotScript, "--mode", mode, "--base-url", baseUrl, "--tech100-path", tech100Path],
        { stdio: "inherit", cwd: rootDir }
      );
      proc.on("close", (code) => {
        if (code === 0) resolve();
        else reject(new Error(`screenshots ${mode} failed: ${code}`));
      });
    });

  try {
    const modes = requestedMode ? [requestedMode] : ["before", "after"];
    for (const mode of modes) {
      await runScreenshots(mode);
    }
  } catch (err) {
    await cleanup();
    logError(err.message);
    const html = await fetchWithHeaders(`${baseUrl}${tech100Path}`, baseHostHeader, baseAuthHeader)
      .then((res) => res.text());
    const failurePath = `/tmp/tech100_failure_body_${targetLabel}.html`;
    fs.writeFileSync(failurePath, html);
    const snippet = html.split("\n").slice(0, 120).join("\n");
    logError(`HTML snippet for ${tech100Path} (saved to ${failurePath}):\n${snippet}`);
    dumpDiagnostics(smokeLogPath);
    process.exit(1);
  }

  if (!requestedMode || requestedMode === "after") {
    const diffScript = path.join(rootDir, "scripts", "tech100_screenshot_diff.mjs");
    const diffProc = spawn("node", [diffScript], { stdio: "inherit" });
    const diffExit = await new Promise((resolve) => diffProc.on("close", resolve));
    await cleanup();
    if (diffExit !== 0) {
      process.exit(diffExit);
    }
  } else {
    await cleanup();
  }

  process.stdout.write(`Screenshots complete. Log: ${logPath}\n`);
  process.stdout.write(`Manual review: ${previewBaseUrl}\n`);

  if (validatePreview) {
    if (!previewAuthHeader) {
      process.stdout.write("Skipping preview validation; creds missing\n");
    } else {
      try {
        await waitFor(`${previewBaseUrl}/`, null, 30000, "", previewAuthHeader);
        process.stdout.write("Preview validation ok\n");
      } catch (err) {
        logError(`Preview validation failed: ${err.message}`);
        process.exit(1);
      }
    }
  }
};

run().catch((err) => {
  logError(err);
  process.exit(1);
});
