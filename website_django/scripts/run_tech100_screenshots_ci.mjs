import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { buildTech100Candidates } from "./tech100_url_candidates.mjs";

const rootDir = process.cwd();
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

const run = async () => {
  const portScript = path.join(rootDir, "scripts", "find_free_port.mjs");
  const portProc = spawn("node", [portScript], { stdio: ["ignore", "pipe", "pipe"] });
  const port = await new Promise((resolve, reject) => {
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

  const baseUrl = `http://127.0.0.1:${port}`;
  process.stdout.write(`Using port ${port}\n`);

  const pythonBin = resolvePython();
  const logPath = `/tmp/tech100_runserver_${port}.log`;
  const unitName = `vm2-tech100-runserver-${port}`;
  const overrideEnvPath = `/tmp/tech100_env_override_${port}.env`;
  const requestedMode = process.env.TECH100_SCREENSHOT_MODE;
  const dataMode = process.env.TECH100_UI_DATA_MODE
    || (requestedMode === "after" ? "fixture" : "");

  fs.writeFileSync(logPath, "");
  fs.writeFileSync(
    overrideEnvPath,
    [
      "DJANGO_ALLOWED_HOSTS=127.0.0.1,localhost,sustainacore.org",
      "DJANGO_DEBUG=1",
      "PYTHONUNBUFFERED=1",
      ...(dataMode ? [`TECH100_UI_DATA_MODE=${dataMode}`] : []),
    ].join("\n") + "\n"
  );

  spawn("sudo", ["systemctl", "stop", unitName]);
  spawn("sudo", ["systemctl", "reset-failed", unitName]);
  const journalProc = spawn("sudo", ["journalctl", "-u", unitName, "-f", "--no-pager"]);
  const logStream = fs.createWriteStream(logPath, { flags: "a" });
  journalProc.stdout.pipe(logStream);
  journalProc.stderr.pipe(logStream);

  spawn("sudo", [
    "systemd-run",
    "--unit",
    unitName,
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
    "runserver",
    `127.0.0.1:${port}`,
    "--noreload",
  ]);

  const cleanup = async () => {
    spawn("sudo", ["systemctl", "stop", unitName]);
    spawn("sudo", ["systemctl", "reset-failed", unitName]);
    if (!journalProc.killed) {
      journalProc.kill("SIGTERM");
    }
    logStream.end();
    if (fs.existsSync(overrideEnvPath)) {
      fs.unlinkSync(overrideEnvPath);
    }
  };

  process.on("SIGINT", cleanup);
  process.on("SIGTERM", cleanup);

  const waitFor = async (url, selector, timeoutMs = 30000) => {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      try {
        const resp = await fetch(url);
        if (resp.ok) {
          const text = await resp.text();
          if (!selector || text.includes(selector)) {
            return text;
          }
        } else if (resp.status >= 500) {
          const body = await resp.text();
          const bodyPath = `/tmp/tech100_500_body_${port}.html`;
          fs.writeFileSync(bodyPath, body);
          const err = new Error(`Server returned ${resp.status} (body saved to ${bodyPath})`);
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

  const smokeLogPath = `/tmp/tech100_oracle_smoke_${port}.log`;
  const smokeUnit = `vm2-tech100-smoke-${port}`;
  spawn("sudo", ["systemctl", "stop", smokeUnit]);
  spawn("sudo", ["systemctl", "reset-failed", smokeUnit]);
  const smokeProc = spawn("sudo", [
    "systemd-run",
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
    console.error("Oracle smoke test failed.");
    dumpDiagnostics(smokeLogPath);
    process.exit(1);
  }

  const resolveTech100Path = async () => {
    const overridePath = process.env.TECH100_SCREENSHOT_PATH;
    const candidates = buildTech100Candidates({
      override: overridePath,
      discovered: ["/tech100/"],
    });
    let lastBody = null;
    const overrideCandidate = overridePath ? candidates[0] : null;
    for (const candidate of candidates) {
      const url = `${baseUrl}${candidate}`;
      const resp = await fetch(url);
      if (resp.ok) {
        const resolved = new URL(resp.url);
        return resolved.pathname.endsWith("/") ? resolved.pathname : `${resolved.pathname}`;
      }
      const body = await resp.text();
      if (resp.status >= 500) {
        const bodyPath = `/tmp/tech100_readiness_body_${port}.txt`;
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
      const bodyPath = `/tmp/tech100_readiness_body_${port}.txt`;
      fs.writeFileSync(bodyPath, lastBody);
    }
    throw new Error("Tech100 readiness failed (no candidate path returned 200)");
  };

  let tech100Path = "/tech100/";
  try {
    await waitFor(`${baseUrl}/`, null, 30000);
    tech100Path = await resolveTech100Path();
    process.stdout.write(`Using Tech100 path ${tech100Path}\n`);
    await waitFor(`${baseUrl}${tech100Path}`, null, 30000);
  } catch (err) {
    await cleanup();
    console.error(`Readiness failed: ${err.message}`);
    dumpDiagnostics(smokeLogPath);
    process.exit(1);
  }

  const screenshotScript = path.join(rootDir, "scripts", "tech100_screenshots.mjs");
  const runScreenshots = (mode) =>
    new Promise((resolve, reject) => {
      const proc = spawn(
        "node",
        [screenshotScript, "--mode", mode, "--base-url", baseUrl, "--tech100-path", tech100Path],
        { stdio: "inherit" }
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
    console.error(err.message);
    const html = await fetch(`${baseUrl}${tech100Path}`).then((res) => res.text());
    const failurePath = `/tmp/tech100_failure_body_${port}.html`;
    fs.writeFileSync(failurePath, html);
    const snippet = html.split("\n").slice(0, 120).join("\n");
    console.error(`HTML snippet for ${tech100Path} (saved to ${failurePath}):\n${snippet}`);
    dumpDiagnostics(smokeLogPath);
    process.exit(1);
  }

  const diffScript = path.join(rootDir, "scripts", "tech100_screenshot_diff.mjs");
  const diffProc = spawn("node", [diffScript], { stdio: "inherit" });
  const diffExit = await new Promise((resolve) => diffProc.on("close", resolve));
  await cleanup();
  if (diffExit !== 0) {
    process.exit(diffExit);
  }

  process.stdout.write(`Screenshots complete. Log: ${logPath}\n`);
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
