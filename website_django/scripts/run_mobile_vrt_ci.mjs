import { spawn, spawnSync } from "node:child_process";
import path from "node:path";

const args = process.argv.slice(2);
const getArg = (name, fallback) => {
  const idx = args.indexOf(name);
  if (idx === -1) return fallback;
  return args[idx + 1] || fallback;
};

const mode = getArg("--mode", process.env.VRT_MODE || "current");
const repoRoot = getArg(
  "--repo-root",
  process.env.VRT_REPO_ROOT || path.resolve(process.cwd(), "..")
);
const port = Number(process.env.VRT_PORT || "8001");
const baseUrl = process.env.VRT_BASE_URL || `http://127.0.0.1:${port}`;
const timeoutMs = Number(process.env.VRT_TIMEOUT_MS || "45000");

const resolvePython = () => {
  if (process.env.VRT_PYTHON) return process.env.VRT_PYTHON;
  return "python3";
};

const waitForServer = async () => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`${baseUrl}/`);
      if (res.ok) return true;
    } catch (err) {
      // ignore
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  return false;
};

const run = async () => {
  const pythonBin = resolvePython();
  const env = {
    ...process.env,
    DJANGO_SECRET_KEY: process.env.DJANGO_SECRET_KEY || "dev-secret",
    DJANGO_DEBUG: "1",
    TECH100_UI_DATA_MODE: "fixture",
    NEWS_UI_DATA_MODE: "fixture",
    VRT_HIDE_CORRECTIONS_LINKS: process.env.VRT_HIDE_CORRECTIONS_LINKS || "1",
    PYTHONUNBUFFERED: "1",
  };

  const managePy = path.join(repoRoot, "website_django", "manage.py");
  const djangoDir = path.join(repoRoot, "website_django");
  const runserver = spawn(
    pythonBin,
    [managePy, "runserver", `127.0.0.1:${port}`, "--noreload"],
    {
      env,
      stdio: "inherit",
      cwd: djangoDir,
    }
  );

  const ready = await waitForServer();
  if (!ready) {
    runserver.kill("SIGTERM");
    throw new Error("runserver did not become ready");
  }

  const scriptPath = path.join(process.cwd(), "scripts", "mobile_vrt.mjs");
  const result = spawnSync("node", [scriptPath, "--mode", mode, "--base-url", baseUrl], {
    env,
    stdio: "inherit",
  });

  runserver.kill("SIGTERM");

  if (result.status !== 0) {
    process.exit(result.status || 1);
  }
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
