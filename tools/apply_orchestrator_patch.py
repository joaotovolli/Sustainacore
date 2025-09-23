#!/usr/bin/env python3
import sys, os, io, re, shutil, importlib.util, types, subprocess, json
from pathlib import Path

SNIPPET_PATH = Path(__file__).with_name("multihit_orchestrator_snippet.py")
TARGET = Path("/opt/sustainacore-ai/app.py")
MARKER = "# --- Multi-Hit Orchestrator (RRF+MMR, in-process) ---"

def load_snippet():
    spec = importlib.util.spec_from_file_location("snippet_mod", SNIPPET_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, "SNIPPET")

def py_compile_ok(path: Path) -> bool:
    try:
        subprocess.check_call([
            "/opt/sustainacore-ai/.venv/bin/python",
            "-m","py_compile", str(path)
        ])
        return True
    except Exception:
        try:
            subprocess.check_call(["python3","-m","py_compile", str(path)])
            return True
        except Exception as e:
            print("py_compile failed:", e)
            return False

def main():
    if not TARGET.exists():
        print(f"ERROR: {TARGET} not found.")
        sys.exit(2)
    content = TARGET.read_text(encoding="utf-8", errors="ignore")
    if MARKER in content:
        print("Already patched; doing nothing.")
        sys.exit(0)

    backup = TARGET.with_name(TARGET.name + ".pre-patch")
    shutil.copy2(TARGET, backup)
    print("Backup created:", backup)

    snippet = load_snippet()
    patched = content.rstrip() + "\n\n" + snippet + "\n"
    TARGET.write_text(patched, encoding="utf-8")

    if py_compile_ok(TARGET):
        print("Patched and compiled OK.")
        sys.exit(0)
    else:
        print("Compile failed — restoring original.")
        shutil.copy2(backup, TARGET)
        sys.exit(1)

if __name__=="__main__":
    main()
