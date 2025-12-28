from __future__ import annotations

import hashlib
import os
import re
import subprocess
import sys
from typing import Iterable


FORBIDDEN_HASHES = {
    "0fb9765e58d296e5762b8a9d884224cf812f739f2815b12625bfc9c309486a4e",
    "64469c596b2d8211edd9bb38edcb8447dc66839db69de1aa2c7cefb41a86e7fa",
    "d5d4f1e5cc5a6d94cba168f97a4e0753a5a2123955180ff743ae71f2464c8a89",
    "a8c9c0a3e7748e786dcd17bdef73b86f2eb9955223dc2c67e181cde56905d608",
    "bb2255fa30c66120732ac53d4efd832ff908d980f2e40358b8eaa3c29d410881",
    "28fa587cdc527807bc759dd405762f4a08fc810a7b1949d46f8579104337b214",
    "e745a89c9d21314afef4a5dbcea406dc4211108c200d982608775e7337aa8a17",
    "e883da666daa0aba277e2667317f59b14c235efd852faff3e2bf9a9c32287204",
}


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _iter_files() -> Iterable[str]:
    result = subprocess.run(
        ["git", "ls-files"],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    binary_exts = {
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".webp",
        ".ico",
        ".pdf",
    }
    for path in result.stdout.splitlines():
        _, ext = os.path.splitext(path.lower())
        if ext in binary_exts:
            continue
        if path.startswith("website_django/staticfiles/"):
            continue
        if path.startswith("website_django/venv/"):
            continue
        if path.startswith("docs/"):
            continue
        yield path


def _scan_file(path: str) -> bool:
    try:
        with open(path, "rb") as handle:
            data = handle.read()
    except OSError:
        return False

    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        text = data.decode("utf-8", errors="ignore")

    for line_no, line in enumerate(text.splitlines(), start=1):
        normalized = re.sub(r"[^a-z0-9]+", " ", line.lower()).strip()
        if not normalized:
            continue
        tokens = normalized.split()
        for token in tokens:
            if len(token) < 3:
                continue
            if _hash_text(token) in FORBIDDEN_HASHES:
                print(f"{path}:{line_no}:forbidden_hash={_hash_text(token)}")
                return True
        for a, b in zip(tokens, tokens[1:]):
            if len(a) < 3 or len(b) < 3:
                continue
            pair = f"{a} {b}"
            if _hash_text(pair) in FORBIDDEN_HASHES:
                print(f"{path}:{line_no}:forbidden_hash={_hash_text(pair)}")
                return True
    return False


def main() -> int:
    flagged = False
    for path in _iter_files():
        if _scan_file(path):
            flagged = True
    if flagged:
        return 1
    print("Forbidden term guard passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
