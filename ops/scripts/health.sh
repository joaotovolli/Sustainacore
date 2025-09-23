#!/usr/bin/env bash
set -euo pipefail
curl -fsS "http://127.0.0.1:8080/ask2?q=health&k=1" | head -c 400; echo
