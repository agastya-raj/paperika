#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="$ROOT_DIR/.venv"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [[ ! -d "$VENV_DIR" ]]; then
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

"$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel
"$VENV_DIR/bin/python" -m pip install -e "$ROOT_DIR[dev]"
"$VENV_DIR/bin/python" -m playwright install chromium

echo "paperika runtime ready: $VENV_DIR"
"$VENV_DIR/bin/python" -c 'import playwright; print("playwright_import=ok")'
