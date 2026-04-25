#!/usr/bin/env bash
# Create a virtualenv and install Python dependencies.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo
echo "Setup complete. Activate the venv with:"
echo "    source .venv/bin/activate"
