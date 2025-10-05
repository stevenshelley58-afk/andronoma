#!/usr/bin/env bash
set -euo pipefail
python -m pip install -U pip
[ -f requirements.txt ] && pip install -r requirements.txt
