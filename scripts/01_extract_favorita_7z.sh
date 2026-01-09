#!/usr/bin/env bash
# Extract Kaggle Favorita .7z files into CSV.
# Raw files are kept untouched except for extraction.

set -euo pipefail

RAW_DIR="../data/raw/favorita"


# Check if raw directory exists
if [ ! -d "${RAW_DIR}" ]; then
  echo "Raw directory not found: $RAW_DIR"
  exit 1
fi

# Check if 7z is installed
if ! command -v 7z >/dev/null 2>&1; then
  echo "7z not installed."
  exit 1
fi

cd "$RAW_DIR"

found=0
for f in *.7z; do
  [ -e "$f" ] || continue
  found=1

  csv="${f%.7z}"
  if [ -f "$csv" ]; then
    echo "skip  $f (already extracted)"
    continue
  fi

  echo "extract $f"
  7z x "$f" -y >/dev/null
done

if [ "$found" -eq 0 ]; then
  echo "No .7z files found in $RAW_DIR"
fi

echo "Done. CSV files:"
ls -lh *.csv 2>/dev/null || echo "(none)"
