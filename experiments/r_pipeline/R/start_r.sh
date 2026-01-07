#!/usr/bin/env bash
set -e

echo "Starting R in Docker..."
echo "Mounting repo root (..) into /app"
echo "Working directory: /app/R"
echo ""

docker run --rm -it \
  -v "$(pwd)/..":/app \
  -w /app/R \
  retailops-r \
  bash -c "
    if [ ! -f renv.lock ]; then
      echo 'First-time setup: installing & initializing renv'
      R -e \"install.packages('renv', repos='https://cloud.r-project.org')\"
      R -e \"renv::init()\"
      echo 'renv initialized. Restarting into interactive shell.'
    fi
    exec bash
  "
