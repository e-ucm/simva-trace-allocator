#!/usr/bin/env bash
set -euo pipefail
[[ "${DEBUG:-false}" == "true" ]] && set -x

if [[ ! -d "node_modules" ]]; then
  npm install
fi

#start trace allocator
echo "${NODE_ENV:-production}"
if [[ "${NODE_ENV:-production}" == "development" ]]; then
  npm run dev
  #perf record -e cycles:u -g -- npm run dev > perf.out
else
  npm start
fi