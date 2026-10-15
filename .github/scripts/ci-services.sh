#!/usr/bin/env bash
#
# Builds the simva-trace-allocator image and verifies that the service can be
# launched on its own.
#
# The full cross-service stack (trace-allocator + API + front, plus their
# dependencies) is exercised together by simva-infra, so this check only covers
# this repository's own service.
#
set -euo pipefail

IMAGE="simva-trace-allocator:ci"
CONTAINER="ci-simva-trace-allocator"

cache_args=()
if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
  cache_args=(
    --cache-from "type=gha,scope=simva-trace-allocator"
    --cache-to "type=gha,mode=max,scope=simva-trace-allocator"
  )
fi

echo "::group::Building simva-trace-allocator"
docker buildx build \
  --load \
  --target prod \
  --tag "${IMAGE}" \
  "${cache_args[@]}" \
  .
echo "::endgroup::"

container_running() {
  docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null | grep -qx true
}

cleanup() {
  docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# The trace allocator is a Kafka worker with no HTTP endpoint: it only needs to
# stay up.
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${CONTAINER}" \
  -e LOG_FOLDER=/tmp \
  "${IMAGE}" node src/index.js >/dev/null

seconds=25
for _ in $(seq 1 "${seconds}"); do
  if ! container_running "${CONTAINER}"; then
    echo "::error::simva-trace-allocator is not running"
    docker logs "${CONTAINER}" 2>&1 | tail -n 50 || true
    exit 1
  fi
  sleep 1
done
echo "OK: simva-trace-allocator stayed up for ${seconds}s"
