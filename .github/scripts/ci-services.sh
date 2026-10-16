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

# HTTP status answered by the service's own health endpoint (GET /health).
# 000 means the endpoint could not be reached at all.
health_status() {
  docker exec "$1" node -e '
    const port = process.env.HEALTH_PORT || 3050;
    fetch("http://127.0.0.1:" + port + "/health")
      .then((r) => process.stdout.write(String(r.status)))
      .catch(() => process.stdout.write("000"))
      .finally(() => process.exit(0));
  ' 2>/dev/null || printf '000'
}

fail() {
  echo "::error::$1"
  docker logs "${CONTAINER}" 2>&1 | tail -n 50 || true
  exit 1
}

cleanup() {
  docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# The trace allocator is a Kafka worker: its health endpoint is the way to tell
# that it started correctly (no other port is published by the service).
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${CONTAINER}" \
  -e LOG_FOLDER=/tmp \
  "${IMAGE}" node src/index.js >/dev/null

seconds=25
answered=false
for _ in $(seq 1 "${seconds}"); do
  if ! container_running "${CONTAINER}"; then
    fail "simva-trace-allocator is not running"
  fi
  if [[ "$(health_status "${CONTAINER}")" == "200" ]]; then
    answered=true
    break
  fi
  sleep 1
done
[[ "${answered}" == true ]] || fail "simva-trace-allocator /health did not answer 200 within ${seconds}s"

# Once healthy, it must stay healthy.
stability=5
for _ in $(seq 1 "${stability}"); do
  sleep 1
  if ! container_running "${CONTAINER}"; then
    fail "simva-trace-allocator stopped after becoming healthy"
  fi
  [[ "$(health_status "${CONTAINER}")" == "200" ]] \
    || fail "simva-trace-allocator /health stopped answering 200"
done

echo "OK: simva-trace-allocator /health answered 200 and stayed healthy"
