#!/usr/bin/env bash
# Smoke test for the RL-Insight image: verify startup, graceful shutdown,
# restart, and that killing one service brings down the foreground stack.
# Runnable in CI and locally: tests/monitor/special_e2e/docker_smoke_test.sh [image]
set -euo pipefail

IMAGE="${1:-rl-insight:smoke}"
NAME=rl-insight-smoke

# Always remove the container; dump its logs only on failure.
cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "smoke test failed; container logs follow:" >&2
    docker logs "$NAME" 2>&1 || true
  fi
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

wait_healthy() {
  for _ in $(seq 1 60); do
    if [ "$(docker inspect --format '{{.State.Health.Status}}' "$NAME")" = healthy ]; then
      return 0
    fi
    sleep 3
  done
  return 1
}

docker run -d --name "$NAME" --stop-timeout 45 "$IMAGE"
wait_healthy

# A graceful stop must exit 0.
docker stop --timeout 45 "$NAME"
test "$(docker inspect --format '{{.State.ExitCode}}' "$NAME")" = 0

# A restarted container must become healthy again despite stale state.
docker start "$NAME"
wait_healthy

# Killing one child must bring down the foreground stack with a failure.
docker exec "$NAME" python -c 'import json, os, signal; from pathlib import Path; state = json.loads((Path.home() / ".rl-insight/run/rl-insight-services.json").read_text()); os.kill(next(s["pid"] for s in state["services"] if s["name"] == "prometheus"), signal.SIGKILL)'
for _ in $(seq 1 20); do
  if [ "$(docker inspect --format '{{.State.Running}}' "$NAME")" = false ]; then
    test "$(docker inspect --format '{{.State.ExitCode}}' "$NAME")" != 0
    echo "smoke test passed: $IMAGE"
    exit 0
  fi
  sleep 3
done
exit 1
