#!/usr/bin/env bash
# Build the SEMOSS executor sandbox image from the worker source in this repo.
#
#   docker/build-executor-image.sh                      # tag semoss-executor:dev
#   TAG=semoss-executor:0.3 docker/build-executor-image.sh
#   NO_CACHE=1 docker/build-executor-image.sh
#
# The build context is py/, so the image carries exactly the python committed here.
# That is the point: there is no staged copy that can go stale. The tag is labelled
# with a hash of the python tree, so `docker inspect` answers "which source is this?"
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
TAG="${TAG:-semoss-executor:dev}"

[ -d "$REPO/py" ] || { echo "no py/ under $REPO" >&2; exit 1; }

PY_SHA="$(cd "$REPO/py" \
  && find . -type f -name '*.py' -print0 \
  | sort -z \
  | xargs -0 sha256sum \
  | sha256sum | cut -d' ' -f1)"

ARGS=(build
  -f "$HERE/Dockerfile.executor"
  -t "$TAG"
  --label "org.semoss.py-sha=$PY_SHA"
  --label "org.semoss.built-at=$(date -u +%Y-%m-%dT%H:%M:%SZ)")
[ "${NO_CACHE:-0}" = "1" ] && ARGS+=(--no-cache)

docker "${ARGS[@]}" "$REPO/py"

echo "built $TAG from py sha ${PY_SHA:0:12}"

# Prove what landed, rather than trusting the build log.
docker run --rm --entrypoint sh "$TAG" -c '
  git --version
  python3 -c "import pandas, jsonpickle; print(\"imports ok\")"
  test -f /opt/semosshome/py/gaas_tcp_socket_server.py && echo "worker present"
  printf "watcher ignore refs: "; grep -c "_IGNORED_DIRS" /opt/semosshome/py/gaas_change_watcher.py
  printf "id: "; id -u
'
