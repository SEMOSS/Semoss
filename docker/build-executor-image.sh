#!/usr/bin/env bash
# Build the SEMOSS executor sandbox image from the worker source in this repo.
#
#   docker/build-executor-image.sh                      # slim, tag semoss-executor:dev
#   MODE=full TAG=semoss-executor:full docker/build-executor-image.sh
#   NO_CACHE=1 docker/build-executor-image.sh
#
# Two modes, one command:
#   slim (default) - three packages; fast to build and start, but user code can only import
#                    pandas, jsonpickle and yfinance.
#   full           - installs the dependency set declared in py/install_config/pyproject.toml
#                    with uv, the same installer docker/Dockerfile.python uses, so a Py()
#                    block keeps the import surface it had outside the sandbox.
#
# Either way the worker python is the committed py/, never a hand-staged copy, and the tag is
# labelled with a hash of that tree so `docker inspect` answers "which source is this?".
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
MODE="${MODE:-slim}"
NO_CACHE="${NO_CACHE:-0}"

case "$MODE" in
  slim) TAG="${TAG:-semoss-executor:dev}" ;;
  full) TAG="${TAG:-semoss-executor:full}" ;;
  *) echo "MODE must be slim or full (got '$MODE')" >&2; exit 1 ;;
esac

[ -d "$REPO/py" ] || { echo "no py/ under $REPO" >&2; exit 1; }

PY_SHA="$(cd "$REPO/py" \
  && find . -type f -name '*.py' -print0 \
  | sort -z \
  | xargs -0 sha256sum \
  | sha256sum | cut -d' ' -f1)"

CTX="$(mktemp -d)"
trap 'rm -rf "$CTX"' EXIT

ARGS=(build
  -t "$TAG"
  --label "org.semoss.py-sha=$PY_SHA"
  --label "org.semoss.executor-mode=$MODE"
  --label "org.semoss.built-at=$(date -u +%Y-%m-%dT%H:%M:%SZ)")
[ "$NO_CACHE" = "1" ] && ARGS+=(--no-cache)

if [ "$MODE" = "slim" ]; then
  # Context is py/ itself; the Dockerfile copies it verbatim.
  cp "$HERE/Dockerfile.executor" "$CTX/Dockerfile"
  cp -a "$REPO/py" "$CTX/py"
  ARGS+=(-f "$CTX/Dockerfile")
else
  # The full build also needs the manifest and the flattening helper next to py/.
  cp "$HERE/Dockerfile.executor-full" "$CTX/Dockerfile"
  cp "$HERE/export_requirements.py" "$CTX/export_requirements.py"
  cp "$REPO/py/install_config/pyproject.toml" "$CTX/pyproject.toml"
  cp -a "$REPO/py" "$CTX/py"
  ARGS+=(-f "$CTX/Dockerfile" --build-arg "EXTRAS=${EXTRAS:-cpu}")
fi

docker "${ARGS[@]}" "$CTX"

echo "built $TAG ($MODE) from py sha ${PY_SHA:0:12}"

# Prove what landed, rather than trusting the build log.
docker run --rm --entrypoint sh "$TAG" -c '
  git --version
  python3 -c "import pandas, jsonpickle; print(\"boot imports ok\")"
  python3 -c "import numpy, boto3; print(\"parity imports ok\")" 2>/dev/null || echo "parity imports: not installed (slim)"
  test -f /opt/semosshome/py/gaas_tcp_socket_server.py && echo "worker present"
  printf "watcher ignore refs: "; grep -c "_IGNORED_DIRS" /opt/semosshome/py/gaas_change_watcher.py
  printf "id: "; id -u
'
