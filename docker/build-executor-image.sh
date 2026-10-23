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

# Prove what landed, rather than trusting the build log: the worker tree in the image must
# hash to the repo's, and the worker's interpreter must import the watcher. Any failure
# fails the build.
docker run --rm --entrypoint sh -e EXPECT="$PY_SHA" "$TAG" -ec '
  cd /opt/semosshome/py
  test -f gaas_tcp_socket_server.py || { echo "worker missing at /opt/semosshome/py" >&2; exit 1; }
  GOT=$(find . -type f -name "*.py" -print0 | sort -z | xargs -0 sha256sum | sha256sum | cut -d" " -f1)
  [ "$GOT" = "$EXPECT" ] || { echo "py tree in image ($GOT) differs from the repo ($EXPECT)" >&2; exit 1; }
  P=/usr/lib/python/semossvenv/bin/python3; [ -x "$P" ] || P=$(command -v python3)
  "$P" -c "import gaas_change_watcher" || { echo "$P cannot import the change watcher" >&2; exit 1; }
  echo "worker tree matches repo; $P imports the watcher"
  git --version
  [ "$(id -u)" = 1000 ] || { echo "image does not run as uid 1000" >&2; exit 1; }
'
echo "smoke test passed for $TAG"
