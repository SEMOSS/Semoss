#!/usr/bin/env bash
# Build the SEMOSS sandbox image locally from this repo's py/ and smoke-test it.
# CI builds and publishes the same image: .github/workflows/sandbox-builder.yml
#
#   docker/build-sandbox-image.sh                              # tag semoss-sandbox:dev
#   TAG=semoss-sandbox:test NO_CACHE=1 docker/build-sandbox-image.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
TAG="${TAG:-semoss-sandbox:dev}"
NO_CACHE="${NO_CACHE:-0}"

[ -d "$REPO/py" ] || { echo "no py/ under $REPO" >&2; exit 1; }

ARGS=(build
  -f "$HERE/Dockerfile.sandbox"
  -t "$TAG"
  --label "org.opencontainers.image.revision=$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)"
  --label "org.opencontainers.image.created=$(date -u +%Y-%m-%dT%H:%M:%SZ)")
[ "$NO_CACHE" = "1" ] && ARGS+=(--no-cache --pull)

# context is py/ itself, so the image carries this checkout's worker (uncommitted edits included)
DOCKER_BUILDKIT=1 docker "${ARGS[@]}" "$REPO/py"

"$HERE/sandbox-smoke-test.sh" "$TAG" "$REPO/py"
