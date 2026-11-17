#!/usr/bin/env bash
# Prove a built sandbox image is usable: the worker tree matches the given py/, the venv
# python imports the worker, git is present and the image runs as uid 1000.
#
#   docker/sandbox-smoke-test.sh <image> [py-dir]      # py-dir defaults to the repo's py/
set -euo pipefail

IMAGE="${1:?usage: sandbox-smoke-test.sh <image> [py-dir]}"
PY_DIR="${2:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/py}"

# same exclusions as Dockerfile.sandbox.dockerignore
py_sha() {
  find . -type f -name '*.py' -not -path '*/__pycache__/*' -not -path './.pytest_cache/*' \
    -not -path './.continue/*' -not -path '*.egg-info/*' -print0 \
    | sort -z | xargs -0 sha256sum | sha256sum | cut -d' ' -f1
}

EXPECT="$(cd "$PY_DIR" && py_sha)"

docker run --rm --entrypoint bash -e EXPECT="$EXPECT" "$IMAGE" -ec "
  $(declare -f py_sha)
  cd /opt/semosshome/py
  test -f gaas_tcp_socket_server.py || { echo 'worker missing at /opt/semosshome/py' >&2; exit 1; }
  GOT=\$(py_sha)
  [ \"\$GOT\" = \"\$EXPECT\" ] || { echo \"py tree in image (\$GOT) differs from $PY_DIR (\$EXPECT)\" >&2; exit 1; }
  P=/usr/lib/python/semossvenv/bin/python3
  [ -x \"\$P\" ] || { echo \"no venv python at \$P\" >&2; exit 1; }
  # the sandbox policy's egress rules name this path (OPENSHELL_PYTHON_BINARY's default)
  [ \"\$(readlink -f \$P)\" = /usr/lib/python/cpython/bin/python3.14 ] \\
    || { echo \"interpreter is \$(readlink -f \$P), not /usr/lib/python/cpython/bin/python3.14\" >&2; exit 1; }
  # same launch shape as OpenShellTransport: explicit interpreter, bare PATH
  env -i PATH=/usr/local/bin:/usr/bin:/bin \"\$P\" -c 'import sys, pandas, numpy, jsonpickle, gaas_tcp_server_handler, gaas_change_watcher; print(sys.version)'
  git --version
  [ \"\$(id -u)\" = 1000 ] || { echo 'image does not run as uid 1000' >&2; exit 1; }
"
echo "smoke test passed for $IMAGE"
