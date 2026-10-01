#!/bin/sh
set -eu

# Run in the digest-pinned linux/amd64 Python 3.14 image.
# Build prerequisites: gcc-c++, python3.14-devel. Runtime prerequisite: enchant2.
cd "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
PYTHON=${PYTHON:-/usr/bin/python3.14}
UV=${UV:-uv}
VENV=${VENV:-"$PWD/.venv"}
export TMPDIR="$PWD/.scratch" UV_CACHE_DIR="$PWD/.cache"
mkdir -p "$TMPDIR"

if ! "$UV" --version >/dev/null 2>&1; then
    "$PYTHON" -m pip install --require-hashes --only-binary=:all: \
        --index-url https://pypi.org/simple --cache-dir "$PWD/.cache/pip" \
        --target "$PWD/.resolver" -r resolver-requirements.lock
    UV="$PWD/.resolver/bin/uv"
fi
[ "$("$UV" --version | cut -d ' ' -f 2)" = "0.11.17" ] || {
    echo "uv 0.11.17 is required" >&2
    exit 1
}

"$PYTHON" -c 'import platform,sys; assert sys.version_info[:2] == (3,14); assert platform.system() == "Linux" and platform.machine() == "x86_64"'
"$UV" --no-config venv --python "$PYTHON" "$VENV"
"$UV" --no-config pip install --python "$VENV/bin/python" \
    --index-url https://pypi.org/simple --require-hashes --only-binary=:all: \
    -r build-requirements.lock

# UBI10 itself requires x86-64-v3; avoid Annoy's host-specific -march=native.
export ANNOY_COMPILER_ARGS="-D_CRT_SECURE_NO_WARNINGS,-fpermissive,-O3,-ffast-math,-fno-associative-math,-DANNOYLIB_MULTITHREADED_BUILD,-std=c++14,-march=x86-64-v3"
"$UV" --no-config pip install --python "$VENV/bin/python" \
    --index-url https://pypi.org/simple --torch-backend cpu \
    --require-hashes --no-build-isolation --overrides overrides.lock \
    -r requirements-cpu.lock
"$UV" --no-config pip sync --python "$VENV/bin/python" \
    --index-url https://pypi.org/simple --torch-backend cpu \
    --require-hashes --no-build-isolation requirements-cpu.lock
"$UV" --no-config pip check --python "$VENV/bin/python"
