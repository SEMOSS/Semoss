#!/usr/bin/env python3
"""Flatten SEMOSS's pyproject into a requirements list for the executor image.

SEMOSS builds its runtime python with ``uv pip install ".[cpu]"`` against
``py/install_config/pyproject.toml``. The executor sandbox must offer the same import
surface, or a ``Py()`` block that worked outside the sandbox starts failing with
ImportError inside it.

Installing the project itself (``.[cpu]``) is not an option here: that pyproject declares
no build-system, so it is a dependency manifest rather than an installable package. This
script reads the same file and emits the dependency list instead, so nothing is duplicated
by hand.

Two things a naive flattening gets wrong, both handled here:

* ``[tool.uv.sources]`` maps torch/torchvision/torchaudio to the pytorch-cpu index.
  Dropping it makes PyPI resolve CUDA builds -- 3.3 GB of ``nvidia`` plus triton in an
  image that never sees a GPU. Those three are skipped by default; install them from the
  CPU index first (see Dockerfile.executor-full).
* ``[tool.uv] override-dependencies`` carries security pins (marshmallow, protobuf, numpy,
  pyasn1, lxml). They are emitted as ordinary requirements so the sandbox keeps the same
  minimum versions as the platform.

usage: export_requirements.py <pyproject.toml> [extra] [--keep-torch] > requirements.txt
"""

import sys
import tomllib

TORCH = ("torch", "torchvision", "torchaudio")


def _name(spec: str) -> str:
    """Package name from a requirement specifier, without version bounds or extras."""
    for separator in (">", "=", "<", "!", "~", "[", " "):
        spec = spec.split(separator)[0]
    return spec.strip().lower()


def main() -> int:
    args = list(sys.argv[1:])
    keep_torch = "--keep-torch" in args
    args = [a for a in args if a != "--keep-torch"]

    if not args:
        print(__doc__, file=sys.stderr)
        return 2
    path = args[0]
    extra = args[1] if len(args) > 1 else ""

    with open(path, "rb") as handle:
        data = tomllib.load(handle)

    project = data.get("project", {})
    dependencies = list(project.get("dependencies", []))
    if extra:
        extras = project.get("optional-dependencies", {})
        if extra not in extras:
            print(
                "unknown extra %r; available: %s" % (extra, ", ".join(sorted(extras))),
                file=sys.stderr,
            )
            return 1
        dependencies.extend(extras[extra])

    if not keep_torch:
        dependencies = [d for d in dependencies if _name(d) not in TORCH]

    # Security pins the platform applies over transitive resolutions.
    overrides = data.get("tool", {}).get("uv", {}).get("override-dependencies", [])
    dependencies.extend(overrides)

    seen = set()
    for dependency in dependencies:
        if dependency in seen:
            continue
        seen.add(dependency)
        print(dependency)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
