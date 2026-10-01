import argparse
import json
from pathlib import Path
import re
import subprocess
import sys


BASE_NAMES = ("MAVEN_IMAGE", "UBI_IMAGE", "PYTHON_IMAGE")
PINNED_IMAGE = re.compile(
    r"(registry1\.dso\.mil/ironbank/[a-z0-9/_-]+:[A-Za-z0-9][A-Za-z0-9_.-]*)"
    r"@sha256:[0-9a-f]{64}"
)


def parse_tags(text):
    definitions = re.findall(
        r"^ARG (MAVEN_IMAGE|UBI_IMAGE|PYTHON_IMAGE)=(.+)$", text, re.MULTILINE
    )
    if len(definitions) != 3 or {name for name, _ in definitions} != set(BASE_NAMES):
        raise ValueError("Expected exactly one definition of each Iron Bank base image")
    tags = {}
    for name, image in definitions:
        match = PINNED_IMAGE.fullmatch(image)
        if not match:
            raise ValueError(f"{name} must use a digest-pinned Iron Bank version tag")
        tags[name] = match.group(1)
    return tags


def resolve_bases(tags):
    resolved = {}
    for name, tag in tags.items():
        result = subprocess.run(
            ["docker", "buildx", "imagetools", "inspect", tag,
             "--format", "{{json .Manifest}}"],
            check=True, capture_output=True, text=True, timeout=120,
        )
        manifest = json.loads(result.stdout)
        digest = manifest.get("digest") if isinstance(manifest, dict) else None
        if not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise ValueError(f"Registry returned an invalid digest for {tag}")
        resolved[name] = f"{tag}@{digest}"
    return resolved


def main(argv=None):
    parser = argparse.ArgumentParser(description="Refresh the existing Iron Bank version tags.")
    parser.add_argument("--dockerfile", type=Path, default=Path("Dockerfile"))
    args = parser.parse_args(argv)
    try:
        images = resolve_bases(parse_tags(args.dockerfile.read_text(encoding="utf-8")))
    except (ValueError, OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        sys.exit(f"Base image refresh failed: {error}")
    for name, image in images.items():
        print(f"{name}={image}")


if __name__ == "__main__":
    main()
