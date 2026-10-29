"""Keep newly created venvs on the patched pip installed in the base interpreter."""

import ensurepip
import importlib
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pip
from pip._vendor import certifi
from pip._vendor.packaging.version import Version

for package, version, minimum in (
    ("pip", pip.__version__, "26.2.2"),
    ("pip's vendored certifi", certifi.__version__, "2026.10.19"),
):
    if Version(version) < Version(minimum):
        raise RuntimeError(f"{package} {version} is below the required {minimum}")

ensurepip_source = Path(ensurepip.__file__)
bundled = ensurepip_source.parent / "_bundled"
with tempfile.TemporaryDirectory() as download_dir:
    subprocess.run(
        [
            sys.executable, "-m", "pip", "download",
            "--disable-pip-version-check", "--no-cache-dir", "--no-deps",
            "--only-binary=:all:", "--dest", download_dir,
            f"pip=={pip.__version__}",
        ],
        check=True,
    )
    wheel = Path(download_dir) / f"pip-{pip.__version__}-py3-none-any.whl"
    shutil.copyfile(wheel, bundled / wheel.name)
    for old_wheel in bundled.glob("pip-*.whl"):
        if old_wheel.name != wheel.name:
            old_wheel.unlink()

# Python 3.12 names the wheel explicitly; newer versions discover it in _bundled.
source = ensurepip_source.read_text()
updated = re.sub(r'(?m)^_PIP_VERSION = .+$', f'_PIP_VERSION = "{pip.__version__}"', source)
if updated != source:
    ensurepip_source.write_text(updated)
    for bytecode in (ensurepip_source.parent / "__pycache__").glob("__init__.*.pyc"):
        bytecode.unlink()

importlib.reload(ensurepip)
if ensurepip.version() != pip.__version__:
    raise RuntimeError(f"ensurepip still reports {ensurepip.version()}, expected {pip.__version__}")
print(f"Updated ensurepip to pip {pip.__version__}")
