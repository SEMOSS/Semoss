"""Test that handle_python's sys.path mutation is safe under concurrency.

Run: pytest py/testing/test_sys_path_race.py -v

Reproduces the production scenario: multiple threads exec() the path_script
(simulating parallel cell executions / Pixel steps) while another thread
repeatedly imports a module from asset_path (simulating mcp_driver lookup).

In production, the GIL releases during I/O (importlib file reads, network),
which widens any gap between separate sys.path mutations.  To simulate this
in a test, we replace sys.path with a list subclass whose remove() injects
a tiny delay -- the same window that I/O creates in production.  The test
dynamically reads the path_script template from the actual source file, so
it always tests whatever pattern the current branch uses.
"""

import importlib
import os
import re
import sys
import tempfile
import textwrap
import threading
import time
import unittest

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PY_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PY_ROOT not in sys.path:
    sys.path.insert(0, PY_ROOT)

HANDLER_PATH = os.path.join(PY_ROOT, "gaas_tcp_server_handler.py")


# ---------------------------------------------------------------------------
# SlowList: a list subclass whose remove() injects a small delay to simulate
# the GIL release that happens during real I/O (importlib file reads, etc.).
# This is test infrastructure -- it makes the timing-dependent race window
# deterministic so the test can catch it.
# ---------------------------------------------------------------------------

class SlowList(list):
    """A list that pauses briefly after remove(), simulating GIL release."""

    def remove(self, value):
        try:
            super().remove(value)
        except ValueError:
            pass  # concurrent thread already removed it
        time.sleep(0.002)  # 2ms -- simulates GIL release during I/O


def _extract_path_script_template() -> str:
    """Read gaas_tcp_server_handler.py and extract the path_script template.

    Returns the dedented f-string body (with {asset_path} placeholder) that
    handle_python uses to build the path_script for each asset_path.
    """
    with open(HANDLER_PATH, "r") as f:
        source = f.read()

    pattern = re.compile(
        r'path_script\s*\+=\s*textwrap\.dedent\(\s*f"""(.*?)"""\s*\)',
        re.DOTALL,
    )
    match = pattern.search(source)
    if not match:
        raise RuntimeError(
            "Could not find the path_script template in "
            f"{HANDLER_PATH} -- has the code structure changed?"
        )
    return textwrap.dedent(match.group(1))


def _build_path_script(asset_path: str) -> str:
    """Build the path_script exactly as handle_python would for one asset_path."""
    template = _extract_path_script_template()
    return template.replace("{asset_path}", asset_path)


class TestSysPathConcurrency(unittest.TestCase):
    """Verify handle_python's path_script doesn't break concurrent imports.

    The test replaces sys.path with a SlowList to simulate the GIL release
    that occurs during production I/O.  If the path_script pattern has a gap
    (e.g. remove() then insert()), the delay in SlowList.remove() opens that
    gap wide enough for concurrent imports to fail with ModuleNotFoundError.
    An atomic pattern (sys.path = [...]) never calls remove() on the old list,
    so the delay has no effect and imports always succeed.
    """

    def setUp(self):
        self._saved_sys_path = list(sys.path)
        self._saved_switch_interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)

    def tearDown(self):
        sys.path[:] = self._saved_sys_path
        sys.setswitchinterval(self._saved_switch_interval)
        sys.modules.pop("__mcp_driver_probe__", None)

    def test_concurrent_path_mutations_do_not_break_imports(self):
        """5 threads exec() the path_script 50x each while an importer
        thread imports a probe module 200x.  sys.path is a SlowList to
        simulate production I/O timing.  No ModuleNotFoundError should
        occur if the sys.path mutation is atomic."""
        with tempfile.TemporaryDirectory() as asset_dir:
            probe = "__mcp_driver_probe__"
            with open(os.path.join(asset_dir, f"{probe}.py"), "w") as f:
                f.write("LOADED = True\n")

            # Replace sys.path with a SlowList to simulate GIL-release timing
            slow_path = SlowList(sys.path)
            slow_path.insert(0, asset_dir)
            sys.path = slow_path

            script = _build_path_script(asset_dir)

            errors = []
            barrier = threading.Barrier(6)  # 5 mutators + 1 importer

            def mutator():
                try:
                    barrier.wait(timeout=2)
                except threading.BrokenBarrierError:
                    return
                for _ in range(50):
                    exec(script)

            def importer():
                try:
                    barrier.wait(timeout=2)
                except threading.BrokenBarrierError:
                    return
                for _ in range(200):
                    try:
                        sys.modules.pop(probe, None)
                        importlib.invalidate_caches()
                        mod = importlib.import_module(probe)
                        assert getattr(mod, "LOADED", False)
                    except ModuleNotFoundError as e:
                        errors.append(str(e))

            threads = [threading.Thread(target=mutator) for _ in range(5)]
            imp_thread = threading.Thread(target=importer)

            imp_thread.start()
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)
            imp_thread.join(timeout=30)

            self.assertEqual(
                len(errors), 0,
                f"Import failed {len(errors)} times during concurrent "
                f"path_script execution — the sys.path mutation in "
                f"handle_python is not safe under concurrency.\n"
                f"First error: {errors[0] if errors else 'N/A'}",
            )


if __name__ == "__main__":
    unittest.main()

