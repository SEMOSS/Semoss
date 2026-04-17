"""Tests for sys.path race conditions in gaas_tcp_server_handler.py.

Run: pytest py/testing/test_sys_path_race.py -v

    Test                                  Maps to fix in
    ----                                  --------------
    1. SeedsSysPathFromAssetPaths         get_insight_globals(asset_paths=...) + _seed_sys_path
    2. SafetyCheckScenarioFindsModule     handle_python seeds before the mcp_driver safety check
    3. ConcurrentInitIsAtomicAndSerialized  InsightGlobalStore._init_lock (RLock) +
                                            atomic `sys.path = paths + [...]` in _seed_sys_path
"""

import importlib
import os
import sys
import tempfile
import threading
import time
import unittest

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PY_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PY_ROOT not in sys.path:
    sys.path.insert(0, PY_ROOT)

from gaas_tcp_server_handler import InsightGlobalStore  # noqa: E402


class _SysPathTestBase(unittest.TestCase):
    """Snapshots sys.path and clears any insight_ids touched, so a failing
    test doesn't leak state into the next case."""

    INSIGHT_IDS: tuple = ()

    def setUp(self):
        self._saved_sys_path = list(sys.path)

    def tearDown(self):
        sys.path[:] = self._saved_sys_path
        store = InsightGlobalStore()
        for iid in self.INSIGHT_IDS:
            store.insight_globals.pop(iid, None)


class SeedsSysPathFromAssetPathsTest(_SysPathTestBase):
    """get_insight_globals must accept asset_paths and seed them into
    sys.path before returning, so any code path that runs between
    get_insight_globals and the per-exec script (notably the mcp_driver
    safety check) sees a seeded sys.path.

    Pre-fix: TypeError on the unknown kwarg. Post-fix: asset_dir on
    sys.path and a fresh import resolves through it.
    """

    INSIGHT_IDS = ("__race_test_insight_seeding__",)

    def test_seeds_sys_path_from_asset_paths(self):
        with tempfile.TemporaryDirectory() as asset_dir:
            module_name = "__semoss_seed_probe__"
            with open(os.path.join(asset_dir, f"{module_name}.py"), "w") as fh:
                fh.write("LOADED = True\n")
            sys.modules.pop(module_name, None)

            self.assertNotIn(
                asset_dir,
                sys.path,
                "precondition: asset_dir must not already be on sys.path",
            )

            try:
                InsightGlobalStore().get_insight_globals(
                    self.INSIGHT_IDS[0], asset_paths=[asset_dir]
                )
            except TypeError as e:
                self.fail(
                    "get_insight_globals does not accept asset_paths kwarg, "
                    f"so it cannot seed sys.path at insight-creation time: {e}"
                )

            self.assertIn(
                asset_dir,
                sys.path,
                "asset_dir was not seeded into sys.path by get_insight_globals",
            )
            mod = importlib.import_module(module_name)
            self.assertTrue(
                getattr(mod, "LOADED", False),
                "module from asset_dir should import after seeding",
            )


class SafetyCheckScenarioTest(_SysPathTestBase):
    """End-to-end repro of the mcp_driver-not-found bug: pre-fix,
    handle_python ran the mcp_driver safety check (which can call
    secure_import) BEFORE the per-exec path_script seeded sys.path.
    Post-fix, handle_python calls get_insight_globals(asset_paths=...)
    first, so the import succeeds.
    """

    INSIGHT_IDS = ("__race_test_safety_check__",)

    def test_import_immediately_after_get_insight_globals(self):
        with tempfile.TemporaryDirectory() as asset_dir:
            module_name = "__semoss_safety_check_probe__"
            with open(os.path.join(asset_dir, f"{module_name}.py"), "w") as fh:
                fh.write("OK = True\n")
            sys.modules.pop(module_name, None)

            try:
                InsightGlobalStore().get_insight_globals(
                    self.INSIGHT_IDS[0], asset_paths=[asset_dir]
                )
            except TypeError as e:
                self.fail(f"get_insight_globals API gap: {e}")

            try:
                mod = importlib.import_module(module_name)
            except ModuleNotFoundError as e:
                self.fail(
                    "import immediately after get_insight_globals failed - "
                    "this is exactly the mcp_driver-not-found bug the safety "
                    f"check would hit. Error: {e}"
                )
            self.assertTrue(getattr(mod, "OK", False))


class ConcurrentInitIsAtomicAndSerializedTest(_SysPathTestBase):
    """Concurrent first-init must be both serialized AND atomic.

    A) Serialization (_init_lock RLock): without the lock, the unguarded
       check-then-set in get_insight_globals lets multiple threads build
       separate globals dicts. Asserted by `len(set(results)) == 1`.

    B) Atomicity (`sys.path = paths + [p for p in sys.path if p not in paths]`
       inside _seed_sys_path): a naive remove/insert seed would still dedup
       (passing count==1) but expose a window where sys.path is observed
       without asset_dir. The observer thread polls during the storm and
       counts those windows; atomic single-assignment guarantees zero.
    """

    INSIGHT_IDS = ("__race_test_concurrent_init__",)

    def test_concurrent_init_is_atomic_and_serialized(self):
        with tempfile.TemporaryDirectory() as asset_dir:
            store = InsightGlobalStore()
            errors = []
            results = []
            barrier = threading.Barrier(11)  # 10 workers + observer
            stop_observer = threading.Event()
            missing_observations = [0]

            def worker():
                try:
                    barrier.wait(timeout=2.0)
                    g = store.get_insight_globals(
                        self.INSIGHT_IDS[0], asset_paths=[asset_dir]
                    )
                    results.append(id(g))
                except TypeError as e:
                    errors.append(("api-gap", repr(e)))
                except Exception as e:
                    errors.append(("worker", repr(e)))

            def observer():
                # Polls during the storm; non-atomic seeding (e.g.
                # remove-then-insert) yanks asset_dir out momentarily.
                try:
                    barrier.wait(timeout=2.0)
                except Exception:
                    return
                deadline = time.time() + 0.5
                while not stop_observer.is_set() and time.time() < deadline:
                    if asset_dir not in sys.path:
                        missing_observations[0] += 1

            threads = [threading.Thread(target=worker) for _ in range(10)]
            obs_thread = threading.Thread(target=observer)

            # Stable baseline so the observer only catches mid-mutation gaps.
            sys.path.insert(0, asset_dir)

            obs_thread.start()
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=5.0)
            stop_observer.set()
            obs_thread.join(timeout=5.0)

            # A) Serialization: every worker received the same globals dict.
            self.assertEqual(errors, [], f"workers raised: {errors}")
            self.assertEqual(
                len(set(results)),
                1,
                f"expected one shared globals dict; got {len(set(results))} "
                "(lock missing or check-then-set unguarded)",
            )
            self.assertEqual(
                sys.path.count(asset_dir),
                1,
                f"expected exactly one asset_dir on sys.path; got "
                f"{sys.path.count(asset_dir)}",
            )

            # B) Atomicity: observer never saw sys.path without asset_dir.
            self.assertEqual(
                missing_observations[0],
                0,
                f"observer saw sys.path without asset_dir "
                f"{missing_observations[0]} times - seeding is not atomic; "
                "use `sys.path = paths + [p for p in sys.path if p not in paths]`",
            )


if __name__ == "__main__":
    unittest.main()
