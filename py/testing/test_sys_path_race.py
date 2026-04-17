"""Tests for sys.path race conditions in gaas_tcp_server_handler.py.

Run: pytest py/testing/test_sys_path_race.py -v

Test                                       dev   ck-load-mdp-driver   after-fix
1a/1b. RemoveInsertPattern                 PASS  PASS                 PASS
2.     AtomicAssignmentPattern             PASS  PASS                 PASS
3.     HandlerShipsAtomicPattern           FAIL  PASS                 PASS
4.     GetInsightGlobalsSeedsSysPath       FAIL  FAIL                 PASS
5a.    SafetyCheckScenarioFindsModule      FAIL  FAIL                 PASS
5b.    ConcurrentGetInsightGlobalsLock     FAIL  FAIL                 PASS
"""

import importlib
import inspect
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

import gaas_tcp_server_handler  # noqa: E402
from gaas_tcp_server_handler import InsightGlobalStore  # noqa: E402


# 1. remove()/insert() exposes a missing-path window.
class RemoveInsertPatternRaceTest(unittest.TestCase):
    TEST_PATH = "/tmp/__semoss_race_test_remove_insert__"

    def setUp(self):
        if self.TEST_PATH not in sys.path:
            sys.path.insert(0, self.TEST_PATH)

    def tearDown(self):
        while self.TEST_PATH in sys.path:
            sys.path.remove(self.TEST_PATH)

    def test_1a_deterministic_observer_sees_missing_path(self):
        mid_snapshot = []
        removed = threading.Event()
        observed = threading.Event()

        def mutator():
            if self.TEST_PATH in sys.path:
                sys.path.remove(self.TEST_PATH)
            removed.set()
            observed.wait(timeout=2.0)
            sys.path.insert(0, self.TEST_PATH)

        def observer():
            removed.wait(timeout=2.0)
            mid_snapshot.append(self.TEST_PATH in sys.path)
            observed.set()

        mt = threading.Thread(target=mutator)
        ot = threading.Thread(target=observer)
        mt.start(); ot.start()
        mt.join(timeout=5.0); ot.join(timeout=5.0)

        self.assertEqual(len(mid_snapshot), 1, "observer did not run")
        self.assertFalse(mid_snapshot[0], "remove->insert exposed a missing-path window")

    def test_1b_tight_loop_catches_missing_path(self):
        stop = threading.Event()
        observed_missing = [0]

        def mutator():
            while not stop.is_set():
                if self.TEST_PATH in sys.path:
                    sys.path.remove(self.TEST_PATH)
                sys.path.insert(0, self.TEST_PATH)

        def observer():
            while not stop.is_set():
                if self.TEST_PATH not in sys.path:
                    observed_missing[0] += 1

        m = threading.Thread(target=mutator)
        o = threading.Thread(target=observer)
        m.start(); o.start()
        time.sleep(0.5)
        stop.set()
        m.join(timeout=5.0); o.join(timeout=5.0)

        self.assertGreater(observed_missing[0], 0, "expected at least one missing-path observation")


# 2. Atomic single-assignment never exposes a missing-path window.
class AtomicAssignmentPatternTest(unittest.TestCase):
    TEST_PATH = "/tmp/__semoss_race_test_atomic__"

    def setUp(self):
        if self.TEST_PATH not in sys.path:
            sys.path.insert(0, self.TEST_PATH)

    def tearDown(self):
        while self.TEST_PATH in sys.path:
            sys.path.remove(self.TEST_PATH)

    def test_atomic_reassignment_never_exposes_missing_path(self):
        stop = threading.Event()
        observed_missing = [0]

        def mutator():
            path = self.TEST_PATH
            while not stop.is_set():
                sys.path = [path] + [p for p in sys.path if p != path]

        def observer():
            path = self.TEST_PATH
            while not stop.is_set():
                if path not in sys.path:
                    observed_missing[0] += 1

        m = threading.Thread(target=mutator)
        o = threading.Thread(target=observer)
        m.start(); o.start()
        time.sleep(0.5)
        stop.set()
        m.join(timeout=5.0); o.join(timeout=5.0)

        self.assertEqual(observed_missing[0], 0)


# 3. Inspect production source: must use atomic single-assignment.
def _find_handle_python_source():
    mod = gaas_tcp_server_handler
    candidates = []
    for name in dir(mod):
        obj = getattr(mod, name)
        if inspect.isclass(obj) and obj.__module__ == mod.__name__:
            fn = getattr(obj, "handle_python", None)
            if fn is not None and callable(fn):
                candidates.append(fn)
        elif inspect.isfunction(obj) and obj.__name__ == "handle_python":
            candidates.append(obj)
    if not candidates:
        raise AssertionError("could not locate handle_python")
    return "\n\n".join(inspect.getsource(fn) for fn in candidates)


class HandlerShipsAtomicPatternTest(unittest.TestCase):
    def test_handle_python_does_not_use_remove_then_insert(self):
        src = _find_handle_python_source()
        unsafe = "sys.path.remove(asset_path)" in src and "sys.path.insert(0, asset_path)" in src
        atomic = "sys.path = [asset_path]" in src

        self.assertFalse(unsafe, "handle_python ships unsafe remove/insert pattern")
        self.assertTrue(atomic, "handle_python should use atomic single-assignment")


# 4. get_insight_globals must seed sys.path from asset_paths.
class GetInsightGlobalsSeedsSysPathTest(unittest.TestCase):
    INSIGHT_ID = "__race_test_insight_seeding__"
    SAVED_SYS_PATH: list = []

    def setUp(self):
        type(self).SAVED_SYS_PATH = list(sys.path)

    def tearDown(self):
        sys.path[:] = type(self).SAVED_SYS_PATH
        InsightGlobalStore().insight_globals.pop(self.INSIGHT_ID, None)

    def test_seeds_sys_path_from_asset_paths(self):
        with tempfile.TemporaryDirectory() as asset_dir:
            module_name = "__semoss_seed_probe__"
            with open(os.path.join(asset_dir, f"{module_name}.py"), "w") as fh:
                fh.write("LOADED = True\n")
            sys.modules.pop(module_name, None)

            self.assertNotIn(asset_dir, sys.path)

            try:
                InsightGlobalStore().get_insight_globals(self.INSIGHT_ID, asset_paths=[asset_dir])
            except TypeError as e:
                self.fail(f"get_insight_globals does not accept asset_paths kwarg: {e}")

            self.assertIn(asset_dir, sys.path, "asset_dir not seeded into sys.path")
            mod = importlib.import_module(module_name)
            self.assertTrue(getattr(mod, "LOADED", False))


# 5. Scenarios the seeding fix actually closes.
# Scope: cannot synchronize against arbitrary imports in unrelated threads
# (sys.path is process-global). 5a is the original mcp_driver-not-found bug
# (single-thread safety check); 5b is concurrent first-init under contention.
class FixedScenariosTest(unittest.TestCase):
    INSIGHT_ID_5A = "__race_test_safety_check_scenario__"
    INSIGHT_ID_5B = "__race_test_concurrent_get_globals__"
    SAVED_SYS_PATH: list = []

    def setUp(self):
        type(self).SAVED_SYS_PATH = list(sys.path)

    def tearDown(self):
        sys.path[:] = type(self).SAVED_SYS_PATH
        store = InsightGlobalStore()
        store.insight_globals.pop(self.INSIGHT_ID_5A, None)
        store.insight_globals.pop(self.INSIGHT_ID_5B, None)

    def test_5a_safety_check_scenario_finds_module(self):
        # Models handle_python's mcp_driver safety check: get_insight_globals
        # then immediate import, before the per-exec path_script runs.
        with tempfile.TemporaryDirectory() as asset_dir:
            module_name = "__semoss_safety_check_probe__"
            with open(os.path.join(asset_dir, f"{module_name}.py"), "w") as fh:
                fh.write("OK = True\n")
            sys.modules.pop(module_name, None)

            try:
                InsightGlobalStore().get_insight_globals(self.INSIGHT_ID_5A, asset_paths=[asset_dir])
            except TypeError as e:
                self.fail(f"API gap: {e}")

            try:
                mod = importlib.import_module(module_name)
            except ModuleNotFoundError as e:
                self.fail(f"import after get_insight_globals failed: {e}")
            self.assertTrue(getattr(mod, "OK", False))

    def test_5b_concurrent_get_insight_globals_is_serialized(self):
        # 10 threads racing on the same insight_id must produce one init
        # and exactly one sys.path entry.
        with tempfile.TemporaryDirectory() as asset_dir:
            store = InsightGlobalStore()
            errors = []
            results = []
            barrier = threading.Barrier(10)

            def worker():
                try:
                    barrier.wait(timeout=2.0)
                    g = store.get_insight_globals(self.INSIGHT_ID_5B, asset_paths=[asset_dir])
                    results.append(id(g))
                except TypeError as e:
                    errors.append(("api-gap", repr(e)))
                except Exception as e:
                    errors.append(("worker", repr(e)))

            threads = [threading.Thread(target=worker) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=5.0)

            self.assertEqual(errors, [])
            self.assertEqual(len(set(results)), 1, "expected one shared globals dict")
            self.assertEqual(sys.path.count(asset_dir), 1, "expected exactly one asset_dir on sys.path")


if __name__ == "__main__":
    unittest.main()
