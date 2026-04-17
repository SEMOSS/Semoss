"""
Tests demonstrating the sys.path race conditions in gaas_tcp_server_handler.py.

Expected results per branch (run with: pytest py/tests/native_py_server/test_sys_path_race.py -v):

    Test                                       dev      ck-load-mdp-driver   after-planned-fix
    ----                                       ---      ------------------   -----------------
    1a. RemoveInsertDeterministicObserver      PASS     PASS                 PASS    (illustrative; deterministic event-driven reproduction of the dev pattern's window)
    1b. RemoveInsertTightLoopObserver          PASS     PASS                 PASS    (illustrative; probabilistic 0.5s tight-loop variant of 1a)
    2. AtomicAssignmentPattern                 PASS     PASS                 PASS    (positive control)
    3. HandlerShipsAtomicPattern               FAIL     PASS                 PASS    (inspects production source)
    4. GetInsightGlobalsSeedsSysPath           FAIL     FAIL                 PASS    (calls get_insight_globals(asset_paths=...))
    5a. SafetyCheckScenarioFindsModule         FAIL     FAIL                 PASS    (single-thread: get_insight_globals then immediate import)
    5b. ConcurrentGetInsightGlobalsSerialized  FAIL*    FAIL*                PASS    (10 threads racing on same insight_id)

    * = pre-fix this raises TypeError on the asset_paths kwarg; the
        concurrency assertions only become meaningful once the API gap closes.

Scope note: the planned fix CANNOT synchronize against arbitrary `import`
calls in unrelated threads (sys.path is process-global; CPython's import
machinery does not hold the InsightGlobalStore lock). Tests 5a/5b cover the
scenarios the fix actually closes - the original mcp_driver-not-found bug
(single-thread safety check) and concurrent insight initialization.

Tests 1a/1b and 2 are pattern demonstrations - they prove the race exists in
the remove/insert pattern and is absent in the atomic-assign pattern,
independent of which branch is checked out. They are expected to PASS (the
assertion is that the unsafe pattern *did* expose a missing-path window).
1a uses threading.Event to deterministically force the observer to sample
between the .remove() and .insert() calls; 1b hammers the same pattern in
a tight loop for 0.5s as a probabilistic backstop.

Tests 3, 4, and 5a/5b are the ones that actually differentiate branches and
prove the real bugs - 3 against the production source, 4 and 5a/5b against
InsightGlobalStore.
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
PY_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if PY_ROOT not in sys.path:
    sys.path.insert(0, PY_ROOT)

import gaas_tcp_server_handler  # noqa: E402
from gaas_tcp_server_handler import InsightGlobalStore  # noqa: E402


# --------------------------------------------------------------------------- #
# 1. Pattern demonstration: remove/insert exposes a missing-path window.      #
#    This is the pattern the `dev` branch ships in handle_python.             #
# --------------------------------------------------------------------------- #
class RemoveInsertPatternRaceTest(unittest.TestCase):
    """Demonstrates the dev-branch pattern's race window.

    `sys.path.remove(x); sys.path.insert(0, x)` is two bytecode operations.
    Between them, x is absent from sys.path. Any concurrent import of a module
    living under x (or any read of sys.path) will see it missing.

    This test runs the dev pattern inline so it passes on every branch -
    its purpose is to prove the pattern itself is unsafe. To check whether
    the production code currently ships this pattern, see test 3.
    """

    TEST_PATH = "/tmp/__semoss_race_test_remove_insert__"

    def setUp(self):
        if self.TEST_PATH not in sys.path:
            sys.path.insert(0, self.TEST_PATH)

    def tearDown(self):
        while self.TEST_PATH in sys.path:
            sys.path.remove(self.TEST_PATH)

    def test_deterministic_observer_sees_missing_path(self):
        mid_snapshot = []
        removed = threading.Event()
        observed = threading.Event()

        def mutator_remove_insert():
            if self.TEST_PATH in sys.path:
                sys.path.remove(self.TEST_PATH)
            removed.set()
            observed.wait(timeout=2.0)
            sys.path.insert(0, self.TEST_PATH)

        def observer():
            removed.wait(timeout=2.0)
            mid_snapshot.append(self.TEST_PATH in sys.path)
            observed.set()

        mt = threading.Thread(target=mutator_remove_insert)
        ot = threading.Thread(target=observer)
        mt.start()
        ot.start()
        mt.join(timeout=5.0)
        ot.join(timeout=5.0)

        self.assertEqual(len(mid_snapshot), 1, "observer did not run")
        self.assertFalse(
            mid_snapshot[0],
            "Expected remove->insert to expose a window where the path is "
            "absent; this is the race the atomic-assignment fix targets.",
        )

    def test_concurrent_tight_loop_catches_missing_path(self):
        """Probabilistic version - hammers the pattern for 0.5s."""
        stop = threading.Event()
        observed_missing = [0]

        def mutator_remove_insert():
            while not stop.is_set():
                if self.TEST_PATH in sys.path:
                    sys.path.remove(self.TEST_PATH)
                sys.path.insert(0, self.TEST_PATH)

        def observer():
            while not stop.is_set():
                if self.TEST_PATH not in sys.path:
                    observed_missing[0] += 1

        m = threading.Thread(target=mutator_remove_insert)
        o = threading.Thread(target=observer)
        m.start()
        o.start()
        time.sleep(0.5)
        stop.set()
        m.join(timeout=5.0)
        o.join(timeout=5.0)

        self.assertGreater(
            observed_missing[0],
            0,
            "Expected at least one missing-path observation within 0.5s. "
            "If this fails, the scheduler happened to never preempt the "
            "mutator between remove() and insert() - rerun, or trust the "
            "deterministic variant above.",
        )


# --------------------------------------------------------------------------- #
# 2. Positive control: atomic single-assignment never exposes the window.     #
# --------------------------------------------------------------------------- #
class AtomicAssignmentPatternTest(unittest.TestCase):
    """Positive control - the atomic single-assignment pattern is safe.

    `sys.path = [x] + [p for p in sys.path if p != x]` is a single STORE_ATTR
    on the sys module. Other threads see either the old list or the new list,
    never an intermediate state.
    """

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

        def mutator_atomic():
            path = self.TEST_PATH
            while not stop.is_set():
                sys.path = [path] + [p for p in sys.path if p != path]

        def observer():
            path = self.TEST_PATH
            while not stop.is_set():
                if path not in sys.path:
                    observed_missing[0] += 1

        m = threading.Thread(target=mutator_atomic)
        o = threading.Thread(target=observer)
        m.start()
        o.start()
        time.sleep(0.5)
        stop.set()
        m.join(timeout=5.0)
        o.join(timeout=5.0)

        self.assertEqual(
            observed_missing[0],
            0,
            f"Atomic single-assignment should never expose sys.path without "
            f"the asset path; got {observed_missing[0]} missing observations.",
        )


# --------------------------------------------------------------------------- #
# 3. Branch differentiator #1: which pattern does production actually ship?   #
#    FAILS on dev, PASSES on ck-load-mdp-driver and after the planned fix.    #
# --------------------------------------------------------------------------- #
def _find_handle_python_source():
    """Locate handle_python on whichever class/function exposes it in this
    branch and return its source. handle_python lives on a handler class in
    gaas_tcp_server_handler; walk module attributes to find it."""
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
        raise AssertionError(
            "Could not locate handle_python in gaas_tcp_server_handler. "
            "Update the test to point at the correct symbol."
        )
    return "\n\n".join(inspect.getsource(fn) for fn in candidates)


class HandlerShipsAtomicPatternTest(unittest.TestCase):
    """Inspects gaas_tcp_server_handler.handle_python source to check whether
    the production branch ships the unsafe remove/insert pattern or the safe
    atomic single-assignment pattern.

    On `dev`: handle_python contains `sys.path.remove(asset_path)` followed
    by `sys.path.insert(0, asset_path)` -> this test FAILS.

    On `ck-load-mdp-driver` and beyond: handle_python uses the atomic
    `sys.path = [asset_path] + [...]` -> this test PASSES.
    """

    def test_handle_python_does_not_use_remove_then_insert(self):
        src = _find_handle_python_source()

        has_unsafe_remove = "sys.path.remove(asset_path)" in src
        has_unsafe_insert = "sys.path.insert(0, asset_path)" in src
        has_atomic_assign = "sys.path = [asset_path]" in src

        self.assertFalse(
            has_unsafe_remove and has_unsafe_insert,
            "handle_python ships the unsafe remove/insert pattern for "
            "asset_path. Other threads can observe sys.path without the "
            "asset path between the two calls. Replace with the atomic "
            "single-assignment pattern.",
        )
        self.assertTrue(
            has_atomic_assign,
            "handle_python should construct sys.path via single-assignment "
            "(`sys.path = [asset_path] + [...]`) so other threads only ever "
            "see a fully-formed sys.path.",
        )


# --------------------------------------------------------------------------- #
# 4. Branch differentiator #2: get_insight_globals must seed sys.path.        #
#    FAILS on dev AND ck-load-mdp-driver, PASSES after the planned fix.       #
# --------------------------------------------------------------------------- #
class GetInsightGlobalsSeedsSysPathTest(unittest.TestCase):
    """The mcp_driver safety check at gaas_tcp_server_handler.py:853-868
    can call reload_mcp_function() BEFORE the per-exec path_script runs.
    Other handler threads can also trigger imports at any time. Because
    get_insight_globals() does not touch sys.path today, those imports can
    miss the asset_paths and fail.

    After the planned fix get_insight_globals(insight_id, asset_paths=...)
    will atomically seed sys.path under the store's init lock, so any later
    import (in this thread or another) sees the asset paths.
    """

    INSIGHT_ID = "__race_test_insight_seeding__"
    SAVED_SYS_PATH: list = []

    def setUp(self):
        # Snapshot sys.path so the seeding test doesn't leak between cases.
        type(self).SAVED_SYS_PATH = list(sys.path)

    def tearDown(self):
        sys.path[:] = type(self).SAVED_SYS_PATH
        store = InsightGlobalStore()
        store.insight_globals.pop(self.INSIGHT_ID, None)

    def test_get_insight_globals_seeds_sys_path_from_asset_paths(self):
        with tempfile.TemporaryDirectory() as asset_dir:
            module_name = "__semoss_seed_probe__"
            module_file = os.path.join(asset_dir, f"{module_name}.py")
            with open(module_file, "w") as fh:
                fh.write("LOADED = True\n")
            sys.modules.pop(module_name, None)

            self.assertNotIn(
                asset_dir,
                sys.path,
                "Test precondition: asset_dir should not already be on sys.path.",
            )

            store = InsightGlobalStore()
            try:
                store.get_insight_globals(
                    self.INSIGHT_ID, asset_paths=[asset_dir]
                )
            except TypeError as e:
                self.fail(
                    "get_insight_globals does not yet accept an asset_paths "
                    "kwarg, so it cannot seed sys.path at insight-creation "
                    "time. This is the API gap part of the bug. "
                    f"Underlying error: {e}"
                )

            self.assertIn(
                asset_dir,
                sys.path,
                "get_insight_globals must seed asset_paths into sys.path so "
                "imports triggered before the per-exec path_script (e.g. the "
                "mcp_driver safety check, or another handler thread) succeed.",
            )

            mod = importlib.import_module(module_name)
            self.assertTrue(
                getattr(mod, "LOADED", False),
                f"Expected to import {module_name} from {asset_dir} after "
                f"get_insight_globals seeded sys.path.",
            )


# --------------------------------------------------------------------------- #
# 5. Branch differentiator #3: scenarios the seeding fix actually closes.     #
#    FAILS on dev AND ck-load-mdp-driver, PASSES after the planned fix.       #
# --------------------------------------------------------------------------- #
#
# Scope note: the planned fix tightens the window by seeding sys.path inside
# get_insight_globals under an RLock. It cannot synchronize against arbitrary
# `import` calls in other threads (sys.path is process-global; no lock is
# held by CPython's import machinery). The two scenarios below are the ones
# the fix DOES close:
#
#   5a. Single-thread safety-check scenario - this is the original
#       'mcp_driver not found on first call' bug. handle_python calls
#       get_insight_globals, then immediately runs the mcp_driver safety
#       check at gaas_tcp_server_handler.py:843, which can call
#       reload_mcp_function() *before* the per-exec path_script runs.
#       Pre-fix, sys.path is not seeded yet -> import fails. Post-fix,
#       get_insight_globals has already seeded sys.path -> import succeeds.
#
#   5b. Concurrent get_insight_globals storm - many handler threads init
#       the same insight at once. Pre-fix there is no lock, so
#       initialization can run multiple times and asset_paths can be lost.
#       Post-fix the RLock + double-checked seeding ensures one init and a
#       consistent sys.path.
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
        """Single-thread reproduction of the mcp_driver safety-check bug.

        Pre-fix get_insight_globals doesn't touch sys.path, so the import
        immediately after it (mimicking handle_python's safety check ->
        reload_mcp_function path) fails. Post-fix it must succeed.
        """
        with tempfile.TemporaryDirectory() as asset_dir:
            module_name = "__semoss_safety_check_probe__"
            module_file = os.path.join(asset_dir, f"{module_name}.py")
            with open(module_file, "w") as fh:
                fh.write("OK = True\n")
            sys.modules.pop(module_name, None)

            store = InsightGlobalStore()
            try:
                store.get_insight_globals(
                    self.INSIGHT_ID_5A, asset_paths=[asset_dir]
                )
            except TypeError as e:
                self.fail(
                    "get_insight_globals does not accept asset_paths kwarg "
                    f"(API gap part of the bug): {e}"
                )

            # This import models the mcp_driver safety check at
            # gaas_tcp_server_handler.py:843 - it runs BEFORE the per-exec
            # path_script. Pre-fix it raises ModuleNotFoundError because
            # sys.path was never seeded. Post-fix it succeeds.
            try:
                mod = importlib.import_module(module_name)
            except ModuleNotFoundError as e:
                self.fail(
                    "Import immediately after get_insight_globals failed - "
                    "this is exactly the mcp_driver-not-found bug. "
                    f"sys.path entries: {sys.path[:3]}... ; error: {e}"
                )
            self.assertTrue(getattr(mod, "OK", False))

    def test_5b_concurrent_get_insight_globals_is_serialized(self):
        """Many threads racing on the same insight_id must produce one init
        and a consistent sys.path. Pre-fix: unlocked check-then-set means
        multiple threads can enter the init block; post-fix the RLock +
        double-checked locking guarantees a single initialization and a
        single seeded sys.path entry."""
        with tempfile.TemporaryDirectory() as asset_dir:
            store = InsightGlobalStore()
            errors = []
            results = []
            barrier = threading.Barrier(10)

            def worker():
                try:
                    barrier.wait(timeout=2.0)
                    g = store.get_insight_globals(
                        self.INSIGHT_ID_5B, asset_paths=[asset_dir]
                    )
                    results.append(id(g))
                except TypeError as e:
                    errors.append(("api-gap", repr(e)))
                except Exception as e:  # pragma: no cover
                    errors.append(("worker", repr(e)))

            threads = [threading.Thread(target=worker) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=5.0)

            self.assertEqual(
                errors, [],
                f"Concurrent get_insight_globals raised: {errors}. The "
                f"planned fix's RLock + asset_paths kwarg should make this "
                f"clean.",
            )
            self.assertEqual(
                len(set(results)), 1,
                f"Expected all threads to receive the same globals dict; "
                f"got {len(set(results))} distinct dicts. Indicates "
                f"check-then-set without a lock allowed multiple inits.",
            )
            self.assertEqual(
                sys.path.count(asset_dir), 1,
                f"Expected asset_dir to appear exactly once in sys.path; "
                f"got {sys.path.count(asset_dir)} occurrences. Indicates "
                f"non-atomic seeding under contention.",
            )


if __name__ == "__main__":
    unittest.main()
