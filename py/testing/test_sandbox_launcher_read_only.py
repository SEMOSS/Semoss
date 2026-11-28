"""A read-only bind the namespace jail cannot make read-only: fatal for the worker
code, best-effort for everything else."""
import errno
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

LAUNCHER = Path(__file__).resolve().parents[1] / "sandbox_launcher.py"


@unittest.skipUnless(sys.platform.startswith("linux"), "the namespace jail is Linux only")
class ReadOnlyBindTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("sandbox_launcher", LAUNCHER)
        cls.launcher = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.launcher)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.target = str(Path(self.tmp.name) / "py")
        self.calls = []

    def tearDown(self):
        self.tmp.cleanup()

    def _bind(self, strict, read_only_after):
        def mount(src, tgt, fs, flags, data=None):
            self.calls.append(flags)
            return 0

        with patch.object(self.launcher, "_mount", side_effect=mount), \
                patch.object(self.launcher, "_is_read_only", side_effect=read_only_after), \
                patch.object(self.launcher, "_locked_flags", return_value=self.launcher.MS_NOSUID):
            self.launcher._bind("/src/py", self.target, ro=True, strict=strict)

    def test_a_strict_path_that_stays_writable_stops_the_sandbox(self):
        with self.assertRaises(OSError) as raised:
            self._bind(strict=True, read_only_after=[False, False])
        self.assertEqual(raised.exception.errno, errno.EROFS)

    def test_a_refused_remount_is_retried_with_the_locked_flags(self):
        self._bind(strict=True, read_only_after=[False, True, True])
        remounts = [f for f in self.calls if f & self.launcher.MS_REMOUNT]
        self.assertEqual(len(remounts), 2)
        self.assertFalse(remounts[0] & self.launcher.MS_NOSUID)
        self.assertTrue(remounts[1] & self.launcher.MS_NOSUID)
        self.assertTrue(remounts[1] & self.launcher.MS_RDONLY)

    def test_a_path_that_is_not_strict_stays_best_effort(self):
        self._bind(strict=False, read_only_after=[False, False])

    def test_a_remount_that_works_first_time_is_not_repeated(self):
        self._bind(strict=True, read_only_after=[True, True])
        self.assertEqual(len([f for f in self.calls if f & self.launcher.MS_REMOUNT]), 1)


if __name__ == "__main__":
    unittest.main()
