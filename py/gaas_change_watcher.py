"""Change tracking for the sandbox worker.

The host needs to know *which* files a block wrote, not just *that* something
changed. A stat-walk of the tree costs O(files) per block (~6 ms for 240 files,
seconds for 100k), and spawning a process to do it costs ~80 ms before it does
any work. inotify is O(events): idle costs nothing, one write costs one event.

Measured on the dev VM (2026-10-23), watching a project's assets tree:

    CREATE/MODIFY/CLOSE_WRITE  watch_bash.txt     (written by bash)
    CREATE/MODIFY/CLOSE_WRITE  watch_py.txt       (written by a *different* python)
    CREATE,DIR                 newdir
    <nothing*                  newdir/inner.txt

The last line is the rule that matters: inotify watches directories, so a new
directory must be armed explicitly or everything written inside it is invisible.

Two honest limits, both handled by the caller:

* events before the watch starts are lost -> the first snapshot reports
  ``baseline`` and is discarded, because the host already holds what it uploaded;
* the kernel queue can overflow -> ``overflow`` is reported and the host must
  fall back to a full reconcile.
"""

import ctypes
import ctypes.util
import errno
import os
import select
import stat
import struct
import threading
import time

# Directories that are never watched, scanned, or pulled back. `.git` is static inside
# the sandbox (the image has no git binary) yet it was 112 directories and 137 of 256
# files in a real project - over half the watch set and over half of every scan, for
# data that cannot move. `node_modules` is worse: one install adds thousands of
# directories, exhausts fs.inotify.max_user_watches and floods the event queue.
_DEFAULT_IGNORE = ("node_modules", "__pycache__", ".venv", ".git")
_env_ignore = os.environ.get("SEMOSS_SYNC_IGNORE", "").strip()
_IGNORED_DIRS = {name.strip() for name in _env_ignore.split(",") if name.strip()} if _env_ignore else set(_DEFAULT_IGNORE)

# The tuple scan exists to catch changes inotify cannot see (persistent mmap writes).
# Running it on every block makes an idle session O(files) per block - measured 0.57 s
# at 100k files. Bounding it by wall-clock instead keeps staleness under this many
# seconds while an idle burst of blocks pays for at most one scan.
_SCAN_MIN_INTERVAL_S = 2.0

# A scan may occupy at most 1/N of wall-clock: the interval stretches to N x the
# measured duration. At 240 files a scan is ~1 ms (interval stays 2 s); at 100k it is
# ~0.57 s, so the interval becomes ~11 s and the backstop costs ~5% instead of 100%.
_SCAN_BUDGET_FACTOR = 20

IN_MODIFY = 0x00000002
IN_CLOSE_WRITE = 0x00000008
IN_MOVED_FROM = 0x00000040
IN_MOVED_TO = 0x00000080
IN_CREATE = 0x00000100
IN_DELETE = 0x00000200
IN_DELETE_SELF = 0x00000400
IN_MOVE_SELF = 0x00000800
IN_Q_OVERFLOW = 0x00004000
IN_UNMOUNT = 0x00002000
IN_IGNORED = 0x00008000
IN_ISDIR = 0x40000000

_EVENT = struct.Struct("iIII")
_MASK = (IN_CREATE | IN_MODIFY | IN_CLOSE_WRITE | IN_MOVED_FROM | IN_MOVED_TO | IN_DELETE
         | IN_DELETE_SELF | IN_MOVE_SELF)


class ChangeWatcher:
    """Watch a set of roots and report the paths that changed between snapshots."""

    def __init__(self, roots, max_changes=2000):
        self.roots = [root for root in roots if root and os.path.isdir(root)]
        self.max_changes = max_changes
        self._fd = None
        self._thread = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._changes = {}
        self._overflow = False
        self._baseline_pending = True
        self._armed = 0
        self._events_seen = 0
        # Set when the watch tree can no longer be trusted (directory rename, watch
        # removed, unmount): the host must fall back to a full capture for that block.
        self._coverage_lost = False
        # inotify events carry a watch descriptor, not a path; this maps it back.
        self._watched = {}
        self._root_ino = {}
        # path -> (mtime_ns, size, ino, ctime_ns) as of the last checkpoint
        self._stat_index = {}
        self._last_scan = 0.0
        self._scans_skipped = 0
        # Grows with the measured cost of a scan so the backstop never takes more than
        # a few percent of wall-clock, at any tree size.
        self._scan_interval = _SCAN_MIN_INTERVAL_S

    # -- lifecycle ---------------------------------------------------------

    def start(self):
        if not self.roots:
            return False
        library = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
        library.inotify_init1.argtypes = [ctypes.c_int]
        library.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        fd = library.inotify_init1(os.O_NONBLOCK)
        if fd < 0:
            return False
        self._fd = fd
        self._libc = library
        for root in self.roots:
            self._arm_tree(root)
        self._thread = threading.Thread(target=self._loop, name="sandbox-change-watcher", daemon=True)
        self._thread.start()
        return True

    def stop(self):
        self._stop.set()
        if self._fd is not None:
            try:
                os.close(self._fd)
            except OSError:
                pass

    # -- watch arming ------------------------------------------------------

    def _arm(self, directory):
        try:
            watch = self._libc.inotify_add_watch(self._fd, directory.encode("utf-8"), _MASK)
        except Exception:
            return False
        if watch >= 0:
            self._watched[watch] = directory
            self._armed += 1
            return True
        return False

    def add_roots(self, roots):
        """Arm roots discovered after start (a session materialises its tree late)."""
        added = 0
        for root in roots:
            if root in self.roots or not os.path.isdir(root):
                continue
            self.roots.append(root)
            self._arm_tree(root)
            added += 1
        return added

    def _arm_tree(self, root):
        try:
            self._root_ino[root] = os.stat(root).st_ino
        except OSError:
            pass
        for current, dirs, _files in os.walk(root):
            # Prune in place so os.walk never descends into an ignored tree at all.
            dirs[:] = [name for name in dirs if name not in _IGNORED_DIRS]
            self._arm(current)

    # -- event loop --------------------------------------------------------

    def _loop(self):
        while not self._stop.is_set():
            try:
                ready, _, _ = select.select([self._fd], [], [], 0.2)
            except (OSError, ValueError):
                break
            if not ready:
                continue
            try:
                data = os.read(self._fd, 65536)
            except OSError as exc:
                if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    continue
                break
            self._consume(data)

    def _consume(self, data):
        offset = 0
        additions = []
        with self._lock:
            while offset + _EVENT.size <= len(data):
                _watch, mask, _cookie, length = _EVENT.unpack_from(data, offset)
                offset += _EVENT.size
                raw = data[offset:offset + length]
                offset += length
                self._events_seen += 1
                if mask & IN_Q_OVERFLOW:
                    self._overflow = True
                    continue
                if mask & (IN_IGNORED | IN_UNMOUNT | IN_MOVE_SELF):
                    # The watch itself is gone, or it moved with its inode: every path
                    # under it is stale. No event on this descriptor is trustworthy now.
                    self._coverage_lost = True
                    continue
                name = raw.split(b"\0", 1)[0].decode("utf-8", "replace")
                if not name:
                    continue
                # The event does not carry the directory, only the watched descriptor's
                # name. Watch descriptors map back through the path we armed, so we keep
                # a lookup from descriptor to directory.
                directory = self._watched.get(_watch) if hasattr(self, "_watched") else None
                if directory is None:
                    continue
                path = os.path.join(directory, name)
                if mask & IN_ISDIR:
                    if mask & IN_MOVED_FROM:
                        # A watched directory left the tree; its descriptor still points
                        # at the inode and would report the old path forever.
                        self._coverage_lost = True
                    if mask & (IN_CREATE | IN_MOVED_TO):
                        # A new directory: arm it *and* scan what landed inside before
                        # the watch existed. Without the scan, `mkdir x; write x/f`
                        # inside one block reports the directory and misses the file.
                        additions.append(path)
                    continue
                if mask & (IN_DELETE | IN_MOVED_FROM):
                    self._record(path, "delete")
                else:
                    self._record(path, "modify")
        for path in additions:
            self._adopt_directory(path)

    def _adopt_directory(self, path):
        # A directory created during a block is armed and scanned, unless it is one we
        # ignore - an `npm install` must not pull thousands of watches into the tree.
        if os.path.basename(path) in _IGNORED_DIRS:
            return
        self._arm_tree(path)
        with self._lock:
            for current, _dirs, files in os.walk(path):
                for name in files:
                    self._record(os.path.join(current, name), "modify")

    def _record(self, path, op):
        if len(self._changes) >= self.max_changes:
            self._overflow = True
            return
        previous = self._changes.get(path)
        if previous == "delete" and op == "modify":
            # Re-created within the same block: it exists, so it is a modify.
            self._changes[path] = "modify"
        else:
            self._changes[path] = op

    # -- snapshot ----------------------------------------------------------

    def _scan_tuples(self):
        """Report files whose metadata moved since the last checkpoint.

        inotify cannot see a persistent mmap write, and a missed event is
        indistinguishable from silence. One stat tuple per file costs ~6 ms for a few
        hundred files (~0.1-0.3 s at 100k) and turns those writes into reported
        changes. It is the correctness floor under the event stream, not a
        replacement for it: the host still decides by content.
        """
        index = {}
        reported = 0
        for root in list(self.roots):
            for base, dirs, files in os.walk(root):
                dirs[:] = [name for name in dirs if name not in _IGNORED_DIRS]
                for name in files:
                    path = os.path.join(base, name)
                    try:
                        info = os.lstat(path)
                    except OSError:
                        continue
                    if not stat.S_ISREG(info.st_mode):
                        continue
                    key = (info.st_mtime_ns, info.st_size, info.st_ino, info.st_ctime_ns)
                    index[path] = key
                    # The first scan only seeds the index: the tree predates the watch.
                    if self._stat_index and self._stat_index.get(path) != key:
                        self._record(path, "modify")
                        reported += 1
        for path in self._stat_index:
            if path not in index:
                self._record(path, "delete")
                reported += 1
        self._stat_index = index
        return reported

    def snapshot(self):
        """Return and clear the accumulated changes.

        The first call after start returns ``baseline: True`` with an empty set:
        everything already in the tree arrived before the watch existed, and the
        host materialised it, so reporting it would be noise.
        """
        rearm = False
        with self._lock:
            now = time.monotonic()
            # Always scan on the first call (seed), after a lost baseline/coverage, and
            # otherwise at most once per interval. Skipping never drops a change: the
            # index is untouched, so the next scan reports everything accumulated.
            due = (self._last_scan == 0.0 or self._baseline_pending or self._coverage_lost
                   or (now - self._last_scan) >= self._scan_interval)
            if due:
                started = time.monotonic()
                scanned = self._scan_tuples()
                elapsed = time.monotonic() - started
                self._scan_interval = max(_SCAN_MIN_INTERVAL_S, elapsed * _SCAN_BUDGET_FACTOR)
                self._last_scan = now
                skipped = 0
                self._scans_skipped = 0
            else:
                scanned = 0
                self._scans_skipped += 1
                skipped = self._scans_skipped
            roots = []
            for root in self.roots:
                try:
                    roots.append({
                        "path": root,
                        "ino_now": os.stat(root).st_ino,
                        "ino_armed": self._root_ino.get(root),
                    })
                except OSError as exc:
                    roots.append({"path": root, "error": str(exc)})
            stale = self._coverage_lost
            payload = {
                "changes": [{"path": path, "op": op} for path, op in self._changes.items()],
                "overflow": self._overflow,
                # A lost watch cannot report its own gaps, so it is equivalent to a
                # baseline: the host must take a full capture for this block.
                "baseline": self._baseline_pending or stale,
                "coverage": not stale,
                "watched": self._armed,
                "events": self._events_seen,
                "stat_scan": scanned,
                "scan_skipped": skipped,
                "roots": roots,
            }
            self._changes = {}
            self._overflow = False
            self._baseline_pending = False
            if stale:
                self._coverage_lost = False
                # Paths may have moved underneath us; drop the tuple index so the next
                # checkpoint reseeds instead of reporting a storm of false deletes.
                self._stat_index = {}
                rearm = True
        if rearm:
            self._rearm_all()
        return payload

    def _rearm_all(self):
        """Rebuild the watch tree after coverage was lost.

        Descriptors left over from the old tree stay in the kernel but are inert once
        ``_watched`` no longer maps them, so re-arming from the roots restores correct
        paths without having to tear the old watches down one by one.
        """
        with self._lock:
            self._watched = {}
            self._armed = 0
            self._root_ino = {}
        for root in list(self.roots):
            self._arm_tree(root)


_shared = None
_shared_lock = threading.Lock()


def shared_watcher(roots):
    """One watcher per process.

    The TCP handler is instantiated per connection, so a per-handler watcher
    re-arms and re-baselines on every reconnect -- which silently pins the host to
    the slow path, because every call then reports ``baseline``. Keeping the watcher
    at module scope means the baseline is consumed once and subsequent calls report
    real deltas.
    """
    global _shared
    with _shared_lock:
        if _shared is not None:
            _shared.add_roots(roots)
            return _shared
        if _shared is None:
            candidate = ChangeWatcher(roots)
            if candidate.start():
                _shared = candidate
        return _shared
