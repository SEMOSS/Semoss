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
import struct
import threading

IN_MODIFY = 0x00000002
IN_CLOSE_WRITE = 0x00000008
IN_MOVED_FROM = 0x00000040
IN_MOVED_TO = 0x00000080
IN_CREATE = 0x00000100
IN_DELETE = 0x00000200
IN_DELETE_SELF = 0x00000400
IN_MOVE_SELF = 0x00000800
IN_Q_OVERFLOW = 0x00004000
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
        # inotify events carry a watch descriptor, not a path; this maps it back.
        self._watched = {}
        self._root_ino = {}

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
        for current, _dirs, _files in os.walk(root):
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

    def snapshot(self):
        """Return and clear the accumulated changes.

        The first call after start returns ``baseline: True`` with an empty set:
        everything already in the tree arrived before the watch existed, and the
        host materialised it, so reporting it would be noise.
        """
        with self._lock:
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
            payload = {
                "changes": [{"path": path, "op": op} for path, op in self._changes.items()],
                "overflow": self._overflow,
                "baseline": self._baseline_pending,
                "watched": self._armed,
                "events": self._events_seen,
                "roots": roots,
            }
            self._changes = {}
            self._overflow = False
            self._baseline_pending = False
            return payload


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
