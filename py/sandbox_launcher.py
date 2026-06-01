#!/usr/bin/env python3
"""
sandbox_launcher.py -- optional, unprivileged OS-level sandbox for the SEMOSS
Python worker, built from Linux user + mount (+ net) namespaces. This is an
additive launch path (SANDBOX_MODE=NSJAIL); the existing fakechroot path is
unchanged and remains the default. It needs only *unprivileged* user namespaces
(works on GKE Autopilot under runtimeClassName: gvisor).

----------------------------------------------------------------------------
The two processes
----------------------------------------------------------------------------
This script forks into two long-lived processes:

  1. SUPERVISOR (the original process, stays as Java's child)
     - Keeps the real SEMOSS filesystem visible in its own private mount view.
     - Owns the live "inject-root(s)" (see below) and bind-mounts authorized
       project / asset / insight folders into them on demand.
     - Listens on a Unix-domain control socket for INJECT / REMOVE / PING
       commands that Java sends as the user is granted access to things.

  2. INTERPRETER (the forked child = the actual gaas Python worker)
     - Gets its own mount namespace, pivot_root()s into a minimal throwaway
       root that contains ONLY /usr,/bin,/lib,... + a private /tmp + the SEMOSS
       py/ code -- the rest of the host filesystem simply does not exist here.
     - unshare(NEWNET) -> empty network namespace -> no route off the box.
     - Drops every Linux capability and installs a seccomp filter, then execs
       gaas_tcp_socket_server.py. From this point the worker cannot escalate,
       cannot reach the network, and cannot see another user's files.

----------------------------------------------------------------------------
What is an "inject-root"? (this is the only non-obvious part)
----------------------------------------------------------------------------
The worker is long-lived and holds Python REPL state, but the set of projects
a user may touch grows *during* the session (lazy load + mid-session grants).
We therefore need a way to make a folder appear inside the already-running,
already-locked-down interpreter -- without restarting it.

Linux shared mount propagation does exactly this. An "inject-root" is a
directory that we mark as a SHARED mount in the supervisor and as a SLAVE
mount in the interpreter. Any bind-mount the supervisor later creates *under*
that directory automatically propagates across the namespace boundary and
shows up, live, inside the interpreter -- while everything NOT injected stays
invisible. Removing the bind makes it disappear again.

Why is the inject-root the SEMOSS home (/opt/semosshome)?
Because every path the worker might be granted -- project app-roots, the user's
asset folder, per-insight scratch folders -- lives somewhere under the SEMOSS
home. Making that one directory the inject-root lets us bind ANY of them in at
their real path on demand. (An earlier version used a narrower root and missed
asset / insight paths -- hence "ERR path not under an inject-root".) Note: we do
NOT bind the SEMOSS home itself -- only the individual authorized subfolders,
so users stay isolated from each other.

Run modes:
  --self-test   build the jail, harden, and assert the security properties
                end-to-end with no gaas server (the CI gate)
  (default)     build the jail, exec the gaas worker, and serve the
                INJECT/REMOVE/PING control protocol on --control-socket
"""
import argparse
import ctypes
import os
import platform
import shutil
import socket
import struct
import sys
import tempfile

libc = ctypes.CDLL("libc.so.6", use_errno=True)

# ---------------------------------------------------------------------------
# namespace / mount constants
# ---------------------------------------------------------------------------
CLONE_NEWNS = 0x00020000
CLONE_NEWUSER = 0x10000000
CLONE_NEWNET = 0x40000000

MS_RDONLY = 1
MS_BIND = 0x1000
MS_REMOUNT = 0x20
MS_REC = 0x4000
MS_PRIVATE = 1 << 18
MS_SLAVE = 1 << 19
MS_SHARED = 1 << 20

MNT_DETACH = 2

# ---------------------------------------------------------------------------
# per-architecture syscall numbers (libc symbols are arch-independent, but
# pivot_root / capset / the seccomp blocklist must be looked up by number).
# ---------------------------------------------------------------------------
_MACHINE = platform.machine()
if _MACHINE in ("x86_64", "amd64"):
    AUDIT_ARCH = 0xC000003E
    SYS_pivot_root = 155
    SYS_capset = 126
    _BLOCK = {
        "mount": 165, "umount2": 166, "unshare": 272, "setns": 308,
        "pivot_root": 155, "chroot": 161, "ptrace": 101,
        "process_vm_readv": 310, "process_vm_writev": 311,
        "bpf": 321, "perf_event_open": 298,
        "add_key": 248, "request_key": 249, "keyctl": 250,
        "init_module": 175, "finit_module": 313, "delete_module": 176,
        "kexec_load": 246, "kexec_file_load": 320,
        "name_to_handle_at": 303, "open_by_handle_at": 304,
        "open_tree": 428, "move_mount": 429, "fsopen": 430, "fsconfig": 431,
        "fsmount": 432, "fspick": 433, "mount_setattr": 442,
    }
    SYS_socket = 41
    _IS_X86_64 = True
elif _MACHINE in ("aarch64", "arm64"):
    AUDIT_ARCH = 0xC00000B7
    SYS_pivot_root = 41
    SYS_capset = 91
    _BLOCK = {
        "mount": 40, "umount2": 39, "unshare": 97, "setns": 268,
        "pivot_root": 41, "chroot": 51, "ptrace": 117,
        "process_vm_readv": 270, "process_vm_writev": 271,
        "bpf": 280, "perf_event_open": 241,
        "add_key": 217, "request_key": 218, "keyctl": 219,
        "init_module": 105, "finit_module": 273, "delete_module": 106,
        "kexec_load": 104, "kexec_file_load": 294,
        "name_to_handle_at": 264, "open_by_handle_at": 265,
        "open_tree": 428, "move_mount": 429, "fsopen": 430, "fsconfig": 431,
        "fsmount": 432, "fspick": 433, "mount_setattr": 442,
    }
    SYS_socket = 198
    _IS_X86_64 = False
else:
    AUDIT_ARCH = None  # seccomp disabled on unknown arch (logged loudly)
    SYS_pivot_root = SYS_capset = SYS_socket = None
    _BLOCK = {}
    _IS_X86_64 = False


# ---------------------------------------------------------------------------
# thin syscall wrappers
# ---------------------------------------------------------------------------
def _chk(rc, what):
    if rc != 0:
        e = ctypes.get_errno()
        raise OSError(e, "%s: %s" % (what, os.strerror(e)))


def _mount(src, tgt, fs, flags, data=None):
    return libc.mount(
        src.encode() if src else None,
        tgt.encode(),
        fs.encode() if fs else None,
        ctypes.c_ulong(flags),
        data.encode() if data else None,
    )


def _umount2(tgt, flags):
    return libc.umount2(tgt.encode(), flags)


def _pivot_root(new, old):
    return libc.syscall(SYS_pivot_root, new.encode(), old.encode())


# ---------------------------------------------------------------------------
# user namespace bootstrap (best-effort uid/gid map -- see plan gotchas:
# when caps are already held under gVisor, /proc/self/setgroups is rejected and
# unnecessary, so all of these writes are best-effort).
# ---------------------------------------------------------------------------
def enter_userns():
    uid, gid = os.getuid(), os.getgid()
    if libc.unshare(CLONE_NEWUSER) != 0:
        e = ctypes.get_errno()
        raise OSError(e, "unshare(CLONE_NEWUSER): " + os.strerror(e))
    try:
        with open("/proc/self/setgroups", "w") as f:
            f.write("deny")
    except OSError:
        pass
    for mapfile, val in (("/proc/self/gid_map", "0 %d 1" % gid),
                         ("/proc/self/uid_map", "0 %d 1" % uid)):
        try:
            with open(mapfile, "w") as f:
                f.write(val)
        except OSError as e:
            sys.stderr.write("note: could not write %s (%s) -- continuing "
                             "(already mapped / privileged)\n" % (mapfile, e))


# ---------------------------------------------------------------------------
# minimal jail root
# ---------------------------------------------------------------------------
# system dirs the interpreter needs. NOT /opt -- that holds the SEMOSS home and
# every user's chroot/project data; individual authorized folders are exposed
# only via the inject-roots, never the whole tree.
DEFAULT_RO_PATHS = [
    "/usr", "/bin", "/sbin", "/lib", "/lib64", "/lib32", "/libx32", "/etc",
]
DEV_NODES = ["null", "zero", "full", "random", "urandom", "tty"]


def _bind(src, tgt, ro):
    os.makedirs(tgt, exist_ok=True)
    _chk(_mount(src, tgt, "", MS_BIND | MS_REC), "bind %s" % src)
    if ro:
        # best-effort recursive read-only remount; failure is non-fatal (the
        # interpreter is unprivileged on the host either way).
        _mount("none", tgt, "", MS_BIND | MS_REMOUNT | MS_REC | MS_RDONLY | MS_BIND)


def build_jail(jail, ro_paths, rw_paths, inject_roots):
    """Construct the interpreter's minimal root under `jail` (runs in the
    supervisor, before the fork).

    `inject_roots` are absolute directories that the supervisor will inject
    projects *into* after the interpreter is running; each is created and made
    a SHARED mount so those later binds PROPAGATE across the namespace boundary
    (see the module docstring). Everything not bound here -- the rest of the
    host filesystem, other users' projects, the un-injected contents of the
    inject-roots -- is invisible inside the jail."""
    os.makedirs(jail, exist_ok=True)
    # new root must itself be a mount point for pivot_root
    _chk(_mount(jail, jail, "", MS_BIND), "bind jail->self")

    # Mark the inject-roots SHARED first, so any ro/rw bind that lands under one
    # (e.g. the py-folder and insight-folder live under the SEMOSS home) is
    # inside the shared subtree and later injects there propagate into the
    # interpreter.
    for root in inject_roots:
        tgt = jail + root
        os.makedirs(tgt, exist_ok=True)
        _chk(_mount(tgt, tgt, "", MS_BIND), "bind inject-root->self %s" % root)
        _chk(_mount("none", tgt, "", MS_SHARED), "make-shared inject-root %s" % root)

    for p in ro_paths:
        if os.path.exists(p):
            _bind(p, jail + p, ro=True)
    for p in rw_paths:
        if p and os.path.exists(p):
            _bind(p, jail + p, ro=False)

    # tmpfs /tmp (writable scratch)
    tmp = os.path.join(jail, "tmp")
    os.makedirs(tmp, exist_ok=True)
    _chk(_mount("tmpfs", tmp, "tmpfs", 0, "size=512m,mode=1777"), "tmpfs /tmp")

    # minimal /dev
    dev = os.path.join(jail, "dev")
    os.makedirs(dev, exist_ok=True)
    _chk(_mount("tmpfs", dev, "tmpfs", 0, "mode=755,size=16m"), "tmpfs /dev")
    for node in DEV_NODES:
        host = "/dev/" + node
        if os.path.exists(host):
            tgt = os.path.join(dev, node)
            open(tgt, "w").close()
            if _mount(host, tgt, "", MS_BIND) != 0:
                os.remove(tgt)
    shm = os.path.join(dev, "shm")
    os.makedirs(shm, exist_ok=True)
    _mount("tmpfs", shm, "tmpfs", 0, "mode=1777,size=64m")

    os.makedirs(os.path.join(jail, "oldroot"), exist_ok=True)


def enter_jail(jail, inject_roots, unshare_net):
    """Run in the interpreter (child): own mount ns, mark inject-roots SLAVE
    (so supervisor injects flow IN but our changes never flow back OUT), pivot
    into the minimal root, detach the old root, optionally drop the network."""
    _chk(libc.unshare(CLONE_NEWNS), "child unshare(CLONE_NEWNS)")
    for root in inject_roots:
        _chk(_mount("none", jail + root, "", MS_SLAVE),
             "child make-slave inject-root %s" % root)
    if unshare_net:
        # empty network namespace -> no route off the box.  AF_UNIX still works
        # (it is filesystem-based), which is why the broker<->worker channel
        # must be a Unix domain socket (plan 6d).
        _chk(libc.unshare(CLONE_NEWNET), "child unshare(CLONE_NEWNET)")
    _chk(_pivot_root(jail, os.path.join(jail, "oldroot")), "pivot_root")
    os.chdir("/")
    _chk(_umount2("/oldroot", MNT_DETACH), "detach oldroot")
    try:
        os.rmdir("/oldroot")
    except OSError:
        pass


# ---------------------------------------------------------------------------
# capability drop + seccomp (applied in the interpreter, after the privileged
# setup is done, before any user code runs).
# ---------------------------------------------------------------------------
PR_CAPBSET_DROP = 24
PR_SET_NO_NEW_PRIVS = 38
PR_SET_SECCOMP = 22
PR_CAP_AMBIENT = 47
PR_CAP_AMBIENT_CLEAR_ALL = 4
SECCOMP_MODE_FILTER = 2
_LINUX_CAPABILITY_VERSION_3 = 0x20080522


class _CapHeader(ctypes.Structure):
    _fields_ = [("version", ctypes.c_uint32), ("pid", ctypes.c_int)]


class _CapData(ctypes.Structure):
    _fields_ = [("effective", ctypes.c_uint32),
                ("permitted", ctypes.c_uint32),
                ("inheritable", ctypes.c_uint32)]


def drop_capabilities():
    # clear the bounding set (needs CAP_SETPCAP, which we still hold) so caps
    # cannot be regained across an execve.
    for cap in range(0, 64):
        libc.prctl(PR_CAPBSET_DROP, cap, 0, 0, 0)
    libc.prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)
    if SYS_capset is not None:
        hdr = _CapHeader(_LINUX_CAPABILITY_VERSION_3, 0)
        data = (_CapData * 2)()  # all zero -> drop effective/permitted/inherit
        libc.syscall(SYS_capset, ctypes.byref(hdr), ctypes.byref(data))
    libc.prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0)


# classic BPF building blocks for the seccomp filter
_BPF_LD = 0x00
_BPF_W = 0x00
_BPF_ABS = 0x20
_BPF_JMP = 0x05
_BPF_JEQ = 0x10
_BPF_JGE = 0x30
_BPF_K = 0x00
_BPF_RET = 0x06

SECCOMP_RET_ALLOW = 0x7FFF0000
SECCOMP_RET_KILL_PROCESS = 0x80000000
SECCOMP_RET_ERRNO = 0x00050000
EPERM = 1
EACCES = 13
AF_UNIX = 1

# seccomp_data field offsets
_OFF_NR = 0
_OFF_ARCH = 4
_OFF_ARG0 = 16


class _SockFilter(ctypes.Structure):
    _fields_ = [("code", ctypes.c_uint16), ("jt", ctypes.c_uint8),
                ("jf", ctypes.c_uint8), ("k", ctypes.c_uint32)]


class _SockFprog(ctypes.Structure):
    _fields_ = [("len", ctypes.c_uint16),
                ("filter", ctypes.POINTER(_SockFilter))]


def _stmt(code, k):
    return _SockFilter(code, 0, 0, k)


def _jump(code, k, jt, jf):
    return _SockFilter(code, jt, jf, k)


def _build_seccomp_filter():
    """Default-ALLOW filter that denies a fixed blocklist of escape/privilege
    syscalls with EPERM, and restricts socket() to AF_UNIX (defence in depth on
    top of the empty netns)."""
    prog = []
    # 1. pin the architecture (prevents x32 / i386 number-aliasing bypass)
    prog.append(_stmt(_BPF_LD | _BPF_W | _BPF_ABS, _OFF_ARCH))
    prog.append(_jump(_BPF_JMP | _BPF_JEQ | _BPF_K, AUDIT_ARCH, 1, 0))
    prog.append(_stmt(_BPF_RET | _BPF_K, SECCOMP_RET_KILL_PROCESS))
    # 2. load the syscall number
    prog.append(_stmt(_BPF_LD | _BPF_W | _BPF_ABS, _OFF_NR))
    if _IS_X86_64:
        # kill x32 syscalls (nr | 0x40000000)
        prog.append(_jump(_BPF_JMP | _BPF_JGE | _BPF_K, 0x40000000, 0, 1))
        prog.append(_stmt(_BPF_RET | _BPF_K, SECCOMP_RET_KILL_PROCESS))
    # 3. blocklist -> EPERM
    for nr in sorted(set(_BLOCK.values())):
        prog.append(_jump(_BPF_JMP | _BPF_JEQ | _BPF_K, nr, 0, 1))
        prog.append(_stmt(_BPF_RET | _BPF_K, SECCOMP_RET_ERRNO | EPERM))
    # 4. socket(): allow AF_UNIX only, else EACCES
    if SYS_socket is not None:
        prog.append(_jump(_BPF_JMP | _BPF_JEQ | _BPF_K, SYS_socket, 0, 3))
        prog.append(_stmt(_BPF_LD | _BPF_W | _BPF_ABS, _OFF_ARG0))
        prog.append(_jump(_BPF_JMP | _BPF_JEQ | _BPF_K, AF_UNIX, 1, 0))
        prog.append(_stmt(_BPF_RET | _BPF_K, SECCOMP_RET_ERRNO | EACCES))
    # 5. default allow
    prog.append(_stmt(_BPF_RET | _BPF_K, SECCOMP_RET_ALLOW))
    return prog


def install_seccomp():
    if AUDIT_ARCH is None:
        sys.stderr.write("WARNING: unknown arch %r -- seccomp filter NOT "
                         "installed\n" % _MACHINE)
        return
    prog = _build_seccomp_filter()
    arr = (_SockFilter * len(prog))(*prog)
    fprog = _SockFprog(len(prog), arr)
    # no_new_privs already set in drop_capabilities(); required so an
    # unprivileged process may install a filter.
    rc = libc.prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER,
                    ctypes.cast(ctypes.byref(fprog), ctypes.c_void_p), 0, 0)
    if rc != 0:
        e = ctypes.get_errno()
        raise OSError(e, "prctl(PR_SET_SECCOMP): " + os.strerror(e))


def harden(unshare_net_already_done=True):
    """Final, irreversible lockdown of the interpreter.  Call after pivot_root
    and (optional) net unshare, immediately before running user code / exec."""
    drop_capabilities()
    install_seccomp()


# ---------------------------------------------------------------------------
# supervisor inject/remove primitives (run in the supervisor, which still holds
# CAP_SYS_ADMIN-in-userns and can see the backing store).
# ---------------------------------------------------------------------------
def inject(jail, src, dst, mode="rw"):
    """Bind `src` to `jail + dst` so it propagates into the running interpreter
    (where it appears at `dst`). `dst` must live under a shared inject-root.
    Idempotent (mirrors today's Files.exists symlink guard)."""
    tgt = jail + dst
    if os.path.ismount(tgt):
        return  # already injected
    if not os.path.isdir(src):
        raise OSError(2, "no such source to inject: %s" % src)
    os.makedirs(tgt, exist_ok=True)
    _chk(_mount(src, tgt, "", MS_BIND | MS_REC), "inject %s" % dst)
    if mode == "ro":
        _mount("none", tgt, "", MS_BIND | MS_REMOUNT | MS_REC | MS_RDONLY)


def remove(jail, dst):
    tgt = jail + dst
    if os.path.ismount(tgt):
        _chk(_umount2(tgt, MNT_DETACH), "remove %s" % dst)


# ===========================================================================
# SELF-TEST -- the EKS/CI gate.  Builds the full jail, hardens, and asserts the
# security properties end-to-end without needing the gaas server.
# ===========================================================================
def _self_test():
    print("=== sandbox_launcher self-test (%s) ===" % _MACHINE)
    try:
        enter_userns()
    except OSError as e:
        print("PREREQ NOT MET: cannot create user namespace unprivileged (%s)." % e)
        print("Flip the container seccomp profile / use runtimeClassName: gvisor")
        print("(see docker/SANDBOX_USERNS_TESTING.md). Exiting 0.")
        sys.exit(0)

    _chk(libc.unshare(CLONE_NEWNS), "supervisor unshare(CLONE_NEWNS)")
    _chk(_mount("none", "/", "", MS_REC | MS_PRIVATE), "make-rprivate /")

    work = tempfile.mkdtemp(prefix="sbx-selftest-")
    backing = os.path.join(work, "backing")
    for proj, content in [("projectB", "B_DATA"), ("projectC", "C_DATA"),
                          ("otherUser_secret", "SHOULD_NOT_LEAK")]:
        os.makedirs(os.path.join(backing, proj))
        with open(os.path.join(backing, proj, "data.csv"), "w") as f:
            f.write(content)

    jail = os.path.join(work, "jail")
    inject_roots = ["/projects"]
    build_jail(jail, DEFAULT_RO_PATHS, rw_paths=[], inject_roots=inject_roots)

    c2p_r, c2p_w = os.pipe()
    p2c_r, p2c_w = os.pipe()
    pid = os.fork()

    # ---------------- interpreter (child) --------------------------------
    if pid == 0:
        os.close(c2p_r)
        os.close(p2c_w)
        enter_jail(jail, inject_roots, unshare_net=True)
        harden()
        x = 1  # in-memory REPL state; must survive every injection

        def emit(s):
            os.write(c2p_w, (s + "\n").encode())

        emit("READY x=%d" % x)
        buf = b""
        while True:
            ch = os.read(p2c_r, 1)
            if not ch:
                break
            if ch != b"\n":
                buf += ch
                continue
            line = buf.decode()
            buf = b""
            if line == "EXIT":
                emit("BYE")
                os._exit(0)
            elif line == "STATE":
                emit("STATE x=%d" % x)
            elif line == "LIST":
                try:
                    emit("LIST %s" % ",".join(sorted(os.listdir("/projects"))))
                except Exception as ex:
                    emit("LIST_ERR %s" % type(ex).__name__)
            elif line.startswith("READ "):
                p = line[5:]
                try:
                    # sanitise: file contents must never carry newlines into the
                    # line-delimited test protocol (e.g. multi-line /etc files).
                    body = open(p).read().strip().replace("\n", " ").replace("\r", " ")
                    emit("READ_OK %s=%s" % (p, body[:200]))
                except Exception as ex:
                    emit("READ_FAIL %s (%s)" % (p, type(ex).__name__))
            elif line == "EGRESS":
                # AF_INET socket must be refused by seccomp (EACCES); even if it
                # were not, the empty netns has no route.
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.close()
                    emit("EGRESS_OPEN")  # bad
                except OSError as ex:
                    emit("EGRESS_BLOCKED %s" % ex.errno)
            elif line == "UNIX_OK":
                try:
                    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    s.close()
                    emit("UNIX_OK")
                except OSError as ex:
                    emit("UNIX_FAIL %s" % ex.errno)
            elif line == "BLOCKED_SYSCALL":
                # a blocked privileged syscall must fail with EPERM, not succeed
                rc = libc.unshare(CLONE_NEWNS)
                emit("UNSHARE rc=%d errno=%d" % (rc, ctypes.get_errno()))
            elif line == "NUMPY":
                try:
                    import numpy  # noqa: F401
                    import pandas  # noqa: F401
                    emit("NUMPY_OK")
                except ImportError:
                    emit("NUMPY_ABSENT")
                except Exception as ex:
                    emit("NUMPY_ERR %s" % type(ex).__name__)
            else:
                emit("UNKNOWN %s" % line)
        os._exit(0)

    # ---------------- supervisor (parent, = "Java") ----------------------
    os.close(c2p_w)
    os.close(p2c_r)
    _acc = b""

    def send(cmd):
        os.write(p2c_w, (cmd + "\n").encode())

    def recv():
        nonlocal _acc
        while b"\n" not in _acc:
            chunk = os.read(c2p_r, 4096)
            if not chunk:
                break
            _acc += chunk
        line, _, _acc = _acc.partition(b"\n")
        return line.decode()

    results = []

    def P(ok, msg):
        results.append(ok)
        print(("  [PASS] " if ok else "  [FAIL] ") + msg)

    print("  " + recv())  # READY x=1
    send("STATE")
    P(recv() == "STATE x=1", "interpreter holds in-memory state x=1")

    print("--- hardening: egress, socket policy, syscall blocklist ---")
    send("EGRESS")
    P(recv().startswith("EGRESS_BLOCKED"), "outbound AF_INET socket refused")
    send("UNIX_OK")
    P(recv() == "UNIX_OK", "AF_UNIX socket still works (data channel viable)")
    send("BLOCKED_SYSCALL")
    r = recv()
    P(r.startswith("UNSHARE rc=-1") and "errno=1" in r,
      "unshare() blocked by seccomp (EPERM) -- cannot escape the jail")

    print("--- isolation: backing store & other users are invisible ---")
    send("READ /projects/projectB/data.csv")
    P(recv().startswith("READ_FAIL"), "projectB not visible before injection")
    send("READ %s/otherUser_secret/data.csv" % backing)
    P(recv().startswith("READ_FAIL"), "cannot reach the backing store by path")
    # uid-independent: a host path that is NOT in the bind allowlist simply
    # does not exist inside the jail (host filesystem is hidden).
    send("READ /home/anyuser/.ssh/id_rsa")
    P(recv().startswith("READ_FAIL"), "host paths outside the allowlist are hidden")

    print("--- lazy load: inject an authorized project (live) ---")
    inject(jail, os.path.join(backing, "projectB"), "/projects/projectB", "rw")
    send("READ /projects/projectB/data.csv")
    P(recv() == "READ_OK /projects/projectB/data.csv=B_DATA",
      "interpreter reads projectB live after injection")

    print("--- mid-session grant: inject a NEVER-staged project (live) ---")
    inject(jail, os.path.join(backing, "projectC"), "/projects/projectC", "rw")
    send("READ /projects/projectC/data.csv")
    P(recv() == "READ_OK /projects/projectC/data.csv=C_DATA",
      "interpreter reads newly-granted projectC without restart")

    send("READ /projects/otherUser_secret/data.csv")
    P(recv().startswith("READ_FAIL"), "un-injected project still invisible")
    send("LIST")
    print("  " + recv() + "  (only injected projects present)")

    print("--- scientific Python imports under seccomp ---")
    send("NUMPY")
    nr = recv()
    P(nr in ("NUMPY_OK", "NUMPY_ABSENT"),
      "numpy/pandas import without SIGSYS (%s)" % nr)

    print("--- state survived every injection ---")
    send("STATE")
    P(recv() == "STATE x=1", "x is STILL 1 -- interpreter never restarted")

    send("EXIT")
    recv()
    os.waitpid(pid, 0)
    shutil.rmtree(work, ignore_errors=True)

    ok = all(results)
    print("=== %s (%d/%d checks passed) ===" %
          ("ALL PASS" if ok else "FAILURES PRESENT", sum(results), len(results)))
    sys.exit(0 if ok else 1)


# ===========================================================================
# REAL MODE -- build the jail, exec the gaas worker, serve inject/remove on the
# control socket.  (Plumbing is in place; exercised once Java sends commands
# over the UDS channel -- plan steps 6c/6d.)
# ===========================================================================
def _serve_control(jail, inject_roots, control_sock, child_pid):
    """Supervisor loop: accept line commands from Java and (un)mount projects.
    Protocol (tab-separated, newline-terminated): one reply line per command.
        INJECT\t<rw|ro>\t<abs_path>   -> OK | ERR <msg>
        REMOVE\t<abs_path>            -> OK | ERR <msg>
        PING                          -> PONG
    `abs_path` is the real project/asset path; it is bound to the same path
    inside the jail (mirroring today's symlink) and must fall under a configured
    inject-root so the mount propagates to the interpreter."""
    if os.path.exists(control_sock):
        os.unlink(control_sock)
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(control_sock)
    srv.listen(8)
    srv.settimeout(1.0)
    try:
        while True:
            # exit when the interpreter is gone
            wpid, _ = os.waitpid(child_pid, os.WNOHANG)
            if wpid == child_pid:
                break
            try:
                conn, _ = srv.accept()
            except socket.timeout:
                continue
            with conn:
                data = b""
                while not data.endswith(b"\n"):
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                for raw in data.decode().splitlines():
                    conn.sendall((_handle_control(jail, inject_roots, raw) + "\n").encode())
    finally:
        srv.close()
        try:
            os.unlink(control_sock)
        except OSError:
            pass


def _under_inject_root(path, inject_roots):
    """True iff `path` is one of, or nested under, a configured inject-root.
    This is the guard that keeps Java from asking us to bind a path that would
    not actually propagate into the interpreter."""
    real = os.path.normpath(path)
    for d in inject_roots:
        d = os.path.normpath(d)
        if real == d or real.startswith(d + os.sep):
            return True
    return False


def _handle_control(jail, inject_roots, raw):
    parts = raw.split("\t")
    cmd = parts[0].strip().upper() if parts else ""
    try:
        if cmd == "PING":
            return "PONG"
        if cmd == "INJECT" and len(parts) >= 3:
            mode = "ro" if parts[1].strip().lower() == "ro" else "rw"
            abs_path = parts[2].strip()
            if not _under_inject_root(abs_path, inject_roots):
                return "ERR path not under an inject-root: %s" % abs_path
            inject(jail, abs_path, abs_path, mode)
            return "OK"
        if cmd == "REMOVE" and len(parts) >= 2:
            abs_path = parts[1].strip()
            if not _under_inject_root(abs_path, inject_roots):
                return "ERR path not under an inject-root: %s" % abs_path
            remove(jail, abs_path)
            return "OK"
        return "ERR bad command"
    except OSError as e:
        return "ERR %s" % e


def _real_mode(args):
    try:
        enter_userns()
    except OSError as e:
        sys.stderr.write("FATAL: cannot create user namespace (%s). "
                         "Sandbox unavailable.\n" % e)
        sys.exit(3)

    _chk(libc.unshare(CLONE_NEWNS), "supervisor unshare(CLONE_NEWNS)")
    _chk(_mount("none", "/", "", MS_REC | MS_PRIVATE), "make-rprivate /")

    # rw paths the worker needs that live OUTSIDE the backing store: the gaas
    # code (py_folder, ro), the per-insight scratch folder (rw), and the io dir
    # that carries the worker's data UDS so Java can reach it across namespaces.
    ro = list(DEFAULT_RO_PATHS)
    if args.py_folder:
        ro.append(os.path.abspath(args.py_folder))
    rw = []
    if args.insight_folder:
        rw.append(os.path.abspath(args.insight_folder))
    if args.io_dir:
        rw.append(os.path.abspath(args.io_dir))

    # absolute dirs under which projects/assets are injected live (marked shared
    # so mid-session grants propagate into the running interpreter -- normally
    # the SEMOSS home; see the module docstring)
    inject_roots = [p for p in args.inject_root if p]

    # if Java passes --jail-root it also owns cleanup; otherwise we make (and
    # later remove) a throwaway tmpdir ourselves.
    own_jail = not args.jail_root
    jail = args.jail_root or tempfile.mkdtemp(prefix="sbx-jail-")
    build_jail(jail, ro, rw, inject_roots)

    # pass-through args for the gaas server; argparse.REMAINDER keeps the "--"
    # separator, so drop a leading one.
    gaas_args = list(args.gaas)
    if gaas_args and gaas_args[0] == "--":
        gaas_args = gaas_args[1:]

    child_pid = os.fork()
    if child_pid == 0:
        enter_jail(jail, inject_roots, unshare_net=not args.no_net)
        harden()
        py = sys.executable
        cmd = [py, os.path.join(args.py_folder or "/usr/lib/semoss/py",
                                "gaas_tcp_socket_server.py")]
        cmd += gaas_args
        os.execv(py, cmd)
        os._exit(127)  # unreachable

    try:
        if args.control_socket:
            _serve_control(jail, inject_roots, args.control_socket, child_pid)
        else:
            os.waitpid(child_pid, 0)
    finally:
        # best-effort: on a graceful exit drop the jail mounts + skeleton we own.
        # (When Java kills us it owns --jail-root cleanup instead.)
        if own_jail:
            _umount2(jail, MNT_DETACH)
            shutil.rmtree(jail, ignore_errors=True)


# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="SEMOSS unprivileged Python sandbox")
    p.add_argument("--self-test", action="store_true",
                   help="build the jail, harden, and assert security properties")
    p.add_argument("--backing-root", default="",
                   help="per-user authorized backing store the supervisor may "
                        "inject projects from")
    p.add_argument("--jail-root", default="",
                   help="where to build the minimal root (default: a tmpdir)")
    p.add_argument("--control-socket", default="",
                   help="UDS the supervisor listens on for INJECT/REMOVE")
    p.add_argument("--inject-root", action="append", default=[],
                   help="absolute dir under which projects/assets are injected; "
                        "marked a shared mount so mid-session grants propagate "
                        "into the running interpreter (repeatable; normally the "
                        "SEMOSS home)")
    p.add_argument("--py-folder", default="",
                   help="SEMOSS py/ folder (gaas server code), bound read-only")
    p.add_argument("--insight-folder", default="",
                   help="per-insight scratch folder, bound read-write")
    p.add_argument("--io-dir", default="",
                   help="dir carrying the worker data UDS (bound read-write)")
    p.add_argument("--no-net", action="store_true",
                   help="do NOT unshare the network (debugging only)")
    p.add_argument("gaas", nargs=argparse.REMAINDER,
                   help="args passed through to gaas_tcp_socket_server.py")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.self_test:
        _self_test()
    else:
        _real_mode(args)
