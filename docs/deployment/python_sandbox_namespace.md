# Python Worker Sandbox: Namespace Isolation

> **Status:** Implementation complete - `SANDBOX_MODE=NAMESPACE` is production-ready.
> The `fakechroot` path is the current default and has a known escape vector.
> This document explains why, how the namespace sandbox closes it, and exactly
> what to change to enable it.

---

## Table of contents

1. [Background: how user Python execution works](#1-background)
2. [Why fakechroot is not real containment](#2-why-fakechroot-fails)
3. [The namespace sandbox: architecture](#3-namespace-sandbox-architecture)
4. [Enabling the namespace sandbox](#4-enabling)
5. [Kubernetes requirements](#5-kubernetes-requirements)
6. [Configuration reference](#6-configuration-reference)
7. [Key source files](#7-key-source-files)
8. [Remaining hardening notes](#8-remaining-hardening-notes)
9. [Verification / self-test](#9-verification)

---

## 1. Background

Each authenticated user gets a dedicated Python worker process. The lifecycle is:

```
User.startPythonSocketServerAndClient()
  +- ClientProcessWrapper.createProcessAndClient()
       +- [SANDBOX_MODE=NAMESPACE]  startTCPServerNativePySandbox()  -> sandbox_launcher.py
       +- [CHROOT_ENABLE=true]      startTCPServerNativePyChroot()   -> fakechroot (legacy)
       +- [neither]                 startTCPServerNativePy()         -> plain process
```

The worker runs `gaas_tcp_socket_server.py`, which accepts user-submitted Python
via a TCP/AF_UNIX socket and executes it through `gaas_tcp_server_handler.py`.

The handler applies a Python-level guard (`InsightGlobalStore.get_insight_globals`)
that blocks certain imports (`socket`, `subprocess`) and deletes certain `os`
attributes (`os.fork`, `os.system`, `os.popen`, the `spawn*` and `exec*`
families). These guards are bypassed at the OS level by the `fakechroot` escape
described below.

---

## 2. Why fakechroot fails

`startTCPServerNativePyChroot` constructs this command:

```
fakechroot fakeroot chroot --userspec=1001:1001 / env -i \
    PATH=<sandbox_path> python gaas_tcp_socket_server.py \
    --userChrootFolder <chrootDir> ...
```

**`fakechroot` is an `LD_PRELOAD` shim.** It intercepts glibc wrapper functions
(`open`, `stat`, `execve`, etc.) and prepends the fake root to their path
arguments. It is not a kernel primitive.

Three structural weaknesses make it bypassable without any privilege:

| Attack vector | Why it bypasses fakechroot |
|---|---|
| `os.posix_spawn(...)` | Calls `posix_spawn(3)` -> `clone(2)` + `execve(2)` through the vDSO, skipping the glibc preload hooks. The child sees the real root. Confirmed by the user report. |
| `ctypes.CDLL("libc.so.6").system(b"id")` | Direct libc call; never touches `forbidden_imports` or the preload layer. No `subprocess` or `os` import needed. |
| `importlib.import_module("subprocess")` | Calls `builtins.__import__` via a different code path that bypasses the `secure_import` shim installed by `InsightGlobalStore`. |

Additionally, the actual `chroot(2)` syscall requires `CAP_SYS_CHROOT`.
The pod runs as `user=default` (member of `root` but not root), which does
not hold that capability. The real chroot never occurs - `fakeroot` only
fakes ownership checks for packaging workflows. The process is simply running
with a modified `LD_PRELOAD` and no actual filesystem root change.

---

## 3. Namespace sandbox architecture

`py/sandbox_launcher.py` implements a true, kernel-enforced, unprivileged
isolation model. It forks into two persistent processes:

```
Java
 +- sandbox_launcher.py  (SUPERVISOR)
      +- real SEMOSS filesystem visible
      +- control socket: accepts INJECT/REMOVE/PING from Java
      +- fork --> gaas_tcp_socket_server.py  (INTERPRETER / worker)
                   +- pivot_root jail (host FS does not exist)
                   +- empty network namespace
                   +- zero capabilities
                   +- seccomp filter
```

### Layer 1 - Unprivileged user namespace (`CLONE_NEWUSER`)

Maps the calling uid/gid to uid 0 *inside* the namespace. This grants the
process enough capability within the namespace to manipulate mounts, without
holding any host privilege. `unshare(CLONE_NEWUSER)` does not require root.

### Layer 2 - Private mount namespace + `pivot_root`

```python
libc.unshare(CLONE_NEWNS)           # interpreter gets its own mount view
# bind ro: /usr /bin /sbin /lib /lib64 /lib32 /libx32 /etc
# tmpfs on /tmp (512 MB, writable scratch)
# minimal /dev (tmpfs + null/zero/urandom/random/full/tty)
_pivot_root(jail, jail + "/oldroot")
_umount2("/oldroot", MNT_DETACH)   # host tree detached and gone
```

After `pivot_root` the host filesystem **does not exist** inside the
interpreter. `os.posix_spawn("/bin/bash", ...)` still works (bash is in the
ro bind of `/bin`) but bash starts inside the same empty jail - there is no
host to escape to, no `/etc/passwd`, no `/root`, no other users' project
directories.

### Layer 3 - Empty network namespace (`CLONE_NEWNET`)

No routes, no interfaces except loopback (and even loopback has no listener).
Data exfiltration via TCP/UDP is structurally impossible regardless of what
Python code does.

The worker communicates with Java over an **AF_UNIX domain socket** (`--uds-path`)
that was opened *before* `pivot_root`. AF_UNIX is filesystem-based and
survives the pivot.

### Layer 4 - Capability drop + `no_new_privs`

```python
for cap in range(0, 64):
    libc.prctl(PR_CAPBSET_DROP, cap, 0, 0, 0)  # clear bounding set
libc.prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)    # suid gains nothing
libc.syscall(SYS_capset, hdr, all_zero_data)   # zero eff/perm/inh
```

After this, `execve` of a setuid binary gains nothing. `capset` to restore
capabilities fails. The state is irreversible for the lifetime of the process.

### Layer 5 - Seccomp default-allow blocklist (classic BPF)

A BPF program is installed via `prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER)`.
Blocked syscalls return `EPERM`. Notably:

```
mount, umount2, unshare, setns, pivot_root, chroot
ptrace, process_vm_readv, process_vm_writev
bpf, perf_event_open
add_key, request_key, keyctl
init_module, finit_module, delete_module
kexec_load, kexec_file_load
name_to_handle_at, open_by_handle_at
open_tree, move_mount, fsopen, fsconfig, fsmount, fspick, mount_setattr
```

`socket()` is additionally restricted by argument: only `AF_UNIX` (domain 1) is
permitted. `AF_INET` / `AF_INET6` calls return `EACCES`.

Seccomp filters are inherited across `fork`/`clone`/`execve`. A child process
spawned by `os.posix_spawn` or `subprocess.Popen` inherits all five layers.

### Layer 6 - Supervisor/interpreter inject protocol

The supervisor process holds the real SEMOSS mount view and listens on a second
AF_UNIX control socket. Java sends commands via `SandboxInjector`:

| Command | Effect |
|---|---|
| `INJECT\t<rw\|ro>\t<abs_path>` | Bind-mounts the path into the jail; propagates live into the running interpreter via shared->slave mount propagation |
| `REMOVE\t<abs_path>` | Lazy-unmounts the bind; the path disappears from the interpreter immediately |
| `PING` | Health check; returns `PONG` |

This replaces the `SymlinkHelper` symlink approach for the namespace mode.
Project/asset/insight access grants take effect without restarting the
interpreter.

---

## 4. Enabling

### Configuration change (one line)

In your `diHelper.properties` / RDF map:

```properties
# Already present - keeps SymlinkHelper construction
CHROOT_ENABLE=true

# Add this - switches from fakechroot to namespace sandbox
SANDBOX_MODE=NAMESPACE

# Directory root for per-user io-dirs, jail-dirs, control-dirs
# Default when unset: <java.io.tmpdir>/semoss-sandbox
# Recommend a dedicated tmpfs mount on the pod
SANDBOX_IO_DIR=/run/semoss-sandbox
```

### What changes at runtime

With `SANDBOX_MODE=NAMESPACE`, `SymlinkHelper.isInjectMode()` returns `true`.
`ClientProcessWrapper.createProcessAndClient` takes the inject branch:

```java
if (this.chrootSymlinkHelper != null && this.chrootSymlinkHelper.isInjectMode()) {
    Object[] ret = ClientProcessWrapper.startTCPServerNativePySandbox(...);
    // process, prefix, udsPath, controlSocketPath, ioDir, jailDir, controlDir
    this.udsPath = (String) ret[2];
    SandboxInjector injector = new SandboxInjector(this.controlSocketPath);
    injector.awaitReady(SOCKET_CLIENT_READY_WAIT_TIMEOUT_MS);
    this.chrootSymlinkHelper.setInjector(injector);
}
```

The socket client connects via `connectUds(this.udsPath)` instead of TCP.
`fakechroot` / `fakeroot` / `chroot` binaries are never invoked.

### What does NOT change

- The Java<->Python wire protocol (same JSON framing, same operations)
- `InsightGlobalStore` and its Python-level guards (still apply; they are
  redundant defense-in-depth, not the security boundary)
- Engine-owned processes - they take the non-chroot path (`chrootSymlinkHelper == null`)
- The `handle_shell` CMD operation - works inside the jail (see section 8)

---

## 5. Kubernetes requirements

### Minimum pod `securityContext`

```yaml
securityContext:
  allowPrivilegeEscalation: false
  runAsNonRoot: true
  seccompProfile:
    type: RuntimeDefault      # must allow unshare(2) and clone(2)
```

`RuntimeDefault` (containerd/runc) permits `unshare` and `clone`. If your
cluster enforces a hardened profile that blocks those syscalls, sandbox setup
will fail with `EPERM` and log it clearly - it does not silently fall through.

### Kernel sysctl

Unprivileged user namespaces must be enabled on the node:

```bash
# verify from inside a pod
kubectl exec <pod> -- sysctl kernel.unprivileged_userns_clone
# or on newer kernels:
kubectl exec <pod> -- sysctl user.max_user_namespaces
```

GKE COS and Ubuntu node pools enable this by default since ~2021. If the value
is `0`, set it via a privileged init container or a node pool flag
(`--sysctls kernel.unprivileged_userns_clone=1`).

### Volume for sandbox working directories

The launcher creates three directories per user session under `SANDBOX_IO_DIR`:
`<prefix>_<hash>/` (io-dir), `<prefix>_<hash>_j/` (jail-dir),
`<prefix>_<hash>_c/` (control-dir). These are cleaned up on
`ClientProcessWrapper.shutdown(true)`.

A `tmpfs` emptyDir is recommended to keep these off durable storage:

```yaml
volumes:
  - name: sandbox-tmp
    emptyDir:
      medium: Memory
      sizeLimit: 4Gi
volumeMounts:
  - name: sandbox-tmp
    mountPath: /run/semoss-sandbox
```

### gVisor

`sandbox_launcher.py` explicitly handles `runtimeClassName: gvisor`. gVisor
implements user namespaces, mount namespaces, and `pivot_root` in its own
kernel emulation. The outer container seccomp profile is irrelevant under
gVisor. The launcher's self-test (`--self-test`) passes on gVisor.

### What you do NOT need

- `privileged: true`
- `CAP_SYS_ADMIN` on the pod (the user namespace grants it *within the namespace*)
- `CAP_SYS_CHROOT` (`pivot_root` in a user namespace does not require it)
- `newuidmap` / `newgidmap` helpers (the launcher writes `/proc/self/uid_map` directly)

---

## 6. Configuration reference

| Property | File | Default | Purpose |
|---|---|---|---|
| `CHROOT_ENABLE` | `diHelper.properties` | `false` | Required `true` to construct `SymlinkHelper` and enter sandbox paths |
| `SANDBOX_MODE` | `diHelper.properties` | _(unset = fakechroot)_ | `NAMESPACE` -> namespace sandbox; `NSJAIL` also accepted as alias |
| `SANDBOX_IO_DIR` | `diHelper.properties` | `<java.io.tmpdir>/semoss-sandbox` | Root directory for per-user jail/io/control dirs |
| `SANDBOX_PATH` | `diHelper.properties` | _(unset)_ | Colon-separated extra paths bind-mounted read-only into the jail (e.g. a custom Python venv outside `/usr`) |
| `CHROOT_DIR` | `diHelper.properties` | _(required when `CHROOT_ENABLE=true`)_ | Parent directory for per-user chroot folders; still used to name the `SymlinkHelper` path in namespace mode |
| `ULIMIT_PROCESS` | `diHelper.properties` | _(unset)_ | Virtual memory ceiling in KB passed to `ulimit -v` before launch (non-Windows only) |
| `NAMESPACE_SHARE_INSIGHT_CACHE` | `diHelper.properties` | `true` | `false` stops granting the whole insight cache (other users' and engines' scratch folders) read-write; only the worker's folder, opened insights and the CSV import folder (read-only) are visible |
| `NAMESPACE_PID_NAMESPACE` | `diHelper.properties` | `false` | Passes `--pid-ns`: the worker is pid 1 of its own PID namespace and cannot signal the JVM or other users' workers |
| `NAMESPACE_BASE_RO_PATHS` | `diHelper.properties` | _(unset = built-in `/usr /bin /sbin /lib* /etc`)_ | Passes `--no-default-ro` and one `--extra-ro` per path: only these are bound read-only |
| `NAMESPACE_REQUIRE_RO` | `diHelper.properties` | `false` | Passes `--require-ro`: a bind that cannot be made read-only (such as `py/`) fails the launch instead of logging a warning |
| `NAMESPACE_MASK_BINARIES` | `diHelper.properties` | `false` | `true` passes `--mask-bins`: `/usr/bin`, `/usr/sbin`, `/usr/local/{bin,sbin}`, `/bin`, `/sbin` show only the allowlisted names |
| `NAMESPACE_BINARY_ALLOWLIST` | `diHelper.properties` | `env,sh,ldconfig` | Names kept by `--mask-bins` (`--allow-bin`); replaces the default |
| `NAMESPACE_MINIMAL_ETC` | `diHelper.properties` | `false` | `true` passes `--minimal-etc`: `/etc` shows only the allowlisted entries |
| `NAMESPACE_ETC_ALLOWLIST` | `diHelper.properties` | loader, user/group, DNS, TLS files | Entries kept by `--minimal-etc` (`--etc-allow`); replaces the default |

The `NAMESPACE_*` rows above are opt-in and leave the jail unchanged when unset. See
[sandbox_configuration.md](sandbox_configuration.md#base-mounts-and-read-only-enforcement) and
[the filesystem narrowing section](sandbox_configuration.md#narrowing-the-jails-filesystem) for what
they do and do not protect against.

---

## 7. Key source files

| File | Role |
|---|---|
| `src/prerna/auth/User.java` | `startPythonSocketServerAndClient()` - per-user process lifecycle, chroot/sandbox branching |
| `src/prerna/om/ClientProcessWrapper.java` | Process launch (`startTCPServerNativePySandbox`, `startTCPServerNativePyChroot`, `startTCPServerNativePy`), socket client setup, cleanup |
| `src/prerna/util/SymlinkHelper.java` | `isInjectMode()`, `enqueueOrInject()`, `symlinkProject()` - project/asset grant routing |
| `src/prerna/util/SandboxInjector.java` | Java client for the supervisor control socket (`INJECT`, `REMOVE`, `PING`) |
| `py/sandbox_launcher.py` | Supervisor + interpreter fork, user/mount/net namespace setup, `pivot_root`, capability drop, seccomp filter, inject/remove control loop |
| `py/gaas_tcp_socket_server.py` | Python TCP server, `--userChrootFolder` / `--uds-path` args, legacy `os.chroot()` path |
| `py/gaas_tcp_server_handler.py` | `InsightGlobalStore.get_insight_globals()` - Python-level `forbidden_imports` / `forbidden_attributes` guards (defense-in-depth, not the security boundary in namespace mode) |

---

## 8. Remaining hardening notes

These are not blockers for enabling `SANDBOX_MODE=NAMESPACE` but are worth
tracking:

### `handle_shell` / CMD operation uses `shell=True`

`TCPServerHandler.exec_cd`, `exec_dir`, `exec_cp`, `exec_generic` all call
`subprocess.Popen(..., shell=True)` with user-controlled payload tokens. Inside
the namespace jail this is structurally contained (the subprocess inherits the
jail, empty netns, and seccomp filter), but `git` can execute arbitrary hooks
and `shell=True` with user-controlled input is fragile. If the CMD operation is
not a core user-facing requirement, consider gating it behind a separate feature
flag or removing it entirely.

### Seccomp blocklist does not cover all new-mount syscalls

The filter blocks `mount`, `umount2`, `unshare`, `setns`, and the `fsopen`
family, but does not filter `clone` by flags. A child process cannot escape the
jail via `clone(CLONE_NEWUSER)` because:
1. It already holds zero capabilities and `no_new_privs=1`.
2. The host filesystem is gone - there is nowhere to pivot to.

However, adding argument-filtered BPF rules for `clone` to reject
`CLONE_NEWUSER | CLONE_NEWNS` flags would add defense-in-depth with low risk.
The existing BPF builder can be extended with an `arg1` check analogous to the
`socket` AF check already there.

### Syscalls worth adding to the blocklist

None of the following are needed by data science workloads:

- `userfaultfd` - timing primitive used in historical kernel exploits
- `io_uring_setup`, `io_uring_enter`, `io_uring_register` - broad kernel attack surface
- `memfd_create` - anonymous in-memory payloads; not needed by numpy/pandas/torch

### Python-level guards are defense-in-depth, not the boundary

Inside `InsightGlobalStore.get_insight_globals`, `forbidden_imports` blocks
`socket` and `subprocess`, and `forbidden_attributes` deletes `os.fork`,
`os.system`, etc. These remain useful as noise reduction (they surface intent
before the syscall layer) but they are not the security boundary in namespace
mode. Document this explicitly so future reviewers do not confuse them for
the primary control.

---

## 9. Verification

### Self-test (CI gate)

`sandbox_launcher.py` ships a built-in end-to-end self-test. Run it as an init
container or job on the target node pool:

```bash
python py/sandbox_launcher.py --self-test
```

It builds the full jail, hardens, and asserts:
- Interpreter holds in-memory REPL state across injections
- Outbound `AF_INET` socket refused (empty netns + seccomp)
- `AF_UNIX` socket still works (data channel viable)
- `unshare()` blocked by seccomp with `EPERM`
- Other users' files not visible before injection
- Host paths outside the allowlist invisible
- Live injection makes an authorized project appear without restart
- Mid-session grant (`INJECT` while REPL is running) works
- State (variable `x=1`) survives every injection
- `numpy`/`pandas` import without `SIGSYS` under seccomp

Exit code 0 = all pass. Exit code 1 = one or more failures (printed inline).

Recommended as a Kubernetes readiness probe or init container to catch
misconfigured node pools (e.g. `unprivileged_userns_clone=0`, overly strict
seccomp profile) before serving traffic.

### Manual smoke test after enabling

```python
# Should run inside the jail with no host escape
import os
print(os.listdir("/"))          # only: usr bin sbin lib lib64 etc tmp dev (+ SEMOSS py dirs)
print(os.path.exists("/root"))  # False
print(os.path.exists("/home"))  # False

import socket
socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # OSError: [Errno 13] Permission denied

import ctypes
ctypes.CDLL("libc.so.6").unshare(0x00020000)       # returns -1, errno=1 (EPERM)
```
