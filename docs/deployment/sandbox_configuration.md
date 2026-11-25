# Sandbox configuration

Every setting that decides how user code is sandboxed, in one place. The list is defined in code by
`prerna.util.sandbox.SandboxSettings`; the first time a jail is used the server logs the effective
values for the active mode (`Sandbox settings: ...`), marking each as configured or default.

## How settings reach the server

All keys below are `RDF_Map.prop` properties, read live. A container environment variable only
changes one when a script in `semoss-artifacts` (`artifacts/scripts/set*.sh`) writes it into
`RDF_Map.prop` with `sed`, and those scripts only replace a line that already exists in the
template. A key with no template line cannot be set from the environment.

| Keys | Container environment | Script |
| --- | --- | --- |
| `CHROOT_ENABLE`, `CHROOT_DIR`, `CHROOT_SYMLINK_PATHS`, `SANDBOX_MODE`, `SANDBOX_PATH`, `SANDBOX_IO_DIR` | same name (`SANDBOX_MODE` also accepts `CHROOT_SANDBOX_MODE`) | `setChroot.sh` |
| `ULIMIT_PROCESS` | same name | `setUlimit.sh` |
| `PY_SERVER_USER` | same name | `setPy.sh` |
| `AGENT_SANDBOX_*` | see script | `setSandbox.sh` |
| `OPENSHELL_*` | none yet: set them in `RDF_Map.prop` | - |

## Which sandbox you get

Two keys decide. `CHROOT_ENABLE` says whether user python is jailed at all; `SANDBOX_MODE` says how.

| `CHROOT_ENABLE` | `SANDBOX_MODE` | Result |
| --- | --- | --- |
| `false` (default) | any | No jail. User python runs as a child of Tomcat. |
| `true` | unset, `FAKECHROOT`, or any other value | fakechroot jail |
| `true` | `NAMESPACE` (legacy `NSJAIL`) | namespace jail, `py/sandbox_launcher.py` |
| `true` | `OPENSHELL` | one OpenShell sandbox per user, see [executor_sandbox.md](executor_sandbox.md) |

`SANDBOX_MODE=OFF` and `SANDBOX_MODE=SECCOMP_ONLY` are not modes. They behave as `FAKECHROOT`, and
the server logs a warning. Only `CHROOT_ENABLE=false` turns jailing off.

## Settings

"Applies to" is `common` (every jail), one mode, or `agent` (the Claude Code and Copilot CLI
sandbox, which is separate from the user-code jail).

### Common

| Key | Default | Meaning |
| --- | --- | --- |
| `CHROOT_ENABLE` | `false` | Master switch for jailing user python, the agent bash tool and project-reactor file access |
| `SANDBOX_MODE` | `FAKECHROOT` | `FAKECHROOT`, `NAMESPACE` (alias `NSJAIL`) or `OPENSHELL` |
| `CHROOT_DIR` | none | Folder holding each user's chroot folder and the shared template. Required when `CHROOT_ENABLE` is true |
| `SANDBOX_PATH` | none | Executable directories visible inside the jail, separated by the OS path separator |
| `ULIMIT_PROCESS` | none | Virtual memory cap in KB (`ulimit -v`) for workers. Not used by `OPENSHELL`; see `OPENSHELL_MEMORY` |
| `INSIGHT_CACHE_DIR` | none | Insight scratch folder, linked into every chroot |
| `SANDBOX_ENV_ALLOWLIST` | none | Names of server environment variables passed to scrubbed jail children, comma-separated. `PATH`, `LANG`, `LANGUAGE`, `LC_ALL` and `TZ` are always passed |
| `PY_SERVER_USER` | none | OS user the unjailed python worker runs as (`sudo -u`). Ignored by the jails |

### fakechroot

| Key | Default | Meaning |
| --- | --- | --- |
| `CHROOT_SYMLINK_PATHS` | none | Comma-separated absolute host paths linked into every chroot |

### namespace

| Key | Default | Meaning |
| --- | --- | --- |
| `SANDBOX_IO_DIR` | `${java.io.tmpdir}/semoss-sandbox` | Folder for the jail, io and control sockets. Keep it short: AF_UNIX paths are limited to about 108 bytes |
| `NAMESPACE_SCRUB_ENV` | `true` | `true` starts the python and node jails with only the kept environment variables; `false` passes the server's whole environment, secrets included |
| `NAMESPACE_EXEC_FALLBACK` | `fakechroot` | One-shot commands have no namespace jail: `fakechroot` runs them in the user's chroot, `refuse` fails them |
| `NAMESPACE_SHARE_INSIGHT_CACHE` | `true` | `true` gives every python jail the whole insight cache folder read-write. `false` grants only the worker's own folder, each insight's folder as it opens, and the CSV import folder read-only. See [Insight cache sharing](#insight-cache-sharing) |
| `NAMESPACE_PID_NAMESPACE` | `false` | `true` runs the python worker in its own PID namespace. See [Process isolation](#process-isolation) |
| `NAMESPACE_BASE_RO_PATHS` | none (built-in `/usr`, `/bin`, `/sbin`, `/lib*`, `/etc`) | Comma-separated absolute host paths, files or folders, bound read-only into the python jail instead of the built-in list. The python install must be covered. See [Base mounts and read-only enforcement](#base-mounts-and-read-only-enforcement) |
| `NAMESPACE_REQUIRE_RO` | `false` | `true` fails the jail's start, or a read-only grant, when a bind that should be read-only cannot be made so. `false` logs a warning in the worker console (`console.txt`) and carries on |
| `NAMESPACE_MASK_BINARIES` | `false` | `true` hides `/usr/bin`, `/usr/sbin`, `/usr/local/bin`, `/usr/local/sbin`, `/bin` and `/sbin` in the python jail except the allowlisted names. See [Narrowing the jail's filesystem](#narrowing-the-jails-filesystem) |
| `NAMESPACE_BINARY_ALLOWLIST` | `env,sh,ldconfig` | Comma-separated binary names kept by `NAMESPACE_MASK_BINARIES`. A configured value replaces the default |
| `NAMESPACE_MINIMAL_ETC` | `false` | `true` binds only the allowlisted entries of `/etc` into the python jail instead of all of it |
| `NAMESPACE_ETC_ALLOWLIST` | see below | Comma-separated entries of `/etc`, relative to it. A configured value replaces the default: `ld.so.cache,nsswitch.conf,passwd,group,hosts,host.conf,resolv.conf,gai.conf,services,protocols,localtime,timezone,ssl/certs,ssl/openssl.cnf` |

#### Insight cache sharing

When a user's chroot is initialised, the namespace jail is granted the shared `py/` folder and the
insight cache (`INSIGHT_CACHE_DIR`). The cache root holds every active worker's scratch folder
(`a<digits>`), every session's insight folders, engine-owned worker folders (`MODEL_*`) and the CSV
import folder. With the default `NAMESPACE_SHARE_INSIGHT_CACHE=true` it is mounted read-write into
every python jail, so any user can read and change another user's and an engine's scratch files. This
was confirmed on a running pod with two users: both listed the same folders, all writable.

`NAMESPACE_SHARE_INSIGHT_CACHE=false` closes that. A jail then gets only:

- its own worker folder, bound read-write by the launcher;
- each insight's folder as the insight opens (`Insight.getInsightFolder` through `grantFolder`);
- the CSV import folder (`CSV_INSIGHT_CACHE_FOLDER`), **read-only**, because `PandasFrame` loads files
  that URL and file sources download there with `read_csv('<path>')`.

`false` also stops linking the whole cache into the user's chroot. Fakechroot is not a jail
(see [Insight cache sharing under fakechroot](#insight-cache-sharing-under-fakechroot)), but the Java
worker, one-shot commands and project Java reactors all work through that chroot, and its link to the
cache root was an absolute symlink to the real folder, so they had the same cross-user access. A chroot
that an earlier start left with that link has it removed (only the link, never its target). Each granted
insight folder is still linked in individually, and the CSV import folder is linked too; a chroot link
cannot be read-only, so it is writable there.

Still shared with `false`: the CSV import folder is one folder for all users, so its files remain
readable by every jail (random names, but listable). Per-user subfolders would be the next step.
Before turning it off, try the flows that touch the cache root outside a granted insight folder:
federation and fuzzy matching, the SharePoint, Dropbox and Google file pulls, and any project Java
reactor or one-shot command that reads or writes there directly. Anything that fails needs a per-file or
per-folder grant. Platform reactors are unaffected, since they run in the server and not in a jail.

The `py/` folder is now always requested read-only in the namespace jail, whatever this setting says.
Before, Java asked for it read-write; it stayed read-only only because the launcher's own read-only bind
was already in place, which `inject()` leaves untouched.

##### Insight cache sharing under fakechroot

Fakechroot rewrites each absolute path a process opens to `<user chroot>/<path>`, so only what is
placed in the user's chroot exists for it. The cache was placed there as one absolute symlink to the
real folder, which the kernel follows to the real cache, so under fakechroot every user also saw every
other user's cache folders, writable by the shared uid. The path rewriting hid what was never linked
(`RDF_Map.prop`, `db/`, other users' projects), not the cache. With `SANDBOX_MODE=FAKECHROOT` the link
is still made, and `NAMESPACE_SHARE_INSIGHT_CACHE` has no effect.

#### Process isolation

By default the namespace jail shares the pod's PID namespace, and the worker runs as the same host
uid as the JVM. User code can therefore signal every other process of that uid: other users' workers
and the JVM itself. If the JVM is pid 1 and handles SIGTERM, that is enough to shut the pod down.
`kill(pid, 0)` over the pid range reports them (`sandbox_probes.py`, `probe_signals`).

`NAMESPACE_PID_NAMESPACE=true` starts the worker in a new PID namespace (launcher flag `--pid-ns`).
It then sees and can signal only its own processes. It does not hide anything else: `/proc` was
never mounted in the jail, and the network, loopback and filesystem views are unchanged.

What to expect:

- The worker is **pid 1** (`os.getpid()` returns 1). The kernel ignores default-action signals sent
  to a namespace's init from inside it, so user code cannot kill the worker with `os.kill`, and the
  `kill(pid, 0)` probe finds nothing else to signal.
- **Shutdown still works.** Java's SIGTERM to the worker is ignored for the same reason, so the
  launcher asks the kernel to SIGKILL the worker when the launcher exits (`PR_SET_PDEATHSIG`). Java
  stops the launcher right after the worker, which ends the worker and everything else in the
  namespace, including subprocesses user code left running (today those can outlive the worker).
- **Zombies are not reaped.** An orphaned child of user code is reparented to the worker, which does
  not wait on it. It lingers until the worker ends. If that matters, add a small init as pid 1.
- Host tools (`ps`, `kubectl exec ... ps`) still show host pids, and `ClientProcessWrapper` signals
  the launcher by host pid as before.
- If the platform cannot create a PID namespace (for example a sandboxed runtime without it), the
  launch fails with `unshare(CLONE_NEWPID)` in the worker console; unset the flag to go back.
- It applies to the python worker only, not the node worker.

Check it with `python py/sandbox_launcher.py --self-test --pid-ns`.

#### Base mounts and read-only enforcement

The shared `py/` folder is bound read-only, and so is every read-only project grant. The read-only
remount used to be best effort and failed silently: a mount inherited into a user namespace has its
`nosuid`, `nodev`, `noexec` and atime flags locked, and a remount that dropped one was refused. For
root-owned `/usr` that is harmless, but the worker runs as the uid that owns `py/`, so a failed remount
left the globally shared `py/` writable. The remount now repeats the mount's existing flags and checks the
result. A bind that is still writable is logged as `WARNING: ... could not be made read-only` in the
worker console; set `NAMESPACE_REQUIRE_RO=true` to refuse to start (or refuse the grant) instead.
`python py/sandbox_launcher.py --self-test` asserts that `py/` and a read-only grant reject writes.

`NAMESPACE_BASE_RO_PATHS` replaces the built-in system folders. Only what you list (plus `py/`, the
insight folder and `SANDBOX_PATH` folders outside the built-in list) exists in the jail. The launcher
refuses to start if the Python interpreter is not covered. A starting point for the Ubuntu image,
to be checked with the self-test and a real workload before use:

```
NAMESPACE_BASE_RO_PATHS=/lib64,/lib/x86_64-linux-gnu,/usr/lib/python,/usr/share/tesseract-ocr
NAMESPACE_MASK_BINARIES=true
NAMESPACE_BINARY_ALLOWLIST=env,sh,ldconfig,tesseract
NAMESPACE_MINIMAL_ETC=true
NAMESPACE_REQUIRE_RO=true
```

Combined with the binary mask this removes the executables still reachable by full path under
`/usr/lib`, `/usr/libexec` and `/usr/share`. Things to expect: `SANDBOX_PATH` folders under the
built-in list are no longer added; a tool needs its shared libraries and data folders listed (find them
with `ldd`); `/usr/share/zoneinfo` and similar data folders are gone unless listed.

#### Narrowing the jail's filesystem

By default the namespace jail binds `/usr`, `/bin`, `/sbin`, `/lib*` and `/etc` whole, read-only, so
user code can list every installed binary and read every file under `/etc`. The two flags above
replace those views with an empty read-only folder holding only what is allowlisted. They apply to the
python worker only (not the node worker) and are applied when a worker starts, so running workers keep
the view they have.

- **What it is:** reduced exposure and discoverability. A binary that is masked cannot be found
  by `ls`, `PATH` lookup or `/usr/bin/<name>`. It is not an execution control: anything else
  executable under the still-visible `/usr/lib`, `/usr/libexec` or `/usr/share` (for example
  `/usr/lib/git-core/git`) can still be run by its full path. The kernel-level controls (no
  capabilities, `no_new_privs`, seccomp, empty or host network namespace) are unchanged.
- **Network:** with `SANDBOX_ALLOW_NETWORK=true` the jail needs DNS and TLS files. The default `/etc`
  list keeps `resolv.conf`, `hosts`, `nsswitch.conf`, `ssl/certs` and `ssl/openssl.cnf` for that.
- **What breaks until allowlisted:** the worker's own CMD handlers (`exec_dir`, `exec_cp`,
  `exec_generic` in `gaas_tcp_server_handler.py`) run `ls`, `cp`, `git` and similar through
  `shell=True`. Tools users call by name, such as `tesseract`, and scripts whose shebang names
  an interpreter (`#!/usr/bin/python3`) need their binary listed. A tool that reads other
  `/etc` files (for example `alternatives`) needs them in `NAMESPACE_ETC_ALLOWLIST`.
- **Names that are symlinks on the host** (`sh` to `dash`) are bound as their target, so list
  `sh`, not `dash`.
- **Checking a config:** run the launcher's self-test with the same flags on the target node pool,
  for example `python py/sandbox_launcher.py --self-test --mask-bins --allow-bin env --allow-bin sh
  --minimal-etc`. Without the flags it checks the default jail as before.

### OpenShell

| Key | Default | Meaning |
| --- | --- | --- |
| `OPENSHELL_CLI` | `openshell` | Path to the CLI |
| `OPENSHELL_CLI_HOME` | inherited | `HOME` for CLI children; the gateway registration and mTLS bundle live under it |
| `OPENSHELL_TEMPLATE` | none | Workload template to create sandboxes from, instead of an image |
| `OPENSHELL_IMAGE` | `quay.io/semoss/semoss-sandbox:dev-3.14-cpu` | Image a new sandbox starts from; ignored when a template is set |
| `OPENSHELL_POLICY` | bundled `sandbox-policy.yaml` | Policy template to render instead of the bundled one |
| `OPENSHELL_PYTHON_BINARY` | `/usr/lib/python/cpython/bin/python3.14` | Interpreter path the policy's egress rules name |
| `OPENSHELL_EGRESS` | `pypi.org:443, files.pythonhosted.org:443` | Hosts sandboxed code may reach; `none` for no egress |
| `OPENSHELL_CPU` | `2` | CPU cap per sandbox (`2`, `500m`); `none` for no cap |
| `OPENSHELL_MEMORY` | `4Gi` | Memory cap per sandbox; `none` for no cap |
| `OPENSHELL_WORKER_PORT` | `7100` | Port the worker listens on inside the sandbox |
| `OPENSHELL_LABEL` | `app=semoss-executor` | Label applied to every executor sandbox |
| `OPENSHELL_READY_TIMEOUT_SECONDS` | `180` | Cold-start budget |
| `OPENSHELL_KEEP_SANDBOX` | `false` | `true` leaves sandboxes in place on teardown, for debugging. Also read as a `-D` JVM property |
| `OPENSHELL_IDLE_EXIT_MINUTES` | `180` | A user's worker exits after this long with no client; keep it above the session timeout |
| `OPENSHELL_WORKER_MAX_CLIENTS` | `16` | Connections one user's worker serves at once |

Per-project secrets are not server settings. They are `SANDBOX_SECRETS` and `SANDBOX_SECRET_<NAME>`
in the project's own `.smss`; see [executor_sandbox.md](executor_sandbox.md). The shared sandbox
registry uses the `REDIS_*` keys.

### Agent CLI sandbox

| Key | Default | Meaning |
| --- | --- | --- |
| `AGENT_SANDBOX_ENABLE` | `false` | `true` wraps the Claude Code and Copilot CLIs in Landlock (Linux) or `sandbox-exec` (macOS) |
| `AGENT_SANDBOX_DEFAULT_READS` | built-in OS list | Comma-separated paths the CLI may read |
| `AGENT_SANDBOX_LOOPBACK_NETWORK` | `true` | Allow the CLI to reach loopback |
| `AGENT_SANDBOX_BLOCK_READS` | none | Comma-separated paths blocked on top of the built-in block list |

## What each jail's child process gets in its environment

This is how it works today. Where the child inherits Tomcat's environment, any secret in it
(database passwords, cloud keys, `LOCAL_KEY`) is visible to code running there.

| Launch path | Child environment |
| --- | --- |
| fakechroot python worker | Scrubbed: `env -i` with only `PATH` from `SANDBOX_PATH`, and the worker clears the rest |
| fakechroot one-shot command | Scrubbed: only `HOME=/home/default` and `PATH` |
| namespace python worker | Scrubbed to the kept variables (`NAMESPACE_SCRUB_ENV`, default on), `PATH` from `SANDBOX_PATH` |
| namespace node worker | Scrubbed to the kept variables (`NAMESPACE_SCRUB_ENV`, default on) |
| OpenShell worker | Only what OpenShell sets, plus `PATH`; project secrets arrive as placeholders |
| unjailed python, node, java workers, one-shot commands | Inherited in full |
| agent CLI | The `SEMOSS_SANDBOX_*` variables are added; the `LD_*` and `DYLD_*` removal list is defined but not yet applied |

`SEMOSS_SANDBOX_*` variables (`TARGET_CLI`, `POLICY_FILE`, `BACKEND`, `PROFILE_FILE`, `JAVA_BIN`,
`CLASSPATH`) are set by the server for the agent CLI launcher and are not meant to be set by hand.
`SEMOSS_SYNC_IGNORE` and `SEMOSS_HOME` are read by the OpenShell write-back watcher inside the
sandbox image.

## For developers: ISandbox

Code that grants a folder, starts a worker or runs a command asks the user's sandbox and does not
check the mode: `user.getSandbox()` returns an `ISandbox` (`prerna.util.sandbox`). The mode is turned
into a backend in one place, `SandboxFactory`.

| | host | fakechroot | namespace | OpenShell |
| --- | --- | --- | --- | --- |
| Python worker | child of the server | fakechroot jail | namespace jail | OpenShell sandbox |
| Node worker | child of the server | child of the server | namespace jail | child of the server |
| Java worker | child of the server | fakechroot jail | fakechroot jail | fakechroot jail |
| One-shot command | host shell | fakechroot | fakechroot (or refused, see `NAMESPACE_EXEC_FALLBACK`) | `openshell sandbox exec` |
| Grant a folder or project | nothing to do | chroot symlink | live mount and symlink | upload |
| Revoke | no | yes | yes | no |

The rows where a backend runs a worker "the way it always has" (Node outside the namespace jail,
Java in every jail mode) are logged once at first use. A backend that was asked for never falls
back to a weaker one: an OpenShell sandbox that cannot be had fails the worker instead of starting
it on the server.

## Known gaps

- Node has no OpenShell or fakechroot jail, and the Java worker has no namespace or OpenShell jail.
- One-shot commands have no namespace jail.
- The agent CLI sandbox defines a list of environment variables to drop (`LD_PRELOAD`,
  `LD_LIBRARY_PATH`, `DYLD_*`) but the Python clients that launch the CLI do not apply it yet.
- `BashCommand` enabled by `AGENT_DEFAULT_TOOLS_ENABLE_BASH` alone runs on host bash.
- Shutdown and OpenShell write-back still live in `ClientProcessWrapper`, not in the sandbox.
