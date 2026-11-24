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
in the project's own `.smss`; see [executor_sandbox.md](executor_sandbox.md#app-secrets). They work in
every sandbox mode: OpenShell brokers them as placeholders, and the fakechroot and namespace jails
hand the code the values themselves. The shared sandbox registry uses the `REDIS_*` keys.

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
