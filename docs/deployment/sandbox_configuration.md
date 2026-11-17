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
| `PY_SERVER_USER` | none | OS user the unjailed python worker runs as (`sudo -u`). Ignored by the jails |

### fakechroot

| Key | Default | Meaning |
| --- | --- | --- |
| `CHROOT_SYMLINK_PATHS` | none | Comma-separated absolute host paths linked into every chroot |

### namespace

| Key | Default | Meaning |
| --- | --- | --- |
| `SANDBOX_IO_DIR` | `${java.io.tmpdir}/semoss-sandbox` | Folder for the jail, io and control sockets. Keep it short: AF_UNIX paths are limited to about 108 bytes |

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
| namespace python worker | **Inherited in full**, with `PATH` overridden |
| namespace node worker | **Inherited in full** |
| OpenShell worker | Only what OpenShell sets, plus `PATH`; project secrets arrive as placeholders |
| unjailed python, node, java workers, one-shot commands | Inherited in full |
| agent CLI | The `SEMOSS_SANDBOX_*` variables are added; the `LD_*` and `DYLD_*` removal list is defined but not yet applied |

`SEMOSS_SANDBOX_*` variables (`TARGET_CLI`, `POLICY_FILE`, `BACKEND`, `PROFILE_FILE`, `JAVA_BIN`,
`CLASSPATH`) are set by the server for the agent CLI launcher and are not meant to be set by hand.
`SEMOSS_SYNC_IGNORE` and `SEMOSS_HOME` are read by the OpenShell write-back watcher inside the
sandbox image.

## Known gaps

- The Java worker, the node worker and one-shot shell commands do not all follow `SANDBOX_MODE`
  (for example, one-shot commands use fakechroot in `NAMESPACE` mode).
- The namespace jail does not scrub the environment.
- `BashCommand` enabled by `AGENT_DEFAULT_TOOLS_ENABLE_BASH` alone runs on host bash.
