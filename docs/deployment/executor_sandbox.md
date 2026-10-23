# OpenShell executor sandbox

How user code runs when SEMOSS is configured with `SANDBOX_MODE=OPENSHELL`, what the sandbox
can and cannot change on the host, and how write-back decides what to copy.

This is an opt-in execution path. With `SANDBOX_MODE` unset or set to any other value, the
platform uses the traditional local child process and none of this applies.

## Why it exists

`Py()` and shell commands are arbitrary code. Running them as a child of the JVM means a
process that shares the host's filesystem, environment and network namespace. The OpenShell
mode moves that process into a separate container, spawned and reaped with the user's
session, and keeps the platform's files authoritative on the host.

## Topology at a glance

```
Tomcat (host) ──CLI──► openshell gateway (host, :17670, mTLS)
   │                        │ provisions
   │                        ▼
   │              supervisor container (uid nobody, trusted side)
   │                        │ control.sock  (unix socket on a docker volume)
   │                        ▼
   │              workload container (network=none)
   │                PID 1  /.openshell/runtime/openshell-sandbox
   │                └── python worker  (loopback :7100)
   │                        ▲
   └── openshell forward service --target-port 7100 --local <port> ─┘
          (long-lived child of Tomcat; Java connects to 127.0.0.1:<port>)
```

Per session there are **two containers**: the workload (your image) and its supervisor (NVIDIA's image).
The supervisor is the trusted half of the boundary and is never inside the workload. The workload has no
network interface at all — its only egress is the channel to its supervisor — so a TCP connection to the
worker has to travel `Tomcat → forward → gateway → supervisor → runtime → loopback`.

## Process model

| Piece | Runs where | Notes |
| --- | --- | --- |
| Java (Tomcat) | host | owns the socket client, decides when to sync |
| `gaas_tcp_socket_server.py` | sandbox, **main process** | the persistent REPL; one per user session |
| shell commands | sandbox | started by `OpenShellShellRunner`, same write-back rules |
| user assets / projects / rooms | host | copied in at session start, written back at block boundaries |

The worker is stateful: variables survive between blocks, which is why it cannot be
restarted to pick up a change. It speaks the existing gaas protocol over a single TCP
connection; a second connection is accepted and then never served.

## Session lifecycle

- Created lazily on the first `Py()` or shell command; the first call pays the sandbox start
  and the upload of the session's roots.
- Deleted on logout, session timeout, and on disconnect, where `ClientProcessWrapper.shutdown`
  reconciles one last time before the sandbox disappears.
- Nothing removes a sandbox orphaned by a JVM crash yet. They carry the `OPENSHELL_LABEL`
  label and a `pyw-` name, so `openshell sandbox list` finds them for manual deletion.

## Write-back pipeline

Every `PYTHON` and `CMD` block ends with a sync. The steps, in order:

1. **Ask for the change set.** `NativePySocketClient` sends a `CHANGES` request on the
   existing socket. The worker answers
   `{changes[], overflow, baseline, coverage, watched, events, stat_scan}`.
2. **Fail closed on doubt.** If the answer is missing, `overflow` is true, `baseline` is true,
   or `coverage` is false, the host falls back to a full capture of each root instead of
   trusting a partial list.
   A change set the host drained but could not apply is owed as a full capture on the next
   block, because the worker has already forgotten those paths.
3. **Pull only what changed.** Changed paths are tarred inside the sandbox under one
   `sandbox exec` and streamed to a temporary file on the host, stopping at a 256 MB cap. The
   archive is refused if its listing fails or names an absolute path or a `..` segment.
4. **Reconcile.** Content is the authority: a differing sandbox file is always written. The
   host copy is quarantined first if it changed since this session last left it (uploaded or
   written back), and the write is skipped if that copy fails. Sandbox timestamps never
   decide anything.
5. **Deletes.** Only a path the sandbox is known to have held — seen in a full capture or
   written back since — is ever deleted on the host, so a file created and removed within one
   block cannot take an unrelated host file with it. Deletes are quarantine-then-delete,
   suppressed if the copy fails, and directories they empty are pruned up to (never including)
   the transported root.

Every host-side write and delete is contained: a path that leaves the root, or crosses a
symlink on the way, is refused. Without that, a link committed into a writable project could
redirect a write or a delete into another space.

Inside `.git`, only git's data is written back — `objects/` (not `objects/info/`), `refs/`,
`logs/`, `HEAD`, `index`, `packed-refs` and a few state files — so a commit made in the sandbox
reaches the host. `config`, `hooks/` and `info/` are never written back: host-side git runs or
obeys them, so a sandbox able to write them could execute commands on the server.

Quarantine lives at `$BASE/writeback-quarantine/<timestamp>/<root>/<relative>`, outside
`project/`, `room/` and `user/`, so it is never pushed to object storage.

A full capture carries survivors, not tombstones. A path the sandbox held before and the
capture no longer contains was deleted there; a host file the sandbox never held — added on the
host mid-session, or never uploaded — is left alone. If `tar` reports anything it could not
read (a directory the user made unreadable, say), the capture is partial: files are written
back and no deletion is inferred.

## Change detection

Two mechanisms, deliberately:

- **inotify** is the primary source. Watch descriptors are per directory, re-armed when a
  directory is created, and scanned on adoption so `mkdir x; write x/f` inside one block does
  not lose the file.
- **A stat-tuple scan** (`mtime_ns`, `size`, `inode`, `ctime`) is the correctness floor under
  it, because a persistent `mmap` write emits no event at all. It runs at most once per
  interval — `max(2 s, 20 × the last measured scan)` — so an idle block costs nothing at any
  tree size. Skipping never drops a change: the index is untouched, so the next scan reports
  the backlog.

Coverage loss is a first-class outcome. `IN_MOVED_FROM`, `IN_MOVE_SELF`, `IN_IGNORED` and
`IN_UNMOUNT` all mean the path map may be stale; so does a watch that could not be added
(`max_user_watches` exhausted, an unreadable directory), a directory the scan could not list,
or a watcher thread that has stopped. In each case the watcher reports `coverage: false`, drops
the tuple index and re-arms, and the host takes one full capture for that block. A scan that
could not list part of the tree never reports the unlisted files as deleted.

Roots are re-evaluated on every `CHANGES` request: a root that appears later in the session (a
room created mid-session, say) is armed and forces one baseline capture.

Excluded from watching and scanning: `node_modules`, `__pycache__`, `.venv`, `.git`.
`.git` is a large share of a typical project's directories; sandbox commits still reach the
host through the full capture. `node_modules` is one install away from exhausting
`fs.inotify.max_user_watches`. Override with `SEMOSS_SYNC_IGNORE`.

## Who may write back

The worker runs outside the platform's permission checks, so the host decides per root:

| Root | Rule |
| --- | --- |
| `project/<name>__<uuid>` | `SecurityProjectUtils.userCanEditProject` — the same check that grants the project read-write |
| `room/<roomId>` | `ModelInferenceLogsUtils.doCheckRoomExistsForUser` — `ROOM.USER_ID` in the model-inference-logs database |
| `user/Asset__<id>` | the id must equal one of the caller's `User.getAssetProjectId(provider)` values |
| anything else | refused |

Every refusal is logged with the path and the reason, and each check fails closed on error. A
read-only member can still write inside the sandbox — that is a scratch area — but the file
never reaches the host.

## Image

The sandbox image is built from this repository, not from a staged copy:

```bash
docker/build-executor-image.sh                          # slim, tag semoss-executor:dev
MODE=full TAG=semoss-executor:full docker/build-executor-image.sh
```

The script stages the committed `py/` into the build context, so the image carries exactly the
worker in this repository. The tag is labelled with `org.semoss.py-sha`, a hash of the python
tree, and the build fails unless the worker tree inside the image hashes to the repository's
and the worker's interpreter imports the change watcher.

Two variants:

| Mode | Contents | Use |
| --- | --- | --- |
| `slim` (default) | `python:3.12-slim`, git, `pandas`, `jsonpickle`, `yfinance` | quick iteration; user code can only import those |
| `full` (`MODE=full`) | the dependency set declared in `py/install_config/pyproject.toml`, installed with uv | parity with the platform runtime — a `Py()` block keeps its imports |

The full build keeps a venv at `/usr/lib/python/semossvenv` — the same path
`docker/Dockerfile.python` uses — and the worker is launched by absolute path, because
**both the environment and the filesystem inside a sandbox belong to the runtime**:

- **`PATH`**: a workload starts with `/usr/local/bin:/usr/bin:/bin`, so an image's `ENV PATH`
  is never applied and a bare `python3` resolves to the base interpreter. Symlinking
  `python3` at the venv does not help either — CPython looks for `pyvenv.cfg` relative to the
  invoked path and silently falls back to the base interpreter.
- **Filesystem**: the policy allowlists `/usr`, `/lib`, `/etc`, `/bin`, `/sbin`, `/proc`
  read-only, plus `/sandbox`, `/tmp`, `/opt/semosshome` read-write. A venv at `/opt/venv` is
  outside every one of those and dies with `PermissionError: pyvenv.cfg`; `/usr/lib/python`
  is inside `/usr`, which is exactly why upstream puts it there.

So `OpenShellTransport` resolves `/usr/lib/python/semossvenv/bin/python3` (falling back to
`command -v python3` for images without a venv) and creates the sandbox with
`--env PATH=/usr/lib/python/semossvenv/bin:…` so user code gets the same interpreter.
Smoke tests must reproduce both constraints, or they pass while the worker fails at boot.

At session start the launcher also uploads the host's `/opt/semosshome/py` over the image's
copy, but the worker's first imports happen before that upload, so the baked copy still has to
be current — a stale image shows up as code that ignores your latest change.

## Configuration

| Key | Meaning |
| --- | --- |
| `SANDBOX_MODE` | `OPENSHELL` enables this path (requires `CHROOT_ENABLE=true`, like every jail) |
| `OPENSHELL_IMAGE` | image a new sandbox starts from |
| `OPENSHELL_TEMPLATE` | workload template to create from instead of an image |
| `OPENSHELL_POLICY` | Landlock paths and per-binary egress rules; required |
| `OPENSHELL_CLI` | path to the `openshell` CLI (default `openshell`) |
| `OPENSHELL_CLI_HOME` | `HOME` for CLI children, where the gateway registration and mTLS bundle live |
| `OPENSHELL_LABEL` | label applied to every executor sandbox (default `app=semoss-executor`) |
| `OPENSHELL_WORKER_PORT` | worker port inside the sandbox (default `7100`) |
| `OPENSHELL_READY_TIMEOUT_SECONDS` | cold-start budget (default `180`) |
| `OPENSHELL_KEEP_SANDBOX` | `true` leaves sandboxes in place on teardown, for debugging |

## Verifying a deployment

- **The class actually deployed.** Unpack `prerna/om/ClientProcessWrapper.class` from the
  deployed `semoss-*-shaded-dependencies.jar` and check it with `javap -p` for
  `fetchBackChangedPaths`; a build log is not proof.
- **A write reaches the host.** Write a file from `Py()` in a loaded project and look for
  `Fetch-back wrote` in the server log and the file in the project folder.
- **The permission gate holds.** Give a test user read-only access to a project, write from
  `Py()`, and confirm the host file never appears and `Refusing write-back` is logged.

## Known limits

- More than ~2000 changed paths in one block sets `overflow` and forces a full capture.
- A *racing* `mmap` writer cannot be captured without cooperation (`msync`) or interception
  this sandbox does not have.
- There is no host→sandbox refresh while a sandbox lives. A `git pull` on the host during a
  session is not reflected inward, and the sandbox wins the next write-back for any file it
  touched (recoverable from quarantine). Branch switching belongs on the host, followed by a
  fresh session.
- Rooms and assets are gated as described above, but the platform has no room-sharing model
  in this build; if one is added, the room branch must resolve the room to its project.
