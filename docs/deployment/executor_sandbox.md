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

- Created lazily on the first `Py()` or shell command (cold start ≈ 10–13 s).
- Deleted on logout, session timeout, and on disconnect, where `ClientProcessWrapper.shutdown`
  reconciles one last time before the sandbox disappears.
- A sandbox left behind after a crash is removed by `OpenShellSandboxReaper`, which only
  touches labelled `pyw-*` sandboxes past an age threshold.

## Write-back pipeline

Every `PYTHON` and `CMD` block ends with a sync. The steps, in order:

1. **Ask for the change set.** `NativePySocketClient` sends a `CHANGES` request on the
   existing socket. The worker answers
   `{changes[], overflow, baseline, coverage, watched, events, stat_scan}`.
2. **Fail closed on doubt.** If the answer is missing, `overflow` is true, `baseline` is true,
   or `coverage` is false, the host falls back to a full capture of each root instead of
   trusting a partial list.
3. **Pull only what changed.** Changed paths are tarred inside the sandbox under one
   `sandbox exec` and extracted to a temporary directory on the host.
4. **Reconcile.** Content is the authority: a differing sandbox file is written, and `mtime`
   only decides whether the host copy is preserved first. A host file newer than its sandbox
   counterpart is copied to quarantine before being replaced.
5. **Deletes.** A path reported as deleted is quarantine-then-delete, and the delete is
   suppressed if the quarantine copy fails. Directories emptied by those removals are pruned
   up to (never including) the transported root.

Quarantine lives at `$BASE/writeback-quarantine/<timestamp>/<root>/<relative>`, outside
`project/`, `room/` and `user/`, so it is never pushed to object storage.

A whole-root fallback carries survivors, not tombstones, so it diffs the host tree against the
capture and quarantines files the capture no longer contains. Directories the capture excludes
(`node_modules`, `__pycache__`, `.venv`) are skipped: “absent from the archive” there means
“never captured”, not “deleted”.

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
`IN_UNMOUNT` all mean the path map may be stale, so the watcher reports `coverage: false`,
drops the tuple index and re-arms; the host takes one full capture for that block and returns
to the fast path afterwards.

Excluded from watching, scanning and pulling: `node_modules`, `__pycache__`, `.venv`, `.git`.
`.git` is static inside the sandbox and dominated a real project (112 directories, 137 of 256
files); `node_modules` is one install away from exhausting `fs.inotify.max_user_watches`.
Override with `SEMOSS_SYNC_IGNORE`.

## Who may write back

The worker runs outside the platform's permission checks, so the host decides per root:

| Root | Rule |
| --- | --- |
| `project/<name>__<uuid>` | `SecurityProjectUtils.getActualUserProjectPermission` must be `OWNER` or `EDIT` |
| `room/<roomId>` | `ModelInferenceLogsUtils.doCheckRoomExistsForUser` — `ROOM.USER_ID` in the model-inference-logs database |
| `user/Asset__<id>` | the id must equal one of the caller's `User.getAssetProjectId(provider)` values |
| anything else | refused |

Every refusal is logged with the path and the reason, and each check fails closed on error. A
read-only member can still write inside the sandbox — that is a scratch area — but the file
never reaches the host.

## Image

The sandbox image is built from this repository, not from a staged copy:

```bash
docker/build-executor-image.sh                  # tag semoss-executor:dev
TAG=semoss-executor:0.1 docker/build-executor-image.sh
```

The build context is `py/`, so the image carries exactly the committed worker;
`docker/Dockerfile.executor` assembles it. The tag is labelled with `org.semoss.py-sha`, a hash
of the python tree, so the image can be traced back to its source. Contents: `python:3.12-slim`,
`git` with a system identity (local repository work only — egress is granted per binary by the
OpenShell policy), `pandas` and `jsonpickle` for the boot-time imports, the insight and grants
layout, and uid 1000 ownership of `/opt/semosshome`.

At session start the launcher also uploads the host's `/opt/semosshome/py` over the image's
copy, but the worker's first imports happen before that upload, so the baked copy still has to
be current — a stale image shows up as code that ignores your latest change.

## Configuration

| Key | Meaning |
| --- | --- |
| `SANDBOX_MODE` | `OPENSHELL` enables this path |
| `OPENSHELL_IMAGE` | image a new sandbox starts from |
| `OPENSHELL_POLICY` | Landlock paths and per-binary egress rules |
| `OPENSHELL_LABEL` | label used to identify sandboxes for the reaper |
| `OPENSHELL_WORKER_PORT` | worker port inside the sandbox |
| `OPENSHELL_READY_TIMEOUT_SECONDS` | cold-start budget |

## Verifying a deployment

```bash
# the class actually deployed (never trust the build log)
cd /tmp && unzip -o -q /opt/tomcat/webapps/Monolith/WEB-INF/lib/semoss-*-shaded-dependencies.jar \
  'prerna/om/ClientProcessWrapper.class' && javap -p prerna/om/ClientProcessWrapper.class | grep -E 'canWriteBack|quarantine'

# a file written by a block reached the host
journalctl -u tomcat-semoss | grep -i "Fetch-back wrote"

# the permission gate refuses a read-only member
sudo -u postgres psql -d semoss_security -c \
  "UPDATE projectpermission SET permission=3 WHERE projectid='<id>' AND userid='<user>';"
#   write from Py(), confirm the host file never appears and "Refusing write-back" is logged,
#   then restore permission=1
```

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
