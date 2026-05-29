#!/usr/bin/env bash
#
# dynamic_mount_poc.sh — prove the behaviour that matters most:
#   1. a persistent Python interpreter runs INSIDE an unprivileged bwrap jail,
#   2. it holds in-memory state (x = 1),
#   3. a TRUSTED, OUTSIDE process (this script, standing in for Java) dynamically
#      bind-mounts a NEW folder into the LIVE jail via `nsenter` — no restart,
#   4. the interpreter's next command can read the new folder,
#   5. and its state (x) is STILL there afterwards.
#
# This is the "lazy mount-on-access keeps working without killing the process"
# claim, demonstrated concretely. It needs unprivileged user namespaces enabled
# (flip the container seccomp profile — see sandbox_probe.sh) and `bwrap`
# installed. If those aren't present it prints what's missing and exits 0.
#
#   bash scripts/dynamic_mount_poc.sh
#
set -u
PY="$(command -v python3 || command -v python || true)"
ok(){ printf '  \033[32m[PASS]\033[0m %s\n' "$*"; }
no(){ printf '  \033[31m[FAIL]\033[0m %s\n' "$*"; }
inf(){ printf '  %s\n' "$*"; }

echo "=== Prerequisite check ==="
missing=0
if ! command -v bwrap >/dev/null 2>&1; then no "bwrap not installed (apt-get install -y bubblewrap, or add to image)"; missing=1; else ok "bwrap present: $(bwrap --version)"; fi
if ! command -v nsenter >/dev/null 2>&1; then no "nsenter not installed (util-linux)"; missing=1; else ok "nsenter present"; fi
if ! unshare --user --map-root-user --mount --pid true 2>/dev/null; then
  no "cannot create user namespace unprivileged — flip the seccomp profile first (see sandbox_probe.sh §1)"; missing=1
else ok "unprivileged user namespace creatable"; fi
[ -z "$PY" ] && { no "no python found"; missing=1; }
if [ "$missing" -ne 0 ]; then
  echo; echo "Prerequisites not met in THIS environment. Enable them (seccomp + bwrap) and re-run."
  exit 0
fi

# --------------------------------------------------------------------------
WORK="$(mktemp -d)"; CTL="$WORK/ctl"
# STAGING = the user's authorized backing area, mounted read-only into the jail
# at a hidden path at launch. "Loading" a project later = binding it from here to
# its working path. Everything under STAGING is already authorized for THIS user,
# so exposing it to this user's own interpreter is not a cross-user leak.
STAGING="$WORK/staging"; DATAB="$STAGING/projectB"
mkdir -p "$CTL" "$DATAB"
mkfifo "$CTL/cmd.fifo"
: > "$CTL/out.log"
echo "csv_from_project_B" > "$DATAB/myCsv.csv"   # the file we'll reveal at a working path
OUT="$CTL/out.log"

cleanup(){ [ -n "${BWRAP_PID:-}" ] && kill "$BWRAP_PID" 2>/dev/null; rm -rf "$WORK" 2>/dev/null; }
trap cleanup EXIT

# The persistent interpreter. Holds x=1, serves commands from a FIFO, writes
# results to a log. It NEVER mounts anything itself (that's the trusted side).
cat > "$CTL/interp.py" <<'PYEOF'
import os, sys
x = 1                                   # <-- the in-memory state we must preserve
OUT = "/ctl/out.log"
def emit(s):
    with open(OUT, "a") as f: f.write(s + "\n"); f.flush()
emit("READY x=%d" % x)
while True:
    with open("/ctl/cmd.fifo") as fifo:  # reopen after each writer closes (EOF)
        for line in fifo:
            line = line.strip()
            if not line: continue
            if line == "EXIT": emit("BYE"); sys.exit(0)
            if line == "STATE": emit("STATE x=%d" % x); continue
            if line.startswith("READ "):
                p = line[5:].strip()
                try:
                    with open(p) as fh: emit("READ_OK %s=%s" % (p, fh.read().strip()))
                except Exception as e: emit("READ_FAIL %s (%s)" % (p, type(e).__name__))
                continue
            emit("UNKNOWN %s" % line)
PYEOF

send(){ printf '%s\n' "$1" > "$CTL/cmd.fifo"; }
wait_for(){ for _ in $(seq 1 60); do grep -q "$1" "$OUT" && return 0; sleep 0.1; done; return 1; }

echo; echo "=== Launching persistent interpreter inside an unprivileged bwrap jail ==="
# /mnt/dyn is an EMPTY dir in the jail now; projectB is NOT visible yet.
bwrap \
  --unshare-user --unshare-pid --unshare-ipc --die-with-parent --new-session \
  --ro-bind /usr /usr \
  --ro-bind /etc /etc \
  --symlink usr/bin /bin \
  --symlink usr/lib /lib \
  --symlink usr/lib64 /lib64 \
  --symlink usr/sbin /sbin \
  --proc /proc --dev /dev \
  --tmpfs /tmp --tmpfs /mnt --dir /mnt/dyn \
  --ro-bind "$STAGING" /staging \
  --bind "$CTL" /ctl \
  -- "$PY" /ctl/interp.py &
BWRAP_PID=$!
wait_for "READY" || { no "interpreter never came up"; exit 1; }
inf "$(grep READY "$OUT" | tail -1)"

echo; echo "=== Step 1: confirm initial state ==="
send "STATE"; wait_for "STATE x=1" && ok "interpreter holds x=1" || no "no initial state"

echo; echo "=== Step 2: read NEW folder BEFORE it is mounted (should fail) ==="
send "READ /mnt/dyn/myCsv.csv"
wait_for "READ_FAIL /mnt/dyn/myCsv.csv" && ok "correctly cannot see projectB yet (not mounted)" \
  || no "unexpected: file was readable before mount"

echo; echo "=== Step 3: TRUSTED side dynamically bind-mounts projectB into the LIVE jail ==="
# Find the host-visible PID of the jailed python (it lives in a different mnt ns).
self_mnt="$(readlink /proc/self/ns/mnt)"; JAIL_PID=""
for p in $(pgrep -f '[i]nterp.py'); do
  m="$(readlink /proc/$p/ns/mnt 2>/dev/null)" || continue
  [ -n "$m" ] && [ "$m" != "$self_mnt" ] && JAIL_PID="$p" && break
done
[ -z "$JAIL_PID" ] && { no "could not locate jailed interpreter pid"; exit 1; }
inf "jailed interpreter host pid = $JAIL_PID"
# Enter its user+mount namespaces (gain CAP_SYS_ADMIN in-ns since we own the userns)
# and bind-mount the real projectB onto /mnt/dyn. This is what Java's helper does.
# Source is /staging/projectB (visible inside the jail); target is the working
# path /mnt/dyn. This is the live "load project on access" step Java would drive.
if nsenter -t "$JAIL_PID" -U -m -- mount --bind /staging/projectB /mnt/dyn 2>"$WORK/nsenter.err"; then
  ok "nsenter bind-mounted projectB to its working path in the running jail (no restart)"
else
  no "nsenter mount failed: $(tr '\n' ' ' < "$WORK/nsenter.err")"; exit 1
fi

echo; echo "=== Step 4: interpreter reads the newly-mounted file (next command) ==="
send "READ /mnt/dyn/myCsv.csv"
wait_for "READ_OK /mnt/dyn/myCsv.csv=csv_from_project_B" \
  && ok "interpreter now reads projectB/myCsv.csv  <-- dynamic add works" \
  || no "interpreter still cannot read the file"

echo; echo "=== Step 5: confirm state SURVIVED the dynamic mount ==="
send "STATE"
# (x=1 was already printed once; check it reports again after the mount)
if [ "$(grep -c 'STATE x=1' "$OUT")" -ge 2 ]; then ok "x is STILL 1 — process never restarted"; else no "state lost"; fi

send "EXIT"; wait_for "BYE" >/dev/null 2>&1
echo; echo "=== Done. Full interpreter log: ==="; sed 's/^/    /' "$OUT"
