#!/usr/bin/env bash
#
# sandbox_probe.sh — determine which Python execution-sandbox mechanisms are
# available INSIDE a SEMOSS pod (non-root, no extra privileges).
#
# Run this in the actual runtime pod, as the same user SEMOSS runs as:
#     kubectl exec -it <semoss-pod> -- bash /path/to/sandbox_probe.sh
# or copy it in and run `bash sandbox_probe.sh`.
#
# It is READ-ONLY except for a couple of temp files under $TMPDIR, which it
# cleans up. It never needs root. Each check is independent; one failing does
# not abort the rest. A summary + recommendation is printed at the end.

set -u

PASS="PASS"; FAIL="FAIL"; WARN="WARN"
declare -A RESULT
note() { printf '  %s\n' "$*"; }
hdr()  { printf '\n=== %s ===\n' "$*"; }
record(){ RESULT["$1"]="$2"; printf '  [%s] %s\n' "$2" "$1"; }

TMP="$(mktemp -d 2>/dev/null || echo /tmp/sbxprobe.$$)"; mkdir -p "$TMP"
cleanup(){ rm -rf "$TMP" 2>/dev/null; }
trap cleanup EXIT

hdr "0. Environment"
note "whoami: $(id -un 2>/dev/null) ($(id 2>/dev/null))"
note "kernel: $(uname -r)"
note "no_new_privs (proc status): $(grep -i NoNewPrivs /proc/self/status 2>/dev/null || echo n/a)"
note "Seccomp (proc status): $(grep -i Seccomp /proc/self/status 2>/dev/null || echo n/a)"

# ---------------------------------------------------------------------------
hdr "1. Unprivileged user namespaces (THE LINCHPIN)"
# sysctls
uncl="$(cat /proc/sys/kernel/unprivileged_userns_clone 2>/dev/null || echo missing)"
maxns="$(cat /proc/sys/user/max_user_namespaces 2>/dev/null || echo missing)"
note "kernel.unprivileged_userns_clone = $uncl"
note "user.max_user_namespaces       = $maxns"
# functional test: the namespace set we actually need for the jail
if unshare --user --map-root-user --mount --pid --net --ipc --uts true 2>"$TMP/userns.err"; then
  record "userns: full set (user+mnt+pid+net+ipc+uts) creatable unprivileged" "$PASS"
elif unshare --user --map-root-user --mount --pid true 2>>"$TMP/userns.err"; then
  record "userns: basic set creatable, but full set failed (see note)" "$WARN"
  note "stderr: $(tr '\n' ' ' < "$TMP/userns.err")"
else
  record "userns: cannot create user namespace unprivileged" "$FAIL"
  note "stderr: $(tr '\n' ' ' < "$TMP/userns.err")"
fi

# ---------------------------------------------------------------------------
hdr "2. bubblewrap (bwrap)"
if command -v bwrap >/dev/null 2>&1; then
  note "bwrap: $(bwrap --version 2>/dev/null)"
  if bwrap --unshare-user --unshare-mount --ro-bind / / true 2>"$TMP/bwrap.err"; then
    record "bwrap: basic unprivileged jail works" "$PASS"
  else
    record "bwrap: present but basic jail failed (needs userns?)" "$FAIL"
    note "stderr: $(tr '\n' ' ' < "$TMP/bwrap.err")"
  fi
  # the egress-kill flag we want
  if bwrap --unshare-user --unshare-net --ro-bind / / true 2>"$TMP/bwrapnet.err"; then
    record "bwrap: --unshare-net (network egress kill) works" "$PASS"
  else
    record "bwrap: --unshare-net failed" "$WARN"
    note "stderr: $(tr '\n' ' ' < "$TMP/bwrapnet.err")"
  fi
  # seccomp acceptance (pass an empty/allow filter fd if available later; here just flag support)
else
  record "bwrap: NOT installed (must be added to the container image)" "$FAIL"
fi

# ---------------------------------------------------------------------------
hdr "3. Bind-mount INSIDE a user namespace (for trusted dynamic mounter)"
# In a userns we are root-in-ns and should have CAP_SYS_ADMIN scoped to it,
# so mount --bind of an already-visible path should succeed.
mkdir -p "$TMP/src" "$TMP/dst"; echo hello > "$TMP/src/file"
if unshare --user --map-root-user --mount bash -c \
     "mount --bind '$TMP/src' '$TMP/dst' && grep -q hello '$TMP/dst/file'" 2>"$TMP/bind.err"; then
  record "bind-mount inside userns works (enables dynamic remount w/o restart)" "$PASS"
else
  record "bind-mount inside userns failed" "$WARN"
  note "stderr: $(tr '\n' ' ' < "$TMP/bind.err")"
fi

# ---------------------------------------------------------------------------
hdr "4. FUSE (for the dynamic per-access authorization file layer)"
if [ -e /dev/fuse ]; then
  note "/dev/fuse present"
  if command -v fusermount3 >/dev/null 2>&1 || command -v fusermount >/dev/null 2>&1; then
    record "FUSE: /dev/fuse + fusermount present (FUSE layer feasible)" "$PASS"
  else
    record "FUSE: /dev/fuse present but no fusermount helper" "$WARN"
  fi
else
  record "FUSE: /dev/fuse NOT present (FUSE-backed layer not available)" "$WARN"
fi

# ---------------------------------------------------------------------------
hdr "5. seccomp filter installation (additional layer / fallback)"
PY="$(command -v python3 || command -v python)"
if [ -n "${PY:-}" ]; then
  note "python: $PY ($($PY -V 2>&1))"
  # can we set no_new_privs + install a trivial seccomp filter unprivileged?
  "$PY" - >"$TMP/seccomp.err" 2>&1 <<'PYEOF'
import ctypes, sys
PR_SET_NO_NEW_PRIVS = 38
libc = ctypes.CDLL("libc.so.6", use_errno=True)
if libc.prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0:
    sys.exit("prctl(NO_NEW_PRIVS) failed")
try:
    import seccomp  # python3-seccomp / pyseccomp
    f = seccomp.SyscallFilter(defaction=seccomp.ALLOW)
    f.add_rule(seccomp.ERRNO(1), "ptrace")
    f.load()
    print("OK: no_new_privs + libseccomp filter loaded")
except ImportError:
    print("PARTIAL: no_new_privs OK, but python 'seccomp' module not installed")
PYEOF
  out="$(tail -n1 "$TMP/seccomp.err" 2>/dev/null)"
  if grep -q '^OK:' "$TMP/seccomp.err" 2>/dev/null || $PY -c "import seccomp" 2>/dev/null; then
    record "seccomp: no_new_privs + libseccomp filter loadable unprivileged" "$PASS"
  elif grep -q 'PARTIAL' "$TMP/seccomp.err" 2>/dev/null; then
    record "seccomp: no_new_privs OK; install python3-seccomp/pyseccomp for filters" "$WARN"
  else
    record "seccomp: could not set no_new_privs / load filter" "$FAIL"
    note "detail: $(tr '\n' ' ' < "$TMP/seccomp.err")"
  fi
else
  record "seccomp: no python found to test" "$WARN"
fi

# ---------------------------------------------------------------------------
hdr "6. Loopback across netns (confirms broker<->worker transport choice)"
# If --unshare-net is used, the worker's loopback is unreachable from the host
# broker, so the broker<->worker channel must move to a Unix domain socket.
# This is informational: just confirm a UDS round-trip works.
if [ -n "${PY:-}" ]; then
  if "$PY" - <<'PYEOF' 2>"$TMP/uds.err"
import socket, os, tempfile
p = os.path.join(tempfile.mkdtemp(), "s.sock")
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); s.bind(p); s.listen(1)
c = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); c.connect(p)
print("UDS ok")
PYEOF
  then
    record "transport: AF_UNIX socket round-trip works (UDS migration viable)" "$PASS"
  else
    record "transport: AF_UNIX test failed" "$WARN"
  fi
fi

# ---------------------------------------------------------------------------
hdr "7. Scientific-Python import smoke (seccomp compat sanity)"
if [ -n "${PY:-}" ]; then
  if "$PY" -c "import numpy, pandas" 2>"$TMP/sci.err"; then
    extra=""; "$PY" -c "import sklearn, pyarrow" 2>/dev/null && extra=" (+sklearn,pyarrow)"
    record "libs: numpy+pandas import OK${extra}" "$PASS"
  else
    record "libs: numpy/pandas import failed" "$WARN"
    note "detail: $(tr '\n' ' ' < "$TMP/sci.err")"
  fi
fi

# ---------------------------------------------------------------------------
hdr "8. Resource limits (ulimit)"
if (ulimit -v 2000000 && ulimit -u 256 && ulimit -t 60) 2>/dev/null; then
  record "rlimits: ulimit -v/-u/-t settable" "$PASS"
else
  record "rlimits: some ulimit settings rejected" "$WARN"
fi

# ===========================================================================
hdr "SUMMARY & RECOMMENDATION"
for k in "${!RESULT[@]}"; do printf '  [%s] %s\n' "${RESULT[$k]}" "$k"; done | sort

userns_ok=false
case "${RESULT[*]}" in *) :;; esac
# crude lookups
g(){ for k in "${!RESULT[@]}"; do case "$k" in $1*) echo "${RESULT[$k]}";; esac; done | head -n1; }

echo
us="$(g 'userns: full' )"; [ -z "$us" ] && us="$(g 'userns:')"
bw="$(g 'bwrap: basic')"
fu="$(g 'FUSE:')"
bind="$(g 'bind-mount inside userns')"

if [ "$us" = "$PASS" ] && [ "$bw" = "$PASS" ]; then
  echo "  -> PRIMARY PATH AVAILABLE: bwrap namespace jail (user+mount+pid+net+ipc)."
  echo "     Run the persistent interpreter inside bwrap. Use --unshare-net + a"
  echo "     Unix-domain-socket broker<->worker channel to kill network egress."
  if [ "$bind" = "$PASS" ]; then
    echo "  -> Dynamic mid-session authz: trusted out-of-jail mounter is viable"
    echo "     (bind-mount inside userns works), so NO worker restart is needed."
  elif [ "$fu" = "$PASS" ]; then
    echo "  -> Dynamic mid-session authz: use the FUSE-backed file layer (zero restart)."
  else
    echo "  -> Dynamic mid-session authz: fall back to mount-at-start + restart on a"
    echo "     genuinely new grant (rare; drops REPL state for that one event)."
  fi
elif [ "$us" = "$PASS" ] && [ "$bw" != "$PASS" ]; then
  echo "  -> userns works but bwrap is missing/broken: add bwrap to the image, OR"
  echo "     drive unshare(2) directly. Then proceed as PRIMARY PATH above."
else
  echo "  -> LINCHPIN MISSING: unprivileged user namespaces are unavailable."
  echo "     In-pod cross-user FILESYSTEM isolation is NOT achievable here."
  echo "     Options: (a) ask cluster admins to enable unprivileged userns, or"
  echo "     (b) accept per-pod isolation, or (c) ship the SECCOMP-ONLY fallback"
  echo "     (protects host/network, but NOT user-vs-user files)."
fi
echo
