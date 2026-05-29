# Enabling & testing the unprivileged Python sandbox (user namespaces)

This is the runbook for unblocking and validating the real Python execution
sandbox (a bwrap user+mount+pid namespace jail with an in-userns mount
supervisor for live, isolated, stateful project loading).

The **only** runtime prerequisite is **unprivileged user namespaces**. Your probe
showed the kernel allows them (`user.max_user_namespaces > 0`) but the
container's default **seccomp profile blocks `unshare(CLONE_NEWUSER)`**. These
steps flip that and prove the mechanism — without granting any real privilege
(no `CAP_SYS_ADMIN`, no privileged pod).

## TL;DR — fastest validation (no image rebuild)

The hardest requirement — **live-injecting a project the user is granted
mid-session, isolated, with interpreter state preserved** — is proven by
`scripts/propagation_supervisor_poc.py`, which uses only Python + `ctypes` and
needs **only user namespaces** (no `bwrap`). So:

```bash
# 1. Stand up a test pod with seccomp unblocked (edit the image first):
kubectl apply -f docker/k8s/sandbox-test-pod.yaml

# 2. Copy the probe + PoCs in:
kubectl cp scripts semoss-sandbox-test:/tmp/scripts

# 3. Confirm userns is now allowed, then prove the core mechanism:
kubectl exec -it semoss-sandbox-test -- bash /tmp/scripts/sandbox_probe.sh
kubectl exec -it semoss-sandbox-test -- python3 /tmp/scripts/propagation_supervisor_poc.py
```

Expected: `sandbox_probe.sh` now reports `userns: ... PASS`, and the propagation
PoC prints all `[PASS]` (reads a newly-granted project live; cannot see the
backing store / other users; `x` stays 1).

To additionally validate `bwrap` itself, use an image built from the updated
Dockerfiles (they now install `bubblewrap`) and run:

```bash
kubectl exec -it semoss-sandbox-test -- bash /tmp/scripts/dynamic_mount_poc.sh
```

## Enabling it on your real Deployment

Patch the workload's pod template `securityContext`. **Quick (test):**

```bash
kubectl patch deployment <semoss-deploy> --type merge -p \
  '{"spec":{"template":{"spec":{"securityContext":{"seccompProfile":{"type":"Unconfined"}}}}}}'
```

**Production (preferred)** — a `Localhost` profile equal to RuntimeDefault minus
the `CAP_SYS_ADMIN` gate on `unshare`/`clone`/`setns`, so the outer host filter
stays in place. (Ask me for the profile JSON — option "Write the custom seccomp
profile".) Place it under the kubelet seccomp dir on nodes and reference:

```yaml
securityContext:
  seccompProfile:
    type: Localhost
    localhostProfile: profiles/allow-userns.json
```

## Per-environment expectations

| Environment | Can enable userns? | Path |
|---|---|---|
| Self-managed / standard GKE | Yes — patch seccompProfile (A or B) | Full sandbox |
| GKE Autopilot | Usually no (can't change seccomp/devices) | Per-pod isolation fallback, or request the feature |
| DoD / hardened | Test per cluster; `hostUsers: false` may be available | Full sandbox if userns or hostUsers works |

If neither userns nor `hostUsers: false` is available, in-pod cross-user
isolation isn't achievable there — fall back to per-pod isolation or the
seccomp-only mode (host/network protection, not user-vs-user files).

## What "good" looks like

After enabling, re-run `scripts/sandbox_probe.sh`. You want:

- `userns: full set ... PASS`
- (with rebuilt image) `bwrap: basic unprivileged jail works PASS`
- `bind-mount inside userns works PASS`

Then both PoCs pass. That confirms this cluster can run the real sandbox.

## Cleanup

```bash
kubectl delete -f docker/k8s/sandbox-test-pod.yaml
```
