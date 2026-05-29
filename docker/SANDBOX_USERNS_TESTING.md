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

| Environment | userns | Path |
|---|---|---|
| **AWS EKS (Amazon Linux 2023)** | **Yes, by default** (verified: `Seccomp 0`, full userns set + propagation PoC all green; Landlock abi=6 also available) | Full in-pod sandbox, no infra change |
| Self-managed / standard GKE | Yes — patch seccompProfile (A or B) | Full sandbox |
| GKE Autopilot (plain pod) | No — managed AppArmor denies mount | use gVisor (next row) |
| **GKE Autopilot + gVisor** | **Yes** (verified: `runtimeClassName: gvisor` + sandbox-perceived `SYS_ADMIN`; userns/bwrap/bind-in-userns all PASS; /dev/fuse present). gVisor's Sentry handles mounts, so host AppArmor is out of the loop. | Full in-pod sandbox inside gVisor |
| DoD (RHEL 8.10, **kernel 4.18**, SELinux/OpenShift) | No — kernel too old for Landlock; seccomp gates userns and SELinux `container_t` would deny mount even after | Per-pod isolation (SELinux MCS already isolates pods) + RWX volume |

If neither userns nor `hostUsers: false` is available, in-pod cross-user
isolation isn't achievable there — fall back to per-pod isolation or the
seccomp-only mode (host/network protection, not user-vs-user files).

### GKE Autopilot — confirmed hard limit

Observed on Autopilot (`gk3-*-nap-*` nodes, `autopilot.gke.io/*` annotations):
`seccompProfile: Unconfined` is accepted and unblocks userns *creation*, but
`mount()` inside the userns is denied with **EACCES** by Autopilot's managed
**AppArmor** profile. Autopilot's admission controller does not permit
unconfined AppArmor, mount privileges, FUSE devices, or host namespaces — so
the in-pod namespace sandbox is **not achievable on Autopilot by design**.

On Autopilot the only path to real isolation is **per-pod**: one Python pod per
user/session, host protection via GKE Sandbox (`runtimeClassName: gvisor`),
egress via NetworkPolicy, and the file-sync requirement via a ReadWriteMany
volume (Filestore / GCS FUSE CSI) mounting only that user's authorized paths.

### GKE Autopilot via gVisor — the in-sandbox capability path

Because gVisor is a userspace kernel that always runs with zero host privileges,
Autopilot permits granting `SYS_ADMIN` *inside* a `runtimeClassName: gvisor`
pod — and that sandbox-perceived `SYS_ADMIN` is exactly what `mount`/`unshare`/
`pivot_root` need, with no host risk. This can re-enable our sandbox primitives
on Autopilot. Test it with `docker/k8s/sandbox-test-pod-gvisor.yaml`:

```bash
kubectl apply -f docker/k8s/sandbox-test-pod-gvisor.yaml
kubectl cp scripts semoss/semoss-sandbox-gvisor:/tmp/scripts
kubectl exec -n semoss -it semoss-sandbox-gvisor -- bash    /tmp/scripts/sandbox_probe.sh
kubectl exec -n semoss -it semoss-sandbox-gvisor -- python3 /tmp/scripts/propagation_supervisor_poc.py
kubectl exec -n semoss -it semoss-sandbox-gvisor -- bash    /tmp/scripts/dynamic_mount_poc.sh
```

Interpretation:
- Warden rejects `SYS_ADMIN` even under gVisor → cluster needs a workload policy
  (or it's disallowed) — escalate to the cluster admins.
- Probe shows `userns`/bind-mount PASS and the **propagation PoC** passes → the
  full multi-user in-pod design works on Autopilot under gVisor.
- Probe PASSes but the **propagation** PoC fails (gVisor lacks shared/slave mount
  propagation) → use the **one-gVisor-pod-per-user** model, where dynamic project
  load is a plain `mount --bind` (no propagation needed) since there are no other
  users to hide inside the pod.

Note: gVisor adds syscall overhead; measure pandas / file-I/O performance.

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
