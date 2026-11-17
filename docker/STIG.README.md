# catalina.policy Replacement — Tomcat 11 Security Controls

Tomcat 11 removed the Java Security Manager and with it `catalina.policy`.
The table below maps each policy class to its modern equivalent and records
what this repository enforces vs. what the container runtime must supply.

## Status by policy class

| Old policy class | Equivalent | Enforced in image | Runtime required |
|------------------|-----------|:-----------------:|:----------------:|
| `FilePermission` — readable/writable paths | `chmod -R u=rwX,g=rX,o=` on `$TOMCAT_HOME/conf` and `lib` | ✅ Dockerfile.tomcat | — |
| `SocketPermission` — outbound/inbound connections | Host firewall (nftables/firewalld) or Kubernetes `NetworkPolicy` | — | ✅ |
| `RuntimePermission` (exitVM, setSecurityManager) | SUID/SGID stripped at image build; one app per container | ✅ all app Dockerfiles | ✅ `NoNewPrivileges` |
| `PropertyPermission`, `AllPermission` for libs | No equivalent; Tomcat is trusted, app code is vetted at deploy time | — | — |
| Privilege escalation via the process | Non-root UID 1001, SUID/SGID stripped | ✅ all app Dockerfiles | ✅ cap-drop + seccomp |

---

## What is enforced at image build time

### Dockerfile.tomcat (base builder)
* Tomcat bin scripts set to `750` (owner rwx, group rx, world none) instead
  of the previous `777`.
* `$TOMCAT_HOME/conf` and `$TOMCAT_HOME/lib` set to `u=rwX,g=rX,o=` so
  world-read is blocked on keystores, `server.xml`, and deployment descriptors.

### All app Dockerfiles (ubuntu22.04, ubuntu24.04, ubuntu26.04, ubi8, ubi9, al2023, nvidia.cuda)
* `find / -xdev -perm /6000 … chmod ug-s` strips every SUID and SGID bit in
  the final image, closing the in-container privilege-escalation path that
  `RuntimePermission/exitVM` guarded.
* `$TOMCAT_HOME/conf` permissions are re-asserted after the `chown -R 1001:0`
  step (which does not change mode bits but documents ownership intent).
* Container runs as UID 1001, GID 0 (`USER 1001`).

---

## Runtime controls — MUST be set by the operator

The following controls cannot be baked into the image; they must be supplied
when the container is started.

### Docker / Podman
```sh
docker run \
  --security-opt no-new-privileges:true \
  --cap-drop ALL \
  --cap-add NET_BIND_SERVICE \   # only if binding ports < 1024
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=256m \
  --tmpfs /opt/apache-tomcat-*/logs:rw,noexec,nosuid \
  --tmpfs /opt/apache-tomcat-*/work:rw,noexec,nosuid \
  --tmpfs /opt/apache-tomcat-*/temp:rw,noexec,nosuid \
  quay.io/semoss/semoss:<tag>
```

| Flag | Policy equivalent |
|------|------------------|
| `--security-opt no-new-privileges:true` | `RuntimePermission` / privilege escalation |
| `--cap-drop ALL` | Privilege escalation via capabilities |
| `--read-only` + `--tmpfs` | `FilePermission` write scope |

### Kubernetes
```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 1001
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop: ["ALL"]
    add: []          # add NET_BIND_SERVICE only if binding < 1024
  seccompProfile:
    type: RuntimeDefault   # or Localhost with a custom profile
```

A `NetworkPolicy` restricting egress to known endpoints replaces
`SocketPermission`. Example — allow only HTTPS egress:
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: semoss-egress
spec:
  podSelector:
    matchLabels:
      app: semoss
  policyTypes: [Egress]
  egress:
    - ports:
        - port: 443
          protocol: TCP
        - port: 53
          protocol: UDP
```

### systemd (bare-metal or VM)
```ini
[Service]
NoNewPrivileges=yes
ProtectSystem=strict
ReadWritePaths=/opt/semosshome /opt/apache-tomcat-*/logs /opt/apache-tomcat-*/work /opt/apache-tomcat-*/temp /tmp
PrivateTmp=yes
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
IPAddressDeny=any
IPAddressAllow=localhost <trusted-cidr>
CapabilityBoundingSet=
AmbientCapabilities=
```

---

## SELinux / AppArmor

For further `FilePermission` scope enforcement on bare metal or VM:
* **SELinux**: label the Tomcat process with a confined context (e.g. the
  `tomcat_t` policy from `selinux-policy-targeted`) so that writes outside
  declared contexts are denied at the kernel level.
* **AppArmor**: ship a profile that allows reads from `$TOMCAT_HOME/conf` and
  writes only to the log/work/temp trees.

Neither is included here because their profiles are deployment-specific, but
they are the strongest replacement for fine-grained `FilePermission` rules.
