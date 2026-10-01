# HTTPS certificate staging and replacement

HTTPS server certificates identify this SEMOSS endpoint. They are **not** CMVP
cryptographic-module validation certificates and do not establish FIPS compliance
or IL4 authorization. Inbound HTTPS identity is also separate from outbound
database CA trust; see [GovCloud RDS trust](./RDS.md).

| Image variant | Server keystore | Format |
| --- | --- | --- |
| BC-FIPS / `IL4-dev` | `/run/secrets/server.bcfks` | BCFKS |
| Experimental ACCP / `IL4-dev-ACCP` | `/run/secrets/server.p12` | PKCS12 |

Both require `/run/secrets/server.password`, with the same password protecting
the keystore and its private-key entry. Mount secrets read-only, readable by
UID/GID `10001:10001`. Do not bake private keys or passwords into images, Git,
build arguments, command lines, or logs. A certificate chain **without its
matching private key** cannot provide the server identity.

## 1. Explicit development-only self-signed setup

[prepare_dev_tls.py](./prepare_dev_tls.py) generates a fresh RSA-3072 key,
SHA-384-signed leaf certificate, and random password in a **new named Docker
volume**, using an already-local SEMOSS image. The certificate lasts seven days,
has server-auth usage, and includes `localhost` plus the requested DNS hostname.
It is not a CA and has no wildcard SAN. The helper container is network-disabled,
has a read-only root and only the CHOWN capability needed to provision files.

Set `SEMOSS_IMAGE` to your built/tested image reference. Pull it separately if
needed. Choose a new volume name for each deployment:

```bash
export SEMOSS_IMAGE='semoss:5.4.0-ubi10-python314-bcfips'
export TLS_FORMAT=BCFKS
export TLS_VOLUME=semoss-bc-dev-tls

python3 prepare_dev_tls.py \
  --image "$SEMOSS_IMAGE" --format "$TLS_FORMAT" --volume "$TLS_VOLUME" \
  --hostname localhost --allow-self-signed > development-tls.json

# The report contains the PUBLIC certificate, never the key or password.
python3 -c 'import json,sys; print(json.load(sys.stdin)["certificate_pem"], end="")' \
  < development-tls.json > development-ca.pem
```

For ACCP, select its image, `TLS_FORMAT=PKCS12`, and a separate TLS volume;
also explicitly set `SEMOSS_ALLOW_NONVALIDATED_ACCP=true` before starting it.
The helper's self-signed opt-in does not acknowledge the separate ACCP experiment.

The helper **refuses existing volumes** instead of regenerating or overwriting
certificates. Keep the successful volume mounted across container restarts.
Before expiry, provision a new volume and deliberately replace the mount.
The explicit opt-in controls generation; it is not a production certificate-policy
enforcement mechanism. Existing images still fail when required TLS files are
missing: no runtime self-signed fallback is added.

Example local startup, with an unused container/home name and port 8443 free:

```bash
docker volume create semoss-cert-demo-home
docker run --detach --name semoss-cert-demo --platform linux/amd64 \
  --env "SEMOSS_ALLOW_NONVALIDATED_ACCP=${SEMOSS_ALLOW_NONVALIDATED_ACCP:-false}" \
  --read-only --cap-drop=ALL --security-opt=no-new-privileges \
  --user 10001:10001 --pids-limit=512 --memory=8g --cpus=4 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev \
  --tmpfs /opt/tomcat/temp:rw,noexec,nosuid,nodev,uid=10001,gid=10001 \
  --tmpfs /opt/tomcat/work:rw,noexec,nosuid,nodev,uid=10001,gid=10001 \
  --tmpfs /opt/tomcat/logs:rw,noexec,nosuid,nodev,uid=10001,gid=10001 \
  --log-driver local --log-opt max-size=10m --log-opt max-file=3 \
  --mount type=volume,src=semoss-cert-demo-home,dst=/opt/semosshome \
  --mount "type=volume,src=$TLS_VOLUME,dst=/run/secrets,readonly" \
  --publish 127.0.0.1:8443:8443 "$SEMOSS_IMAGE"

curl --fail --cacert development-ca.pem \
  https://localhost:8443/Monolith/health/ready
```

Wait for backend readiness; require HTTP 200 and `startupComplete: true`.
Do not use `curl -k`, disable browser certificate checks, or install this test
certificate as an organization-wide trusted root. Browser testing needs a
deliberately managed development trust configuration. Never expose bootstrap
endpoints publicly. Custom DNS/ports also require SEMOSS's actual absolute
application URL to be configured; adding a SAN does not update application URLs.

## 2. Supply an organization-issued certificate chain

Obtain these through the deployment's approved certificate/secret processes:

- `server.crt`: PEM server/leaf certificate, with the deployment hostname in SAN,
  appropriate server-auth usage, and approved key/signature algorithms.
- `server.key`: matching private key, handled as a secret.
- `intermediates.pem`: issuing intermediates, nearest issuer first.
- `approved-root.pem`: independently verified trust anchor(s), for validation and
  readiness clients. A root normally does not need to be served by Tomcat.

Use OpenSSL 3 and a **new**, restricted, versioned staging directory. The example
paths below are placeholders; do not point them at existing live secret files.
Provision the password from the approved secret system, or generate it locally
for a controlled test. These are packaging instructions, not a claim that the
operator's OpenSSL environment is a validated cryptographic module.

```bash
set -euo pipefail
umask 077
export TLS_RELEASE_DIR=/secure/semoss/tls-v1
mkdir -m 700 "$TLS_RELEASE_DIR"   # Must fail if this release already exists.

openssl verify -purpose sslserver -verify_hostname semoss.example.mil \
  -CAfile /secure/incoming/approved-root.pem \
  -untrusted /secure/incoming/intermediates.pem /secure/incoming/server.crt
openssl x509 -in /secure/incoming/server.crt -noout -checkend 604800

openssl rand -base64 48 > "$TLS_RELEASE_DIR/server.password"
openssl pkcs12 -export -name server \
  -in /secure/incoming/server.crt -inkey /secure/incoming/server.key \
  -certfile /secure/incoming/intermediates.pem \
  -keypbe AES-256-CBC -certpbe AES-256-CBC -macalg SHA256 -iter 100000 \
  -passout "file:$TLS_RELEASE_DIR/server.password" \
  -out "$TLS_RELEASE_DIR/server.p12"
cp /secure/incoming/approved-root.pem "$TLS_RELEASE_DIR/readiness-ca.pem"
```

An encrypted input key may prompt for its password; never place it in command
arguments. Export rejects a leaf/key mismatch. If the leaf is directly signed by
the approved root, omit the `-untrusted` and `-certfile` options rather than
supplying an empty intermediates file.

**ACCP:** use the resulting `server.p12`.

**BC-FIPS:** convert the PKCS12 entry to BCFKS using the BC image:

```bash
docker run --rm --platform linux/amd64 --network none --read-only \
  --cap-drop=ALL --security-opt=no-new-privileges \
  --user "$(id -u):$(id -g)" --tmpfs /tmp:rw,noexec,nosuid,nodev \
  --mount "type=bind,src=$TLS_RELEASE_DIR,dst=/tls" \
  --entrypoint keytool "$SEMOSS_IMAGE" \
  -importkeystore -noprompt -srcalias server -destalias server \
  -srckeystore /tls/server.p12 -srcstoretype PKCS12 \
  -srcstorepass:file /tls/server.password -srckeypass:file /tls/server.password \
  -destkeystore /tls/server.bcfks -deststoretype BCFKS \
  -deststorepass:file /tls/server.password -destkeypass:file /tls/server.password \
  -providerclass org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
  -providerpath /opt/fips/bc-fips-2.1.3.jar \
  -J-Dorg.bouncycastle.native.cpu_variant=java
```

Use `keytool -list -v` with the same store type/password-file/provider options to
confirm exactly one `PrivateKeyEntry` named `server` and the expected chain.
Importing leaf/intermediate certificates as separate `trustedCertEntry` entries
is **not** a replacement for the chain attached to the private key.

On a Linux Docker host, set the selected keystore and password to mode `0600`
and owner `10001:10001`; make the staging directory traversable by that identity
(for example owner `10001:10001`, mode `0700`). The public readiness CA can be
`0644`. Apply equivalent approved secret ownership on other platforms.
Do not solve access failures with `chmod 777` or a root application process.
Retain/delete the intermediate PKCS12 export under the site's secret policy;
mount only the final store, password, and public readiness trust file.

## 3. Replace the development identity and validate

For [compose.dev.yml](./compose.dev.yml), supply existing absolute paths:

```bash
export SEMOSS_TLS_KEYSTORE="$TLS_RELEASE_DIR/server.bcfks"  # server.p12 for ACCP
export SEMOSS_TLS_PASSWORD="$TLS_RELEASE_DIR/server.password"
export SEMOSS_READINESS_CA="$TLS_RELEASE_DIR/readiness-ca.pem"
export SEMOSS_READINESS_URL=https://semoss.example.mil:8443/Monolith/health/ready
```

The readiness hostname must resolve to **this container**, not another service.
Configure deployment DNS/network routing or the orchestrator's equivalent probe.
Keep the existing application home volume; do not delete/reseed it for a
certificate change. Deliberately recreate/restart the application with the new
read-only mounts: Tomcat is not configured for automatic certificate reload.
Do not repoint an existing BC home at ACCP.

Verify the served leaf fingerprint/expiry, intermediate chain, hostname, backend
readiness, and administrator login from a trusted client. Independently check
that a wrong CA and wrong hostname fail. Retain the previous secret version for
controlled rollback until acceptance passes; follow the site's retirement policy.
Replacing inbound HTTPS certificates does not modify outbound Java/Python trust.
