# SEMOSS 5.4.0 / UBI 10.2 / Java 25 / Python 3.14 / BC FIPS template

This template assembles the **published SEMOSS 5.4.0 release**, including Monolith,
SemossWeb, and semosshome. It does not rebuild the Java/web release.
It applies two approved JDBC driver upgrades and one approved Python audio
compatibility patch, described below.
Source reference: [SEMOSS/Semoss v5.4.0](https://github.com/SEMOSS/Semoss/tree/v5.4.0),
commit `565b2c1825011304a93eabcb21cf2cd33135a42a`.

Per the deployment owner, SEMOSS 5.4 has been assessed by VA TRM and is undergoing
DoD assessment. This template does not dispute or independently verify that status.
The container's configuration and dependency changes below must remain visible
when comparing it with the assessed application baseline.

## Images and supply chain

| Component | Selection |
| --- | --- |
| Build stage and JDK source | `registry1.dso.mil/ironbank/opensource/maven/maven-openjdk-25:3.9.16` |
| Final OS | `registry1.dso.mil/ironbank/redhat/ubi/ubi10:10.2` |
| Python runtime donor | `registry1.dso.mil/ironbank/opensource/python:v3.14` (Python 3.14.7, UBI 10.2) |
| Tomcat | 9.0.119 (SEMOSS 5.4 uses `javax.servlet`, not Tomcat 10/11's Jakarta API) |
| BC FIPS | bc-fips 2.1.3, bctls-fips 2.1.24, bcpkix-fips 2.1.12, bcutil-fips 2.1.7 |

All three images have **digest-pinned defaults** in [Dockerfile](./Dockerfile).
The development GitHub build resolves fresh digests behind those same version
tags on every run and passes immutable references as build arguments. It does
not automatically move to newer version tags. Therefore the tested baseline
versions and catalog observations below are historical, not an assessment of
each newly resolved base. The final base
has moved from UBI 9.8 to UBI 10.2. The template retains the selected Maven
image's Corretto Java 25 JDK, dereferencing its external CA-store link, rather
than changing the OS and JDK source simultaneously. The build executes that
JDK and the FIPS crypto probe on the final UBI base.

The Python donor's `v3.14` tag was verified as **Compliant** in the authenticated
Iron Bank catalog (98.3% findings verified, 76% overall score at observation).
This is distinct from `v3.12`, which was Non-compliant, and from the Alpine
variant. Catalog status can change; re-check the approved digest at promotion.
The template copies only its interpreter, standard library, and libpython,
not the donor's complete filesystem or user environment. Native library
dependencies come from the final UBI repositories.

Maven is used only to resolve official release artifacts. It is **not copied into
the final image**. [artifacts.lock.json](./artifacts.lock.json) pins all eleven
application, Tomcat, FIPS, and JDBC payloads with SHA-256. A mismatch aborts before
extraction. Archive traversal and links are rejected. Ordinary BC JARs are removed,
and remaining Tomcat/application JARs are scanned for bundled BC classes, including
relocated copies. FIPS JARs themselves are never unpacked or rewritten.

The checksums were obtained from the upstream publishers and compared with
SEMOSS's FIPS recipe where available. They prevent later drift; they do not
independently establish publisher identity or prove absence of vulnerabilities.
Review them against your approved artifact inventory before promotion.

### Important scope and baseline changes

- Three standard BC 1.78.1 JARs are replaced by the system-classloader FIPS suite.
- Approved JDBC overlay: MariaDB 1.1.9 -> 3.5.10 and SQL Server
  11.2.4.jre11 -> 13.6.0.jre11. PostgreSQL 42.7.11 remains unchanged.
  Original/replacement hashes are recorded; duplicate or unexpected originals
  fail assembly. See [database integration requirements](./integrations/jdbc/README.md).
- **Snowflake is unavailable**: its 3.22.0 driver contains another BC implementation
  and is excluded with the owner's approval. Other discovered BC copies fail the
  build rather than being silently removed.
- **Java/web and Python CPU execution are enabled**, with all three SEMOSS
  application bundles. Python uses the native socket worker and a root-owned
  virtual environment at `/opt/semoss-python`; it does not require JEP.
  `USE_R=false` and `R_KILL_ON_STARTUP=false` remain in effect. R, JEP, browser
  binaries, and privileged/chroot sandbox execution are not provisioned.
- `NOTIFICATION_DATABASE_ENABLED=false`: the 5.4 home bundle does not include its
  notification database. Provision that database before enabling notifications.
- Native self-registration is disabled. Configure authentication and bootstrap
  the initial administrator on an isolated deployment before exposing it.
- Home paths are normalized for Linux; sessions expire after 15 minutes and use
  Secure/HttpOnly cookies. SameSite=Lax is the default; review SSO POST flows.
- HTTPS only, port 8443; TLS 1.2/1.3 with AES-GCM suites. No AJP, shutdown socket,
  manager, host-manager, examples, or default ROOT application.
- Application data is writable, but the JDK, Python environment, webapps, FIPS JARs, and Tomcat
  configuration are root-owned. Runtime UID/GID is `10001:10001`.

Every build records the artifact lock, removed dependencies, runtime flags, and
installed RPM inventory in `/opt/provenance`. Embedded private/shaded crypto
outside the `org.bouncycastle` namespace is not exhaustively detected by this
check; application cryptographic routing still needs review.

## Functional validation and integration status

The localhost-only deployment has passed native administrator login, CSRF-protected
Pixel execution (`1+1`), a real Python/pandas calculation through the SEMOSS worker,
and browser login into the rendered workspace. These checks do not establish that
every connector or external service is operational.

- New homes use `https://localhost:8443/SemossWeb/` as the absolute application
  redirect and `DEFAULT_SCRIPTING_LANGUAGE=PY`. Configure the actual absolute
  deployment URL before production; a relative URL is not supported by the
  application's base-URL handling. Existing data volumes require deliberate
  configuration migration rather than automatic overwrite.
- SQLite's existing checksum-covered JNI library is extracted at build time to
  root-owned `/opt/sqlite`; startup executes [JdbcCheck.java](./JdbcCheck.java).
  This permits native loading without making the writable temporary directory
  executable. It does not upgrade the SQLite driver.
- [functional_smoke.py](./tests/functional_smoke.py) supports CA-verified localhost
  login and Pixel checks. Keep its administrator credential file private
  (mode `0600`); never bake it into the image. Any temporarily enabled native
  registration must be disabled again immediately after administrator bootstrap.
- **AWS Bedrock:** the actual packaged SEMOSS adapter passed a live Claude Sonnet
  4.5 request in GovCloud using the HTTPS FIPS endpoint with certificate
  verification enabled. See [Bedrock checks and deployment requirements](./integrations/bedrock/README.md).
  Persistent ModelEngine registration and a UI conversation remain unverified.
- **PostgreSQL:** a local TLS/SCRAM PostgreSQL 15.19 fixture is registered as
  `LocalPostgresTLS` and visible in the authenticated Database Catalog.
  Persisted connector reload, raw SQL aggregation (3 rows, total 12), and
  metadata-driven selection passed. Wrong CA, hostname and password were rejected;
  the read-only account cannot insert rows. This uses a test-only upstream
  PostgreSQL image, not an approved production Iron Bank server.
- **SQL Server and MariaDB:** drivers are present, but live connections remain
  unverified. Production connections to the owner's existing PostgreSQL servers
  also remain unverified.
  Selected versions are PostgreSQL 42.7.11, SQL Server 13.6.0.jre11, and MariaDB
  3.5.10 following the owner's approval. Strict profiles and a credential-safe
  standalone validation command are documented in the
  [JDBC integration guide](./integrations/jdbc/README.md).
  Require server-certificate and hostname validation, least-privilege accounts,
  and trusted CA material before enabling production connectors.

SEMOSS external database registration writes connection properties, potentially
including passwords, to `.smss` files when no supported secrets connector is
configured. Generic `${ENV}` or `_FILE` credential substitution is not established
by this template. Configure and verify an approved secrets backend before
registering production credentials. No enterprise database password or AWS
credential is included in this workspace.

## Python compatibility and supply chain

[requirements-cpu.lock](./python-dependencies/requirements-cpu.lock) locks all
**260 distributions** needed by the 71 upstream runtime requirements and three
CPU-extra requirements. No requested Python dependency is silently dropped.
GPU and development groups are not installed. The original manifest is preserved
in [pyproject.upstream.toml](./python-dependencies/pyproject.upstream.toml), including
its internal `5.3.0` metadata at the pinned SEMOSS 5.4.0 source commit.

Python 3.14 requires these explicitly documented compatibility changes:

| Dependency | Original | Selected | Reason |
| --- | --- | --- | --- |
| pandas | 2.2.3 | 2.3.3 | First CPython 3.14 wheels |
| pyarrow | 20.0.0 | 22.0.0 | First CPython 3.14 wheels |
| datasets | 2.14.3 | 4.4.0 | Arrow API and Python 3.14 pickling compatibility |
| pipecat-ai | 0.0.103 | 1.0.0 | Compatible ONNX Runtime, Numba, and soxr dependencies |

Additional constraints are `dill>=0.4.0` and `protobuf<7`; the upstream security
overrides remain applied. Exact pins, failed older candidates, source references,
and caveats are recorded in [compatibility.json](./python-dependencies/compatibility.json).
Datasets 4.x no longer supports dataset loading scripts. Review existing saved
datasets, pickle migrations, custom loaders, and external model integrations
before migrating production data.

The build uses hash-pinned uv and build tools, verifies dependency artifact hashes,
routes PyTorch packages explicitly to its CPU index, and checks the installed
dependency graph. Annoy, pandasql, and swifter require source builds; their sdists,
tooling, and compiler flags are recorded in
[source-builds.json](./python-dependencies/source-builds.json). Annoy targets
`x86-64-v3`, not the build host's `-march=native`. Build tools and caches are not
copied into the final image. The lock pins **inputs**, not reproducible byte hashes
of locally compiled wheels. No pre-approved offline wheelhouse is claimed.

The complete environment is copied to the same absolute path in the runtime.
Set `PYTHONHOME /opt/semoss-python` in SEMOSS's RDF properties, **not** as an
exported CPython `PYTHONHOME` environment variable. `NETTY_PYTHON`,
`NATIVE_PY_SERVER`, and `USE_PYTHON` are all `true` in newly seeded homes.
The entrypoint checks Python 3.14 and core imports; it performs no installation.
Model weights, provider credentials, and application-specific assets are not
implicitly downloaded or supplied by this template.

### Approved audio patch

Pipecat 1.0 removed `TranscriptProcessor`. [audio_compat.py](./audio_compat.py)
patches only `py/audio/lk_to_pcat.py` in the extracted home:

- Replaces the removed processor with the existing user/assistant aggregators'
  turn-stopped events.
- Preserves the client message fields `type`, `role`, `text`, `ts`, and `isFinal`.
  Speech-to-speech transcript messages now follow aggregated turn boundaries;
  `isFinal=true` means that turn has closed, including interrupted assistant turns.
  Empty assistant turns are not sent. Other transcription/translation paths remain
  unchanged.
- Rejects a changed source hash or non-unique patch target and records both
  original and patched SHA-256 values in `/opt/provenance/assembly.json`.

Pipecat also attempts to download NLTK tokenizer data during import if absent.
The build instead supplies `punkt_tab` from an immutable NLTK data commit with an
explicit SHA-256 in the Dockerfile. `NLTK_DATA=/opt/nltk_data` is root-owned.

Offline tests construct all three SEMOSS audio pipelines using the installed
Pipecat classes and test transcript payloads. The runner, outgoing messages, and
AWS client are doubled: **no live LiveKit/OpenAI/AWS audio session is certified**.
Supported-but-deprecated service constructor arguments currently emit warnings;
they are retained to keep the patch narrowly scoped.

## Build

Docker BuildKit, registry access, and a trusted connection to Maven Central and
UBI repositories, PyPI, the PyTorch CPU index, and the pinned NLTK data URL are
required. The tested platform is `linux/amd64` (UBI10 requires x86-64-v3); do not assume
native SEMOSS dependencies support arm64 without a separate test.

```sh
docker login registry1.dso.mil
docker build --platform linux/amd64 \
  --tag semoss:5.4.0-ubi10-python314-bcfips .
docker run --rm --platform linux/amd64 \
  semoss:5.4.0-ubi10-python314-bcfips --check
```

For a controlled Maven mirror, supply Maven settings as a **BuildKit secret**:

```sh
docker build --platform linux/amd64 \
  --secret id=maven_settings,src=/secure/maven-settings.xml \
  --tag semoss:5.4.0-ubi10-python314-bcfips .
```

Do not pass credentials in build arguments or copy them into the build context.
The allowlisted [.dockerignore](./.dockerignore) excludes unrelated local files.
No secrets or registry credentials are stored in this project.

For higher assurance:

1. Mirror approved, signature-verified payloads and Maven plugins/transitive
   plugin dependencies internally. The nine payload checksums do **not** lock all
   Maven plugin dependencies.
2. Freeze and approve a signed UBI RPM repository snapshot. Package installation
   currently uses the repositories configured in the pinned UBI base; those
   repositories can change, so this is **not a bit-for-bit reproducible OS build**.
3. Run your approved SBOM and vulnerability scanners against the **final image**,
   inspect license notices, and attach build provenance and test results.
4. Sign the resulting image digest and deploy that digest, not a mutable tag.
   The parent image's assessment score does not cover added application content.
5. Restrict build/runtime egress and enforce signature/admission policy. No
   application artifacts are downloaded by the entrypoint, but enabled SEMOSS
   features can make network calls; enforce allowed destinations externally.

### GitHub Actions test-image build

[il4-container.yml](../../.github/workflows/il4-container.yml) builds on the
`SEMOSS/Semoss` **`IL4-dev` branch** using the existing
`codebuild-semoss-github-runner` project. It publishes the tested image to
`ghcr.io/semoss/semoss-il4`, separate from other SEMOSS images.
Changes under `docker/il4` or to the workflow trigger a build on that branch;
manual dispatch is also supported when the workflow is available for dispatch.
Once the workflow is merged into the default `dev` branch, GitHub also schedules
it **every Monday at 09:23 UTC**, checking out `IL4-dev` rather than building
the default branch. GitHub may delay scheduled runs; this is not an exact-time
service guarantee. Keeping the workflow only on `IL4-dev` does not activate
the weekly schedule.
It does not deploy or auto-promote. Run the local build commands in this guide
from `docker/il4`, not from the repository root.

For this initial test build, the owner explicitly approved using the existing
repository secrets **without a GitHub environment approval gate**. `IL4-dev`
was unprotected when prepared. This is not a production promotion pipeline;
add branch protection and an approved environment before production use.

Before uploading, review the allowlisted [.gitignore](./.gitignore) and the
actual staged files. Local SBOM-review outputs, caches, TLS material, credentials,
and unrelated files are excluded by default. An ignore rule is not a secret
scanner; review allowed source/configuration files too. Add future source files
to the allowlist deliberately.

Provision these prerequisites in the destination repository:

1. This destination is the public `SEMOSS/Semoss` repository. Include only
   approved public source and build metadata, never CUI or operational secrets.
   Restrict Actions and CodeBuild webhook access to reviewed workflows.
   Protect `IL4-dev` and require review of workflow changes. No PR event starts
   this workflow; pushes and manual runs on other branches are excluded.
   Scheduled events from the default branch explicitly check out `IL4-dev`.
2. Before production use, create the `container-build` **GitHub environment** with required reviewers,
   prevent self-review where supported, and allow deployments from the protected
   `IL4-dev` branch only. Ensure your GitHub plan supports and enforces these
   protections and add `environment: container-build` to the workflow job.
   The initial test workflow intentionally does not reference this absent
   environment, rather than silently creating an unprotected one.
3. Use the existing **ephemeral Linux x64 CodeBuild runner** with the dynamic
   label `codebuild-semoss-github-runner-<run-id>-<run-attempt>`.
   Its webhook must accept this repository's queued workflow jobs. It must not
   share a Docker daemon with production workloads or execute untrusted pull
   requests. The job uses the same `quay.io/semoss/test-quay:ubuntu-dind`
   tooling container as existing SEMOSS builds, pinned to digest
   `sha256:5711ae2ade8db7de5e95f16bee23a89addd5d9aeb5b905c41c7ccca183ecda20`.
   The CodeBuild host's older glibc cannot run checkout's Node 24 directly;
   the Ubuntu job container supplies the compatible userland. This tooling
   container is not an application base or part of the published image.
   The runner must support Node 24 actions (at least 2.327.1); the job needs
   Bash, Git, Python 3.10+, Docker Engine access, and Buildx. Bedrock checks
   are streamed over stdin rather than bind-mounting job-container paths into
   the host daemon. Preflight fails if required tooling is missing.
   The `default` Buildx builder must use the `docker` driver; the workflow uses
   that daemon's embedded BuildKit rather than downloading a builder image.
   Its BuildKit must support `ADD --checksum`. Provision x86-64-v3 CPU support,
   at least 4 CPUs and 8 GiB RAM available to test containers, and enough disk for
   the large Python/Java build and cache; size these from a trial build.
4. The owner confirmed repository secrets `REPO_ONE_USERNAME` and
   `REPO_ONE_PASSWORD` are the existing Iron Bank credentials. They must
   authorize pulling all three pinned images and should be least-privilege.
   Optionally add `MAVEN_SETTINGS` containing the approved Maven settings XML;
   it is passed only as a BuildKit secret, never as a build argument.
   Do not add production TLS keys, database credentials, AWS credentials, or
   administrator passwords to this build environment.
5. Allow the repository's `GITHUB_TOKEN` to write packages. The workflow grants
   only `contents: read` and `packages: write`; it does not require a PAT.
   If the GHCR package already exists, grant this repository Actions write access
   to it. Verify package visibility and repository linkage after the first push.
6. Approve runner egress to GitHub/Actions, GHCR, Iron Bank and the dependency
   sources listed above, including the approved UBI repositories. Configure
   organization trust roots and any required mirrors on the runner/daemon;
   do not bypass TLS validation. Destroy the ephemeral runner and its disks
   after every job, including failures/cancellation. Login actions log out at
   job end and the unique local image tag is removed, but those operations do
   not erase the Docker build cache, workspace, or all credential remnants.

Push reviewed build changes to `IL4-dev`. No environment approval is requested
by this initial test workflow. The push trigger supports the first branch-only run
without modifying `dev` or `main`. GitHub normally exposes **Run workflow** only
when the workflow also exists on the default branch; when available, choose
**Build IL4 test container** and select `IL4-dev`.
The workflow:

- Runs isolated assembly, HTTP-client, and Python/audio unit tests plus
  shell syntax checks.
- Logs in to Iron Bank and builds the existing digest-pinned `linux/amd64`
  Dockerfile, including its build-time validation.
  [refresh_bases.py](./refresh_bases.py) first resolves the current digests for
  Maven `3.9.16`, UBI `10.2`, and Python `v3.14`; any failed lookup or invalid
  digest aborts without falling back to the old pins. The build uses `--pull`
  and `--no-cache`, passing these newly resolved immutable references. This
  refreshes the development bases and RPM installation without automatically
  changing the SEMOSS release or locked application/Python artifacts.
- Loads the candidate locally and repeats the FIPS/JDBC startup probe and Python
  dependency/worker/audio checks without network access, with a read-only root,
  no capabilities, and a noexec temporary filesystem. Bedrock checker unit tests
  run under the same restrictions inside the candidate, where their required
  packaged SEMOSS Python modules are available; no AWS request is made.
- Only after those checks pass, logs in to GHCR and pushes **that same image**,
  without rebuilding. The tag is
  `sha-<full-commit>-<run-id>-<run-attempt>`; no `latest` tag is produced.
- Records the published `ghcr.io/semoss/semoss-il4@sha256:...` reference in the
  run summary. Use that digest for test deployment, with separately provisioned
  BCFKS TLS secrets as described below.

Action revisions are SHA-pinned. The image source/revision labels identify the
build repository and the actual checked-out `IL4-dev` commit, including for
scheduled runs; the SEMOSS source reference remains documented
above and release artifact hashes remain in the artifact lock. Registry
credentials use a job-specific Docker config.
The three resolved base references are recorded in the run summary and the
`org.semoss.base.maven`, `org.semoss.base.ubi`, and `org.semoss.base.python` image
labels. Record the published application image digest for rollback; a later
weekly build intentionally may use different base contents.
The load/test/push path deliberately disables BuildKit attestations because they
are not reliably preserved by a classic local Docker image-store round trip.
The image still contains `/opt/provenance`, but this is **not a signed registry
attestation or a complete SBOM**. Approved scanning, SBOM generation, signing,
and admission policy must be added before production promotion.

For workflow changes, run the following from the repository root with actionlint
and ShellCheck installed:

```sh
actionlint .github/workflows/il4-container.yml
```

These validation tools run locally; no source upload to a third-party lint
service is required.

This build gate does **not** start Tomcat or exercise a live HTTPS handshake,
backend readiness, administrator login, or live connectors. Follow the runtime
and acceptance procedures below on the published digest. BC-FIPS/BCJSSE TLS
configuration is preserved; successful offline checks alone do not prove an
IL4-authorized deployment. A self-hosted runner and Iron Bank base also do not
authorize GitHub/GHCR to hold CUI, sensitive build inputs, or operational secrets;
obtain the applicable approvals before using those services.

## TLS and runtime

Provision these read-only secret files, readable by UID 10001:

| File inside container | Contents |
| --- | --- |
| `/run/secrets/server.bcfks` | BCFKS private-key entry and complete server certificate chain |
| `/run/secrets/server.password` | Keystore/key password (use the same password for both) |

Use organization-issued certificates and approved key sizes/algorithms. The
private key is not part of the image. Tomcat reads its password from the file,
not a JVM argument. Missing TLS secrets cause startup to fail explicitly.

The generated `/opt/fips/cacerts.bcfks` contains the builder JDK's public trust
anchors; `changeit` is the integrity password for this **public-certificate-only**
store, not a private-key password. Replace it with an approved, appropriately
limited BCFKS truststore at that path when enterprise/internal CAs are needed;
keep its integrity password consistent with [setenv.sh](./conf/setenv.sh).

Example, after provisioning `./tls` with the two secret files:

```sh
docker volume create semoss-home
docker run --detach --name semoss --platform linux/amd64 \
  --read-only --cap-drop=ALL --security-opt=no-new-privileges \
  --pids-limit=512 --memory=8g --cpus=4 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev \
  --tmpfs /opt/tomcat/temp:rw,noexec,nosuid,nodev,uid=10001,gid=10001 \
  --tmpfs /opt/tomcat/work:rw,noexec,nosuid,nodev,uid=10001,gid=10001 \
  --tmpfs /opt/tomcat/logs:rw,noexec,nosuid,nodev,uid=10001,gid=10001 \
  --mount type=volume,src=semoss-home,dst=/opt/semosshome \
  --mount type=bind,src="$(pwd)/tls",dst=/run/secrets,readonly \
  --publish 127.0.0.1:8443:8443 \
  semoss:5.4.0-ubi10-python314-bcfips
```

Docker initializes a **new named volume** from the image's semosshome payload.
An empty bind mount or Kubernetes PVC will hide that payload: seed it in a
controlled init step first. Existing volumes retain their old configuration;
image rebuilds do not update it. Back up databases before migration. This example
uses local bundled databases and is not an HA/external-database architecture.
For an existing home, explicitly migrate the four Python RDF properties listed
above and deploy the patched audio script from the new image. Updating only the
image leaves the old script hidden by the volume. Preserve customizations and
compare script hashes; do not overwrite an existing data volume wholesale.

UI: `https://localhost:8443/SemossWeb/`. Use the DNS name on your certificate.
Backend readiness: `/Monolith/health/ready`, which must return HTTP 200 with
`startupComplete: true`; HTTP 200 from the UI alone is not sufficient.

```sh
curl --fail --cacert /secure/approved-ca.pem \
  https://semoss.example.mil:8443/Monolith/health/ready
docker logs semoss
```

Configure an orchestrator HTTPS readiness probe that verifies that endpoint and
its certificate. No insecure `curl -k` health check is included. Centralize
stdout/stderr and application audit logs; Tomcat access logs omit query strings.
Bootstrap/admin endpoints and authentication policy must be restricted before
network exposure. Review data-source passwords, SSO, proxy trust, upload limits,
external databases, backup, audit retention, and network policy for your deployment.

`CATALINA_OPTS` can set reviewed heap limits. The entrypoint rejects global
`JAVA_TOOL_OPTIONS`, `JDK_JAVA_OPTIONS`, and `JAVA_OPTS` injection. Treat all
deployment-controlled JVM flags as trusted configuration.

## FIPS boundaries

The startup and build check exercises BC self-tests and approved-only mode,
provider ordering, secure random, SHA-256, AES-256-GCM, PBKDF2, BCFKS trust loading,
and default BCJSSE key/trust managers and TLS context.

BC native acceleration is explicitly disabled using its supported pure-Java
selector so no native BC library must be loaded from executable temporary
storage. Tomcat session randomness uses `BCFIPS/DEFAULT`. BCJSSE-compatible
algorithm restrictions disallow SHA-1 signatures and weak keys rather than
silently relying on unsupported JDK constraint syntax.

**This does not certify the whole application or make the host FIPS-enabled.**
Python's OpenSSL and cryptography packages do not route through Java's BCFIPS
provider. The Python donor's catalog status and a successful Python smoke test
do not establish FIPS operation for Python TLS, cryptography, or AI libraries.
Only the cryptographic module has a CMVP validation boundary. `SUN` remains
registered for JVM functionality and can still supply algorithms outside BCFIPS;
pure-Java algorithms and explicitly selected providers need separate analysis.
Java 25 + this OS/module combination must be checked against the module's
applicable security policy and your authorization requirements. The
[certificate 4943 page](https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/4943)
reviewed for this template lists Java 8/11/17/21 test environments, not Java 25.
Do not infer Java 25 validation from a successful runtime test.

FIPS-mode PBKDF2 has password/key-strength constraints (commonly at least 112 bits
of password input). Test real database authentication, stored encrypted data,
SSO, and enabled connectors; a provider swap can expose compatibility issues.

## Validation and release notes

```sh
python3 -m unittest discover -s tests -v
sh -n entrypoint.sh conf/setenv.sh python-dependencies/install.sh
docker run --rm --platform linux/amd64 --network none \
  --read-only --cap-drop=ALL --security-opt=no-new-privileges \
  --tmpfs /tmp:rw,noexec,nosuid,nodev --workdir /tmp \
  --entrypoint /bin/sh semoss:5.4.0-ubi10-python314-bcfips -c \
  'python /opt/python-check/validate.py &&
   python /opt/python-check/python_check.py &&
   python /opt/python-check/check_audio.py'
```

Verified after Python integration on `linux/amd64`:

- Runtime OS reports RHEL/UBI 10.2 with glibc 2.39.
- Full image build and non-root Java 25/FIPS crypto probe passed.
- HTTPS UI returned 200; `/Monolith/health/ready` returned
  `{"startupComplete":true,"status":"READY"}`.
- Session creation returned 200 and Secure/HttpOnly/SameSite=Lax cookies.
- Missing TLS secrets and global JVM-option injection were rejected.
- All 260 installed Python distributions exactly matched the lock, without extra
  distributions; the venv was non-writable by runtime UID 10001.
- All 29 dependency checks, the actual packaged SEMOSS worker calculation, and
  audio pipeline construction/event checks passed with networking disabled,
  a read-only root, no capabilities, and a noexec temporary filesystem.
- 32 isolated assembly/HTTP/Python unit tests passed. Standard-library trace
  measured executable-line coverage of 99.1% for assembly, 98.3% for the functional
  HTTP client, 100% for the audio patch, and 98.8% for the worker checker.
  Eight additional Bedrock tests passed with 92.2% checker coverage.
  These measurements are not branch coverage.
- Both webapps deployed and backend readiness passed. ESAPI logged filesystem
  lookup fallbacks, then loaded its properties successfully from the classpath;
  its existing missing logging-property warnings remain visible.

No production SSO, user login, external connector, or application workload has
been certified by these smoke tests. The config API on a fresh home redirects
to initial-admin setup, as expected.

The latest local JDBC-overlay image is
`sha256:e1d25218b3a0e87415d05b7501378374e6b7f55ef22f072afedf3a45af59a4ed`
(local image index, not a published registry reference). The existing
localhost-only test deployment was recreated from it with its home/TLS volumes
and administrator intact. CA-verified readiness, native login/CSRF, and a real
Python/pandas calculation returning `12` passed after replacement.
The default scripting language is now Python and native registration is disabled.
Its disposable localhost certificate is for testing only; replace it before its
seven-day validity expires. The default Docker bridge is not egress isolation.

The Docker build runs unit tests, verifies payload hashes, scans BC class
duplication, compiles the crypto probe, and runs it as UID 10001 on UBI. It also
verifies the Python dependency graph and runs the worker/audio integration checks.
Runtime smoke testing must additionally cover HTTPS, backend readiness, initial
admin setup/login, session cookies, and each connector you intend to enable.
An untested feature is not implied to work by a passing crypto check.

Initial template: pinned SEMOSS 5.4.0 with an explicit BC FIPS overlay and
documented Snowflake/Python/notification exclusions; no SEMOSS source changes.

UBI 10.2 migration: changed only the final OS base, image title, and deployment
documentation. Maven/JDK, SEMOSS, Tomcat, FIPS artifact pins, and feature flags
remain unchanged.

Python integration: added the digest-pinned Iron Bank Python 3.14.7 runtime,
complete hash-locked CPU dependencies, documented compatibility updates, and
the separately approved SEMOSS audio event patch. Python is enabled; R,
notifications, and the excluded Snowflake connector remain unchanged.

Functional/JDBC integration: corrected absolute HTTPS redirects and the default
scripting language, preserved noexec protection with immutable SQLite JNI loading,
and added the approved SHA-pinned MariaDB/SQL Server driver replacements.
Startup checks exercise the actual drivers' strict TLS property parsing under
BCJSSE. The live Bedrock adapter check and offline safety tests are reproducible;
production database endpoints, secrets-backend registration and live connector
acceptance tests remain deployment prerequisites.

GitHub build preparation: added an IL4-dev CodeBuild
workflow with Iron Bank authentication, restricted offline image checks, and
GHCR publication of the exact tested candidate. Added an upload allowlist;
runner/environment prerequisites and the first successful GitHub run must be
verified before using the image. No production deployment or authorization
is implied. The owner approved existing repository credentials and no
environment approval gate for the initial test build.
Development refresh: resolve fresh digests for the same three Iron Bank version
tags on every run, use uncached builds, and record the inputs in image labels.
A weekly default-branch workflow checks out `IL4-dev`; its scheduling becomes
active only after the corresponding workflow PR is merged into `dev`.

Local PostgreSQL validation: added a private-network TLS/SCRAM fixture and the
`LocalPostgresTLS` connector without changing the application image. The local
runtime now mounts a read-only truststore containing the disposable PostgreSQL
test CA. PostgreSQL has no published host port. Normal and metadata-driven
SEMOSS queries passed with a read-only account; the optional `SMSS_DATEDIFF`
auto-creation attempt was denied and remains a documented limitation.
