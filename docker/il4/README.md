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
| pyarrow | 20.0.0 | 23.0.1 | First CPython 3.14 wheels (22.0.0), then CVE-2026-25087 |
| datasets | 2.14.3 | 4.4.0 | Arrow API and Python 3.14 pickling compatibility |
| pipecat-ai | 0.0.103 | 1.0.0 | Compatible ONNX Runtime, Numba, and soxr dependencies |
| pipecat-ai | 1.0.0 | 1.4.0 | Closes CVE-2026-44716, CVE-2026-54695 (AWS Inspector2 findings) |

Additional constraints are `dill>=0.4.0` and `protobuf<7`; the upstream security
overrides remain applied. Exact pins, failed older candidates, source references,
and caveats are recorded in [compatibility.json](./python-dependencies/compatibility.json).
Datasets 4.x no longer supports dataset loading scripts. Review existing saved
datasets, pickle migrations, custom loaders, and external model integrations
before migrating production data.

`fsspec` stays pinned at 2023.10.0 rather than bumped to 2026.6.0: reaching
that version needs `datasets>=5.1.0`, which needs `pyarrow>=24.0.0`, reopening
the pyarrow pin above. CVE-2026-104851 (GHSA-27vj-qcqg-25rc, RCE via
`ReferenceFileSystem`'s Kerchunk template parsing, a feature SEMOSS does not
use) is closed instead with a targeted, hash-guarded source patch
([fsspec_compat.py](./python-dependencies/fsspec_compat.py)) applying the
exact upstream fix. See `applied_source_patches` in
[compatibility.json](./python-dependencies/compatibility.json) for the full
verification, including reproducing the advisory's RCE proof-of-concept
against both the unpatched and patched package.

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
   It lacks Python, so the workflow explicitly installs the Ubuntu `python3`
   package for orchestration and isolated source tests. This requires access
   to the tooling image's configured Ubuntu package repositories and does not
   change the Iron Bank Python runtime in the published image.
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

### Optional SBOM and vulnerability reports

Reporting is **off by default**. For an individual manual run on `IL4-dev`,
select the `image_reports` input. To opt in for push and scheduled builds, set
the repository Actions variable `IL4_IMAGE_REPORTS` to the exact value `true`.
This change does not set that variable or activate the still-pending schedule.

Reports run **after successful publication**, inspect the same local image,
and never gate publishing on vulnerability severity. Trivy 0.74.0 is downloaded
from its versioned release and checked against a pinned SHA-256. It runs locally
on the build runner, not through a hosted scanning service. It requires egress
for the release, vulnerability databases, and Java metadata; image contents
are not uploaded to a third-party scanner.

The optional GitHub artifact `il4-image-reports-<run-id>-<run-attempt>` contains:

- `vulnerabilities.json`: OS/library vulnerability matches, including unfixed
  findings; only the vulnerability scanner is enabled, not secret scanning.
- `sbom.cdx.json`: CycloneDX inventory converted from the same scan's package
  inventory.
- `image-reference.txt` and `image-digests.json`: the image that was analyzed.
- `scanner-version.json`: scanner and downloaded database version information.

Artifacts expire after **14 days**; they are development diagnostics, not a
long-term audit archive or signed attestation. The repository is public: review
report visibility before opting in and never scan images containing sensitive
inputs here. Scanner/DB failures and artifact-upload failures are explicitly
reported in the run summary and warnings, while remaining non-blocking. A
partial artifact or a green publishing job must not be treated as a clean scan.
Disabling the input/variable returns to the existing publishing-only behavior.

This build gate does **not** start Tomcat or exercise a live HTTPS handshake,
backend readiness, administrator login, or live connectors. Follow the runtime
and acceptance procedures below on the published digest. BC-FIPS/BCJSSE TLS
configuration is preserved; successful offline checks alone do not prove an
IL4-authorized deployment. A self-hosted runner and Iron Bank base also do not
authorize GitHub/GHCR to hold CUI, sensitive build inputs, or operational secrets;
obtain the applicable approvals before using those services.

## TLS and runtime

See [HTTPS certificate staging and replacement](./TLS.md) for explicit
development-only generation of a unique seven-day self-signed certificate,
organization-issued chain/private-key packaging, and controlled certificate
rotation. No shared private key or automatic runtime fallback is built in.
Outbound [AWS GovCloud RDS trust](./RDS.md) is a separate configuration surface.

Use the [portable configuration baseline and deployment checklist](./DEPLOYMENT-CHECKLIST.md)
to separate reusable container safeguards from site-specific identity, network,
storage, audit, and authorization requirements. It includes prerequisites,
post-deployment acceptance, evidence ownership, and recurring checks; completion
does not itself establish IL4 authorization. FIPS alternatives are being
evaluated, but the default cryptographic stack is unchanged and its evidence
gaps remain open.

Provision these read-only secret files, readable by UID 10001:

| File inside container | Contents |
| --- | --- |
| `/run/secrets/server.bcfks` | BCFKS private-key entry and complete server certificate chain |
| `/run/secrets/server.password` | Keystore/key password (use the same password for both) |

Use organization-issued certificates and approved key sizes/algorithms. The
private key is not part of the image. Tomcat reads its password from the file,
not a JVM argument. Missing TLS secrets cause startup to fail explicitly.

The generated `/opt/fips/cacerts.bcfks` contains the builder JDK's public trust
anchors plus six pinned GovCloud RDS roots; `changeit` is the integrity password for this **public-certificate-only**
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

### Opt-in hardened development deployment

[compose.dev.yml](./compose.dev.yml) provides a separate `hardened-dev` profile.
It does not change the image or any running container. Java, providers, cipher
settings, and existing TLS requirements are unchanged. Authentication, existing
data, repository permissions, and outbound connectivity are also unchanged.

From this directory, with already-provisioned TLS and CA files:

```sh
export SEMOSS_IMAGE='ghcr.io/semoss/semoss-il4@sha256:060aa47f04e5f6c69c8fc6e6d020f0beae5e9ab0469df6f1ebc5eb3091ec73ae'
export SEMOSS_TLS_KEYSTORE='/absolute/path/server.bcfks'
export SEMOSS_TLS_PASSWORD='/absolute/path/server.password'
export SEMOSS_READINESS_CA='/absolute/path/approved-ca.pem'

docker compose -p semoss-hardened-dev -f compose.dev.yml \
  --profile hardened-dev config --quiet
docker compose -p semoss-hardened-dev -f compose.dev.yml \
  --profile hardened-dev up -d
```

Use an approved image digest; the example identifies the previously tested
development image, not an automatically updated tag. The host paths must exist;
missing files are not silently created. The configuration command validates
Compose structure, not file readability or certificate validity.
Bind mounts do **not** fix ownership or permissions: provision files readable
by container UID/GID `10001:10001`, restricting private-key/password access.
Port 8443 must be free; the profile will not replace an existing container
already using it. Use a distinct, unused Compose project name for a new instance.

The profile enforces the existing documented runtime restrictions: non-root,
read-only root, dropped capabilities, no new privileges, noexec/nosuid/nodev
temporary mounts, and memory/CPU/PID limits. A 60-second stop grace period
allows orderly shutdown. Docker's local logging driver bounds each container's
diagnostic logs to three 10 MB files (plus rotation/compression overhead).
Those logs are not centralized audit retention; existing logs are not migrated.

A new project-scoped home volume is seeded from the image. Subsequent launches
retain it; existing homes are not selected, migrated, overwritten, or deleted.
Do not use `down -v` for data you intend to keep. Keep these files available:
the readiness helper is bind-mounted read-only rather than baked into the image.

[readiness.py](./readiness.py) verifies CA trust and hostname, HTTP 200, and the
exact JSON boolean `startupComplete: true`. It rejects redirects, credentials
in URLs, malformed/oversized responses, and bounded-timeout failures without
logging response bodies. The default URL is
`https://localhost:8443/Monolith/health/ready`; its hostname must match the
certificate. If setting `SEMOSS_READINESS_URL`, use a certificate-matching
hostname that resolves to **this container**, not another deployment.
Health checks report status only: Compose does not restart an unhealthy
container automatically. This Python probe does not establish FIPS validation
or IL4 compliance, and does not weaken TLS verification.

Run its hermetic tests with
`python3 -m unittest discover -s tests -p 'test_readiness.py' -v`.
The helper and its tests are deliberately excluded from the image build
context; the full host-side test suite includes them.

`CATALINA_OPTS` can set reviewed heap limits. The entrypoint rejects global
`JAVA_TOOL_OPTIONS`, `JDK_JAVA_OPTIONS`, and `JAVA_OPTS` injection. Treat all
deployment-controlled JVM flags as trusted configuration.

### Read-only runtime configuration and drift audit

[audit_runtime.py](./audit_runtime.py) is a host-side, standard-library Python
3.9+ helper for Docker deployments. It only inspects an existing container and
an already-local expected image; it does not pull, start, stop, modify, or repair
anything. The caller needs Docker inspection access. Other orchestrators need
equivalent runtime-specific inspection rather than fabricated Docker metadata.

```sh
umask 077
python3 audit_runtime.py \
  --container semoss-hardened-dev-semoss-1 \
  --expected-image "$SEMOSS_IMAGE" \
  > /secure/evidence/runtime-current.json
```

Use a repository-qualified `@sha256:` image reference, not a tag. The audit
compares the container's actual image ID to the inspected expected image and
checks that image's repository digests; a matching configured tag is not enough.
The expected image must already be available to the Docker daemon.

The 17 named checks cover image identity, running/healthy state, numeric
non-root identity, no privileged mode, dropped capabilities, no new privileges,
read-only root, positive memory/CPU/PID limits, namespace isolation, device and
runtime-socket exposure, writable home, allowed writable mounts, hardened
temporary mounts, read-only TLS mounts, bounded local logs, and a health check.
The filesystem/logging checks target this template's reference layout and
Docker `local` logging pattern, not every valid production architecture.
Loopback/non-loopback publication counts are recorded, not treated as a
universal ingress policy. Resource sizes remain workload-specific.

Default output is **report-only**: inspect `checks` even when exit status is 0.
Use `--strict` to opt into exit 1 for a failed check or detected drift. Invalid
input, malformed inspection/baseline data, unavailable Docker resources, or
inspection failure returns 2 with a concise diagnostic, never a clean report.
Each Docker inspection has a 10-second timeout.

After reviewing a successful report and the deployment manifest, save that
report as the approved baseline in the site's controlled evidence repository.
Compare later without overwriting that baseline:

```sh
python3 audit_runtime.py \
  --container semoss-hardened-dev-semoss-1 \
  --expected-image "$SEMOSS_IMAGE" \
  --baseline /secure/evidence/runtime-approved.json --strict \
  > /secure/evidence/runtime-current.json
```

Reports contain `schema_version`, `scope`, `image_id`, named boolean `checks`,
a normalized `snapshot`, and `drift` with `compared`, `detected`, and changed
field names. Baselines must be valid versioned reports; they cannot waive the
built-in checks. Snapshots exclude environment values, host paths, secret
contents, raw health output, and publication addresses. Comparison covers
only this normalized subset, including observed health/running state; it will
not detect every manifest difference, changed mounted file content, credential
rotation, or application configuration change. Compare reviewed manifests and
appropriate content evidence separately. Keep reports controlled.

This metadata audit cannot prove effective kernel enforcement, actual file
permissions, certificate trust, readiness endpoint identity, FIPS validation,
or authorization. Test those separately using the deployment checklist.
It does not impose egress policy, schedule checks, or send alerts.

Run all host-side hermetic tests from this directory:

```sh
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

The audit, readiness helper, and their tests remain excluded from the image
build context. Adding them does not change the built image or existing data.

### Isolated portable acceptance result

On 2026-10-01, the published image
`ghcr.io/semoss/semoss-il4@sha256:cad8db15ce6704e728f5a23a4a75b86a0b543aead097585000fdb9ab70358b1f`
was pulled by digest and tested with the opt-in Compose profile on Docker
Desktop, running `linux/amd64` on an Apple Silicon development host. Tests used
a new, disposable home and short-lived synthetic localhost TLS material, not
an existing deployment or organization-issued production credentials.

- All 76 host-side unit tests passed, including 19 audit tests. Audit
  executable-line coverage was 98.2%.
- All 17 live metadata checks passed. Unchanged-baseline and synthetic-drift
  comparisons verified report-only and strict exit behavior.
- Verified HTTPS UI and backend readiness passed; wrong hostname and untrusted
  certificate connections were rejected. The readiness helper's positive and
  wrong-hostname paths were exercised against the live instance.
- Effective UID/GID, zero capabilities, no-new-privileges, seccomp filtering,
  denied root writes, denied execution from temporary storage, temporary mount
  options, required writable paths, and restricted TLS file permissions passed.
  HTTP TRACE was rejected and the synthetic query canary was absent from logs.
- Existing Java/provider/JDBC checks, the Python worker/data-science checks,
  audio integration, and eight mocked Bedrock adapter tests passed under the
  hardened profile. These were not external-service or FIPS certification tests.
- A measured SIGTERM shutdown completed in about 6 seconds with Java exit 143
  (not forced SIGKILL/OOM), and restart became healthy in about 47 seconds.
  Synthetic data in the isolated home survived. Tomcat emitted application
  thread/ThreadLocal cleanup warnings; this was not a warning-free shutdown.
- Local Trivy 0.74.0 report generation and CycloneDX 1.7 conversion completed
  for the exact digest, with 1,366 SBOM components and vulnerability findings.
  Detailed reports remain outside source control. This was not a clean scan or
  production acceptance, and the GitHub artifact-upload path was not exercised.
  Conversion also reported some unrecognized license expressions.

Repeat acceptance on the actual host/orchestrator, ingress, IdP, and storage.
This local exercise does not complete site checklist items, test SSO/production
login, approve vulnerability dispositions, or resolve the FIPS evidence hold.

## FIPS boundaries

### Alternative-provider feasibility (2026-10-01)

No provider migration has been applied to this image or the publishing workflow.
Two alternatives were investigated separately from the working deployment:

| Candidate | Evidence | Disposition |
| --- | --- | --- |
| Red Hat OpenJDK 25 / NSS on RHEL 9 | Certificate #5022 identifies NSS module `3.90.0-4408e3bb8a34af3a`, supplied in `nss-softokn` and `nss-softokn-freebl` `3.90.0-6.el9_2`. The current public UBI9/OpenJDK25 image inspected contains NSS `3.124.0-5.el9_6`, not this documented baseline. Red Hat Java25 support starts at RHEL9.7; the validated RPMs are from the RHEL9.2 EUS stream. | Paused: no supported public package/runtime combination was established. Do not blindly downgrade NSS or assume the newest package is validated. |
| ACCP-FIPS / AWS-LC | Maven Central's published `2.5.0` Linux x86-64 artifact was checksum-verified and reports AWS-LC-FIPS `3.0.0` at runtime. Isolated Java25 compatibility checks passed, but required PBKDF2 operations fell back to SunJCE. | Evaluation only: neither complete cryptographic routing nor exact certificate coverage was established. Do not promote it as a compliant replacement. |

The inspected Red Hat base was
`registry.access.redhat.com/ubi9/openjdk-25@sha256:68f712c440bccc81af8a1935dda4beff4fbed6466f4037f6d1437c0f455328bc`.
Its existence does not establish NSS certificate applicability. Exact validated
RPMs were absent from the current public UBI repositories checked; Red Hat
documents subscription access for superseded packages. A historical public image
containing the exact binaries was not verified. Acquisition and support for the
combined Java/module/container/host environment remain unresolved.

Compatibility constraints discovered during this investigation:

- Trino JDBC 476 contains base class files at version 66 and needs Java 22+.
  A Java21 downgrade would break that connector without a reviewed driver change.
- The current Python3.14 donor uses RHEL10 RPMs. A RHEL9 migration needs a new
  Python/dependency build; public RHEL9 Python3.12 packages were located but not
  integrated or tested with the full dependency set.
- CAC certificate parsing and GitHub App private-key parsing reference Bouncy
  Castle APIs. Removing all BC libraries would break those features. Replacing
  the registered cryptographic provider is distinct from removing parsing APIs.

The isolated ACCP probe used the existing published Java25/UBI10 image, not a new
SEMOSS release. The public artifact was
`software.amazon.cryptools:AmazonCorrettoCryptoProvider-FIPS:2.5.0:linux-x86_64`,
SHA-256 `55f20ce012de076137c35bb6d505ac87334e8315b4c762289bf1a225a4da3796`.
It demonstrated:

- Non-experimental FIPS-build reporting and passing provider self-tests.
- Default random, SHA-256, AES key generation, and AES-GCM routing to ACCP.
- PKCS12 in-memory key handling and hostname-verified SunJSSE TLS1.2/TLS1.3
  loopback handshakes.
- Native loading from a fixed read-only mount with non-root execution, dropped
  capabilities, read-only root, and noexec temporary storage.
- Explicit rejection when the native library was missing.
- **Missing ACCP PBKDF2WithHmacSHA256/SHA512 implementations** in this release;
  default calls selected SunJCE. A first-priority provider is not a complete
  approved-cryptography enforcement policy.

BC certificate-encoding APIs were used for the synthetic in-memory certificate;
BCFIPS/BCJSSE providers were not registered in the probe. This was not a full
Tomcat, JDBC, login/SSO, Python, or approved-service validation test.

The ACCP main-branch documentation describes `2.6.0` using AWS-LC-FIPS `3.1.0`,
but that release was not available from Maven Central when checked. Certificate
#5314 covers the static `3.1.0` module; it must not be attributed to the tested
`3.0.0` binary without separate evidence. Its listed operating environment is
Amazon Linux 2023 on specified hardware, not UBI10. Any applicable CMVP
user-affirmation portability route requires a documented, accepted assessment;
a local compatibility pass does not supply it. Python cryptography remains a
separate assessment regardless of Java provider.

Before another candidate is promoted, resolve exact module/build/certificate
mapping, approved-service routing (including PBKDF2 and keystores), operating
environment applicability, and the deployment's cryptographic boundary. Preserve
the current image while those gates remain unresolved.

Sources:

- [Red Hat compliance matrix](https://access.redhat.com/compliance/fips),
  [NSS certificate #5022](https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/5022),
  and [validated-package advisory](https://access.redhat.com/errata/RHSA-2024:0791).
- [Red Hat OpenJDK support matrix](https://access.redhat.com/articles/1299013)
  and [UBI content availability](https://access.redhat.com/support/policy/updates/ubi).
- [Published ACCP-FIPS versions](https://repo.maven.apache.org/maven2/software/amazon/cryptools/AmazonCorrettoCryptoProvider-FIPS/maven-metadata.xml)
  and [ACCP 2.5.0 documentation](https://github.com/corretto/amazon-corretto-crypto-provider/blob/2.5.0/README.md).
- [AWS-LC certificate #5314](https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/5314)
  and [its security policy](https://csrc.nist.gov/CSRC/media/projects/cryptographic-module-validation-program/documents/security-policies/140sp5314.pdf).

### Current BC-based image

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
The resolver and its mocked unit tests are also copied into the assembly stage;
they are not copied into the runtime image. The Docker context explicitly
re-excludes directory contents before allowing individual files, so opening a
parent directory does not accidentally include caches or unlisted files.

Additive development safeguards: optional hardened Compose profile,
CA-verified readiness, bounded local container logs, and opt-in post-publication
SBOM/vulnerability reports. Defaults, existing deployments and data, FIPS
configuration, authentication, and network access are unchanged.

Local PostgreSQL validation: added a private-network TLS/SCRAM fixture and the
`LocalPostgresTLS` connector without changing the application image. The local
runtime now mounts a read-only truststore containing the disposable PostgreSQL
test CA. PostgreSQL has no published host port. Normal and metadata-driven
SEMOSS queries passed with a read-only account; the optional `SMSS_DATEDIFF`
auto-creation attempt was denied and remains a documented limitation.
