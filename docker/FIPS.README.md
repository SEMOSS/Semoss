# FIPS 140-3 Builds

How SEMOSS produces a FIPS 140-3 image, what changes across the three repos, and
what is deliberately still out of scope.


## 1. What is actually being validated

FIPS 140-3 validates a **cryptographic module**, not an application. Tomcat and
SEMOSS are consumers. Three things have to be true:

1. A validated module is present
2. It operates in approved mode
3. All security-relevant cryptography goes through it

The module is **BouncyCastle BC-FJA** (`bc-fips`). The companion jars
(`bctls-fips`, `bcpkix-fips`, `bcutil-fips`) sit **outside** the certificate
boundary and call into it. Only `bc-fips` carries the CMVP certificate.

Note what this does NOT require: the JVM does not have to be incapable of
computing a non-approved algorithm. See section 8.


## 2. Layout across the repos

| Repo | Owns |
|------|------|
| **Semoss** | `Dockerfile.tomcat` (both tomcat bases), the six final Dockerfiles, all the docker workflows, `PBEncryptionUtility` |
| **semoss-artifacts** | `update_latest_dev.sh`, `artifacts/lib/pom.xml`, `context-*.xml` |
| **Monolith** | `libraries.xml` / `libraries-fips.xml`, the local dev overlay, `fips-check.sh` |

Two switches have to agree for an image to be correct:

```
-fips tomcat (base)        supplies bc-fips on the system classloader
libraries-fips (tarball)   leaves BouncyCastle out of WEB-INF/lib
```

One without the other gives you either two copies of `org.bouncycastle.*` across
two classloaders (ClassCastException on key objects) or none at all. Neither
fails at build time, which is why the pairing is wired to a single matrix value
rather than left to whoever runs the build.


## 3. Publish: Monolith

A normal deploy publishes **three** artifacts under one version. No flag:

```
monolith-<ver>.war                     shared by both image variants
monolith-<ver>-libraries.tar.gz        includes BouncyCastle
monolith-<ver>-libraries-fips.tar.gz   BouncyCastle removed
```

The war is shared because it contains only `semoss-<ver>-shaded-dependencies.jar`,
which Semoss builds and which does not differ between variants. Publishing it
twice would duplicate 82 MB for nothing.

`libraries-fips.xml` excludes BouncyCastle **by filename**, not by Maven scope.
That is what lets both tarballs come out of one build: scope changes what lands
in `target/<finalName>/WEB-INF/lib`, so it would need two builds, whereas a
filename filter reads the same directory twice.

Both the standard and `-fips` images resolve the same `SEMOSS_VERSION`, so both
tarballs must exist under that version. A Maven profile toggle would publish only
one of them per run and the other image would fail to resolve.

The deploy and release jobs assert the split is real in both directions: the fips
tarball must contain no BouncyCastle, and the standard one must still contain it.
The second half matters - a descriptor that excluded everything would otherwise
pass silently.


## 4. Build: the tomcat base

`docker/Dockerfile.tomcat` builds both variants from one definition, switched by
`FIPS_ENABLED`:

```
tomcat-builder:<ver>        FIPS_ENABLED=false   nothing FIPS-related installed
tomcat-builder:<ver>-fips   FIPS_ENABLED=true
```

With `FIPS_ENABLED=true` it, in this order:

1. Downloads the four BouncyCastle jars into `$JAVA_HOME/lib/fips`, verified
   against SHA-256 hashes pinned in the Dockerfile
2. Converts `cacerts` to BCFKS
3. Writes a BCFIPS / BCJSSE / SUN provider list to `java.security.fips`, leaving
   the JDK default `java.security` untouched. `setenv.sh` selects it with
   `-Djava.security.properties==`, so it applies to the Tomcat JVM only and
   Maven, keytool and the Playwright installer keep working TLS
4. Appends the FIPS block to `setenv.sh`

**Step 2 must precede step 3.** JKS integrity checking is SHA-1 based, so once
approved-only mode is in force keytool can no longer read the source truststore.

Hashes are pinned in the Dockerfile rather than fetched next to the jars.
Downloading an artifact and its checksum from the same source verifies nothing.

`tomcat-builder.yml` crosses `arch` with `fips` (4 build jobs, 2 merge jobs) and
asserts each variant is genuinely what it claims - including that the standard
image contains **no** FIPS artifacts, which catches `FIPS_ENABLED` leaking.


## 5. Build: the final images

Each OS workflow runs the same `fips` matrix and passes two build args:

| `matrix.fips` | `TOMCAT_TAG_SUFFIX` | `SEMOSS_FIPS` |
|---------------|---------------------|---------------|
| `false`       | (empty)             | `false`       |
| `true`        | `-fips`             | `true`        |

- `TOMCAT_TAG_SUFFIX` selects the base:
  `FROM ...tomcat-builder:${TOMCAT_VERSION}${TOMCAT_TAG_SUFFIX}`.
  Only the suffix crosses the workflow boundary, so `TOMCAT_VERSION` stays
  defined in exactly one place - the Dockerfile.
- `SEMOSS_FIPS` becomes an `ENV` that `update_latest_dev.sh` reads.

Resulting tags, with `-fips` on the flavour segment so `latest` stays trailing:

```
semoss-dev:<ver>-SNAPSHOT-ubuntu22-latest
semoss-dev:<ver>-SNAPSHOT-ubuntu22-fips-latest
semoss:<ver>-ubuntu22
semoss:<ver>-ubuntu22-fips
```

Same pattern for ubuntu24, ubi8, ubi9, al2023 and cuda.


## 6. Resolve: semoss-artifacts

`update_latest_dev.sh` runs inside the image build. It resolves
`org.semoss:monolith` from Sonatype and unpacks the war and the libraries tarball
into `$TOMCAT_HOME/webapps/Monolith`.

FIPS is opt-in, off by default. You can run FIPS by either:

```bash
SEMOSS_FIPS=true update_latest_dev.sh
update_latest_dev.sh --fips
```

Either form adds `-Dmonolith.lib.classifier=libraries-fips`, which
`artifacts/lib/pom.xml` uses to select the tarball. The war has no classifier
because it is shared.


## 7. Session IDs

Tomcat's `SessionIdGeneratorBase` defaults to `secureRandomAlgorithm="SHA1PRNG"`
with no provider. **BCFIPS does not implement SHA1PRNG**, so that lookup resolves
to SUN and JSESSIONID entropy comes from outside the validated module, with
nothing logged.

`context-*.xml` in semoss-artifacts carries a parameterised Manager:

```xml
<Manager secureRandomProvider="${SESSION_SECURE_RANDOM_PROVIDER:-}"
         secureRandomAlgorithm="${SESSION_SECURE_RANDOM_ALGORITHM:-SHA1PRNG}" />
```

The `:-` defaults reproduce stock Tomcat exactly, so the file is safe everywhere.
The `-fips` base defaults both variables to `BCFIPS` / `DEFAULT` in `setenv.sh`,
and a container environment value overrides that.

Env var substitution in Tomcat config needs
`org.apache.tomcat.util.digester.PROPERTY_SOURCE=...EnvironmentPropertySource`,
which `setenv.sh` sets for all images. Without it the `:-` defaults still apply,
so nothing breaks on an older base.

The setenv.sh is predefined in Dockerfile.tomcat inside the FIPS_ENABLED=true block.


## 8. What approved-only mode does and does not catch

`-Dorg.bouncycastle.fips.approved_only=true` constrains **BCFIPS**. It cannot
remove algorithms from another provider.

SUN has to stay registered: BCFIPS seeds its DRBG from a core `SecureRandom`, and
without SUN that lookup recurses into BCFIPS and dies with `StackOverflowError`.
SUN as a seed source is fine for compliance - the approved DRBG is still BCFIPS's.

The consequence is that **SUN also serves MD5**, so `MessageDigest.getInstance("MD5")`
succeeds. Non-approved algorithm use in application code will not fail at
runtime and has to be caught by code review.

Things that DO fail loudly:

- Passwords under 112 bits through PBKDF2 (see section 10)
- Any BCFIPS-mediated non-approved operation

Things that fail silently:

- MD5 and other SUN-provided algorithms
- Pure-Java crypto that never touches JCA, such as bcrypt


## 9. Verification

`Monolith/local-docker-testing/fips-check.sh` exercises the primitives rather
than reading config, and exits non-zero on failure:

```bash
./fips-check.sh                              # running container named "semoss"
./fips-check.sh my-container
./fips-check.sh --image local-monolith-fips
```

It checks provider order, that SecureRandom and SHA-256 come from BCFIPS,
AES-256-GCM, PBKDF2, that a sub-112-bit password is rejected, that the BCFKS
truststore loads, and that `context.xml` resolves session IDs to BCFIPS through
the same `IntrospectionUtils` path the Tomcat Digester uses.

It also reports which provider serves MD5, as a standing reminder of section 8.


## 10. Operational constraints

**Credentials must be at least 14 characters.** SP 800-132 puts a 112-bit floor
on PBKDF2 password input and BC-FIPS enforces it. PostgreSQL authenticates with
SCRAM-SHA-256, which runs the password through PBKDF2, so a shorter password
fails before a connection is attempted:

```
FipsUnapprovedOperationError: password must be at least 112 bits
  at org.postgresql.shaded.com.ongres.scram.common.CryptoUtil.hi
```

That reads like a database problem and is not - the driver refuses to compute the
hash. The same floor applies to MinIO/S3 keys and to
`PM_SEMOSS_EXECUTE_SQL_ENCRYPTION_PASSWORD`.

`POSTGRES_PASSWORD` only applies at `initdb`, so an existing stack needs
`docker compose down -v`.

**Cross-mode caution:** `PBEncryptionUtility` will encrypt with a short password
on a non-FIPS deployment, and that ciphertext cannot be decrypted on a FIPS one.
Use 14+ characters everywhere.

**SFTP is reduced under FIPS.** The `fips` profile strips sshj's non-FIPS
BouncyCastle, so sshj falls back to its JCE-only algorithm set: it loses
`curve25519-sha256` KEX and `ssh-ed25519` host keys, and keeps
`ecdh-sha2-nistp256`, `diffie-hellman-group14/16-sha256`, `aes-ctr`, `aes-gcm`
and `hmac-sha2-*`. Those are the approved ones. `SFTPStorageEngine` authenticates
with a password and never loads a key, so no key-parsing path is affected.


## 11. Local development

```bash
cd Monolith/local-docker-testing
./createLocalFipsDockerScript.sh          # build + tag
./createLocalFipsDockerScript.sh --push   # and publish
```

Builds Monolith with `-P dev,fips`, verifies no BouncyCastle in the war, then
overlays it on a `-fips` base via `Dockerfile.fips`.

This is the **only** place the Maven `fips` profile is used. CI does not use it,
because the tarball split handles the same thing and yields both variants from
one build. Local dev needs it because `Dockerfile.fips` consumes the raw `dev`
war, which has no `packagingExcludes` and no tarball split.

Compose stacks are in `local-docker-compose-fips/`, with project names prefixed
so a FIPS and a non-FIPS stack can run side by side.