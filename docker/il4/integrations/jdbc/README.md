# Hardened SEMOSS database clients

This configures **clients of existing servers**, not PostgreSQL, SQL Server, or
MariaDB servers. Live connections still need approved endpoints, certificates and
authentication. No production server endpoint or credential has been supplied.
A local synthetic PostgreSQL fixture has now been tested as described below.

## Reviewed dependency overlay

| SEMOSS `RDBMS_TYPE` | Driver | Release-bundle version | Selected version |
| --- | --- | --- | --- |
| `POSTGRES` | `org.postgresql.Driver` | 42.7.11 | 42.7.11 (unchanged) |
| `SQL_SERVER` | `com.microsoft.sqlserver.jdbc.SQLServerDriver` | 11.2.4.jre11 | 13.6.0.jre11 |
| `MARIA_DB` | `org.mariadb.jdbc.Driver` | 1.1.9 | 3.5.10 |

The owner explicitly approved the two JDBC upgrades as a documented deviation
from the assessed SEMOSS 5.4 bundle. The original Java/web application binaries
are retained. MariaDB 1.1.9 is unsupported; Microsoft documents Java 25 support
for the selected 13.6 release.

[artifacts.lock.json](../../artifacts.lock.json) pins both replacement JAR hashes
from Maven Central. Assembly rejects missing or duplicate original drivers,
removes only the two expected originals, and records old/new hashes and Maven
coordinates in `/opt/provenance/assembly.json`. Replacement JARs undergo the same
bundled-BC-class scan as the application. Maven transitive dependencies are not
silently added. Optional authentication systems, such as Azure identity/Kerberos,
are not implied to be provisioned by this SQL-password validation path.

Sources:
[MariaDB support](https://mariadb.com/docs/connectors/mariadb-connector-j/about-mariadb-connector-j),
[SQL Server release notes](https://learn.microsoft.com/en-us/sql/connect/jdbc/release-notes-for-the-jdbc-driver),
[SQL Server FIPS options](https://learn.microsoft.com/en-us/sql/connect/jdbc/fips-mode),
[PostgreSQL SSL](https://jdbc.postgresql.org/documentation/ssl/).

## Strict connection profiles

[JdbcTls.java](../../JdbcTls.java) is the single source for the tested URL profiles.
Print the exact URLs without making a network connection:

```sh
docker run --rm --platform linux/amd64 --network none --read-only \
  --cap-drop=ALL --security-opt=no-new-privileges \
  --tmpfs /tmp:rw,noexec,nosuid,nodev \
  --entrypoint /bin/sh semoss:5.4.0-ubi10-python314-bcfips -c \
  '. /opt/tomcat/bin/setenv.sh
   java $CATALINA_OPTS -cp "/opt/fips/*:/opt/fips-check:/opt/tomcat/webapps/Monolith/WEB-INF/lib/*" JdbcTls --templates'
```

Each line identifies the SEMOSS `RDBMS_TYPE` and corresponding `CONNECTION_URL`.
Replace `database.example.invalid`, the port and database name with approved
values; keep all strict TLS options. Do not put passwords in URLs.

- PostgreSQL: `sslmode=verify-full` with `DefaultJavaSSLFactory`, deliberately
  using the JVM BCFKS truststore/BCJSSE context rather than pgjdbc's default
  libpq-style PEM truststore. SCRAM-SHA-256 is recommended; test existing password
  strength against BC approved-mode PBKDF2 constraints.
- SQL Server: `encrypt=true`, `trustServerCertificate=false`, `fips=true`,
  BCFKS truststore, TLS 1.2. No hostname override is used: the endpoint DNS name
  must match the certificate. TLS 1.2 is selected for server compatibility, not
  because TLS 1.3 is inherently unsafe.
- MariaDB: `sslMode=verify-full`, explicit BCFKS truststore, TLS 1.2/1.3 and
  `allowLocalInfile=false`. Supplying explicit trust material avoids depending on
  MariaDB's server-version-specific zero-configuration TLS authentication.
- All profiles have connection and socket timeouts; the standalone probe also
  limits SQL statements to 30 seconds.

These are **reviewed per-connection profiles**, not global enforcement on every
SEMOSS engine. Administrators can still enter other URLs. Restrict engine
creation/configuration permissions and audit saved connection properties.

## Trust and secrets

Provision approved database CA certificates in the public-certificate-only
BCFKS truststore mounted read-only at `/opt/fips/cacerts.bcfks` in **both** the
application and the probe. The integrity password `changeit` in these profiles
is not a database or private-key password. Verify CA fingerprints out of band.
Use dedicated, least-privilege accounts and an appropriate server-side TLS policy.
Do not set `trustServerCertificate=true`, `sslMode=trust`, `sslmode=require`, or
any non-validating socket factory as a workaround.

The default generated truststore contains JDK public roots plus six pinned
[AWS GovCloud RDS roots](../../RDS.md), not arbitrary enterprise/private CAs.
Review whether to retain public roots or use a narrower trust set; replacing the
global store also affects Java outbound HTTPS and other Java integrations.
Python/Bedrock uses its separate PEM trust configuration.

SEMOSS's external database registration writes properties including passwords
to `.smss` unless a supported secrets connector is configured. Configure and
test that backend before production registration. An arbitrary `${ENV}` or
`PASSWORD_FILE` value is **not** established as supported secret resolution.
Do not send passwords in saved Pixel examples, chat, source control, or URLs.

## Live JDBC validation

The standalone probe accepts a Java-properties document on stdin:

```properties
RDBMS_TYPE=POSTGRES
HOST=database.example.invalid
PORT=5432
DATABASE=semoss
USERNAME=validation_account
PASSWORD=REPLACE_USING_APPROVED_SECRET_DELIVERY
```

Use `SQL_SERVER`/1433 or `MARIA_DB`/3306 as appropriate. The probe currently
supports DNS/IPv4 hosts, simple database names (letters, digits, underscore, dot,
hyphen) and username/password authentication. Input follows Java-properties
escaping rules; for example, literal backslashes must be doubled. Do not paste a
real password into a shell command. Use a restricted secret file delivered by
your approved secret system, or feed its output directly to stdin.

```sh
docker run --rm -i --platform linux/amd64 --read-only \
  --cap-drop=ALL --security-opt=no-new-privileges \
  --tmpfs /tmp:rw,noexec,nosuid,nodev \
  --mount type=bind,src=/secure/database-ca.bcfks,dst=/opt/fips/cacerts.bcfks,readonly \
  --entrypoint /bin/sh semoss:5.4.0-ubi10-python314-bcfips -c \
  '. /opt/tomcat/bin/setenv.sh
   exec java $CATALINA_OPTS -cp "/opt/fips/*:/opt/fips-check:/opt/tomcat/webapps/Monolith/WEB-INF/lib/*" JdbcTls --live' \
  < /secure/validation.properties
```

It executes `SELECT 1` and requires the server to confirm encryption for the
current session (`pg_stat_ssl`, `sys.dm_exec_connections`, or `Ssl_cipher`).
Failure returns nonzero with SQLState/vendor code instead of dumping driver
messages that may expose connection details. Keep driver debug logging off.
The SQL Server DMV may require additional diagnostic permissions on your server;
do not grant broad production privileges merely to make the probe pass.

Required acceptance tests before declaring an integration usable:

1. Correct CA and certificate-matching hostname connect and execute SQL.
2. Wrong CA and wrong hostname independently fail.
3. A server without TLS fails; invalid credentials fail.
4. A least-privilege account can access only the intended database objects.
5. Register the connector with the approved secrets backend and verify actual
   SEMOSS metadata discovery and a user-authorized read-only query.

Offline startup checks validate real driver loading, versions, strict URL
property parsing and the default BCJSSE context. Build tests additionally check
input injection rejection, the SQL/encryption-result checks and resource cleanup
using fakes. **They are not database TLS handshakes or live SEMOSS connector tests.**

## Verified local PostgreSQL fixture

Live validation completed on 2026-09-26 UTC:

| Item | Value |
| --- | --- |
| PostgreSQL container | `semoss-postgres-74b77ed9` |
| Test server | PostgreSQL 15.19, native arm64 |
| Image | `postgres@sha256:926f8799aef36e00001cfe15fba7abbd37d3c5224ea57e4c858e4bb670f10561` |
| Private Docker network | `semoss-postgres-test-74b77ed9` (internal) |
| Host-published PostgreSQL ports | None |
| PostgreSQL database | `semoss_validation` |
| Reader role | `semoss_reader` |
| SEMOSS catalog name | `LocalPostgresTLS` |
| SEMOSS engine ID | `45c17b26-2c88-4adc-9aee-4e308961db83` |
| Actual SEMOSS transport | TLS 1.3, `TLS_AES_256_GCM_SHA384` |

This cached upstream PostgreSQL image is a **local test fixture**, not an Iron
Bank production-server selection or a server-FIPS claim. It runs non-root with
a read-only root, dropped capabilities, no-new-privileges, bounded resources,
and named data/fixture volumes. TCP access requires TLS and SCRAM-SHA-256.
The SEMOSS application retains its original bridge for existing functionality
and is also attached to this internal database network; it is not egress-isolated.

Verified outcomes:

- Real Java/BCJSSE connection: `SELECT 1` and server-reported encrypted session.
- Wrong CA and wrong hostname: rejected with SQLState `08006`.
- Wrong password: rejected with SQLState `28P01`.
- Unencrypted PostgreSQL TCP request: rejected by the server's HBA rules.
- Reader has no superuser, create-role, create-database, or schema-create rights.
  A transactional `INSERT` attempt was denied, leaving sample data unchanged.
- `RdbmsExternalUpload` registered the database and discovered the selected
  `measurements` columns. The catalog entry is private to the local administrator.
- Reload from the persisted connector configuration succeeded.
- Raw SQL through SEMOSS returned `row_count=3`, `total_amount=12`.
- Metadata-driven `Select(measurements__id, measurements__amount)` returned
  `[1,2]`, `[2,4]`, `[3,6]`.
- An authenticated browser showed `LocalPostgresTLS` in the Database Catalog.

After local login, open the sidebar and select **Database**, or visit
`https://localhost:8443/SemossWeb/packages/client/dist/#/database`.
This Pixel expression can be run from the local Terminal:

```text
Database("45c17b26-2c88-4adc-9aee-4e308961db83")
  | Query("SELECT COUNT(*) AS row_count, SUM(amount) AS total_amount FROM public.measurements")
  | Collect(10);
```

The sample table contains only synthetic integers. Generated test credentials
remain in protected session storage; the local `.smss` file is mode `0600`.
The password also passes through SEMOSS's normal registration/audit path, so
this is **not** evidence of production secrets-backend integration.
Never substitute a production credential into this test procedure.

The read-only Java truststore mounted into the local application contains the
original Java roots plus a disposable PostgreSQL test CA. Its source is the
session's `files/local-test/postgres/cacerts.bcfks`; keep it in place while this
local deployment is in use. Test certificates expire after seven days.

### Observed read-only-account limitation

On connection, SEMOSS tries to create its optional `SMSS_DATEDIFF` function in
`public`. PostgreSQL correctly rejects that DDL for the reader role. SEMOSS logs
the error and continues: registration, reload, ordinary SQL and metadata-driven
queries all passed. Date-difference expressions depending on that helper are
not validated. Do not grant broad schema-creation privileges to suppress the
warning; review the helper provisioning/detection with the DBA before using it
in production. No application-binary patch was made for this behavior.

The containers and data remain available locally. Stop the fixture explicitly
when no longer needed with `docker stop semoss-postgres-74b77ed9`; this does not
delete its data volume. The SEMOSS catalog entry will then be unavailable until
PostgreSQL is restarted.
