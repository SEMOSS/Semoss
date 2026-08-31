# Supporting Engines

Optional standalone services you can run alongside SEMOSS.
Each is a self-contained compose file - bring one up, point SEMOSS at it, tear it
down when done.

| File | Service | Host port(s) | Auth | SEMOSS engine |
|------|---------|--------------|------|---------------|
| [semoss-weaviate.yml](semoss-weaviate.yml) | Weaviate 1.38.6 | 8081 (-> 8080) REST, 50051 gRPC | API key `test-key` | vector (`WEAVIATE`) |
| [semoss-chroma.yml](semoss-chroma.yml) | Chroma 1.0.0 | 8000 | none | vector (`CHROMA`) |
| [semoss-opensearch.yml](semoss-opensearch.yml) | OpenSearch 2.19.5 + Dashboards | 9200 (API), 5601 (UI) | admin / password | vector (`OPEN_SEARCH`) |
| [semoss-pgvector.yml](semoss-pgvector.yml) | Postgres 18 + pgvector | 5433 (-> 5432) | pgvector / pgvector | vector (`PGVECTOR`) |
| [semoss-clickhouse.yml](semoss-clickhouse.yml) | ClickHouse 25.8 | 8123 (HTTP), 9010 (-> 9000 native) | clickhouse / clickhouse | database (`CLICKHOUSE`) |
| [semoss-minio.yml](semoss-minio.yml) | MinIO 2025-09-07 | 9100 (-> 9000 S3 API), 9101 (-> 9001 UI) | minioadmin / minioadmin | storage (`MINIO`) |
| [semoss-sftp.yml](semoss-sftp.yml) | atmoz/sftp (alpine) | 2222 (-> 22) | foo / pass | storage (`SFTP`) |
| [semoss-mail.yml](semoss-mail.yml) | GreenMail 2.1.13 | 3025 SMTP, 3110 POP3, 3143 IMAP, 3465/3995/3993 TLS, 8085 (-> 8080) API | semoss@semoss.local / semoss, reports@semoss.local / reports | function (`SMTP`, `POP3`, `IMAP`) |

```bash
docker compose -f semoss-weaviate.yml up -d      # or -chroma / -opensearch / -pgvector / -clickhouse / -minio / -sftp / -mail
docker compose -f semoss-weaviate.yml down       # add -v to also wipe the data volume
```

Quick health checks:

```bash
curl http://localhost:8081/v1/.well-known/ready          # weaviate
curl http://localhost:8000/api/v2/version                # chroma
curl -k -u admin:Str0ngVectorP@ss1 https://localhost:9200  # opensearch
docker exec semoss-pgvector pg_isready -U pgvector        # pgvector
curl http://localhost:8123/ping                          # clickhouse (returns "Ok.")
curl -i http://localhost:9100/minio/health/live          # minio (204 when ready)
docker exec semoss-sftp nc -z localhost 22 && echo ok    # sftp
curl http://localhost:8085/api/service/readiness          # mail (greenmail)
```

## Networking: connecting from SEMOSS

These engine files and the SEMOSS compose files all share one Docker network,
`semoss-net`, so a dockerized SEMOSS reaches an engine **by its container name** -
no port mapping or `host.docker.internal` juggling needed.

Create the shared network once (it persists until you delete it); every SEMOSS and
engine file references it as external:

```bash
docker network create semoss-net
```

### Addressing

**SEMOSS in Docker** - use the container name and the *internal* port:

```
http://semoss-weaviate:8080                       (weaviate)
http://semoss-chroma:8000                         (chroma)
https://semoss-opensearch:9200                    (opensearch)
jdbc:postgresql://semoss-pgvector:5432/vectordb   (pgvector, internal port 5432)
jdbc:clickhouse://semoss-clickhouse:8123/semoss   (clickhouse, HTTP port 8123)
http://semoss-minio:9000                          (minio, internal S3 API port 9000)
semoss-sftp:22                                    (sftp, internal port 22)
semoss-mail:3025 / :3110 / :3143                   (mail, smtp / pop3 / imap)
```

**SEMOSS on your host** (not in Docker) - use `localhost` and the published port:

```
http://localhost:8081                               (weaviate)
http://localhost:8000                               (chroma)
https://localhost:9200                              (opensearch)
jdbc:postgresql://localhost:5433/vectordb           (pgvector, host port 5433)
jdbc:clickhouse://localhost:8123/semoss             (clickhouse, host port 8123)
http://localhost:9100                               (minio, host port 9100)
localhost:2222                                      (sftp, host port 2222)
localhost:3025 / :3110 / :3143                       (mail, same ports on the host)
```

> **pgvector port note:** pgvector is just Postgres, the same as the SEMOSS `db`
> service (host `5432`). To avoid a clash it is published on host `5433`
> (`5433:5432`). The in-container port is still `5432`, so SEMOSS-in-Docker
> connects on `5432` by container name; only host access uses `5433`. If `5433`
> is also taken, change the left side of `"5433:5432"` in
> [semoss-pgvector.yml](semoss-pgvector.yml).

> **ClickHouse port note:** JDBC uses the HTTP interface on `8123`. The native
> protocol port `9000` clashes with the MinIO S3 API in the SEMOSS compose files, so
> it is published on host `9010` (`9010:9000`) and only matters if you want to attach
> `clickhouse-client` from the host.

> **MinIO port note:** the SEMOSS compose files that bundle MinIO publish it on
> host `9000` (S3 API) and `9001` (console). The standalone file uses host `9100`
> and `9101` so both can run side by side. The in-container ports are still
> `9000` / `9001`, so SEMOSS-in-Docker connects on `9000` by container name; only
> host access uses `9100`. If those host ports are taken, change the left side of
> `"9100:9000"` / `"9101:9001"` in [semoss-minio.yml](semoss-minio.yml).

> **Mail port note:** GreenMail serves on `3025` / `3110` / `3143` instead of the
> real `25` / `110` / `143` so it needs no privileges, and it publishes them
> unmapped, so the port is the same from Docker and from the host. The mail engines
> default to the real ports, so `SMTP_PORT` / `POP3_PORT` / `IMAP_PORT` always have
> to be set for this server.

## SEMOSS vector engine settings

Create the vector DB engine in SEMOSS with the settings below. Pick the
`HOSTNAME` value from the networking section above that matches your setup. The
parameter names are the SMSS property keys the engines read.

### Weaviate

```
VECTOR_TYPE          WEAVIATE
HOSTNAME             http://semoss-weaviate:8080   (SEMOSS in Docker; use http://localhost:8081 if SEMOSS runs on host)
API_KEY              test-key
WEAVIATE_CLASSNAME   default
WEAVIATE_GRPC_PORT   50051                         (gRPC port)
WEAVIATE_GRPC_HOST   <optional; defaults to the HOSTNAME host>
WEAVIATE_HTTP_PORT   <optional; defaults to 443 for https / 80 for http, or the port in HOSTNAME>
EMBEDDER_ENGINE_ID   <an existing embedder model engine>
```

Weaviate uses gRPC in addition to REST, but `WEAVIATE_GRPC_HOST` defaults to the
host parsed from `HOSTNAME`, so the settings above are all you need.

### Chroma

```
VECTOR_TYPE              CHROMA
HOSTNAME                 http://semoss-chroma:8000   (SEMOSS in Docker; use http://localhost:8000 if SEMOSS runs on host)
CHROMA_COLLECTION_NAME   <collection name>
EMBEDDER_ENGINE_ID       <an existing embedder model engine>
```

### OpenSearch

```
VECTOR_TYPE          OPEN_SEARCH
HOSTNAME             https://semoss-opensearch:9200    (SEMOSS in Docker; use https://localhost:9200 if SEMOSS runs on host)
USERNAME             admin
PASSWORD             Str0ngVectorP@ss1            (OPENSEARCH_INITIAL_ADMIN_PASSWORD)
INDEX_NAME           <index name>
EMBEDDER_ENGINE_ID   <an existing embedder model engine>
```

> OpenSearch serves HTTPS with a self-signed certificate, so use `https://` and
> make sure SEMOSS is allowed to trust/skip verification for it. Override the
> admin password by exporting `OPENSEARCH_INITIAL_ADMIN_PASSWORD` (or a `.env`
> file) before `up`; it must meet OpenSearch's complexity rules.

### pgvector

pgvector extends `RDBMSNativeEngine`, so it takes JDBC connection settings rather
than a plain `HOSTNAME`. SEMOSS creates the `vector` extension and the tables
automatically on first connect.

```
VECTOR_TYPE                    PGVECTOR
RDBMS_TYPE                     POSTGRES
DRIVER                         org.postgresql.Driver
CONNECTION_URL                 jdbc:postgresql://semoss-pgvector:5432/vectordb   (host: jdbc:postgresql://localhost:5433/vectordb)
USERNAME                       pgvector
PASSWORD                       pgvector
PGVECTOR_TABLE_NAME            <table name>
PGVECTOR_METADATA_TABLE_NAME   <metadata table name>
EMBEDDER_ENGINE_ID             <an existing embedder model engine>
```

## SEMOSS database engine settings

### ClickHouse

ClickHouse is a relational (OLAP) database, not a vector DB, so create it as a
database engine:

```
RDBMS_TYPE   CLICKHOUSE
HOSTNAME     semoss-clickhouse   (host: localhost)
PORT         8123
DATABASE     semoss
USERNAME     clickhouse
PASSWORD     clickhouse
```

ClickHouse has databases only, no schemas, so leave `SCHEMA` unset. Instead of
`HOSTNAME` / `PORT` / `DATABASE` you can set `CONNECTION_URL` directly to
`jdbc:clickhouse://semoss-clickhouse:8123/semoss`.

## SEMOSS storage engine settings

### MinIO

MinIO speaks the S3 API, so it uses the same engine as S3. `MinioStorageEngine`
is a small subclass of `S3StorageEngine` that exists only to report the MINIO
storage type and to turn path style access on by default, so the settings are the
S3 ones pointed at the local endpoint. Nothing extra needs installing wherever
SEMOSS runs - the AWS SDK talks to it directly.

```
STORAGE_TYPE            MINIO
S3_ENDPOINT             http://semoss-minio:9000   (SEMOSS in Docker; use http://localhost:9100 if SEMOSS runs on host)
S3_REGION               us-east-1
S3_BUCKET               semoss
S3_ACCESS_KEY           minioadmin
S3_SECRET_KEY           minioadmin
S3_PATH_STYLE_ACCESS    <optional; defaults to true for MINIO, see below>
S3_KMS_ID               <optional; leave unset for MinIO, it has no KMS>
```

Two things about that list are worth knowing:

- **`S3_ENDPOINT` is what redirects the SDK away from AWS.** Without it the engine
  talks to real S3. It is the only setting that makes this MinIO rather than S3.
- **Path style access matters, but you do not have to set it.** It puts the bucket
  in the URL path (`http://semoss-minio:9000/semoss/key`) instead of the hostname
  (`http://semoss.semoss-minio:9000/key`). MinIO does not resolve
  bucket-as-subdomain, so without it you get connection failures that look like DNS
  problems. `MinioStorageEngine` defaults it to true, so only set
  `S3_PATH_STYLE_ACCESS` if you need to force it off.

Using `STORAGE_TYPE S3` instead works too, but then path style is not defaulted
and you have to set `S3_PATH_STYLE_ACCESS true` yourself. `MINIO` is the better
choice: it keeps the MinIO icon in the catalog and gets the default.

Older catalogs that still carry `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`,
`MINIO_REGION`, `MINIO_BUCKET` or `MINIO_ENDPOINT` keep working - those names are
read as a fallback and logged with a warning - but new ones should use the `S3_`
names above.

The compose file creates the `semoss` bucket on startup (the one-shot
`createbucket` service, which exits once the bucket exists). Make any other
buckets from the console at http://localhost:9101 or with:

```bash
docker exec semoss-minio sh -c \
  "mc alias set s http://localhost:9000 minioadmin minioadmin && mc mb -p s/<bucket>"
```

> This standalone MinIO is for MinIO-as-an-engine. It is not the same thing as
> pointing SEMOSS's own cloud storage at MinIO - that is the
> `SEMOSS_STORAGE_PROVIDER: 'minio'` setup in the
> [semoss-with-postgres-minio*.yml](../) files.

### SFTP

```
STORAGE_TYPE          SFTP
HOSTNAME              semoss-sftp    (SEMOSS in Docker; use localhost if SEMOSS runs on host)
PORT                  22             (host: 2222)
USERNAME              foo
PASSWORD              pass
KEEP_ALIVE_INTERVAL   <optional; seconds between keepalives, defaults to 60>
SSH_TIMEOUT           <optional; ssh timeout in ms, defaults to 300000>
NEW_CONNECTION        <optional; true to open a fresh connection per call instead of holding one open>
```

**Every storage path has to start with `upload/`.** atmoz/sftp chroots the user to
`/home/foo`, which must stay root owned for `ChrootDirectory` to work, so the user
cannot write at the top level. Only `upload/` is writable. A push to `myfolder`
fails with "Permission denied"; use `upload/myfolder`.

The compose file mounts its volume at `/home/foo` rather than at
`/home/foo/upload` on purpose. If the volume covers the upload folder, the
entrypoint logs `Directory already exists` and skips the chown, leaving it root
owned and unwritable. Mounting the home dir lets the entrypoint create and chown
the folder itself.

> The engine trusts any host key (`PromiscuousVerifier`), so no `known_hosts`
> setup is needed for local testing. That also means it will not notice a changed
> host key, which matters beyond your machine.

## SEMOSS function engine settings

[semoss-mail.yml](semoss-mail.yml) runs GreenMail, which speaks SMTP, POP3 and
IMAP at once, so one container backs all three mail function engines. Mail lives
in memory: restarting the container is the fastest way to get an empty mailbox
again.

A function engine takes three keys on top of its own settings, and all three have
to be present - `AbstractFunctionEngine` refuses to open an SMSS that is missing
`FUNCTION_NAME` or `FUNCTION_DESCRIPTION`. The description may be left blank
though, and each engine then publishes its own:

```
FUNCTION_TYPE           SMTP | POP3 | IMAP
FUNCTION_NAME           <what a model calls this, e.g. send_email>
FUNCTION_DESCRIPTION    <what it does; blank falls back to the engine's own wording>
```

Leave `FUNCTION_PARAMETERS` and `FUNCTION_REQUIRED_PARAMETERS` unset unless you
want to override what the engine publishes. Each engine fills them in from its own
settings, so the parameter descriptions already carry the limits it was given.

### SMTP (sending)

```
FUNCTION_TYPE              SMTP
SMTP_HOST                  semoss-mail    (SEMOSS in Docker; use localhost if SEMOSS runs on host)
SMTP_PORT                  3025
SMTP_SECURITY              none           (starttls / ssl / none - see the TLS note below)
SMTP_USERNAME              semoss@semoss.local
SMTP_PASSWORD              semoss
SMTP_SENDER                semoss@semoss.local
SMTP_SENDER_NAME           <optional; display name on the from header>
ALLOW_SENDER_OVERRIDE      <optional; true to let a call send as another address, defaults to false>
ALLOWED_RECIPIENT_DOMAINS  <optional; e.g. semoss.local - blank allows any recipient>
DEFAULT_TO                 <optional; recipients used when a call passes none>
DEFAULT_CC                 <optional>
DEFAULT_BCC                <optional>
SUBJECT_PREFIX             <optional; prepended to every subject>
HTML                       <optional; default for the html parameter, defaults to false>
MAX_RECIPIENTS             <optional; cap per email, defaults to 25>
ALLOW_ATTACHMENTS          <optional; true to allow attachments, defaults to false>
CONNECTION_TIMEOUT         <optional; ms, defaults to 10000>
READ_TIMEOUT               <optional; ms, defaults to 30000>
```

Sending cannot be undone, so the guardrails sit in the SMSS rather than with the
caller: the sender is pinned to `SMTP_SENDER` unless `ALLOW_SENDER_OVERRIDE` is
on, recipients outside `ALLOWED_RECIPIENT_DOMAINS` are rejected, the recipient
count is capped, and attachments are refused until `ALLOW_ATTACHMENTS` is set.
Attachments also have to already be files of the insight making the call.

Read what arrived at http://localhost:8085 (the GreenMail UI), or with
`curl "http://localhost:8085/api/user/reports@semoss.local/messages/"`.

### POP3 (reading, single inbox)

```
FUNCTION_TYPE              POP3
POP3_HOST                  semoss-mail    (host: localhost)
POP3_PORT                  3110
POP3_SECURITY              none
POP3_USERNAME              reports@semoss.local
POP3_PASSWORD              reports
MAX_MESSAGES               <optional; cap per call, defaults to 25>
DEFAULT_MESSAGES           <optional; returned when a call passes no limit, defaults to 10>
MAX_BODY_CHARS             <optional; longer bodies come back truncated, defaults to 10000>
ALLOWED_SENDER_DOMAINS     <optional; only mail from these domains is surfaced at all>
ALLOW_ATTACHMENT_DOWNLOAD  <optional; true to save attachments into the calling insight, defaults to false>
MAX_ATTACHMENT_SIZE        <optional; bytes, defaults to 5242880>
CONNECTION_TIMEOUT         <optional; ms, defaults to 10000>
READ_TIMEOUT               <optional; ms, defaults to 30000>
```

POP3 has one inbox, no folders, no record of what has been read, and no search, so
the engine filters the mailbox itself: it walks back from the newest message and
stops once it has enough matches. A search that matches nothing therefore reads
the whole mailbox, so prefer a `sinceDays` on a big one. The engine never deletes
anything - a POP3 delete happens the moment the connection closes and cannot be
walked back.

### IMAP (reading, folders, and changes)

```
FUNCTION_TYPE              IMAP
IMAP_HOST                  semoss-mail    (host: localhost)
IMAP_PORT                  3143
IMAP_SECURITY              none
IMAP_USERNAME              reports@semoss.local
IMAP_PASSWORD              reports
DEFAULT_FOLDER             <optional; folder a call reads when it names none, defaults to INBOX>
ALLOWED_FOLDERS            <optional; e.g. INBOX,Archive - blank allows any folder>
MARK_AS_READ               <optional; true to mark what the engine returns as read, defaults to false>
ALLOW_FLAG_CHANGES         <optional; true to allow markRead / markUnread, defaults to false>
ALLOW_MOVE                 <optional; true to allow move, defaults to false>
ALLOW_DELETE               <optional; true to allow delete, defaults to false>
MAX_MESSAGES               <optional; cap per call, also caps how many uids one change can touch, defaults to 25>
DEFAULT_MESSAGES           <optional; defaults to 10>
MAX_BODY_CHARS             <optional; defaults to 10000>
ALLOWED_SENDER_DOMAINS     <optional>
ALLOW_ATTACHMENT_DOWNLOAD  <optional; defaults to false>
MAX_ATTACHMENT_SIZE        <optional; bytes, defaults to 5242880>
CONNECTION_TIMEOUT         <optional; ms, defaults to 10000>
READ_TIMEOUT               <optional; ms, defaults to 30000>
```

A search opens the folder read only, so looking at a mailbox does not mark
anything seen unless `MARK_AS_READ` is on. Changing the mailbox goes through the
`action` parameter - `markRead`, `markUnread`, `move`, `delete` - and each is
missing from what the engine publishes until the matching `ALLOW_` key is set, so
a model never sees an action it cannot use. Every change takes the `uid` a search
returned. `move` is a copy into the target folder followed by deleting the
original, since IMAP has no move, and the engine reads folders but never creates
them: make `Archive` from a mail client (or `docker exec` an IMAP `CREATE`) before
moving into it.

To try the whole set against GreenMail, create the IMAP engine with
`ALLOW_FLAG_CHANGES`, `ALLOW_MOVE` and `ALLOW_DELETE` set to true, send yourself a
few messages through the SMTP engine, then search with `unreadOnly true` and pass
a returned `uid` back with `action markRead`.

### GreenMail specifics worth knowing

- **The login is the whole address.** `-Dgreenmail.users=semoss@semoss.local:semoss`
  creates a mailbox whose username is `semoss@semoss.local`, not `semoss`. Add
  `-Dgreenmail.auth.disabled` to `GREENMAIL_OPTS` to accept any login instead,
  which also creates the mailbox on the fly.
- **POP3 and IMAP share one store.** Reading a message over POP3 marks it seen for
  IMAP, so an `unreadOnly` IMAP search right after a POP3 read comes back empty.
- **Mail is only in memory.** `docker compose -f semoss-mail.yml restart` is the
  reset button.
- **Any recipient is accepted.** GreenMail delivers to addresses that were never
  configured, so you can send to `whoever@semoss.local` and read it back from the
  API even though no such mailbox was declared.

### Using the TLS ports

GreenMail also serves SMTPS / POP3S / IMAPS on `3465` / `3995` / `3993`, but with a
self-signed certificate that does not match the host. The engines require TLS that
is both trusted and hostname-matched, so they refuse it, correctly:

```
Error occurred connecting to the mail server defined. Detailed error: PKIX path
building failed ... unable to find valid certification path to requested target
```

Any key starting with `mail.` is passed straight through to jakarta.mail, applied
after the engine's own defaults so it wins, and matched whichever case it is
written in - the reactor that creates an engine upper cases every key, and
jakarta.mail only answers to its own lower case names. Two of those keys relax
exactly this:

```
IMAP_PORT                            3993
IMAP_SECURITY                        ssl
MAIL.IMAPS.SSL.TRUST                 localhost   (or semoss-mail from Docker)
MAIL.IMAPS.SSL.CHECKSERVERIDENTITY   false
```

Use `MAIL.POP3S.*` or `MAIL.SMTP.*` for the other two protocols. Both keys are
needed: trusting the certificate still leaves the hostname check to fail.

> Those two keys are the point of the passthrough, and they are also the two you
> must never carry to a real mail server - together they accept any certificate
> from anyone. For local testing the plaintext ports are the simpler choice.

### Testing the SendEmail pixel

`SendEmail` can be pointed at a mail server per call, without creating an engine at
all, which is the quickest way to check a template:

```
SendEmail(smtpHost=["localhost"], smtpPort=["3025"], smtpSecurity=["none"],
          from=["semoss@semoss.local"], to=["reports@semoss.local"],
          subject=["Test"], message=["Hello"]);
```

`smtpSecurity` matters here: with no credentials the call defaults to `none`, but
with `username` / `password` it defaults to `starttls`, which GreenMail's plaintext
port does not offer. Left to itself, `SendEmail` uses the instance wide mail server
from `social.properties` instead - the `smtp_*` keys, which the
[docker configuration guide](../../docs/deployment/docker_configuration.md)
covers.

## Notes

- All credentials here (`test-key`, `admin` / `Str0ngVectorP@ss1`,
  `pgvector` / `pgvector`, `clickhouse` / `clickhouse`,
  `minioadmin` / `minioadmin`, `foo` / `pass`,
  `semoss@semoss.local` / `semoss`, `reports@semoss.local` / `reports`) are
  local-dev defaults - change them before using any of this beyond your machine.
- Each service uses fixed container names, so run one instance of each at a time.
- `EMBEDDER_ENGINE_ID` must reference an embedder model engine that already exists
  in your SEMOSS instance.
