# Storage engines

File storage you can run alongside SEMOSS. Bring the container up first - see
[the engine index](README.md) for the compose commands and for how to address a
container from SEMOSS.

## MinIO

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

## SFTP

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

---

One of the [supporting engines](README.md).
