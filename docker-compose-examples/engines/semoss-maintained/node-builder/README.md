# node-builder

The SEMOSS build service, published as `quay.io/semoss/smss-node-builder`.
When a project is published (`BuildAndPublishApp`, and project restore), SEMOSS
zips the project's `assets/client` folder, posts it here, and unpacks the built
app it gets back into the project's `portals` folder.

```bash
docker compose -f semoss-node-builder.yml up -d
curl http://localhost:3000/health       # {"status":"ok","activeBuilds":0,"maxConcurrent":3}
docker compose -f semoss-node-builder.yml down
```

## API

| Method | Path | What it does |
|--------|------|--------------|
| `GET` | `/health` | build counts, always HTTP 200 |
| `GET` | `/ready` | HTTP 503 while every build slot is busy |
| `POST` | `/build?buildCmd=...` | zip upload in the multipart `source` field, runs the build and returns `portals.zip` |

## Pointing SEMOSS at it

SEMOSS reads `NODE_SERVER_ENDPOINT` from RDF_Map.prop. In the SEMOSS image,
`runCS.sh` copies the `NODE_SERVER_ENDPOINT` environment variable into
RDF_Map.prop at startup.

| Where SEMOSS runs | Setting |
|-------------------|---------|
| one of the SEMOSS compose files (already set) | `NODE_SERVER_ENDPOINT: 'http://semoss-node-builder:3000'` |
| on your host | `NODE_SERVER_ENDPOINT	http://localhost:3000` in RDF_Map.prop |

## Settings

All optional, set as environment variables on the `node-builder` service:

| Variable | Default | What it does |
|----------|---------|--------------|
| `MAX_CONCURRENT_BUILDS` | `3` | builds that run at once; more get HTTP 503 |
| `BUILD_MEMORY_LIMIT_MB` | `1024` | node heap cap (`--max-old-space-size`) for each build |
| `BUILD_TIMEOUT_MS` | `300000` | how long a build may run |
| `MAX_UPLOAD_MB` | `100` | largest accepted upload |
| `PORT` | `3000` | listen port inside the container |
