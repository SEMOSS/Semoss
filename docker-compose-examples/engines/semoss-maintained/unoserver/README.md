# unoserver

LibreOffice document conversion over HTTP, published as
`quay.io/semoss/unoserver`. SEMOSS sends it a file and gets the converted file
back. It backs the `ConvertFileUnoserver` reactor, the file conversion agent hook,
and the `InspectPptx` agent tool, which renders a deck to PDF before a vision
model reviews it.

```bash
docker compose -f semoss-unoserver.yml up -d
curl http://localhost:8082/health       # {"ok":true} once LibreOffice is up
docker compose -f semoss-unoserver.yml down
```

The image is published for `linux/amd64` only, so the compose file sets
`platform: linux/amd64`. On Apple Silicon it runs under emulation, so startup and
conversions are slower.

## API

| Method | Path | What it does |
|--------|------|--------------|
| `GET` | `/health` | `{"ok": true}` when LibreOffice answers, otherwise HTTP 503 |
| `POST` | `/convert?to=pdf` | multipart upload in the `file` field, returns the converted file |

## Pointing SEMOSS at it

SEMOSS reads `UNOSERVER` from RDF_Map.prop and falls back to an environment
variable of the same name.

| Where SEMOSS runs | Setting |
|-------------------|---------|
| one of the SEMOSS compose files (already set) | `UNOSERVER: 'http://semoss-unoserver:8080'` |
| on your host | `UNOSERVER	http://localhost:8082` in RDF_Map.prop |

`UNOSERVER_TIMEOUT_SECONDS` (RDF_Map.prop or environment) bounds how long SEMOSS
waits for a health check or conversion response: default `180`, allowed `1` to
`1800`.
