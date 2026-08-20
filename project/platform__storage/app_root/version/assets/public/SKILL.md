---
name: storage
description: Use when writing code in an app that saves, retrieves, lists, syncs, or deletes files in a storage engine on the platform (S3, Azure Blob, Google Cloud Storage, MinIO, SFTP, local file system, and similar). Covers the PushToStorage(), PullFromStorage(), ListStoragePath(), ListStoragePathDetails(), DeleteFromStorage(), SyncLocalToStorage(), SyncStorageToLocal(), GetStorageFileAsBase64(), and UpdateStorageFileMetadata() pixel commands via @semoss/sdk's runPixel, plus listing storage engines with MyEngines(engineTypes=["STORAGE"]). Do not use for attaching files to an LLM prompt (see file-uploads), app asset files, or SQL data (see database).
---

# Storage Engine

A **storage engine** is a durable blob/file store the platform proxies for you — S3, Azure Blob, Google Cloud Storage, MinIO, Ceph, SFTP, SMB, or a server-local file system. All storage calls go through `runPixel` from `@semoss/sdk`.

The critical mental model: **the browser never talks to the storage engine directly.** Files move between a *server-side space* (the insight workspace by default) and the storage engine. Getting a browser file into storage — or a storage file back to the user — is therefore always two steps:

- **Upload:** browser → insight space (`uploadInsight`), then `PushToStorage` (insight space → storage).
- **Download:** `PullFromStorage` (storage → insight space), then serve it to the user (`DownloadAsset` flow) — or skip the round trip with `GetStorageFileAsBase64` for in-page display.

## Usage — save a user's file into storage

```typescript
import { runPixel, uploadInsight } from "@semoss/sdk";

const STORAGE_ID = "c8f6cbc0-27ab-4d29-a17e-11d4f3f423bf"; // the project's selected storage engine

// 1. browser -> insight space
const { data } = await uploadInsight(insightId, "", fileOrFiles);
const uploaded = data[0]; // { fileName, fileLocation }

// 2. insight space -> storage
const push = await runPixel(
  `PushToStorage(storage=["${STORAGE_ID}"], storagePath=["/reports/2026"], filePath=["${uploaded.fileName}"]);`,
  insightId,
);
if (push.errors.length) throw new Error(push.errors[0]);
// pixelReturn[0].output === true on success
```

`filePath` is relative to the insight space unless you pass `space` (see below). `storagePath` is the folder path inside the storage engine; the file keeps its name.

## Reading storage

### List a folder

```typescript
// names only -> string[]
const list = await runPixel(
  `ListStoragePath(storage=["${STORAGE_ID}"], storagePath=["/reports"]);`,
  insightId,
);
const entries = list.pixelReturn[0].output as string[];

// with metadata -> array of maps (keys vary by engine subtype: sizes, dates, etc.)
const details = await runPixel(
  `ListStoragePathDetails(storage=["${STORAGE_ID}"], storagePath=["/reports"]);`,
  insightId,
);
const rows = details.pixelReturn[0].output as Record<string, unknown>[];
```

Folder entries typically end with `/`. Treat the detail-map keys as engine-specific — inspect one response before binding UI to field names.

### Pull a file back to the insight space

```typescript
const pull = await runPixel(
  `PullFromStorage(storage=["${STORAGE_ID}"], storagePath=["/reports/2026/summary.pdf"], filePath=["downloads"]);`,
  insightId,
);
// pixelReturn[0].output === true; the file now exists at downloads/ in the insight space
```

`filePath` here is a local *directory* (created if missing). To then hand the file to the user, run the `DownloadAsset` flow from the app-bootstrap skill (`insight.actions.download("downloads/summary.pdf")`).

### Display a file without the round trip

```typescript
const b64 = await runPixel(
  `GetStorageFileAsBase64(storage=["${STORAGE_ID}"], storagePath=["/reports/2026/summary.pdf"]);`,
  insightId,
);
const base64 = b64.pixelReturn[0].output as string;
// e.g. <img src={`data:image/png;base64,${base64}`} /> or a PDF viewer blob
```

Optional `convertToPdf=[true]` converts doc/docx, xlsx, pptx, and txt content to PDF before encoding — useful for a uniform preview pane. Not every engine supports in-memory reads; the pixel errors with "In-memory blob reading is not supported" when the engine cannot do it, so keep the `PullFromStorage` path as the fallback. Base64 inflates size by ~33% — do not use this for large files.

## Writing and deleting

```typescript
// delete a file or folder
await runPixel(
  `DeleteFromStorage(storage=["${STORAGE_ID}"], storagePath=["/reports/2025"], leaveFolderStructure=[false]);`,
  insightId,
);

// sync a whole local folder up (rclone-style sync, not copy: makes storage match local)
await runPixel(
  `SyncLocalToStorage(storage=["${STORAGE_ID}"], storagePath=["/backup"], filePath=["exports"]);`,
  insightId,
);

// and back down (makes local match storage)
await runPixel(
  `SyncStorageToLocal(storage=["${STORAGE_ID}"], storagePath=["/backup"], filePath=["restore"]);`,
  insightId,
);

// attach engine-level metadata to an object (S3 tags, etc.)
await runPixel(
  `UpdateStorageFileMetadata(storage=["${STORAGE_ID}"], storagePath=["/reports/2026/summary.pdf"], metadata=[{"owner":"finance"}]);`,
  insightId,
);
```

`PushToStorage` also accepts `metadata=[{...}]` to tag the object at upload time. Metadata support varies by engine; local file system engines ignore it.

All mutation pixels return `true` in `pixelReturn[0].output`, or an `ERROR` operationType with a message — check `errors` on every call.

## The space argument

`PushToStorage`, `PullFromStorage`, and both sync pixels accept an optional `space` controlling where `filePath` resolves:

- omitted — the current insight workspace (the default; matches where `uploadInsight` puts files)
- `space=["<projectId>"]` — the project's asset folder
- `space=["user"]` — the logged-in user's personal space

## Access control

The platform enforces engine permissions server-side:

- **Read** access to the engine: `PullFromStorage`, `ListStoragePath`, `ListStoragePathDetails`
- **Edit** access to the engine: `PushToStorage`, `DeleteFromStorage`, `SyncLocalToStorage`, `UpdateStorageFileMetadata`, and `GetStorageFileAsBase64`

A permission failure surfaces as a pixel error ("User does not have permission..."). Gate write UI on the user's engine permission (see the permissions skill) rather than letting the pixel fail.

## Listing available storage engines

```typescript
const { pixelReturn } = await runPixel(
  `MyEngines(engineTypes=["STORAGE"], limit=[50], offset=[0]);`,
  insightId,
);
const storages = pixelReturn[0].output as Array<{
  engine_id: string;
  engine_name: string;
  engine_display_name: string;
  engine_subtype: string; // e.g. "S3", "AZURE_BLOB", "GOOGLE_CLOUD", "MINIO", "SFTP", "LOCAL_FILE_SYSTEM"
}>;
```

`MyEngines` supports the same `filterWord`, `onlyFavorites`, `sort`, `limit`, and `offset` arguments described in the database skill.

Never hardcode or guess a storage engine ID. Use the project's selected storage engine (see the selected-engines skill); if none is selected, ask the user to choose or attach one.
