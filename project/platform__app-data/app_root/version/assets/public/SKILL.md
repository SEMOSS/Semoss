---
name: app-data
description: Use when writing code in an app that persists data or files durably - saving app config, storing user-generated files, reading bundled data files at runtime, browsing folders, or querying an uploaded/stored CSV in memory. Covers the project asset pixels (SaveAppAssets, GetAppAssets, GetAppAssetsBase64, BrowseAppAssets, DownloadAppAsset, CopyAppAssetsToInsight and family), the per-user asset space (SaveUserAssets and family), insight asset reads, the space argument, and the FileRead | Import | Frame query flow for CSVs. Also covers what does NOT work: which space is ephemeral, the assets/public jail for view-only users, and why project images must be served as base64. Do not use for storage engines like S3 (see storage), SQL data (see database), or one-shot uploads for LLM prompts (see file-uploads).
---

# App data and durable files

The platform has three file spaces an app can touch. Choosing the wrong one is the most common persistence bug:

| Space | Lifetime | Who can write | Use for |
| --- | --- | --- | --- |
| **Insight** | ephemeral - dies with the session/insight | any user of the app | scratch files, upload staging, export staging |
| **Project** | durable, git-versioned, cloud-synced | **EDIT-permission users only** | app config, bundled data, content the app team curates |
| **User** | durable per-user, git-versioned | the user themselves | that user's saved files, drafts, personal artifacts |

Two hard rules before anything else:

1. **Ordinary end users cannot write project space.** Every project-space write requires EDIT permission on the project. User-generated content from viewers must go to a database (see database), a storage engine (see storage), or the user space - never project space.
2. **View-only users can read project space only under `assets/public/`.** A read anywhere else throws for them (and silently scopes a root listing down to `public/`). Any file the app must read at runtime for normal users - config.json, lookup data, images - **must live under `public/`**. Reading `GetAppAssets(filePath=["config.json"])` works for you (an editor) and breaks for every end user; it has to be `public/config.json`.

## Project space - the durable app store

All paths are relative to the project's `assets/` folder (do NOT include any `version/assets/` prefix). Writes are automatically git-committed (optional `comment` becomes the commit message) and pushed to cloud storage - there is no separate commit step.

```typescript
import { runPixel } from "@semoss/sdk";
const APP_ID = appId; // this app's project id

// write (editor only) - filePath/content are parallel lists, one call can save many files
await runPixel(
  `SaveAppAssets(project=["${APP_ID}"], filePath=["public/config.json"], content=["<encode>${JSON.stringify(cfg)}</encode>"]);`,
  insightId,
);

// read (any user, because it is under public/)
const r = await runPixel(
  `GetAppAssets(project=["${APP_ID}"], filePath=["public/config.json"]);`,
  insightId,
);
const cfg = JSON.parse(r.pixelReturn[0].output as string);
```

The `<encode>...</encode>` wrapper passes content verbatim without escaping quotes/backslashes - use it for any non-trivial content.

The full family (all take `project` first; `comment` is the optional git message):

| Pixel | Key params | Purpose |
| --- | --- | --- |
| `SaveAppAssets` | `filePath`, `content`, `comment` | Write text file(s) |
| `SaveAppAssetsBase64` | `filePath`, `content`, `comment`, `decode` | Write binary (images, PDFs) from base64 |
| `GetAppAssets` | `filePath` | Read a file as text |
| `GetAppAssetsBase64` | `filePath` | Read binary as base64 |
| `BrowseAppAssets` | `filePath` (optional) | List a directory |
| `SearchAppAssets` | `filePath`, `search`, `options` (`"case"`, `"word"`, `"regex"`) | Recursive search |
| `NewAppAssetsFile` / `NewAppAssetsDirectory` | `filePath`, `comment` | Create empty file/dir |
| `DeleteAppAssets` / `RenameAppAsset` / `CopyAppAsset` | `filePath` (+ `newValue`) | Manage files |
| `DownloadAppAsset` | `filePath` | Returns a download key (FILE_DOWNLOAD) for the two-step download flow |
| `CopyAppAssetsToInsight` | `filePath` | Bridge a project file into the insight space (viewers: public/ only) |
| `UnzipAppAssetFile` | `filePath` | Unzip in place |

## Serving images: there is no static URL

Project-space files are **not** URL-addressable at runtime - the asset download REST endpoint corrupts binaries and cannot take nested paths, so `<img src="/api/...">` does not work. Two working patterns:

```typescript
// dynamic: read as base64 and inline
const img = await runPixel(
  `GetAppAssetsBase64(project=["${APP_ID}"], filePath=["public/logo.png"]);`,
  insightId,
);
const src = `data:image/png;base64,${img.pixelReturn[0].output}`;
```

- **Static at publish:** files under `assets/portals/` are copied to the served portal at publish time and CAN be referenced with plain relative URLs from the app's HTML - but that copy is a **snapshot**; a file written there at runtime is invisible until the project is republished.
- Keep base64 for small/medium images only (~33% size inflation, no browser caching).

## User space - durable per-user files

Same API shape, no `project` param, writable by the user themselves: `SaveUserAssets` / `SaveUserAssetsBase64` (`filePath`, `content`, `comment`), `GetUserAssets` / `GetUserAssetsBase64`, `BrowseUserAssets`, `SearchUserAssets`, `DownloadUserAsset`, `CopyUserAssetsToInsight`, `NewUserAssetsFile`/`Directory`, `RenameUserAsset`, `CopyUserAsset`, `DeleteUserAssets`, `UnzipUserAssetFile`.

This is the right place for "my saved drafts" features. For small preferences (theme, flags, defaults), prefer the user metadata store (see the user skill) over files.

The insight space has the same read/browse family (`GetInsightAssets`, `BrowseInsightAssets`, `SaveInsightAssets`, `DownloadInsightAsset`, ...) for working with files uploaded during the session.

## The space argument on generic file pixels

Generic file pixels (`FileRead`, `SendEmail` attachments, `GzipFile`, `UnzipFile`, `WriteObjectToFile`, ...) take an optional `space`:

- omitted -> the current insight folder (ephemeral)
- `space=["user"]` -> the user's durable space
- `space=["<projectId>"]` -> that project's folder, permission-checked

**Path convention trap:** `space` resolves to the project's `app_root/` folder, so paths need the `version/assets/` prefix - e.g. `FileRead(filePath=["version/assets/public/data.csv"], space=["${APP_ID}"])`. The `*AppAssets` pixels above resolve to `app_root/version/assets/` and must NOT have that prefix. Mixing up the two conventions is a frequent bug.

## Query a CSV without a database

For real filtering/aggregation over a user-supplied or project-stored CSV, load it into an in-memory frame and query it:

```typescript
// 1. load the CSV into a named grid frame (from project space via space=, or from an upload in insight space)
await runPixel(
  `FileRead(filePath=["version/assets/public/data.csv"], space=["${APP_ID}"]) | Import(frame=[CreateFrame(frameType=["GRID"], override=[true], alias=["dataFrame"])]);`,
  insightId,
);

// 2. query it
const rows = await runPixel(
  `Frame(frame=["dataFrame"]) | QueryAll() | Collect(500);`,
  insightId,
);
const { headers, values } = rows.pixelReturn[0].output.data;
```

The frame lives in the insight - reload it per session. `ToCsv`/`ToExcel` exporters cannot write to project space directly (no `space` key); to persist a computed result durably, export into the insight space, then `SaveAppAssets` the content.

## What is NOT durable

`SetAppConfig` / `GetAppConfig` and `SetInsightConfig` / `GetInsightConfig` store their value in the live insight's variable store only - despite the names, they do **not** persist across sessions. Use `SaveAppAssets` (app-wide) or user metadata / user assets (per-user) for anything that must survive a reload.

Helpers worth knowing: `GetCurrentInsightId()` and `GetCurrentContextProjectId()` (no args) return the current insight id and app project id - useful when composing the pixels above.
