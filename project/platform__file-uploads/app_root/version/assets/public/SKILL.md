---
name: file-uploads
description: Use when writing code in an app that attaches files of any type (image, PDF, Word, spreadsheet, CSV, audio, video) to an LLM prompt, or that uploads a file into the current insight workspace. Covers the two-step pattern - POST the bytes to /api/uploadFile/baseUpload (or call uploadInsight from @semoss/sdk) to get back {fileName, fileLocation}, then reference those filenames in the pixel call with the media argument, never re-sending the bytes. Also covers the MEDIA parts form used by the room/playground path, the multipart field name, why Content-Type must not be set by hand, extension filtering, and retry handling when the upload fails. Do not use for the LLM call itself (see model) or for reading files already in the app's asset folder.
---

# File uploads

Attaching a file to an LLM prompt is always two sequential network calls, in this order:

1. **Upload.** POST the raw bytes as `multipart/form-data` to the SEMOSS file-upload endpoint. The backend stores them in the insight workspace and returns one `{ fileName, fileLocation }` per file.
2. **Pixel call.** Send the LLM message, referencing the names from step 1. The bytes are not re-sent.

Run step 1 to completion before starting step 2. If the user attached nothing, or step 1 returns an empty array, skip straight to step 2 with no media.

Any file type can be uploaded and attached - image, PDF, Word, spreadsheet, CSV, audio, video. Nothing in this two-step pattern is images-only. Whether the model can read a given type is a property of the model, so pick one that supports what the user is attaching.

**Step 2 has two forms, and picking the wrong one fails silently.** Use the one that matches the pixel the app calls:

| Calling | Attach with | Section |
| --- | --- | --- |
| `LLM()` | the top-level `media` argument | [Step 2a](#step-2a---llm-with-the-media-argument) |
| the room / playground chat path | a `parts` array with `MEDIA` parts | [Step 2b](#step-2b---room-path-with-media-parts) |

A `MEDIA` part handed to `LLM()` inside `full_prompt` is silently ignored, and `media=` is not read by the room parts path. See the `model` skill for the `LLM()` call itself.

## Step 1 - upload the bytes

Prefer the SDK helper, which builds the form data and the URL for you:

```typescript
import { uploadInsight } from "@semoss/sdk";

// returns { fileName: string; fileLocation: string }[], in input order
const uploaded = await uploadInsight(insightId, "", files);
```

Calling the endpoint directly:

```
POST {MODULE}/api/uploadFile/baseUpload?insightId={insightId}&path={encodedPath}&userSpace=false
```

- `MODULE` is the SEMOSS base URL (for example `/Monolith_Dev`).
- `insightId` is the insight the LLM call will run under. Required.
- `path` is the destination subpath inside the insight workspace, URL-encoded. `""` is valid and uploads to the workspace root.

Every file goes under the same field name, `file`:

```typescript
const fd = new FormData();
for (const f of files) fd.append("file", f); // same key for every file
```

Send with `credentials: "include"` so the SEMOSS session cookie rides along. **Do not set `Content-Type` by hand** - the browser has to set it to add the multipart boundary, and setting it yourself produces a doubled header the backend rejects.

The response is a JSON array with one entry per input file, in the same order:

```typescript
{ fileName: string; fileLocation: string }[]
```

`fileLocation` is the server-side path step 2 needs. Keep the array in memory until step 2 completes.

## Step 2a - `LLM()` with the `media` argument

For a direct `LLM()` call, pass the uploaded `fileName` values straight into the top-level `media` argument. No parts array is involved:

```typescript
const media = uploaded.map((f) => f.fileName);

const { errors, pixelReturn } = await runPixel(
	`LLM(engine="${MODEL_ID}", command=["${prompt}"], media=${JSON.stringify(media)});`,
	insightId,
);
```

`media` accepts a single string or an array, and is not images-only - pass any file type. Always use `media`; an older `image` argument is still accepted by the server but is a deprecated legacy alias, so never write it in new code.

This only works with the `command=` form. `media` is dropped when `full_prompt` is used; see the `model` skill for how media travels inside `full_prompt`.

## Step 2b - room path with `MEDIA` parts

The room / playground chat path takes an input message instead, built as a `parts` array: the user's text first, then one `MEDIA` part per uploaded file.

```typescript
type Part =
	| { type: "TEXT"; text: string; uiText: string }
	| {
			type: "MEDIA";
			mediaInfo: {
				fileName: string; // from the upload response
				fileLocation?: string; // from the upload response - what the backend reads
				mediaInputType: "FILE"; // literal
				base64Data?: string; // leave "" - the file is already on disk
				fileFormat?: string; // leave ""
				mimeType?: string; // leave ""
			};
	  };

const parts: Part[] = [{ type: "TEXT", text: prompt, uiText: prompt }];
for (const f of uploaded) {
	parts.push({
		type: "MEDIA",
		mediaInfo: {
			base64Data: "",
			fileFormat: "",
			fileName: f.fileName,
			fileLocation: f.fileLocation,
			mediaInputType: "FILE",
			mimeType: "",
		},
	});
}
```

Submit `parts` as the input message to the room/chat call the app already uses, with the same `insightId` from step 1 and the target model id.

Do not hand this `parts` array to `LLM()`. `LLM()` does not read `MEDIA` parts - inside `full_prompt` they are silently skipped, so the model answers as if no file were attached. For `LLM()`, use [Step 2a](#step-2a---llm-with-the-media-argument).

## Filtering by extension

If the app has an allow-list, apply it to the upload *response*, not the upload. Filter which entries become `MEDIA` parts rather than blocking the POST, so a rejected file still fails visibly rather than silently:

```typescript
const normalize = (v: string) => v.trim().toLowerCase().replace(/^\./, "");
const allowed = new Set(allowedExtensions.map(normalize));
const media = uploaded.filter((f) => {
	const ext = normalize(f.fileName.split(".").pop() ?? "");
	return ext && allowed.has(ext);
});
```

Files dropped by the filter stay on the server but are never referenced by the LLM.

## Error handling

- If the upload fails, **do not issue the pixel call.** Surface the error and let the user retry.
- Keep the original `File` objects in component state so a retry does not make the user re-pick them.
- The upload is not streaming. Await it fully before building `parts`.
- An empty `files` array is valid: skip step 1 and send a text-only pixel call.

## Reference implementation

In the `semoss-ui` repo:

| What | Where |
| --- | --- |
| Upload helper | `libs/sdk/src/api/insight.ts` - `uploadInsight(insightId, path, files)` |
| Orchestration (upload, filter, build parts, run pixel) | `packages/playground/src/stores/room/room.store.ts` - `askMessage(prompt, files)` |
| Part type definitions | `packages/playground/src/types.d.ts` - `PixelMessageTextPart`, `PixelMessageMediaPart` |
| File picker, drag-drop, paste UI | `packages/playground/src/components/room/room-input.tsx` |
