---
name: file-uploads
description: Implementing two-step image upload + LLM pixel call (SEMOSS pattern)
---

Overview
When a user attaches a file (e.g. an image) to an LLM prompt, the client performs two sequential network calls:

baseUpload — POST the raw file(s) as multipart/form-data to the SEMOSS file-upload endpoint. The backend stores them in the insight workspace and returns a list of { fileName, fileLocation }.
Pixel call — POST the chat/LLM request. The fileLocation strings returned by step 1 are embedded as MEDIA parts in the message payload — the file bytes are not re-sent.
Always run step 1 to completion before step 2. If step 1 returns no files (or the user attached none), skip straight to step 2 with only a TEXT part.

Step 1 — Upload
Endpoint

POST {MODULE}/api/uploadFile/baseUpload?insightId={insightId}&path={encodedPath}
MODULE is the SEMOSS base URL (e.g. /Monolith_Dev).
insightId is the current insight/session id the LLM call will run under. Required.
path is the destination subpath inside the insight workspace; URL-encoded. Empty string ("") is valid and uploads to the workspace root.
Body — multipart/form-data with each file appended under the same field name file:

const fd = new FormData();
for (const f of files) fd.append("file", f); // same key for every file
Send with credentials: "include" (or whatever your app uses to carry the SEMOSS session cookie). Do not set Content-Type manually — let the browser set the multipart boundary.

Response — JSON array, one entry per file in input order:

{ fileName: string; fileLocation: string }[]
fileLocation is the server-side path that the pixel call needs. Persist the array in memory until step 2 completes.

Step 2 — Pixel call (LLM message)
Build a parts array. First part is the user's text, then one MEDIA part per uploaded file:

type Part =
| { type: "TEXT"; text: string; uiText: string }
| {
type: "MEDIA";
mediaInfo: {
fileName: string; // from upload response
fileLocation: string; // from upload response — this is what the backend reads
mediaInputType: "FILE"; // literal
base64Data?: string; // leave "" — file is already on disk
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
Submit parts as the input message to whatever pixel/chat endpoint your app already uses for LLM calls, alongside the same insightId from step 1 and the target modelId/engineId.

Optional: client-side file-type filter
If your app has an allow-list of extensions, apply it to the upload response after step 1 (don't block the upload — filter which entries become MEDIA parts). Normalize by lowercasing and stripping a leading dot:

const normalize = (v: string) => v.trim().toLowerCase().replace(/^\./, "");
const allowed = new Set(allowedExtensions.map(normalize));
const media = uploaded.filter(f => {
const ext = normalize(f.fileName.split(".").pop() ?? "");
return ext && allowed.has(ext);
});
Files dropped by this filter are still on the server but won't be referenced by the LLM.

Error handling
If baseUpload fails, do not issue the pixel call. Surface the error and let the user retry — preserve the original File objects in component state so the retry doesn't require re-picking them.
The upload is non-streaming; await it fully before constructing parts.
An empty files array is valid — just skip step 1 and send a text-only pixel call.
Reference implementation pointers (SEMOSS repo)
Upload helper: libs/sdk/src/api/insight.ts → uploadInsight(insightId, path, files)
Orchestration: packages/playground/src/stores/room/room.store.ts → askMessage(prompt, files) (upload → filter → build parts → run pixel call)
Part type definitions: packages/playground/src/types.d.ts → PixelMessageTextPart, PixelMessageMediaPart
File-picker / drag-drop / paste UI: packages/playground/src/components/room/room-input.tsx
