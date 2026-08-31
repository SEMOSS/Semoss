/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.io.connector.ms;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.security.HttpHelperUtility;

/**
 * Path addressed operations against a single Microsoft Graph drive, rooted at a
 * given drive item.
 *
 * <p>
 * Everything is expressed as a path relative to that root item, so a caller can
 * treat a SharePoint document library, a OneDrive folder or a Teams channel
 * folder as an ordinary file tree. The root is a drive item id rather than the
 * drive root, which is what allows a channel folder to be the root.
 * </p>
 *
 * <p>
 * The token is supplied lazily so the same client works for an app-only service
 * identity and for a delegated user token. Nothing here is specific to Teams.
 * </p>
 */
public class MicrosoftGraphDriveClient {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftGraphDriveClient.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final String GRAPH_BASE = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0";

	public static final String ID = "id";
	public static final String NAME = "name";
	public static final String SIZE = "size";
	public static final String FILE = "file";
	public static final String FOLDER = "folder";
	public static final String WEB_URL = "webUrl";
	public static final String MIME_TYPE = "mimeType";
	public static final String CHILD_COUNT = "childCount";
	public static final String LAST_MODIFIED = "lastModifiedDateTime";
	public static final String DOWNLOAD_URL = "@microsoft.graph.downloadUrl";

	private static final String VALUE = "value";
	private static final String NEXT_LINK = "@odata.nextLink";
	private static final String UPLOAD_URL = "uploadUrl";
	private static final String CONFLICT_BEHAVIOR_PARAM = "@microsoft.graph.conflictBehavior";

	/**
	 * Graph recommends a resumable upload session above 4MB.
	 */
	private static final long SIMPLE_UPLOAD_MAX_BYTES = 4L * 1024L * 1024L;

	/**
	 * Upload session chunk size. Graph requires every chunk except the last to be a
	 * multiple of 320 KiB.
	 */
	private static final int UPLOAD_CHUNK_BYTES = 320 * 1024 * 10;

	private final String driveId;
	private final String rootItemId;
	private final Supplier<String> tokenSupplier;

	/**
	 * @param driveId       Graph drive id that holds the tree
	 * @param rootItemId    drive item id that acts as the root for every relative
	 *                      path
	 * @param tokenSupplier supplies a valid bearer token per request
	 */
	public MicrosoftGraphDriveClient(String driveId, String rootItemId, Supplier<String> tokenSupplier) {
		if (isBlank(driveId)) {
			throw new IllegalArgumentException("A drive id is required.");
		}
		if (isBlank(rootItemId)) {
			throw new IllegalArgumentException("A root item id is required.");
		}
		if (tokenSupplier == null) {
			throw new IllegalArgumentException("A token supplier is required.");
		}
		this.driveId = driveId.trim();
		this.rootItemId = rootItemId.trim();
		this.tokenSupplier = tokenSupplier;
	}

	/**
	 * Lists the immediate children of a folder.
	 *
	 * @param relativePath folder path relative to the root, blank for the root
	 *                     itself
	 * @return the child drive items, following pagination to the end
	 * @throws Exception if the listing fails
	 */
	public List<Map<String, Object>> listChildren(String relativePath) throws Exception {
		// $top is the page size, not a cap: getPagedValues follows @odata.nextLink to
		// the end, so a folder larger than this still comes back whole, just in more
		// requests
		return getPagedValues(itemUrl(relativePath) + "/children?$top=200");
	}

	/**
	 * Lists the stored versions of a file.
	 *
	 * <p>
	 * Graph returns versions newest first and does not support {@code $orderby}, so
	 * the order it gives back is preserved. Whether any history exists at all
	 * depends on the versioning settings of the document library.
	 * </p>
	 *
	 * @param itemId drive item id of the file
	 * @return the version entries, newest first
	 * @throws Exception if the listing fails
	 */
	public List<Map<String, Object>> listVersions(String itemId) throws Exception {
		return getPagedValues(itemIdUrl(itemId) + "/versions");
	}

	/**
	 * Downloads the content of one stored version of a file.
	 *
	 * @param itemId    drive item id of the file
	 * @param versionId version identifier, as reported by {@link #listVersions}
	 * @return the bytes of that version
	 * @throws Exception if the download fails
	 */
	public byte[] downloadVersionBytes(String itemId, String versionId) throws Exception {
		if (isBlank(versionId)) {
			throw new IllegalArgumentException("A version id is required.");
		}
		String versionUrl = itemIdUrl(itemId) + "/versions/" + encodeSegment(versionId.trim());

		// a version carries a pre-authenticated download url only sometimes, so read
		// the version first and fall back to its content endpoint. As with items, the
		// pre-authenticated url is called without the bearer header
		byte[] bytes = null;
		Map<String, Object> version = parseObject(
				HttpHelperUtility.getRequest(versionUrl, authHeaders(), null, null, null));
		if (version != null) {
			Object preAuthUrl = version.get(DOWNLOAD_URL);
			if (preAuthUrl != null && !preAuthUrl.toString().trim().isEmpty()) {
				bytes = HttpHelperUtility.getRequestBytes(preAuthUrl.toString(), null, null, null, null);
			}
		}
		if (bytes == null) {
			bytes = HttpHelperUtility.getRequestBytes(versionUrl + "/content", authHeaders(), null, null, null);
		}
		if (bytes == null) {
			throw new IllegalStateException(
					"Downloaded content was empty for version " + versionId + " of item " + itemId);
		}
		return bytes;
	}

	/**
	 * Runs a Graph collection request, following {@code @odata.nextLink} to the
	 * end.
	 */
	private List<Map<String, Object>> getPagedValues(String url) throws Exception {
		List<Map<String, Object>> collected = new ArrayList<>();
		String nextUrl = url;
		while (nextUrl != null) {
			String response = HttpHelperUtility.getRequest(nextUrl, authHeaders(), null, null, null);
			Map<String, Object> json = parseObject(response);
			if (json == null) {
				break;
			}
			Object value = json.get(VALUE);
			if (value instanceof List) {
				for (Object item : (List<?>) value) {
					if (item instanceof Map) {
						collected.add(asStringKeyedMap(item));
					}
				}
			}
			Object next = json.get(NEXT_LINK);
			nextUrl = next == null ? null : next.toString();
		}
		return collected;
	}

	/**
	 * Reads the metadata for one item.
	 *
	 * @param relativePath item path relative to the root, blank for the root itself
	 * @return the drive item, or null when nothing exists at that path
	 * @throws Exception if the lookup fails for a reason other than the item being
	 *                   absent
	 */
	public Map<String, Object> getItem(String relativePath) throws Exception {
		try {
			String response = HttpHelperUtility.getRequest(itemUrl(relativePath), authHeaders(), null, null, null);
			return parseObject(response);
		} catch (IllegalArgumentException e) {
			if (isNotFound(e)) {
				return null;
			}
			throw e;
		}
	}

	/**
	 * Whether an item exists at the given path.
	 *
	 * @param relativePath item path relative to the root
	 * @return true when Graph returns an item
	 * @throws Exception if the lookup fails
	 */
	public boolean exists(String relativePath) throws Exception {
		return getItem(relativePath) != null;
	}

	/**
	 * Walks a folder and returns every file beneath it, folders excluded.
	 *
	 * @param relativePath folder path relative to the root, blank for the root
	 * @return each file's path relative to {@code relativePath}, mapped to its
	 *         drive item
	 * @throws Exception if any listing fails
	 */
	public Map<String, Map<String, Object>> listFilesRecursively(String relativePath) throws Exception {
		Map<String, Map<String, Object>> files = new HashMap<>();
		collectFiles(relativePath, "", files);
		return files;
	}

	private void collectFiles(String basePath, String prefix, Map<String, Map<String, Object>> collected)
			throws Exception {
		for (Map<String, Object> child : listChildren(basePath)) {
			Object nameObj = child.get(NAME);
			if (nameObj == null) {
				continue;
			}
			String name = nameObj.toString();
			String childRelative = prefix.isEmpty() ? name : prefix + "/" + name;
			if (isFolder(child)) {
				collectFiles(joinPath(basePath, name), childRelative, collected);
			} else {
				collected.put(childRelative, child);
			}
		}
	}

	/**
	 * Downloads a file into memory.
	 *
	 * @param relativePath file path relative to the root
	 * @return the file bytes
	 * @throws Exception             if the download fails
	 * @throws IllegalStateException if nothing exists at that path
	 */
	public byte[] downloadBytes(String relativePath) throws Exception {
		Map<String, Object> item = getItem(relativePath);
		if (item == null) {
			throw new IllegalStateException("No file exists at path: " + relativePath);
		}
		return downloadBytes(item, relativePath);
	}

	/**
	 * Downloads a file into memory using an item that has already been read.
	 *
	 * <p>
	 * The pre-authenticated download url is used when present and is called without
	 * the bearer header, since SharePoint rejects a request that carries both.
	 * </p>
	 *
	 * @param item         a drive item, typically from a listing
	 * @param relativePath the item's path, used only for error messages
	 * @return the file bytes
	 * @throws Exception if the download fails
	 */
	public byte[] downloadBytes(Map<String, Object> item, String relativePath) throws Exception {
		Object preAuthUrl = item.get(DOWNLOAD_URL);
		byte[] bytes;
		if (preAuthUrl != null && !preAuthUrl.toString().trim().isEmpty()) {
			bytes = HttpHelperUtility.getRequestBytes(preAuthUrl.toString(), null, null, null, null);
		} else {
			Object itemId = item.get(ID);
			String contentUrl = itemId != null
					? GRAPH_BASE + "/drives/" + this.driveId + "/items/" + itemId.toString().trim() + "/content"
					: itemUrl(relativePath) + "/content";
			bytes = HttpHelperUtility.getRequestBytes(contentUrl, authHeaders(), null, null, null);
		}
		if (bytes == null) {
			throw new IllegalStateException("Downloaded content was empty for path: " + relativePath);
		}
		return bytes;
	}

	/**
	 * Uploads a local file to a path relative to the root, creating any missing
	 * intermediate folders.
	 *
	 * <p>
	 * Files at or below 4MB go up in a single request; larger files stream through
	 * a resumable upload session.
	 * </p>
	 *
	 * @param localFile        the file to upload
	 * @param relativePath     destination path relative to the root, including the
	 *                         file name
	 * @param conflictBehavior {@code fail}, {@code rename} or {@code replace}
	 * @return the resulting drive item, or null when Graph returns no body
	 * @throws Exception if the upload fails
	 */
	public Map<String, Object> uploadFile(File localFile, String relativePath, String conflictBehavior)
			throws Exception {
		if (localFile == null || !localFile.exists() || !localFile.isFile()) {
			throw new IllegalArgumentException("Not a readable file: " + localFile);
		}
		String normalized = normalize(relativePath);
		if (normalized.isEmpty()) {
			throw new IllegalArgumentException("A destination path including the file name is required.");
		}
		// Graph does not create missing parents on a path addressed upload, it 404s
		int lastSlash = normalized.lastIndexOf('/');
		if (lastSlash > 0) {
			createFolderPath(normalized.substring(0, lastSlash));
		}

		String behavior = isBlank(conflictBehavior) ? "replace" : conflictBehavior.trim();
		String response;
		if (localFile.length() <= SIMPLE_UPLOAD_MAX_BYTES) {
			String url = itemUrl(normalized) + "/content?" + CONFLICT_BEHAVIOR_PARAM + "=" + behavior;
			byte[] fileBytes = Files.readAllBytes(localFile.toPath());
			response = HttpHelperUtility.putRequestBytesBody(url, authHeaders(), fileBytes,
					ContentType.APPLICATION_OCTET_STREAM, null, null, null);
		} else {
			response = sessionUpload(localFile, normalized, behavior);
		}
		return parseObject(response);
	}

	private String sessionUpload(File localFile, String relativePath, String conflictBehavior) throws Exception {
		final String CONTENT_RANGE = "Content-Range";

		String sessionUrl = itemUrl(relativePath) + "/createUploadSession";
		Map<String, Object> item = new HashMap<>();
		item.put(CONFLICT_BEHAVIOR_PARAM, conflictBehavior);
		Map<String, Object> body = new HashMap<>();
		body.put("item", item);

		String sessionResponse = HttpHelperUtility.postRequestStringBody(sessionUrl, jsonHeaders(), GSON.toJson(body),
				ContentType.APPLICATION_JSON, null, null, null);
		Map<String, Object> sessionJson = parseObject(sessionResponse);
		if (sessionJson == null || sessionJson.get(UPLOAD_URL) == null) {
			throw new IllegalStateException("Graph did not return an upload session for path: " + relativePath);
		}
		String uploadUrl = sessionJson.get(UPLOAD_URL).toString();

		// the upload session url is pre-authenticated, so no bearer header is sent
		long totalSize = localFile.length();
		long offset = 0;
		String lastResponse = null;
		try (InputStream is = new BufferedInputStream(new FileInputStream(localFile))) {
			byte[] chunk = new byte[UPLOAD_CHUNK_BYTES];
			int bytesRead;
			while ((bytesRead = readChunk(is, chunk)) > 0) {
				byte[] payload = bytesRead == chunk.length ? chunk : Arrays.copyOf(chunk, bytesRead);
				Map<String, String> chunkHeaders = new HashMap<>();
				chunkHeaders.put(CONTENT_RANGE, "bytes " + offset + "-" + (offset + bytesRead - 1) + "/" + totalSize);
				String chunkResponse = HttpHelperUtility.putRequestBytesBody(uploadUrl, chunkHeaders, payload,
						ContentType.APPLICATION_OCTET_STREAM, null, null, null);
				if (chunkResponse != null && !chunkResponse.trim().isEmpty()) {
					lastResponse = chunkResponse;
				}
				offset += bytesRead;
			}
		}
		if (offset != totalSize) {
			throw new IllegalStateException(
					"Uploaded " + offset + " of " + totalSize + " bytes for path: " + relativePath);
		}
		return lastResponse;
	}

	/**
	 * Creates a folder and any missing folders above it.
	 *
	 * @param relativePath folder path relative to the root; blank is a no-op
	 * @throws Exception if a folder cannot be created
	 */
	public void createFolderPath(String relativePath) throws Exception {
		String normalized = normalize(relativePath);
		if (normalized.isEmpty()) {
			return;
		}
		StringBuilder built = new StringBuilder();
		for (String segment : normalized.split("/")) {
			if (segment.isEmpty()) {
				continue;
			}
			String parent = built.toString();
			if (built.length() > 0) {
				built.append("/");
			}
			built.append(segment);
			if (exists(built.toString())) {
				continue;
			}
			createFolder(parent, segment);
		}
	}

	private void createFolder(String parentRelativePath, String folderName) throws Exception {
		String url = itemUrl(parentRelativePath) + "/children";
		Map<String, Object> body = new HashMap<>();
		body.put(NAME, folderName);
		body.put(FOLDER, new HashMap<String, Object>());
		// another writer may have created it between the exists check and here
		body.put(CONFLICT_BEHAVIOR_PARAM, "fail");
		try {
			HttpHelperUtility.postRequestStringBody(url, jsonHeaders(), GSON.toJson(body), ContentType.APPLICATION_JSON,
					null, null, null);
		} catch (IllegalArgumentException e) {
			if (isAlreadyExists(e)) {
				classLogger.debug("Folder '{}' already existed under '{}'", folderName, parentRelativePath);
				return;
			}
			throw e;
		}
	}

	/**
	 * Deletes an item, and everything beneath it when it is a folder.
	 *
	 * @param relativePath item path relative to the root
	 * @return true when something was deleted, false when nothing was there
	 * @throws IllegalArgumentException if the path resolves to the root itself
	 * @throws Exception                if the delete fails
	 */
	public boolean delete(String relativePath) throws Exception {
		String normalized = normalize(relativePath);
		if (normalized.isEmpty()) {
			throw new IllegalArgumentException(
					"Refusing to delete the root of the drive. Name a path beneath it instead.");
		}
		try {
			HttpHelperUtility.deleteRequestStringBody(itemUrl(normalized), authHeaders(), null, null, null);
			return true;
		} catch (IllegalArgumentException e) {
			if (isNotFound(e)) {
				return false;
			}
			throw e;
		}
	}

	/**
	 * Deletes everything inside a folder while keeping the folder itself.
	 *
	 * @param relativePath folder path relative to the root, blank for the root
	 * @throws Exception if a delete fails
	 */
	public void deleteChildren(String relativePath) throws Exception {
		for (Map<String, Object> child : listChildren(relativePath)) {
			Object nameObj = child.get(NAME);
			if (nameObj == null) {
				continue;
			}
			delete(joinPath(relativePath, nameObj.toString()));
		}
	}

	/**
	 * @param item a drive item
	 * @return true when the item is a folder
	 */
	public static boolean isFolder(Map<String, Object> item) {
		return item != null && item.get(FOLDER) != null;
	}

	/**
	 * @param item a drive item
	 * @return the item size in bytes, or 0 when absent
	 */
	public static long getSize(Map<String, Object> item) {
		Object size = item == null ? null : item.get(SIZE);
		if (size instanceof Number) {
			return ((Number) size).longValue();
		}
		return 0L;
	}

	/**
	 * @param item a drive item
	 * @return the mime type reported for a file, or null for folders
	 */
	@SuppressWarnings("unchecked")
	public static String getMimeType(Map<String, Object> item) {
		Object file = item == null ? null : item.get(FILE);
		if (file instanceof Map) {
			Object mimeType = ((Map<String, Object>) file).get(MIME_TYPE);
			return mimeType == null ? null : mimeType.toString();
		}
		return null;
	}

	/**
	 * Joins a base path and a child name into a relative path.
	 *
	 * @param basePath  base path, may be blank
	 * @param childName the child segment
	 * @return the joined relative path
	 */
	public static String joinPath(String basePath, String childName) {
		String base = normalize(basePath);
		String child = normalize(childName);
		if (base.isEmpty()) {
			return child;
		}
		if (child.isEmpty()) {
			return base;
		}
		return base + "/" + child;
	}

	/**
	 * Builds the Graph url for an item at a path relative to the root. A blank path
	 * addresses the root item itself, which cannot use the path addressed form.
	 */
	private String itemUrl(String relativePath) {
		String normalized = normalize(relativePath);
		String base = itemIdUrl(this.rootItemId);
		if (normalized.isEmpty()) {
			return base;
		}
		return base + ":/" + encodePath(normalized) + ":";
	}

	/**
	 * Builds the Graph url for an item addressed by its id rather than by path.
	 * Version endpoints are only documented in the id addressed form, so callers
	 * resolve the path to an item first.
	 */
	private String itemIdUrl(String itemId) {
		if (isBlank(itemId)) {
			throw new IllegalArgumentException("An item id is required.");
		}
		return GRAPH_BASE + "/drives/" + this.driveId + "/items/" + itemId.trim();
	}

	private Map<String, String> authHeaders() {
		return MicrosoftLoginUtils.getAuthorizationHeader(this.tokenSupplier.get());
	}

	private Map<String, String> jsonHeaders() {
		return MicrosoftLoginUtils.getBearerHeader(this.tokenSupplier.get());
	}

	private static Map<String, Object> parseObject(String response) {
		if (response == null || response.trim().isEmpty()) {
			return null;
		}
		return GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> asStringKeyedMap(Object value) {
		return (Map<String, Object>) value;
	}

	/**
	 * Fills the buffer as far as the stream allows so that every chunk except the
	 * last keeps the size Graph requires.
	 */
	private static int readChunk(InputStream is, byte[] buffer) throws Exception {
		int total = 0;
		while (total < buffer.length) {
			int read = is.read(buffer, total, buffer.length - total);
			if (read == -1) {
				break;
			}
			total += read;
		}
		return total;
	}

	/**
	 * Normalizes a relative path to use forward slashes with no leading or trailing
	 * separator.
	 */
	private static String normalize(String path) {
		if (path == null) {
			return "";
		}
		String normalized = path.trim().replace('\\', '/');
		while (normalized.startsWith("/")) {
			normalized = normalized.substring(1);
		}
		while (!normalized.isEmpty() && normalized.endsWith("/")) {
			normalized = normalized.substring(0, normalized.length() - 1);
		}
		return normalized;
	}

	/**
	 * Encodes each segment of a relative path, keeping the separators intact.
	 */
	private static String encodePath(String path) {
		StringBuilder encoded = new StringBuilder();
		for (String segment : path.split("/")) {
			if (segment.isEmpty()) {
				continue;
			}
			if (encoded.length() > 0) {
				encoded.append("/");
			}
			encoded.append(encodeSegment(segment));
		}
		return encoded.toString();
	}

	/**
	 * Encodes one path segment for use in a url.
	 */
	private static String encodeSegment(String segment) {
		return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
	}

	/**
	 * HttpHelperUtility surfaces a non-2xx as IllegalArgumentException carrying the
	 * status and body, so the Graph error has to be read back out of the message.
	 */
	private static boolean isNotFound(IllegalArgumentException e) {
		return hasStatus(e, 404) || messageContains(e, "itemnotfound");
	}

	private static boolean isAlreadyExists(IllegalArgumentException e) {
		return hasStatus(e, 409) || messageContains(e, "namealreadyexists");
	}

	/**
	 * Matches on the status phrase the helper builds rather than on the bare
	 * digits, which a url or a response body could contain by coincidence.
	 */
	private static boolean hasStatus(IllegalArgumentException e, int statusCode) {
		return messageContains(e, "returned http " + statusCode);
	}

	private static boolean messageContains(IllegalArgumentException e, String needle) {
		String message = e.getMessage();
		return message != null && message.toLowerCase().contains(needle);
	}

	private static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}
}
