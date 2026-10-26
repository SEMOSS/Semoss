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
package prerna.io.connector.ms.teams;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.MicrosoftTokenFiller;
import prerna.security.HttpHelperUtility;

/**
 * Helper utility for Microsoft Teams connector operations.
 *
 * <p>
 * Channel files are not stored in Teams itself; every channel maps to a folder
 * inside the team's SharePoint document library. The file operations therefore
 * resolve the channel's {@code filesFolder} first to obtain the drive id and
 * the folder item id, and then act against the Graph drive item endpoints.
 * </p>
 */
public class MicrosoftTeamsHelper {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final String GRAPH_BASE = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0";

	private static final String ID = "id";
	private static final String NAME = "name";
	private static final String SIZE = "size";
	private static final String VALUE = "value";
	private static final String FOLDER = "folder";
	private static final String SUCCESS = "success";
	private static final String WEB_URL = "webUrl";
	private static final String DRIVE_ID = "driveId";
	private static final String FILE_PATH = "filePath";
	private static final String IS_FOLDER = "isFolder";
	private static final String MIME_TYPE = "mimeType";
	private static final String FOLDER_ID = "folderId";
	private static final String DESCRIPTION = "description";
	private static final String DISPLAY_NAME = "displayName";
	private static final String CHILD_COUNT = "childCount";
	private static final String UPLOAD_URL = "uploadUrl";
	private static final String PARENT_REFERENCE = "parentReference";
	private static final String MEMBERSHIP_TYPE = "membershipType";
	private static final String LAST_MODIFIED = "lastModifiedDateTime";
	private static final String DOWNLOAD_URL = "@microsoft.graph.downloadUrl";
	private static final String CONFLICT_BEHAVIOR_PARAM = "@microsoft.graph.conflictBehavior";

	private static final String CONFLICT_FAIL = "fail";
	private static final String CONFLICT_RENAME = "rename";
	private static final String CONFLICT_REPLACE = "replace";
	private static final List<String> CONFLICT_BEHAVIORS = Arrays.asList(CONFLICT_FAIL, CONFLICT_RENAME,
			CONFLICT_REPLACE);

	/**
	 * Graph recommends a resumable upload session for anything larger than 4MB.
	 */
	private static final long SIMPLE_UPLOAD_MAX_BYTES = 4L * 1024L * 1024L;

	/**
	 * Upload session chunk size. Graph requires every chunk except the last to be a
	 * multiple of 320 KiB.
	 */
	private static final int UPLOAD_CHUNK_BYTES = 320 * 1024 * 10;

	/**
	 * Utility class constructor intentionally hidden.
	 */
	private MicrosoftTeamsHelper() {

	}

	/**
	 * Lists the Microsoft Teams the current user has joined.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param limit       maximum number of teams to return; values less than or
	 *                    equal to 0 return every team
	 * @return list of team metadata maps containing {@code id}, {@code displayName}
	 *         and {@code description}
	 * @throws Exception if the list retrieval fails
	 */
	public static List<Map<String, Object>> listTeams(String accessToken, int limit) throws Exception {
		final String JOINED_TEAMS = GRAPH_BASE + "/me/joinedTeams";

		try {
			Map<String, String> headers = MicrosoftLoginUtils.getBearerHeader(accessToken);
			String response = HttpHelperUtility.getRequest(JOINED_TEAMS, headers, null, null, null);
			List<Map<String, Object>> teams = new ArrayList<>();
			for (Map<String, Object> team : getValueList(response)) {
				Map<String, Object> map = new HashMap<>();
				map.put(ID, team.get(ID));
				map.put(DISPLAY_NAME, team.get(DISPLAY_NAME));
				map.put(DESCRIPTION, team.get(DESCRIPTION));
				teams.add(map);
				if (limit > 0 && teams.size() >= limit) {
					break;
				}
			}
			return teams;
		} catch (Exception e) {
			classLogger.error("Failed to list the Microsoft Teams for the current user.", e);
			throw e;
		}
	}

	/**
	 * Lists the channels within a Microsoft Team.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param teamId      id of the team whose channels are listed
	 * @param limit       maximum number of channels to return; values less than or
	 *                    equal to 0 return every channel
	 * @return list of channel metadata maps
	 * @throws IllegalArgumentException if {@code teamId} is null or blank
	 * @throws Exception                if the list retrieval fails
	 */
	public static List<Map<String, Object>> listChannels(String accessToken, String teamId, int limit)
			throws Exception {
		final String CHANNELS = GRAPH_BASE + "/teams/%s/channels";

		try {
			requireValue(teamId, "Team ID is required to list Microsoft Teams channels.");
			Map<String, String> headers = MicrosoftLoginUtils.getBearerHeader(accessToken);
			String url = String.format(CHANNELS, teamId.trim());
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			List<Map<String, Object>> channels = new ArrayList<>();
			for (Map<String, Object> channel : getValueList(response)) {
				Map<String, Object> map = new HashMap<>();
				map.put(ID, channel.get(ID));
				map.put(DISPLAY_NAME, channel.get(DISPLAY_NAME));
				map.put(DESCRIPTION, channel.get(DESCRIPTION));
				map.put(MEMBERSHIP_TYPE, channel.get(MEMBERSHIP_TYPE));
				map.put(WEB_URL, channel.get(WEB_URL));
				channels.add(map);
				if (limit > 0 && channels.size() >= limit) {
					break;
				}
			}
			return channels;
		} catch (Exception e) {
			classLogger.error("Failed to list the channels for Microsoft Team '{}'.", teamId, e);
			throw e;
		}
	}

	/**
	 * Resolves the SharePoint folder that backs a Teams channel.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param teamId      id of the team that owns the channel
	 * @param channelId   id of the channel
	 * @return map containing the {@code driveId}, the {@code folderId} of the
	 *         channel root folder, the folder {@code name} and its {@code webUrl}
	 * @throws IllegalArgumentException if required inputs are missing
	 * @throws IllegalStateException    if the response does not carry a drive id
	 * @throws Exception                if the lookup fails
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> getChannelFilesFolder(String accessToken, String teamId, String channelId)
			throws Exception {
		final String FILES_FOLDER = GRAPH_BASE + "/teams/%s/channels/%s/filesFolder";

		try {
			requireValue(teamId, "Team ID is required to resolve the Microsoft Teams channel folder.");
			requireValue(channelId, "Channel ID is required to resolve the Microsoft Teams channel folder.");

			Map<String, String> headers = MicrosoftLoginUtils.getBearerHeader(accessToken);
			String url = String.format(FILES_FOLDER, teamId.trim(), channelId.trim());
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			if (json == null) {
				throw new IllegalStateException(
						"Microsoft Graph returned no folder for channel id = " + channelId + " in team " + teamId);
			}

			String driveId = null;
			Object parentReference = json.get(PARENT_REFERENCE);
			if (parentReference instanceof Map) {
				Object parentDriveId = ((Map<String, Object>) parentReference).get(DRIVE_ID);
				driveId = parentDriveId == null ? null : parentDriveId.toString();
			}
			if (driveId == null || driveId.trim().isEmpty()) {
				throw new IllegalStateException("Unable to resolve the SharePoint drive backing channel id = "
						+ channelId + " in team " + teamId);
			}
			Object folderId = json.get(ID);
			if (folderId == null || folderId.toString().trim().isEmpty()) {
				throw new IllegalStateException("Unable to resolve the SharePoint folder backing channel id = "
						+ channelId + " in team " + teamId);
			}

			Map<String, Object> folder = new HashMap<>();
			folder.put(DRIVE_ID, driveId.trim());
			folder.put(FOLDER_ID, folderId.toString().trim());
			folder.put(NAME, json.get(NAME));
			folder.put(WEB_URL, json.get(WEB_URL));
			return folder;
		} catch (Exception e) {
			classLogger.error("Failed to resolve the files folder for channel '{}' in Microsoft Team '{}'.", channelId,
					teamId, e);
			throw e;
		}
	}

	/**
	 * Lists the files and folders stored in a Microsoft Teams channel.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param teamId      id of the team that owns the channel
	 * @param channelId   id of the channel
	 * @param folderPath  optional path relative to the channel root folder; when
	 *                    blank the channel root folder is listed
	 * @param limit       maximum number of items to return; values less than or
	 *                    equal to 0 return every item
	 * @return list of drive item metadata maps, each carrying the {@code driveId}
	 *         needed to download the item
	 * @throws IllegalArgumentException if required inputs are missing
	 * @throws Exception                if the list retrieval fails
	 */
	public static List<Map<String, Object>> listChannelFiles(String accessToken, String teamId, String channelId,
			String folderPath, int limit) throws Exception {
		final String ROOT_CHILDREN = GRAPH_BASE + "/drives/%s/items/%s/children";
		final String PATH_CHILDREN = GRAPH_BASE + "/drives/%s/items/%s:/%s:/children";

		try {
			Map<String, Object> folder = getChannelFilesFolder(accessToken, teamId, channelId);
			String driveId = folder.get(DRIVE_ID).toString();
			String folderId = folder.get(FOLDER_ID).toString();

			String relativePath = encodeRelativePath(folderPath);
			String url;
			if (relativePath.isEmpty()) {
				url = String.format(ROOT_CHILDREN, driveId, folderId);
			} else {
				url = String.format(PATH_CHILDREN, driveId, folderId, relativePath);
			}

			Map<String, String> headers = MicrosoftLoginUtils.getBearerHeader(accessToken);
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			List<Map<String, Object>> items = new ArrayList<>();
			for (Map<String, Object> item : getValueList(response)) {
				items.add(toDriveItemMap(item, driveId));
				if (limit > 0 && items.size() >= limit) {
					break;
				}
			}
			return items;
		} catch (Exception e) {
			classLogger.error("Failed to list the files in channel '{}' of Microsoft Team '{}' under path '{}'.",
					channelId, teamId, folderPath, e);
			throw e;
		}
	}

	/**
	 * Downloads a file from a Microsoft Teams channel to the local filesystem.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param teamId      id of the team that owns the channel; only required when
	 *                    {@code driveId} is not supplied
	 * @param channelId   id of the channel; only required when {@code driveId} is
	 *                    not supplied
	 * @param driveId     optional SharePoint drive id as returned by
	 *                    {@link #listChannelFiles}; skips the channel lookup when
	 *                    supplied
	 * @param itemId      drive item id of the file to download
	 * @param path        local file path, or the destination directory when
	 *                    {@code fileName} is supplied
	 * @param fileName    optional local file name to use when {@code path} is a
	 *                    directory
	 * @return map containing the item {@code id}, the item {@code name}, the local
	 *         {@code filePath} written and {@code success}
	 * @throws IllegalArgumentException if required inputs are missing or invalid
	 * @throws IllegalStateException    if directory creation fails or the
	 *                                  downloaded content is empty
	 * @throws Exception                if the download or the file write fails
	 */
	public static Map<String, Object> downloadFile(String accessToken, String teamId, String channelId, String driveId,
			String itemId, String path, String fileName) throws Exception {
		final String ITEM = GRAPH_BASE + "/drives/%s/items/%s";
		final String ITEM_CONTENT = GRAPH_BASE + "/drives/%s/items/%s/content";

		try {
			requireValue(itemId, "File ID is required to download a Microsoft Teams file.");
			requireValue(path, "Destination path is required to download a Microsoft Teams file.");

			String resolvedDriveId = driveId;
			if (resolvedDriveId == null || resolvedDriveId.trim().isEmpty()) {
				resolvedDriveId = getChannelFilesFolder(accessToken, teamId, channelId).get(DRIVE_ID).toString();
			}

			Map<String, String> headers = MicrosoftLoginUtils.getBearerHeader(accessToken);
			String metadataUrl = String.format(ITEM, resolvedDriveId.trim(), itemId.trim());
			String metadataResponse = HttpHelperUtility.getRequest(metadataUrl, headers, null, null, null);
			Map<String, Object> metadata = GSON.fromJson(metadataResponse, new TypeToken<Map<String, Object>>() {
			}.getType());
			if (metadata == null) {
				throw new IllegalStateException("Microsoft Graph returned no metadata for file id = " + itemId);
			}
			Object itemName = metadata.get(NAME);

			// the pre-authenticated download url must be called without the bearer header,
			// otherwise SharePoint rejects the duplicated credentials
			byte[] fileBytes;
			Object preAuthUrl = metadata.get(DOWNLOAD_URL);
			if (preAuthUrl != null && !preAuthUrl.toString().trim().isEmpty()) {
				fileBytes = HttpHelperUtility.getRequestBytes(preAuthUrl.toString(), null, null, null, null);
			} else {
				String contentUrl = String.format(ITEM_CONTENT, resolvedDriveId.trim(), itemId.trim());
				fileBytes = HttpHelperUtility.getRequestBytes(contentUrl, headers, null, null, null);
			}
			if (fileBytes == null || fileBytes.length == 0) {
				throw new IllegalStateException("Downloaded file content is empty for file id = " + itemId);
			}

			String normalizedFileName = fileName == null ? null : fileName.trim();
			if ((normalizedFileName == null || normalizedFileName.isEmpty()) && new File(path).isDirectory()
					&& itemName != null) {
				// the destination is a directory, so fall back to the name the file carries in
				// the channel
				normalizedFileName = itemName.toString();
			}
			String targetPath;
			if (normalizedFileName == null || normalizedFileName.isEmpty()) {
				targetPath = path;
			} else {
				targetPath = Paths.get(path, normalizedFileName).toString();
			}
			File file = new File(targetPath);
			File parent = file.getParentFile();
			if (parent != null && !parent.exists()) {
				boolean created = parent.mkdirs();
				if (!created) {
					throw new IllegalStateException(
							"Unable to create destination directory at: " + parent.getAbsolutePath());
				}
			}
			try (FileOutputStream fos = new FileOutputStream(file)) {
				fos.write(fileBytes);
				fos.flush();
			}

			Map<String, Object> result = new HashMap<>();
			result.put(ID, itemId);
			result.put(NAME, itemName);
			result.put(FILE_PATH, Path.of(path).relativize(file.toPath()).toString());
			result.put(SUCCESS, true);
			return result;
		} catch (Exception e) {
			classLogger.error("Failed to download Microsoft Teams file '{}' to path '{}'.", itemId, path, e);
			throw e;
		}
	}

	/**
	 * Uploads a local file into a Microsoft Teams channel.
	 *
	 * <p>
	 * Files at or below 4MB are sent as a single request; larger files are streamed
	 * through a resumable Graph upload session.
	 * </p>
	 *
	 * @param accessToken      Microsoft Graph access token for the user
	 * @param teamId           id of the team that owns the channel
	 * @param channelId        id of the channel to upload into
	 * @param folderPath       optional path relative to the channel root folder;
	 *                         when blank the file lands in the channel root folder
	 * @param fileName         name to assign to the uploaded file in the channel
	 * @param filePath         local file path to upload
	 * @param conflictBehavior optional behavior when a file of the same name
	 *                         already exists; one of {@code fail}, {@code rename}
	 *                         or {@code replace}, defaulting to {@code rename}
	 * @return map containing the uploaded item {@code id}, {@code name},
	 *         {@code size}, {@code webUrl} and {@code success}
	 * @throws IllegalArgumentException if required inputs are missing or invalid
	 * @throws Exception                if the upload fails
	 */
	public static Map<String, Object> uploadFile(String accessToken, String teamId, String channelId, String folderPath,
			String fileName, String filePath, String conflictBehavior) throws Exception {
		try {
			requireValue(fileName, "File name is required to upload to a Microsoft Teams channel.");
			requireValue(filePath, "File path is required to upload to a Microsoft Teams channel.");

			File file = new File(filePath);
			if (!file.exists() || !file.isFile()) {
				throw new IllegalArgumentException("The file path does not point to a valid file: " + filePath);
			}

			String conflict = normalizeConflictBehavior(conflictBehavior);
			Map<String, Object> folder = getChannelFilesFolder(accessToken, teamId, channelId);
			String driveId = folder.get(DRIVE_ID).toString();
			String folderId = folder.get(FOLDER_ID).toString();
			String targetPath = buildTargetPath(folderPath, fileName);

			String response;
			if (file.length() <= SIMPLE_UPLOAD_MAX_BYTES) {
				response = simpleUpload(accessToken, driveId, folderId, targetPath, conflict, file);
			} else {
				response = sessionUpload(accessToken, driveId, folderId, targetPath, conflict, fileName, file);
			}

			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			Map<String, Object> result = new HashMap<>();
			if (json != null) {
				result.put(ID, json.get(ID));
				result.put(NAME, json.get(NAME));
				result.put(SIZE, json.get(SIZE));
				result.put(WEB_URL, json.get(WEB_URL));
			} else {
				result.put(NAME, fileName);
			}
			result.put(SUCCESS, true);
			return result;
		} catch (Exception e) {
			classLogger.error("Failed to upload '{}' to channel '{}' of Microsoft Team '{}' as '{}'.", filePath,
					channelId, teamId, fileName, e);
			throw e;
		}
	}

	/**
	 * Uploads the whole file in one request. Used for payloads at or below 4MB.
	 */
	private static String simpleUpload(String accessToken, String driveId, String folderId, String targetPath,
			String conflictBehavior, File file) throws Exception {
		final String UPLOAD = GRAPH_BASE + "/drives/%s/items/%s:/%s:/content?" + CONFLICT_BEHAVIOR_PARAM + "=%s";

		String url = String.format(UPLOAD, driveId, folderId, targetPath, conflictBehavior);
		byte[] fileBytes = Files.readAllBytes(file.toPath());
		Map<String, String> headers = MicrosoftLoginUtils.getAuthorizationHeader(accessToken);
		return HttpHelperUtility.putRequestBytesBody(url, headers, fileBytes, ContentType.APPLICATION_OCTET_STREAM,
				null, null, null);
	}

	/**
	 * Streams the file through a resumable upload session. Used for payloads above
	 * 4MB, where a single request is not supported.
	 */
	private static String sessionUpload(String accessToken, String driveId, String folderId, String targetPath,
			String conflictBehavior, String fileName, File file) throws Exception {
		final String CREATE_SESSION = GRAPH_BASE + "/drives/%s/items/%s:/%s:/createUploadSession";
		final String CONTENT_RANGE = "Content-Range";

		String sessionUrl = String.format(CREATE_SESSION, driveId, folderId, targetPath);
		Map<String, Object> item = new HashMap<>();
		item.put(CONFLICT_BEHAVIOR_PARAM, conflictBehavior);
		item.put(NAME, fileName);
		Map<String, Object> body = new HashMap<>();
		body.put("item", item);

		Map<String, String> headers = MicrosoftLoginUtils.getBearerHeader(accessToken);
		String sessionResponse = HttpHelperUtility.postRequestStringBody(sessionUrl, headers, GSON.toJson(body),
				ContentType.APPLICATION_JSON, null, null, null);
		Map<String, Object> sessionJson = GSON.fromJson(sessionResponse, new TypeToken<Map<String, Object>>() {
		}.getType());
		if (sessionJson == null || sessionJson.get(UPLOAD_URL) == null) {
			throw new IllegalStateException("Microsoft Graph did not return an upload session for file: " + fileName);
		}
		String uploadUrl = sessionJson.get(UPLOAD_URL).toString();

		// the upload session url is pre-authenticated, so the bearer header is omitted
		long totalSize = file.length();
		long offset = 0;
		String lastResponse = null;
		try (InputStream is = new BufferedInputStream(new FileInputStream(file))) {
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
			throw new IllegalStateException("Uploaded " + offset + " of " + totalSize + " bytes for file: " + fileName);
		}
		return lastResponse;
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
	 * Projects the Graph drive item JSON onto the fields the reactors return.
	 */
	@SuppressWarnings("unchecked")
	private static Map<String, Object> toDriveItemMap(Map<String, Object> item, String driveId) {
		final String FILE = "file";

		Map<String, Object> map = new HashMap<>();
		map.put(ID, item.get(ID));
		map.put(NAME, item.get(NAME));
		map.put(DRIVE_ID, driveId);
		map.put(SIZE, item.get(SIZE));
		map.put(WEB_URL, item.get(WEB_URL));
		map.put(LAST_MODIFIED, item.get(LAST_MODIFIED));

		Object folder = item.get(FOLDER);
		map.put(IS_FOLDER, folder != null);
		if (folder instanceof Map) {
			map.put(CHILD_COUNT, ((Map<String, Object>) folder).get(CHILD_COUNT));
		}
		Object file = item.get(FILE);
		if (file instanceof Map) {
			map.put(MIME_TYPE, ((Map<String, Object>) file).get(MIME_TYPE));
		}
		return map;
	}

	/**
	 * Reads the {@code value} collection out of a Graph list response.
	 */
	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> getValueList(String response) {
		Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
		if (json == null) {
			return new ArrayList<>();
		}
		Object value = json.get(VALUE);
		if (!(value instanceof List)) {
			return new ArrayList<>();
		}
		return (List<Map<String, Object>>) value;
	}

	/**
	 * Builds the drive item path, relative to the channel root folder, that the
	 * upload targets.
	 */
	private static String buildTargetPath(String folderPath, String fileName) {
		String encodedFolder = encodeRelativePath(folderPath);
		String encodedFile = encodePathSegment(fileName.trim());
		return encodedFolder.isEmpty() ? encodedFile : encodedFolder + "/" + encodedFile;
	}

	/**
	 * Encodes each segment of a drive relative path, keeping the separators intact.
	 *
	 * @param path optional path, which may use either separator style
	 * @return the encoded path without leading or trailing separators, or an empty
	 *         string when nothing was supplied
	 */
	private static String encodeRelativePath(String path) {
		if (path == null || path.trim().isEmpty()) {
			return "";
		}
		String normalized = path.trim().replace('\\', '/');
		StringBuilder encoded = new StringBuilder();
		for (String segment : normalized.split("/")) {
			if (segment.isEmpty()) {
				continue;
			}
			if (encoded.length() > 0) {
				encoded.append("/");
			}
			encoded.append(encodePathSegment(segment));
		}
		return encoded.toString();
	}

	/**
	 * URL encodes a single user supplied path segment. Graph ids are opaque but
	 * already URL safe, so they are placed into the path as they are received.
	 */
	private static String encodePathSegment(String segment) {
		return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
	}

	/**
	 * Validates the requested conflict behavior, defaulting to the non destructive
	 * {@code rename}.
	 */
	private static String normalizeConflictBehavior(String conflictBehavior) {
		if (conflictBehavior == null || conflictBehavior.trim().isEmpty()) {
			return CONFLICT_RENAME;
		}
		String normalized = conflictBehavior.trim().toLowerCase(Locale.ROOT);
		if (!CONFLICT_BEHAVIORS.contains(normalized)) {
			throw new IllegalArgumentException(
					"Conflict behavior must be one of " + CONFLICT_BEHAVIORS + " but received: " + conflictBehavior);
		}
		return normalized;
	}

	/**
	 * Guards against missing required string inputs.
	 */
	private static void requireValue(String value, String message) {
		if (value == null || value.trim().isEmpty()) {
			throw new IllegalArgumentException(message);
		}
	}

}
