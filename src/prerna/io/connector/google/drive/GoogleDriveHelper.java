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
package prerna.io.connector.google.drive;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.security.HttpHelperUtility;

/**
 * Helper utility for Google Drive connector operations.
 */
public class GoogleDriveHelper {

	private static final Logger classLogger = LogManager.getLogger(GoogleDriveHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final String ID = "id";
	private static final String SUCCESS = "success";
	private static final String BOUNDARY = "----MyBoundary" + UUID.randomUUID();

	/**
	 * Utility class constructor intentionally hidden.
	 */
	private GoogleDriveHelper() {

	}

	/**
	 * Uploads a local file to Google Drive using a multipart request.
	 *
	 * @param accessToken OAuth access token for the Google user
	 * @param fileName    target file name to assign in Google Drive
	 * @param filePath    local file path to upload
	 * @return map containing upload result metadata, including {@code id} and
	 *         {@code success}
	 * @throws IllegalArgumentException if required inputs are missing or invalid
	 * @throws Exception                if the upload fails
	 */
	public static Map<String, Object> uploadFile(String accessToken, String fileName, String filePath)
			throws Exception {
		final String NAME = "name";
		final String UTF_8 = "UTF-8";
		final String SEPARATOR = "--";
		final String MIME_TYPE = "mimeType";
		final String LINE_FEED = "\r\n";
		final String CONTENT_TYPE = "Content-Type: ";
		final String CONTENT_TYPE_APPLICATION_JSON_CHARSET_UTF_8 = "Content-Type: application/json; charset=UTF-8";
		final String GOOGLE_DRIVE_UPLOAD = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart";

		try {
			if (accessToken == null || accessToken.trim().isEmpty()) {
				throw new IllegalArgumentException("Access token is required to upload a Google Drive file.");
			}
			if (fileName == null || fileName.trim().isEmpty()) {
				throw new IllegalArgumentException("File name is required to upload to Google Drive.");
			}
			if (filePath == null || filePath.trim().isEmpty()) {
				throw new IllegalArgumentException("File path is required to upload to Google Drive.");
			}

			File file = new File(filePath);
			if (!file.exists() || !file.isFile()) {
				throw new IllegalArgumentException("The file path does not point to a valid file: " + filePath);
			}

			Map<String, String> headers = getBearerHeader(accessToken, BOUNDARY);
			Map<String, Object> metadata = new HashMap<>();
			metadata.put(NAME, fileName);
			metadata.put(MIME_TYPE, mimeType(fileName));
			String metadataJson = GSON.toJson(metadata);

			try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
					FileInputStream fis = new FileInputStream(file)) {
				baos.write((SEPARATOR + BOUNDARY + LINE_FEED).getBytes(UTF_8));
				baos.write((CONTENT_TYPE_APPLICATION_JSON_CHARSET_UTF_8 + LINE_FEED).getBytes(UTF_8));
				baos.write(LINE_FEED.getBytes(UTF_8));
				baos.write(metadataJson.getBytes(UTF_8));
				baos.write(LINE_FEED.getBytes(UTF_8));

				baos.write((SEPARATOR + BOUNDARY + LINE_FEED).getBytes(UTF_8));
				baos.write((CONTENT_TYPE + mimeType(fileName) + LINE_FEED).getBytes(UTF_8));
				baos.write(LINE_FEED.getBytes(UTF_8));

				byte[] buffer = new byte[4096];
				int bytesRead;
				while ((bytesRead = fis.read(buffer)) != -1) {
					baos.write(buffer, 0, bytesRead);
				}

				baos.write(LINE_FEED.getBytes(UTF_8));
				baos.write((SEPARATOR + BOUNDARY + SEPARATOR + LINE_FEED).getBytes(UTF_8));

				String response = HttpHelperUtility.postRequestBytesBody(GOOGLE_DRIVE_UPLOAD, headers,
						baos.toByteArray(), ContentType.APPLICATION_OCTET_STREAM, null, null, null);
				Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
				}.getType());
				Map<String, Object> result = new HashMap<>();
				result.put(ID, json.get(ID));
				result.put(SUCCESS, true);
				return result;
			}
		} catch (Exception e) {
			classLogger.error("Failed to upload Google Drive file '{}' from path '{}'.", fileName, filePath, e);
			throw e;
		}
	}

	/**
	 * Reads metadata for a Google Drive file.
	 *
	 * @param accessToken OAuth access token for the Google user
	 * @param fileId      Google Drive file ID
	 * @return map containing selected metadata fields and a generated
	 *         {@code viewLink}
	 * @throws IllegalArgumentException if {@code fileId} is null or blank
	 * @throws Exception                if metadata retrieval fails
	 */
	public static Map<String, Object> readFile(String accessToken, String fileId) throws Exception {
		final String VIEW_LINK = "viewLink";
		final String GOOGLE_FILE_VIEW_LINK = "https://drive.google.com/file/d/%s/view";
		final String GOOGLE_DRIVE_READ = "https://www.googleapis.com/drive/v3/files/%s";

		try {
			if (fileId == null || fileId.trim().isEmpty()) {
				throw new IllegalArgumentException("File ID is required to read a Google Drive file.");
			}
			Map<String, String> headers = getBearerHeader(accessToken, null);
			String url = String.format(GOOGLE_DRIVE_READ, fileId);
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			String viewLink = String.format(GOOGLE_FILE_VIEW_LINK, fileId);
			Map<String, Object> map = new HashMap<>();
			map.put(ID, json.get(ID));
			map.put(VIEW_LINK, viewLink);
			return map;
		} catch (Exception e) {
			classLogger.error("Failed to read Google Drive file metadata for file id '{}'.", fileId, e);
			throw e;
		}
	}

	/**
	 * Downloads a Google Drive file to the local filesystem.
	 *
	 * @param accessToken OAuth access token for the Google user
	 * @param fileId      Google Drive file ID
	 * @param path        local file path or destination directory
	 * @param fileName    optional file name to use when {@code path} is a directory
	 * @throws IllegalArgumentException if required inputs are missing or invalid
	 * @throws IllegalStateException    if directory creation fails or downloaded
	 *                                  content is empty
	 * @throws Exception                if download or file write fails
	 */
	public static void downloadFile(String accessToken, String fileId, String path, String fileName) throws Exception {
		final String GOOGLE_DRIVE_DOWNLOAD = "https://www.googleapis.com/drive/v3/files/%s?alt=media";

		try {
			if (fileId == null || fileId.trim().isEmpty()) {
				throw new IllegalArgumentException("File ID is required to download a Google Drive file.");
			}
			if (path == null || path.trim().isEmpty()) {
				throw new IllegalArgumentException("Destination path is required to download a Google Drive file.");
			}

			Map<String, String> headers = getBearerHeader(accessToken, null);
			String downloadUrl = String.format(GOOGLE_DRIVE_DOWNLOAD, fileId);
			byte[] fileBytes = HttpHelperUtility.getRequestBytes(downloadUrl, headers, null, null, null);
			if (fileBytes == null || fileBytes.length == 0) {
				throw new IllegalStateException("Downloaded file content is empty for fileId = " + fileId);
			}
			String normalizedFileName = fileName == null ? null : fileName.trim();
			String filePath;
			if (normalizedFileName == null || normalizedFileName.isEmpty()) {
				filePath = path;
			} else {
				filePath = Paths.get(path, normalizedFileName).toString();
			}
			File file = new File(filePath);
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
		} catch (Exception e) {
			classLogger.error("Failed to download Google Drive file '{}' to path '{}'.", fileId, path, e);
			throw e;
		}
	}

	/**
	 * Deletes a file from Google Drive.
	 *
	 * @param accessToken OAuth access token for the Google user
	 * @param fileId      Google Drive file ID
	 * @return map containing operation status
	 * @throws IllegalArgumentException if {@code fileId} is null or blank
	 * @throws Exception                if delete fails
	 */
	public static Map<String, Object> deleteFile(String accessToken, String fileId) throws Exception {
		final String GOOGLE_DRIVE_DELETE = "https://www.googleapis.com/drive/v3/files/%s";

		try {
			if (fileId == null || fileId.trim().isEmpty()) {
				throw new IllegalArgumentException("File ID is required to delete a Google Drive file.");
			}
			Map<String, String> headers = getBearerHeader(accessToken, null);
			String deleteUrl = String.format(GOOGLE_DRIVE_DELETE, fileId);
			HttpHelperUtility.deleteRequestStringBody(deleteUrl, headers, null, null, null);
			Map<String, Object> result = new HashMap<>();
			result.put(SUCCESS, true);
			return result;
		} catch (Exception e) {
			classLogger.error("Failed to delete Google Drive file '{}'.", fileId, e);
			throw e;
		}
	}

	/**
	 * Retrieves a list of Google Drive files for the current user.
	 *
	 * @param accessToken OAuth access token for the Google user
	 * @param limit       maximum number of files to return
	 * @return list of file metadata maps
	 * @throws IllegalArgumentException if {@code limit} is less than or equal to 0
	 * @throws Exception                if list retrieval fails
	 */
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> fileIdList(String accessToken, int limit) throws Exception {
		final String FILES = "files";
		final String GOOGLE_DRIVE_LIST = "https://www.googleapis.com/drive/v3/files?pageSize=%s&fields=files(id,name,mimeType)";

		try {
			if (limit <= 0) {
				throw new IllegalArgumentException("Limit must be greater than 0 when listing Google Drive files.");
			}
			Map<String, String> headers = getBearerHeader(accessToken, null);
			String listUrl = String.format(GOOGLE_DRIVE_LIST, limit);
			String response = HttpHelperUtility.getRequest(listUrl, headers, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			return (List<Map<String, Object>>) json.get(FILES);
		} catch (Exception e) {
			classLogger.error("Failed to list Google Drive files with limit {}.", limit, e);
			throw e;
		}
	}

	/**
	 * Determines a MIME type from a file name extension.
	 *
	 * @param fileName file name whose extension is used for MIME detection
	 * @return MIME type string, defaulting to {@code application/octet-stream}
	 */
	private static String mimeType(String fileName) {
		final String EXT_JPG = ".jpg";
		final String EXT_PNG = ".png";
		final String EXT_PDF = ".pdf";
		final String EXT_TXT = ".txt";
		final String EXT_PPT = ".ppt";
		final String EXT_JPEG = ".jpeg";
		final String EXT_DOCX = ".docx";
		final String MIME_PNG = "image/png";
		final String MIME_TXT = "text/plain";
		final String MIME_JPEG = "image/jpeg";
		final String MIME_PDF = "application/pdf";
		final String MIME_PPT = "application/vnd.ms-powerpoint";
		final String MIME_OCTET_STREAM = "application/octet-stream";
		final String MIME_DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";

		if (fileName == null || fileName.trim().isEmpty()) {
			return MIME_OCTET_STREAM;
		}
		String normalizedFileName = fileName.toLowerCase(Locale.ROOT);

		if (normalizedFileName.endsWith(EXT_JPG) || normalizedFileName.endsWith(EXT_JPEG)) {
			return MIME_JPEG;
		} else if (normalizedFileName.endsWith(EXT_PNG)) {
			return MIME_PNG;
		} else if (normalizedFileName.endsWith(EXT_PDF)) {
			return MIME_PDF;
		} else if (normalizedFileName.endsWith(EXT_TXT)) {
			return MIME_TXT;
		} else if (normalizedFileName.endsWith(EXT_PPT)) {
			return MIME_PPT;
		} else if (normalizedFileName.endsWith(EXT_DOCX)) {
			return MIME_DOCX;
		}
		return MIME_OCTET_STREAM;
	}

	/**
	 * Builds standard request headers for Google API requests.
	 *
	 * @param accessToken OAuth access token
	 * @param boundary    optional multipart boundary value
	 * @return header map containing authorization and optional content type headers
	 * @throws IllegalArgumentException if {@code accessToken} is null or blank
	 */
	public static Map<String, String> getBearerHeader(String accessToken, String boundary) {
		final String HEADER_AUTHORIZATION = "Authorization";
		final String BEARER = "Bearer ";
		final String HEADER_CONTENT_TYPE = "Content-Type";
		final String MULTIPART_RELATED_BOUNDARY = "multipart/related; boundary=";

		if (accessToken == null || accessToken.trim().isEmpty()) {
			throw new IllegalArgumentException("Access token is required to build Google API headers.");
		}

		Map<String, String> headers = new HashMap<>();
		headers.put(HEADER_AUTHORIZATION, BEARER + accessToken);
		if (boundary != null && !boundary.isEmpty()) {
			headers.put(HEADER_CONTENT_TYPE, MULTIPART_RELATED_BOUNDARY + boundary);
		}
		return headers;
	}

}
