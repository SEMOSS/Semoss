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
package prerna.io.connector.google.docs;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.io.connector.google.GoogleLoginUtils;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;

public class GoogleDocsHelper {

	private static final Logger classLogger = LogManager.getLogger(GoogleDocsHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final String BODY = "body";
	private static final String CONTENT_KEY = "content";
	private static final String FILES = "files";
	private static final String SUCCESS_KEY = "success";
	private static final String TITLE_KEY = "title";

	private static final String GOOGLE_DOCS_GET_URL = "https://docs.googleapis.com/v1/documents/%s";

	private GoogleDocsHelper() {

	}

	/**
	 * Creates a Google Docs document and optionally populates it with content.
	 *
	 * @param accessToken OAuth access token for Google APIs.
	 * @param title       document title to create; must be unique for the user.
	 * @param content     optional initial body content for the document.
	 * @return a result map containing the created document ID and success status.
	 * @throws Exception if document creation fails or validation fails.
	 */
	public static Map<String, Object> createDoc(String accessToken, String title, String content) throws Exception {
		final String DOCUMENT_ID_KEY = "documentId";
		final String GOOGLE_DOCS_CREATE_URL = "https://docs.googleapis.com/v1/documents";

		try {
			if (title == null || title.trim().isEmpty()) {
				throw new IllegalArgumentException("Document title is required and cannot be empty.");
			}
			if (titleExists(accessToken, title)) {
				throw new IllegalArgumentException("Title " + title + " already exists");
			}
			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			Map<String, String> body = new HashMap<>();
			body.put(TITLE_KEY, title);
			String jsonBody = GSON.toJson(body);
			String response = HttpHelperUtility.postRequestStringBody(GOOGLE_DOCS_CREATE_URL, headers, jsonBody,
					ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			String docId = (String) json.get(DOCUMENT_ID_KEY);
			if (content != null) {
				updateDoc(accessToken, docId, content);
			}
			Map<String, Object> map = new HashMap<>();
			map.put(Constants.USER_MAP_ID, json.get(DOCUMENT_ID_KEY));
			map.put(SUCCESS_KEY, true);
			return map;
		} catch (Exception e) {
			classLogger.error("Failed to create Google Docs document with title '{}'", title, e);
			throw e;
		}
	}

	/**
	 * Reads a Google Docs document and returns its title and plain text content.
	 *
	 * @param accessToken OAuth access token for Google APIs.
	 * @param docId       unique Google Docs document ID.
	 * @return a map containing the document title and extracted text content.
	 * @throws Exception if the document cannot be read or parsed.
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> readDoc(String accessToken, String docId) throws Exception {
		final String PARAGRAPH = "paragraph";
		final String ELEMENTS = "elements";
		final String TEXT_RUN = "textRun";

		try {
			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String url = String.format(GOOGLE_DOCS_GET_URL, docId);
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			String title = (String) json.get(TITLE_KEY);
			StringBuilder contentText = new StringBuilder();
			Map<String, Object> body = (Map<String, Object>) json.get(BODY);
			if (body != null) {
				List<Map<String, Object>> contentList = (List<Map<String, Object>>) body.get(CONTENT_KEY);
				if (contentList != null) {
					for (Map<String, Object> contentItem : contentList) {
						Map<String, Object> paragraph = (Map<String, Object>) contentItem.get(PARAGRAPH);
						if (paragraph != null) {
							List<Map<String, Object>> elements = (List<Map<String, Object>>) paragraph.get(ELEMENTS);
							if (elements != null) {
								for (Map<String, Object> element : elements) {
									Map<String, Object> textRun = (Map<String, Object>) element.get(TEXT_RUN);
									if (textRun != null) {
										String text = (String) textRun.get(CONTENT_KEY);
										if (text != null) {
											contentText.append(text);
										}
									}
								}
							}
						}
					}
				}
			}
			Map<String, Object> map = new HashMap<>();
			map.put(TITLE_KEY, title);
			map.put(CONTENT_KEY, contentText.toString());
			return map;
		} catch (Exception e) {
			classLogger.error("Failed to read Google Docs document id {}", docId, e);
			throw e;
		}
	}

	/**
	 * Replaces the body content of an existing Google Docs document.
	 *
	 * @param accessToken OAuth access token for Google APIs.
	 * @param docId       unique Google Docs document ID.
	 * @param newContent  replacement text content for the document body.
	 * @return a status map indicating whether the update request succeeded.
	 * @throws Exception if the document cannot be updated.
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> updateDoc(String accessToken, String docId, String newContent) throws Exception {
		final String RANGE = "range";
		final String INDEX = "index";
		final String TEXT = "text";
		final String REQUESTS = "requests";
		final String LOCATION = "location";
		final String END_INDEX = "endIndex";
		final String START_INDEX = "startIndex";
		final String INSERT_TEXT = "insertText";
		final String DELETE_CONTENT_RANGE = "deleteContentRange";
		final String GOOGLE_DOCS_BATCH_UPDATE_URL = "https://docs.googleapis.com/v1/documents/%s:batchUpdate";

		try {
			if (newContent == null) {
				throw new IllegalArgumentException("Document content cannot be null.");
			}
			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String getDocUrl = String.format(GOOGLE_DOCS_GET_URL, docId);
			String docResponse = HttpHelperUtility.getRequest(getDocUrl, headers, null, null, null);
			Map<String, Object> docJson = GSON.fromJson(docResponse, new TypeToken<Map<String, Object>>() {
			}.getType());
			Map<String, Object> body = (Map<String, Object>) docJson.get(BODY);
			List<Map<String, Object>> content = null;
			if (body != null) {
				content = (List<Map<String, Object>>) body.get(CONTENT_KEY);
			}
			int endIndex = 1;
			if (content != null && !content.isEmpty()) {
				Map<String, Object> lastElement = content.get(content.size() - 1);
				Object endIdxObj = lastElement.get(END_INDEX);
				if (endIdxObj instanceof Number) {
					endIndex = ((Number) endIdxObj).intValue();
				}
			}
			List<Map<String, Object>> requests = new ArrayList<>();
			int startIndex = 1;
			int deleteEndIndex = endIndex - 1;
			if (deleteEndIndex > startIndex) {
				Map<String, Object> range = new HashMap<>();
				range.put(START_INDEX, startIndex);
				range.put(END_INDEX, deleteEndIndex);
				Map<String, Object> deleteContentRange = new HashMap<>();
				deleteContentRange.put(RANGE, range);
				Map<String, Object> deleteRequest = new HashMap<>();
				deleteRequest.put(DELETE_CONTENT_RANGE, deleteContentRange);
				requests.add(deleteRequest);
			}
			Map<String, Object> location = new HashMap<>();
			location.put(INDEX, 1);
			Map<String, Object> insertText = new HashMap<>();
			insertText.put(LOCATION, location);
			insertText.put(TEXT, newContent);
			Map<String, Object> insertTextRequest = new HashMap<>();
			insertTextRequest.put(INSERT_TEXT, insertText);
			requests.add(insertTextRequest);
			Map<String, Object> payload = new HashMap<>();
			payload.put(REQUESTS, requests);
			String jsonBody = GSON.toJson(payload);
			String updateUrl = String.format(GOOGLE_DOCS_BATCH_UPDATE_URL, docId);
			HttpHelperUtility.postRequestStringBody(updateUrl, headers, jsonBody, ContentType.APPLICATION_JSON, null,
					null, null);
			Map<String, Object> map = new HashMap<>();
			map.put(SUCCESS_KEY, true);
			return map;
		} catch (Exception e) {
			classLogger.error("Failed to update Google Docs document id {}", docId, e);
			classLogger.warn("Unable to update document id {}: {}", docId, e.getMessage());
			throw e;
		}
	}

	/**
	 * Deletes a Google Docs document by deleting the underlying Drive file.
	 *
	 * @param accessToken OAuth access token for Google APIs.
	 * @param docId       unique Google Docs document ID.
	 * @return a status map indicating successful deletion.
	 * @throws Exception if deletion fails.
	 */
	public static Map<String, Object> deleteDoc(String accessToken, String docId) throws Exception {
		final String GOOGLE_DRIVE_FILE_URL = "https://www.googleapis.com/drive/v3/files/%s";

		try {
			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String url = String.format(GOOGLE_DRIVE_FILE_URL, docId);
			HttpHelperUtility.deleteRequestStringBody(url, headers, null, null, null);
			Map<String, Object> map = new HashMap<>();
			map.put(SUCCESS_KEY, true);
			return map;
		} catch (Exception e) {
			classLogger.error("Failed to delete Google Docs document id {}", docId, e);
			throw e;
		}
	}

	/**
	 * Checks whether the logged-in user already has a document with the given
	 * title.
	 *
	 * @param accessToken OAuth access token for Google APIs.
	 * @param title       candidate document title.
	 * @return {@code true} if a document with the title exists; otherwise
	 *         {@code false}.
	 * @throws Exception if the Drive file list request fails.
	 */
	@SuppressWarnings("unchecked")
	public static boolean titleExists(String accessToken, String title) throws Exception {
		final String GOOGLE_DRIVE_FILES_LIST_URL = "https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.document'&fields=files(id,name)";

		try {
			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String response = HttpHelperUtility.getRequest(GOOGLE_DRIVE_FILES_LIST_URL, headers, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			List<Map<String, Object>> files = (List<Map<String, Object>>) json.get(FILES);
			if (files == null || files.isEmpty()) {
				return false;
			}
			for (Map<String, Object> file : files) {
				if (file.get(Constants.USER_MAP_NAME) != null
						&& file.get(Constants.USER_MAP_NAME).toString().equalsIgnoreCase(title)) {
					return true;
				}
			}
			return false;
		} catch (Exception e) {
			classLogger.error("Failed to verify whether Google Docs title '{}' exists", title, e);
			throw e;
		}
	}

	/**
	 * Lists Google Docs documents for the logged-in user.
	 *
	 * @param accessToken OAuth access token for Google APIs.
	 * @param limit       maximum number of documents to return.
	 * @return a list of maps containing document titles and IDs.
	 * @throws Exception if the list request fails.
	 */
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> getDocsList(String accessToken, int limit) throws Exception {
		final String FIELDS_PARAM = "files(id,name)";
		final String QUERY_PARAM_TEMPLATE = "mimeType='%s'";
		final String MIME_TYPE = "application/vnd.google-apps.document";
		final String DRIVE_API_URL = "https://www.googleapis.com/drive/v3/files";

		try {
			if (limit <= 0) {
				throw new IllegalArgumentException("Limit must be a positive integer.");
			}

			List<Map<String, Object>> docList = new ArrayList<>();
			String queryParam = String.format(QUERY_PARAM_TEMPLATE, MIME_TYPE);
			String fullUrl = DRIVE_API_URL + "?q=" + URLEncoder.encode(queryParam, StandardCharsets.UTF_8)
					+ "&pageSize=" + limit + "&fields=" + URLEncoder.encode(FIELDS_PARAM, StandardCharsets.UTF_8);

			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String response = HttpHelperUtility.getRequest(fullUrl, headers, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			List<Map<String, Object>> files = (List<Map<String, Object>>) json.get(FILES);
			if (files != null) {
				for (Map<String, Object> file : files) {
					Map<String, Object> map = new HashMap<>();
					String name = (String) file.get(Constants.USER_MAP_NAME);
					String id = (String) file.get(Constants.USER_MAP_ID);
					if (name != null && id != null) {
						map.put(TITLE_KEY, name);
						map.put(Constants.USER_MAP_ID, id);
						docList.add(map);
					}
				}
			}
			return docList;
		} catch (Exception e) {
			classLogger.error("Failed to list Google Docs documents with limit {}", limit, e);
			throw e;
		}
	}
}
