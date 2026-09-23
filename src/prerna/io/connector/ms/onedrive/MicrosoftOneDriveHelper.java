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
package prerna.io.connector.ms.onedrive;

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
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
 * The OneDrive operations of Microsoft Graph, as plain calls.
 *
 * <p>
 * Everything here is delegated: the token says who the signed in user is, so a
 * call that names no drive is rooted at {@code /me/drive} and reads the user's
 * own OneDrive. A drive id names some other drive, and Graph only answers for
 * one the signed in user has been given access to, so naming a drive cannot
 * reach further than the user could reach themselves.
 * </p>
 *
 * <p>
 * An item is addressed by a drive, an item id and a path, in that order of
 * precedence: an item id addresses the item directly, a path addresses it from
 * the drive root, and the two together address a path beneath a folder item.
 * That is the one addressing scheme every method here takes, because it is the
 * scheme that describes a file in the user's own drive and a file in somebody
 * else's with the same three arguments.
 * </p>
 *
 * <p>
 * Files somebody else shared are reached two ways, and both matter:
 * </p>
 * <ul>
 * <li>By drive and item, from the {@code driveId} every listing returns.
 * {@link MicrosoftOneDriveItemMapper} reads that off the {@code remoteItem}
 * facet, so a shared file listed here can be read, downloaded and shared on by
 * the same methods that serve the user's own files.</li>
 * <li>By sharing link, for a link somebody sent rather than a share the user
 * has already accepted. {@link #resolveSharingLink} turns the link into the
 * item it points at.</li>
 * </ul>
 *
 * <p>
 * Finding shared files is the part Graph makes awkward. Microsoft has
 * deprecated {@code /me/drive/sharedWithMe}: it is documented to stop returning
 * data in November 2026 and already answers with a reduced set. The supported
 * way to find files across the drives a user can reach is the search index, so
 * {@link #listSharedFiles} asks both and merges what comes back, which keeps
 * the listing working through that cutoff rather than emptying out on the day
 * it lands.
 * </p>
 */
public class MicrosoftOneDriveHelper {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOneDriveHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final String GRAPH_BASE = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0";

	private static final String ID = "id";
	private static final String NAME = "name";
	private static final String SIZE = "size";
	private static final String VALUE = "value";
	private static final String ITEMS = "/items/";
	private static final String FOLDER = "folder";
	private static final String SUCCESS = "success";
	private static final String DRIVE_ID = "driveId";
	private static final String FILE_PATH = "filePath";
	private static final String NEXT_LINK = "@odata.nextLink";
	private static final String UPLOAD_URL = "uploadUrl";
	private static final String DOWNLOAD_URL = "@microsoft.graph.downloadUrl";
	private static final String CONFLICT_BEHAVIOR_PARAM = "@microsoft.graph.conflictBehavior";

	private static final String CONFLICT_FAIL = "fail";
	private static final String CONFLICT_RENAME = "rename";
	private static final String CONFLICT_REPLACE = "replace";
	private static final List<String> CONFLICT_BEHAVIORS = Arrays.asList(CONFLICT_FAIL, CONFLICT_RENAME,
			CONFLICT_REPLACE);

	/** What a sharing link lets whoever holds it do. */
	private static final List<String> LINK_TYPES = Arrays.asList("view", "edit", "embed");

	/** Who a sharing link works for. */
	private static final List<String> LINK_SCOPES = Arrays.asList("anonymous", "organization", "users");

	/**
	 * Graph recommends a resumable upload session for anything larger than 4MB.
	 */
	private static final long SIMPLE_UPLOAD_MAX_BYTES = 4L * 1024L * 1024L;

	/**
	 * Upload session chunk size. Graph requires every chunk except the last to be a
	 * multiple of 320 KiB.
	 */
	private static final int UPLOAD_CHUNK_BYTES = 320 * 1024 * 10;

	/** How many items one page of a listing asks Graph for. */
	private static final int PAGE_SIZE = 200;

	/** How many hits the search index is asked for when nothing says otherwise. */
	private static final int DEFAULT_SEARCH_SIZE = 50;

	/** The most hits the search index is asked for in one request. */
	private static final int MAX_SEARCH_SIZE = 200;

	/**
	 * What the search index is asked for when the caller wants a listing rather
	 * than a search. Graph accepts this for a driveItem query, though not for a
	 * drive one.
	 */
	private static final String MATCH_EVERYTHING = "*";

	/**
	 * Utility class constructor intentionally hidden.
	 */
	private MicrosoftOneDriveHelper() {

	}

	/**
	 * Reads the signed in user's own OneDrive.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @return the drive, which carries the id the other methods take as their
	 *         {@code driveId}
	 * @throws Exception if the read fails
	 */
	public static Map<String, Object> getMyDrive(String accessToken) throws Exception {
		try {
			String response = HttpHelperUtility.getRequest(GRAPH_BASE + "/me/drive", headers(accessToken), null, null,
					null);
			Map<String, Object> drive = readMap(response);
			if (drive == null) {
				throw new IllegalStateException("Microsoft Graph returned no OneDrive for the signed in user.");
			}
			return MicrosoftOneDriveItemMapper.toDrive(drive);
		} catch (Exception e) {
			classLogger.error("Failed to read the OneDrive of the signed in user.", e);
			throw e;
		}
	}

	/**
	 * Lists the drives the signed in user can reach: their own, and the drives that
	 * the files shared with them live in.
	 *
	 * <p>
	 * There is no Graph call that enumerates other people's drives, and there
	 * should not be. What this does instead is read the shared files and report the
	 * distinct drives they came from, which is how a caller gets the
	 * {@code driveId} it needs to browse a folder somebody shared.
	 * </p>
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param limit       maximum number of drives to return; values less than or
	 *                    equal to 0 return every drive found
	 * @return the drives, the signed in user's own first
	 * @throws Exception if the user's own drive cannot be read
	 */
	public static List<Map<String, Object>> listDrives(String accessToken, int limit) throws Exception {
		Map<String, Object> myDrive = getMyDrive(accessToken);
		Map<String, Object> own = new LinkedHashMap<>();
		own.put("isMine", true);
		own.putAll(myDrive);

		List<Map<String, Object>> drives = new ArrayList<>();
		drives.add(own);

		// the drives of other people are only knowable through what they shared, so
		// a failure to read that is a thinner answer rather than no answer at all
		try {
			List<String> seen = new ArrayList<>();
			seen.add(String.valueOf(myDrive.get(ID)));
			for (Map<String, Object> shared : listSharedFiles(accessToken, null, 0)) {
				Object driveId = shared.get(DRIVE_ID);
				if (driveId == null || seen.contains(driveId.toString())) {
					continue;
				}
				seen.add(driveId.toString());
				Map<String, Object> drive = new LinkedHashMap<>();
				drive.put("isMine", false);
				drive.put(ID, driveId);
				putIfPresent(drive, "sharedBy", shared.get("sharedBy"));
				putIfPresent(drive, "sharedOwner", shared.get("sharedOwner"));
				drives.add(drive);
				if (limit > 0 && drives.size() >= limit) {
					break;
				}
			}
		} catch (Exception e) {
			classLogger.warn("Could not work out which other drives the signed in user can reach", e);
		}
		if (limit > 0 && drives.size() > limit) {
			return new ArrayList<>(drives.subList(0, limit));
		}
		return drives;
	}

	/**
	 * Lists what is inside a folder.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param driveId     optional drive holding the folder; the signed in user's
	 *                    own OneDrive is read when blank
	 * @param itemId      optional id of the folder; the drive root is read when
	 *                    blank
	 * @param path        optional path of the folder, relative to the drive root or
	 *                    to the folder the item id names
	 * @param limit       maximum number of items to return; values less than or
	 *                    equal to 0 return every item
	 * @return the items, each carrying the drive and item ids that address it again
	 * @throws Exception if the list retrieval fails
	 */
	public static List<Map<String, Object>> listFiles(String accessToken, String driveId, String itemId, String path,
			int limit) throws Exception {
		try {
			String url = itemUrl(driveId, itemId, path) + "/children?$top=" + pageSize(limit);
			List<Map<String, Object>> items = new ArrayList<>();
			for (Map<String, Object> item : getPagedValues(accessToken, url, limit)) {
				items.add(MicrosoftOneDriveItemMapper.toDriveItem(item, driveId));
			}
			return items;
		} catch (Exception e) {
			classLogger.error("Failed to list the OneDrive items in drive '{}' item '{}' path '{}'.", driveId, itemId,
					path, e);
			throw e;
		}
	}

	/**
	 * Lists the files other people have shared with the signed in user.
	 *
	 * <p>
	 * Two sources are read and merged. The first is {@code /me/drive/sharedWithMe},
	 * which is the list Outlook and the OneDrive web app show, and which Microsoft
	 * has deprecated: it is documented to stop returning data in November 2026 and
	 * already answers with fewer items than it should. The second is the search
	 * index, which covers every drive the user can read, including the shared ones,
	 * and is the path Microsoft points at instead. Hits from the user's own drive
	 * are dropped from the second source, since a file of their own is not a file
	 * shared with them.
	 * </p>
	 *
	 * <p>
	 * Because the search index reaches SharePoint as well as OneDrive, a file in a
	 * document library the user can read comes back here too. That is the same
	 * answer to the same question - somebody gave them access to a file that is not
	 * theirs - and the {@code driveId} says which library it is in.
	 * </p>
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param search      optional text the file name or contents has to match; when
	 *                    blank, everything shared is returned
	 * @param limit       maximum number of files to return; values less than or
	 *                    equal to 0 return everything found
	 * @return the shared files, each carrying the drive and item ids that address
	 *         it
	 * @throws Exception if both sources fail
	 */
	public static List<Map<String, Object>> listSharedFiles(String accessToken, String search, int limit)
			throws Exception {
		Map<String, Map<String, Object>> merged = new LinkedHashMap<>();
		String wanted = isBlank(search) ? null : search.trim().toLowerCase(Locale.ROOT);

		Exception sharedWithMeFailure = null;
		try {
			String response = HttpHelperUtility.getRequest(GRAPH_BASE + "/me/drive/sharedWithMe", headers(accessToken),
					null, null, null);
			for (Map<String, Object> item : valueList(response)) {
				Map<String, Object> described = MicrosoftOneDriveItemMapper.toDriveItem(item, null);
				// this source cannot be asked to narrow, so a search term is matched
				// against the name of what it returned
				if (wanted != null && !nameContains(described, wanted)) {
					continue;
				}
				merged.putIfAbsent(addressOf(described), described);
			}
		} catch (Exception e) {
			// expected as the deprecation lands, and the search index below is the
			// answer to it, so the listing carries on with one source
			classLogger.warn("The deprecated sharedWithMe listing failed, falling back to the search index", e);
			sharedWithMeFailure = e;
		}

		try {
			String ownDriveId = ownDriveId(accessToken);
			for (Map<String, Object> hit : searchAllDriveItems(accessToken, isBlank(search) ? MATCH_EVERYTHING : search,
					searchSize(limit))) {
				Map<String, Object> described = MicrosoftOneDriveItemMapper.toDriveItem(hit, null);
				Object hitDriveId = described.get(DRIVE_ID);
				// a hit that does not say where it lives cannot be read afterwards, so
				// reporting it would only be something for a caller to trip over
				if (hitDriveId == null || described.get(ID) == null) {
					continue;
				}
				if (ownDriveId != null && ownDriveId.equals(hitDriveId.toString())) {
					continue;
				}
				merged.putIfAbsent(addressOf(described), described);
			}
		} catch (Exception e) {
			classLogger.error("Failed to search the index for the files shared with the signed in user.", e);
			if (sharedWithMeFailure != null) {
				throw e;
			}
		}

		List<Map<String, Object>> shared = new ArrayList<>(merged.values());
		if (limit > 0 && shared.size() > limit) {
			return new ArrayList<>(shared.subList(0, limit));
		}
		return shared;
	}

	/**
	 * Finds files by name and contents within one drive.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param driveId     optional drive to search; the signed in user's own
	 *                    OneDrive is searched when blank
	 * @param itemId      optional folder to search under; the whole drive is
	 *                    searched when blank
	 * @param path        optional path of the folder to search under
	 * @param search      the text to look for
	 * @param limit       maximum number of files to return; values less than or
	 *                    equal to 0 return every match
	 * @return the matching items
	 * @throws IllegalArgumentException if no search text was given
	 * @throws Exception                if the search fails
	 */
	public static List<Map<String, Object>> searchDrive(String accessToken, String driveId, String itemId, String path,
			String search, int limit) throws Exception {
		try {
			requireValue(search, "Search text is required to search a OneDrive.");

			String url = itemUrl(driveId, itemId, path) + "/search(q='" + encodeSearchTerm(search.trim()) + "')?$top="
					+ pageSize(limit);
			List<Map<String, Object>> items = new ArrayList<>();
			for (Map<String, Object> item : getPagedValues(accessToken, url, limit)) {
				items.add(MicrosoftOneDriveItemMapper.toDriveItem(item, driveId));
			}
			return items;
		} catch (Exception e) {
			classLogger.error("Failed to search drive '{}' for '{}'.", driveId, search, e);
			throw e;
		}
	}

	/**
	 * Finds files by name and contents across everything the signed in user can
	 * read, their own drive and every drive shared with them alike.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param search      the text to look for
	 * @param limit       maximum number of files to return; values less than or
	 *                    equal to 0 use the default page of hits
	 * @return the matching items
	 * @throws IllegalArgumentException if no search text was given
	 * @throws Exception                if the search fails
	 */
	public static List<Map<String, Object>> searchEverywhere(String accessToken, String search, int limit)
			throws Exception {
		try {
			requireValue(search, "Search text is required to search OneDrive and SharePoint.");

			List<Map<String, Object>> items = new ArrayList<>();
			for (Map<String, Object> hit : searchAllDriveItems(accessToken, search.trim(), searchSize(limit))) {
				items.add(MicrosoftOneDriveItemMapper.toDriveItem(hit, null));
				if (limit > 0 && items.size() >= limit) {
					break;
				}
			}
			return items;
		} catch (Exception e) {
			classLogger.error("Failed to search OneDrive and SharePoint for '{}'.", search, e);
			throw e;
		}
	}

	/**
	 * Reads one item.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param driveId     optional drive holding the item; the signed in user's own
	 *                    OneDrive is read when blank
	 * @param itemId      optional id of the item
	 * @param path        optional path of the item, relative to the drive root or
	 *                    to the folder the item id names
	 * @return the item
	 * @throws Exception if the read fails
	 */
	public static Map<String, Object> getItem(String accessToken, String driveId, String itemId, String path)
			throws Exception {
		try {
			Map<String, Object> item = readItem(accessToken, itemUrl(driveId, itemId, path));
			if (item == null) {
				throw new IllegalStateException("Microsoft Graph returned no OneDrive item for drive = " + driveId
						+ ", item = " + itemId + ", path = " + path);
			}
			return MicrosoftOneDriveItemMapper.toDriveItem(item, driveId);
		} catch (Exception e) {
			classLogger.error("Failed to read the OneDrive item in drive '{}' item '{}' path '{}'.", driveId, itemId,
					path, e);
			throw e;
		}
	}

	/**
	 * Reads the item a sharing link points at.
	 *
	 * <p>
	 * This is how a file reaches the signed in user without being in any drive they
	 * can list: somebody sent them a link. The item that comes back carries the
	 * drive and item ids that address it, so it can be downloaded like any other.
	 * </p>
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param shareUrl    the sharing link, as it was sent
	 * @return the item the link points at
	 * @throws IllegalArgumentException if no link was given
	 * @throws Exception                if the read fails
	 */
	public static Map<String, Object> resolveSharingLink(String accessToken, String shareUrl) throws Exception {
		try {
			requireValue(shareUrl, "A sharing link is required to read a shared OneDrive item.");

			Map<String, Object> item = readItem(accessToken, shareUrl(shareUrl));
			if (item == null) {
				throw new IllegalStateException("Microsoft Graph returned no item for the sharing link: " + shareUrl);
			}
			return MicrosoftOneDriveItemMapper.toDriveItem(item, null);
		} catch (Exception e) {
			classLogger.error("Failed to read the OneDrive item behind the sharing link '{}'.", shareUrl, e);
			throw e;
		}
	}

	/**
	 * Downloads a file to the local filesystem.
	 *
	 * <p>
	 * A file in the signed in user's own drive, a file in a drive somebody shared,
	 * and a file behind a sharing link are all downloaded the same way: whatever
	 * addressed it is read first, and the drive and item ids off that reading are
	 * what the content is fetched with. That is what lets a shared item, which is
	 * only a stub in the user's own drive, be fetched from the drive it really
	 * lives in.
	 * </p>
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param driveId     optional drive holding the file
	 * @param itemId      optional id of the file
	 * @param path        optional path of the file
	 * @param shareUrl    optional sharing link to the file, which stands in for the
	 *                    drive, item and path
	 * @param destination local directory the file is written into, or the local
	 *                    file path when no file name is given
	 * @param fileName    optional local file name to write as; the name the file
	 *                    carries in the drive is used when blank
	 * @return map carrying the item {@code id}, its {@code name}, the
	 *         {@code driveId} it came from, the local {@code filePath} written, the
	 *         {@code size} in bytes and {@code success}
	 * @throws IllegalArgumentException if required inputs are missing or the item
	 *                                  is a folder
	 * @throws Exception                if the download or the file write fails
	 */
	public static Map<String, Object> downloadFile(String accessToken, String driveId, String itemId, String path,
			String shareUrl, String destination, String fileName) throws Exception {
		try {
			requireValue(destination, "A destination path is required to download a OneDrive file.");

			Map<String, Object> item;
			if (!isBlank(shareUrl)) {
				item = readItem(accessToken, shareUrl(shareUrl));
			} else {
				if (isBlank(itemId) && isBlank(path)) {
					throw new IllegalArgumentException(
							"An item id, a path or a sharing link is required to download a OneDrive file.");
				}
				item = readItem(accessToken, itemUrl(driveId, itemId, path));
			}
			if (item == null) {
				throw new IllegalStateException("Microsoft Graph returned no OneDrive item to download.");
			}
			if (MicrosoftOneDriveItemMapper.isFolder(item)) {
				throw new IllegalArgumentException("A folder cannot be downloaded: "
						+ MicrosoftOneDriveItemMapper.nameOf(item) + ". Name a file beneath it instead.");
			}

			String resolvedDriveId = MicrosoftOneDriveItemMapper.driveIdOf(item, driveId);
			String resolvedItemId = MicrosoftOneDriveItemMapper.itemIdOf(item);
			String itemName = MicrosoftOneDriveItemMapper.nameOf(item);

			// a shared item read out of the user's own drive is only a stub, so it
			// carries no download url of its own and has to be read again where it
			// really lives
			if (item.get(DOWNLOAD_URL) == null && !isBlank(resolvedDriveId) && !isBlank(resolvedItemId)) {
				Map<String, Object> remote = readItem(accessToken,
						GRAPH_BASE + "/drives/" + resolvedDriveId + ITEMS + resolvedItemId);
				if (remote != null) {
					item = remote;
				}
			}

			byte[] fileBytes = downloadBytes(accessToken, item, resolvedDriveId, resolvedItemId);
			File file = write(fileBytes, destination, fileName, itemName);

			Map<String, Object> result = new LinkedHashMap<>();
			result.put(ID, resolvedItemId);
			result.put(NAME, itemName);
			putIfPresent(result, DRIVE_ID, resolvedDriveId);
			result.put(FILE_PATH, Path.of(destination).relativize(file.toPath()).toString());
			result.put(SIZE, fileBytes.length);
			result.put(SUCCESS, true);
			return result;
		} catch (Exception e) {
			classLogger.error("Failed to download the OneDrive file in drive '{}' item '{}' path '{}' to '{}'.",
					driveId, itemId, path, destination, e);
			throw e;
		}
	}

	/**
	 * Uploads a local file into a drive.
	 *
	 * <p>
	 * Files at or below 4MB are sent as a single request; larger files are streamed
	 * through a resumable Graph upload session. The folder has to exist already,
	 * since Graph answers a path addressed upload into a missing folder with a 404
	 * rather than creating it.
	 * </p>
	 *
	 * @param accessToken      Microsoft Graph access token for the user
	 * @param driveId          optional drive to upload into; the signed in user's
	 *                         own OneDrive is used when blank
	 * @param itemId           optional id of the folder to upload into; the drive
	 *                         root is used when blank
	 * @param folderPath       optional path of the folder to upload into, relative
	 *                         to the drive root or to the folder the item id names
	 * @param fileName         name to give the file in the drive
	 * @param filePath         local file to upload
	 * @param conflictBehavior optional behavior when a file of that name is already
	 *                         there; one of {@code fail}, {@code rename} or
	 *                         {@code replace}, defaulting to {@code rename}
	 * @return the uploaded item
	 * @throws IllegalArgumentException if required inputs are missing or invalid
	 * @throws Exception                if the upload fails
	 */
	public static Map<String, Object> uploadFile(String accessToken, String driveId, String itemId, String folderPath,
			String fileName, String filePath, String conflictBehavior) throws Exception {
		try {
			requireValue(fileName, "A file name is required to upload to OneDrive.");
			requireValue(filePath, "A file path is required to upload to OneDrive.");
			if (fileName.indexOf('/') >= 0 || fileName.indexOf('\\') >= 0) {
				// the name is one segment of the destination path, so a separator in it
				// would silently move the file somewhere else
				throw new IllegalArgumentException(
						"A file name cannot contain a path separator. Name the folder in the path instead: "
								+ fileName);
			}

			File file = new File(filePath);
			if (!file.exists() || !file.isFile()) {
				throw new IllegalArgumentException("The file path does not point to a valid file: " + filePath);
			}

			String conflict = normalizeConflictBehavior(conflictBehavior);
			String targetPath = joinPath(folderPath, fileName.trim());

			String response;
			if (file.length() <= SIMPLE_UPLOAD_MAX_BYTES) {
				response = simpleUpload(accessToken, driveId, itemId, targetPath, conflict, file);
			} else {
				response = sessionUpload(accessToken, driveId, itemId, targetPath, conflict, fileName.trim(), file);
			}

			Map<String, Object> uploaded = readMap(response);
			if (uploaded == null) {
				// the last chunk of a session upload is the only one that answers with
				// the item, so a missing body is worth saying out loud rather than
				// reporting an item with nothing in it
				throw new IllegalStateException(
						"Microsoft Graph returned no item for the OneDrive upload of: " + fileName);
			}
			Map<String, Object> result = new LinkedHashMap<>(
					MicrosoftOneDriveItemMapper.toDriveItem(uploaded, driveId));
			result.put(SUCCESS, true);
			return result;
		} catch (Exception e) {
			classLogger.error("Failed to upload '{}' to drive '{}' item '{}' path '{}' as '{}'.", filePath, driveId,
					itemId, folderPath, fileName, e);
			throw e;
		}
	}

	/**
	 * Creates a folder.
	 *
	 * @param accessToken      Microsoft Graph access token for the user
	 * @param driveId          optional drive to create in; the signed in user's own
	 *                         OneDrive is used when blank
	 * @param itemId           optional id of the folder to create inside; the drive
	 *                         root is used when blank
	 * @param parentPath       optional path of the folder to create inside
	 * @param folderName       name of the folder to create
	 * @param conflictBehavior optional behavior when a folder of that name is
	 *                         already there; one of {@code fail}, {@code rename} or
	 *                         {@code replace}, defaulting to {@code fail} so an
	 *                         existing folder and everything in it is left alone
	 * @return the folder as Graph created it
	 * @throws IllegalArgumentException if required inputs are missing or invalid
	 * @throws Exception                if the create fails
	 */
	public static Map<String, Object> createFolder(String accessToken, String driveId, String itemId, String parentPath,
			String folderName, String conflictBehavior) throws Exception {
		try {
			requireValue(folderName, "A folder name is required to create a OneDrive folder.");

			Map<String, Object> body = new LinkedHashMap<>();
			body.put(NAME, folderName.trim());
			body.put(FOLDER, new LinkedHashMap<String, Object>());
			body.put(CONFLICT_BEHAVIOR_PARAM,
					isBlank(conflictBehavior) ? CONFLICT_FAIL : normalizeConflictBehavior(conflictBehavior));

			String url = itemUrl(driveId, itemId, parentPath) + "/children";
			String response = HttpHelperUtility.postRequestStringBody(url, headers(accessToken), GSON.toJson(body),
					ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> created = readMap(response);
			if (created == null) {
				throw new IllegalStateException(
						"Microsoft Graph returned no folder for the create request: " + folderName);
			}
			return MicrosoftOneDriveItemMapper.toDriveItem(created, driveId);
		} catch (Exception e) {
			classLogger.error("Failed to create the folder '{}' in drive '{}' item '{}' path '{}'.", folderName,
					driveId, itemId, parentPath, e);
			throw e;
		}
	}

	/**
	 * Deletes an item, and everything beneath it when it is a folder.
	 *
	 * <p>
	 * The item goes to the drive's recycle bin rather than disappearing, so this is
	 * recoverable from OneDrive itself for as long as that bin keeps it.
	 * </p>
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param driveId     optional drive holding the item
	 * @param itemId      optional id of the item to delete
	 * @param path        optional path of the item to delete
	 * @throws IllegalArgumentException if nothing beneath the drive root was named
	 * @throws Exception                if the delete fails
	 */
	public static void deleteItem(String accessToken, String driveId, String itemId, String path) throws Exception {
		try {
			if (isBlank(itemId) && isBlank(path)) {
				throw new IllegalArgumentException(
						"Refusing to delete the root of the drive. Name an item id or a path beneath it instead.");
			}

			// a successful delete answers 204 with no body
			HttpHelperUtility.deleteRequestStringBody(itemUrl(driveId, itemId, path), headers(accessToken), null, null,
					null);
		} catch (Exception e) {
			classLogger.error("Failed to delete the OneDrive item in drive '{}' item '{}' path '{}'.", driveId, itemId,
					path, e);
			throw e;
		}
	}

	/**
	 * Creates a sharing link to an item, which is how the signed in user shares a
	 * file with somebody else.
	 *
	 * @param accessToken        Microsoft Graph access token for the user
	 * @param driveId            optional drive holding the item
	 * @param itemId             optional id of the item to share
	 * @param path               optional path of the item to share
	 * @param type               what the link lets whoever holds it do, one of
	 *                           {@code view}, {@code edit} or {@code embed},
	 *                           defaulting to {@code view}
	 * @param scope              who the link works for, one of {@code anonymous},
	 *                           {@code organization} or {@code users}, defaulting
	 *                           to {@code organization} so a link does not leave
	 *                           the tenant unless it was asked to
	 * @param recipients         email addresses the link is for, required when the
	 *                           scope is {@code users}
	 * @param password           optional password the link asks for
	 * @param expirationDateTime optional moment the link stops working, as an ISO
	 *                           8601 date and time
	 * @return the link
	 * @throws IllegalArgumentException if required inputs are missing or invalid
	 * @throws Exception                if the create fails
	 */
	public static Map<String, Object> createSharingLink(String accessToken, String driveId, String itemId, String path,
			String type, String scope, String[] recipients, String password, String expirationDateTime)
			throws Exception {
		try {
			if (isBlank(itemId) && isBlank(path)) {
				throw new IllegalArgumentException("An item id or a path is required to share a OneDrive item.");
			}

			String linkType = isBlank(type) ? LINK_TYPES.get(0) : oneOf(type, LINK_TYPES, "type");
			String linkScope = isBlank(scope) ? "organization" : oneOf(scope, LINK_SCOPES, "scope");

			Map<String, Object> body = new LinkedHashMap<>();
			body.put("type", linkType);
			body.put("scope", linkScope);
			if ("users".equals(linkScope)) {
				if (recipients == null || recipients.length == 0) {
					throw new IllegalArgumentException(
							"At least one recipient is required to share a OneDrive item with named people.");
				}
				List<Map<String, Object>> named = new ArrayList<>();
				for (String recipient : recipients) {
					named.add(Map.of("email", recipient));
				}
				body.put("recipients", named);
			}
			if (!isBlank(password)) {
				body.put("password", password.trim());
			}
			if (!isBlank(expirationDateTime)) {
				body.put("expirationDateTime", expirationDateTime.trim());
			}

			String url = itemUrl(driveId, itemId, path) + "/createLink";
			String response = HttpHelperUtility.postRequestStringBody(url, headers(accessToken), GSON.toJson(body),
					ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> permission = readMap(response);
			if (permission == null) {
				throw new IllegalStateException("Microsoft Graph returned no sharing link for the OneDrive item.");
			}
			return MicrosoftOneDriveItemMapper.toSharingLink(permission);
		} catch (Exception e) {
			classLogger.error(
					"Failed to create a sharing link for the OneDrive item in drive '{}' item '{}' path '{}'.", driveId,
					itemId, path, e);
			throw e;
		}
	}

	/**
	 * Asks the search index for drive items.
	 *
	 * <p>
	 * The search index is the only Graph call that reaches across drives, which is
	 * what makes it the one that finds a file somebody else owns. It answers with
	 * hits wrapped in containers, and what this returns is the drive items out of
	 * them.
	 * </p>
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param search      the text to look for
	 * @param size        how many hits to ask for
	 * @return the drive items the hits point at, as Graph returned them
	 * @throws Exception if the search fails
	 */
	private static List<Map<String, Object>> searchAllDriveItems(String accessToken, String search, int size)
			throws Exception {
		Map<String, Object> request = new LinkedHashMap<>();
		request.put("entityTypes", List.of("driveItem"));
		request.put("query", Map.of("queryString", search));
		request.put("from", 0);
		request.put("size", size);
		Map<String, Object> body = Map.of("requests", List.of(request));

		String response = HttpHelperUtility.postRequestStringBody(GRAPH_BASE + "/search/query", headers(accessToken),
				GSON.toJson(body), ContentType.APPLICATION_JSON, null, null, null);

		List<Map<String, Object>> resources = new ArrayList<>();
		for (Map<String, Object> answer : valueList(response)) {
			for (Map<String, Object> container : mapList(answer.get("hitsContainers"))) {
				for (Map<String, Object> hit : mapList(container.get("hits"))) {
					Object resource = hit.get("resource");
					if (resource instanceof Map) {
						resources.add(asStringKeyedMap(resource));
					}
				}
			}
		}
		return resources;
	}

	/**
	 * @param accessToken Microsoft Graph access token for the user
	 * @return the id of the signed in user's own drive, or null when it cannot be
	 *         read, in which case nothing is filtered out rather than everything
	 */
	private static String ownDriveId(String accessToken) {
		try {
			Object id = getMyDrive(accessToken).get(ID);
			return id == null ? null : id.toString();
		} catch (Exception e) {
			classLogger.warn("Could not read the id of the signed in user's own OneDrive", e);
			return null;
		}
	}

	/**
	 * Fetches the bytes of a file.
	 *
	 * <p>
	 * The pre-authenticated download url is used when there is one, and is called
	 * without the bearer header, since SharePoint rejects a request carrying both.
	 * </p>
	 */
	private static byte[] downloadBytes(String accessToken, Map<String, Object> item, String driveId, String itemId)
			throws Exception {
		byte[] fileBytes = null;
		Object preAuthUrl = item.get(DOWNLOAD_URL);
		if (preAuthUrl != null && !preAuthUrl.toString().trim().isEmpty()) {
			fileBytes = HttpHelperUtility.getRequestBytes(preAuthUrl.toString(), null, null, null, null);
		}
		if ((fileBytes == null || fileBytes.length == 0) && !isBlank(driveId) && !isBlank(itemId)) {
			String contentUrl = GRAPH_BASE + "/drives/" + driveId + ITEMS + itemId + "/content";
			fileBytes = HttpHelperUtility.getRequestBytes(contentUrl, headers(accessToken), null, null, null);
		}
		if (fileBytes == null || fileBytes.length == 0) {
			throw new IllegalStateException("Downloaded file content is empty for OneDrive item id = " + itemId);
		}
		return fileBytes;
	}

	/**
	 * Writes the downloaded bytes, treating an existing directory as the place to
	 * write into rather than as the file to write.
	 */
	private static File write(byte[] fileBytes, String destination, String fileName, String itemName) throws Exception {
		String normalizedFileName = fileName == null ? null : fileName.trim();
		if (isBlank(normalizedFileName) && new File(destination).isDirectory() && itemName != null) {
			normalizedFileName = itemName;
		}
		String targetPath = isBlank(normalizedFileName) ? destination
				: Paths.get(destination, normalizedFileName).toString();

		File file = new File(targetPath);
		File parent = file.getParentFile();
		if (parent != null && !parent.exists() && !parent.mkdirs()) {
			throw new IllegalStateException("Unable to create destination directory at: " + parent.getAbsolutePath());
		}
		try (FileOutputStream fos = new FileOutputStream(file)) {
			fos.write(fileBytes);
			fos.flush();
		}
		return file;
	}

	/**
	 * Uploads the whole file in one request. Used for payloads at or below 4MB.
	 */
	private static String simpleUpload(String accessToken, String driveId, String itemId, String targetPath,
			String conflictBehavior, File file) throws Exception {
		String url = itemUrl(driveId, itemId, targetPath) + "/content?" + CONFLICT_BEHAVIOR_PARAM + "="
				+ conflictBehavior;
		byte[] fileBytes = Files.readAllBytes(file.toPath());
		return HttpHelperUtility.putRequestBytesBody(url, MicrosoftLoginUtils.getAuthorizationHeader(accessToken),
				fileBytes, ContentType.APPLICATION_OCTET_STREAM, null, null, null);
	}

	/**
	 * Streams the file through a resumable upload session. Used for payloads above
	 * 4MB, where a single request is not supported.
	 */
	private static String sessionUpload(String accessToken, String driveId, String itemId, String targetPath,
			String conflictBehavior, String fileName, File file) throws Exception {
		final String CONTENT_RANGE = "Content-Range";

		Map<String, Object> item = new LinkedHashMap<>();
		item.put(CONFLICT_BEHAVIOR_PARAM, conflictBehavior);
		item.put(NAME, fileName);
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("item", item);

		String sessionUrl = itemUrl(driveId, itemId, targetPath) + "/createUploadSession";
		String sessionResponse = HttpHelperUtility.postRequestStringBody(sessionUrl, headers(accessToken),
				GSON.toJson(body), ContentType.APPLICATION_JSON, null, null, null);
		Map<String, Object> sessionJson = readMap(sessionResponse);
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
	 * Reads one item, or null when there is nothing to read.
	 */
	private static Map<String, Object> readItem(String accessToken, String url) throws Exception {
		return readMap(HttpHelperUtility.getRequest(url, headers(accessToken), null, null, null));
	}

	/**
	 * Runs a Graph collection request, following {@code @odata.nextLink} until the
	 * limit is met or the collection runs out.
	 */
	private static List<Map<String, Object>> getPagedValues(String accessToken, String url, int limit)
			throws Exception {
		List<Map<String, Object>> collected = new ArrayList<>();
		String nextUrl = url;
		while (nextUrl != null) {
			String response = HttpHelperUtility.getRequest(nextUrl, headers(accessToken), null, null, null);
			Map<String, Object> json = readMap(response);
			if (json == null) {
				break;
			}
			for (Map<String, Object> item : mapList(json.get(VALUE))) {
				collected.add(item);
				if (limit > 0 && collected.size() >= limit) {
					return collected;
				}
			}
			Object next = json.get(NEXT_LINK);
			nextUrl = next == null ? null : next.toString();
		}
		return collected;
	}

	/**
	 * The part of a Graph url that says which drive this is.
	 *
	 * @param driveId the drive, or null for the signed in user's own OneDrive
	 * @return the url up to the drive
	 */
	private static String drivePath(String driveId) {
		if (isBlank(driveId)) {
			return GRAPH_BASE + "/me/drive";
		}
		return GRAPH_BASE + "/drives/" + driveId.trim();
	}

	/**
	 * The Graph url of one item.
	 *
	 * <p>
	 * An item id addresses the item directly, a path addresses it from the drive
	 * root, and the two together address a path beneath the folder the item id
	 * names, which is how a caller reaches into a folder somebody shared without
	 * knowing where it sits in the owner's drive.
	 * </p>
	 *
	 * @param driveId the drive, or null for the signed in user's own OneDrive
	 * @param itemId  the item, or null to address by path alone
	 * @param path    the path, or null to address by item alone
	 * @return the url of the item
	 */
	private static String itemUrl(String driveId, String itemId, String path) {
		String base = drivePath(driveId);
		String relativePath = encodeRelativePath(path);
		if (!isBlank(itemId)) {
			String itemBase = base + ITEMS + itemId.trim();
			return relativePath.isEmpty() ? itemBase : itemBase + ":/" + relativePath + ":";
		}
		return relativePath.isEmpty() ? base + "/root" : base + "/root:/" + relativePath + ":";
	}

	/**
	 * The Graph url of the item a sharing link points at.
	 *
	 * <p>
	 * Graph takes the link itself as the id of the share, base64 encoded in the url
	 * safe alphabet with the padding taken off and a {@code u!} in front, which is
	 * the encoding the shares endpoint documents.
	 * </p>
	 *
	 * @param shareUrl the sharing link
	 * @return the url of the item behind it
	 */
	private static String shareUrl(String shareUrl) {
		String encoded = Base64.getUrlEncoder().withoutPadding()
				.encodeToString(shareUrl.trim().getBytes(StandardCharsets.UTF_8));
		return GRAPH_BASE + "/shares/u!" + encoded + "/driveItem";
	}

	/**
	 * Builds the headers a OneDrive call carries.
	 */
	private static Map<String, String> headers(String accessToken) {
		return MicrosoftLoginUtils.getBearerHeader(accessToken);
	}

	/**
	 * How large a page to ask Graph for, which is the limit itself when the caller
	 * wants fewer items than a page holds.
	 */
	private static int pageSize(int limit) {
		return limit > 0 && limit < PAGE_SIZE ? limit : PAGE_SIZE;
	}

	/**
	 * How many hits to ask the search index for. More than the limit is asked for,
	 * because hits in the user's own drive are dropped after they come back.
	 */
	private static int searchSize(int limit) {
		if (limit <= 0) {
			return DEFAULT_SEARCH_SIZE;
		}
		return Math.min(limit * 2, MAX_SEARCH_SIZE);
	}

	/**
	 * The drive and item a described item addresses, which is what makes it the
	 * same file when two sources both report it.
	 */
	private static String addressOf(Map<String, Object> described) {
		return described.get(DRIVE_ID) + "/" + described.get(ID);
	}

	/**
	 * @param described a described item
	 * @param wanted    the text to look for, already lower cased
	 * @return true when the item's name contains the text
	 */
	private static boolean nameContains(Map<String, Object> described, String wanted) {
		Object name = described.get(NAME);
		return name != null && name.toString().toLowerCase(Locale.ROOT).contains(wanted);
	}

	/**
	 * Joins a folder path and a name into one path relative to the same root.
	 *
	 * <p>
	 * The result is not encoded, because what it is handed to is
	 * {@link #itemUrl(String, String, String)}, which encodes every segment of the
	 * path it is given. Encoding here as well would put a literal {@code %} into
	 * the name of the uploaded file.
	 * </p>
	 */
	private static String joinPath(String folderPath, String name) {
		String folder = normalizeRelativePath(folderPath);
		return folder.isEmpty() ? name : folder + "/" + name;
	}

	/**
	 * Normalizes a relative path to forward slashes with no leading or trailing
	 * separator, leaving the segments as they were written.
	 */
	private static String normalizeRelativePath(String path) {
		if (isBlank(path)) {
			return "";
		}
		StringBuilder normalized = new StringBuilder();
		for (String segment : path.trim().replace('\\', '/').split("/")) {
			if (segment.isEmpty()) {
				continue;
			}
			if (normalized.length() > 0) {
				normalized.append("/");
			}
			normalized.append(segment);
		}
		return normalized.toString();
	}

	/**
	 * Encodes each segment of a drive relative path, keeping the separators intact.
	 *
	 * @param path optional path, which may use either separator style
	 * @return the encoded path without leading or trailing separators, or an empty
	 *         string when nothing was supplied
	 */
	private static String encodeRelativePath(String path) {
		if (isBlank(path)) {
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
	 * already url safe, so they are placed into the path as they are received.
	 */
	private static String encodePathSegment(String segment) {
		return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
	}

	/**
	 * Puts a search term into a Graph search function call, where it sits inside
	 * single quotes and so has to have its own quotes doubled before the whole
	 * thing is url encoded.
	 */
	private static String encodeSearchTerm(String search) {
		return encodePathSegment(search.replace("'", "''"));
	}

	/**
	 * Reads the {@code value} collection out of a Graph response.
	 */
	private static List<Map<String, Object>> valueList(String response) {
		Map<String, Object> json = readMap(response);
		if (json == null) {
			return new ArrayList<>();
		}
		return mapList(json.get(VALUE));
	}

	/**
	 * Reads a json array of objects, skipping anything in it that is not one.
	 */
	private static List<Map<String, Object>> mapList(Object value) {
		List<Map<String, Object>> maps = new ArrayList<>();
		if (!(value instanceof List)) {
			return maps;
		}
		for (Object entry : (List<?>) value) {
			if (entry instanceof Map) {
				maps.add(asStringKeyedMap(entry));
			}
		}
		return maps;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> asStringKeyedMap(Object value) {
		return (Map<String, Object>) value;
	}

	/**
	 * @param response the response body
	 * @return the response as a map, or null when there is nothing to read
	 */
	private static Map<String, Object> readMap(String response) {
		if (isBlank(response)) {
			return null;
		}
		return GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
	}

	/**
	 * Validates the requested conflict behavior, defaulting to the non destructive
	 * {@code rename}.
	 */
	private static String normalizeConflictBehavior(String conflictBehavior) {
		if (isBlank(conflictBehavior)) {
			return CONFLICT_RENAME;
		}
		return oneOf(conflictBehavior, CONFLICT_BEHAVIORS, "Conflict behavior");
	}

	/**
	 * Validates a value against a fixed set of words, matched however the caller
	 * happened to capitalize it.
	 */
	private static String oneOf(String value, List<String> accepted, String what) {
		String normalized = value.trim().toLowerCase(Locale.ROOT);
		if (!accepted.contains(normalized)) {
			throw new IllegalArgumentException(what + " must be one of " + accepted + " but received: " + value);
		}
		return normalized;
	}

	/**
	 * Set a value, and only when there is one.
	 */
	private static void putIfPresent(Map<String, Object> output, String key, Object value) {
		if (value != null) {
			output.put(key, value);
		}
	}

	/**
	 * Guards against missing required string inputs.
	 */
	private static void requireValue(String value, String message) {
		if (isBlank(value)) {
			throw new IllegalArgumentException(message);
		}
	}

	private static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}

}
