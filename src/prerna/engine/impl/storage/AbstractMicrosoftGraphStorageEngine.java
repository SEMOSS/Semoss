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
package prerna.engine.impl.storage;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.reflect.TypeToken;

import prerna.engine.api.StorageTypeEnum;
import prerna.io.connector.ms.MicrosoftGraphAppTokenProvider;
import prerna.io.connector.ms.MicrosoftGraphDriveClient;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.MicrosoftTokenFiller;
import prerna.security.HttpHelperUtility;

/**
 * Shared behavior for storage engines backed by a Microsoft Graph drive.
 *
 * <p>
 * Teams channels, SharePoint document libraries and OneDrive folders are all
 * the same thing underneath: a Graph drive plus an item within it that acts as
 * the root. Everything below that root behaves identically, so all of the file
 * operations live here and a subclass only has to say how to authenticate and
 * how to find its root.
 * </p>
 *
 * <p>
 * These engines authenticate as an application rather than as the signed in
 * user, so every operation is carried out by the service identity configured on
 * the engine and is unaffected by which user triggered it.
 * </p>
 *
 * <p>
 * Storage paths are always relative to the resolved root, which keeps drive ids
 * and item ids out of the paths callers deal with. Unlike the object stores,
 * SharePoint has real folders, so these engines have no folder placeholder
 * objects and never synthesize directories from key prefixes.
 * </p>
 * 
 * @see MicrosoftTeamsStorageEngine
 * @see SharePointStorageEngine
 */
public abstract class AbstractMicrosoftGraphStorageEngine extends AbstractStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractMicrosoftGraphStorageEngine.class);

	/**
	 * What to do when an upload targets a name that already exists: {@code fail},
	 * {@code rename} or {@code replace}.
	 */
	public static final String MS_CONFLICT_BEHAVIOR = "MS_CONFLICT_BEHAVIOR";

	protected static final String GRAPH_BASE = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0";

	private static final String KEY_PATH = "Path";
	private static final String KEY_NAME = "Name";
	private static final String KEY_IS_DIR = "IsDir";
	private static final String KEY_SIZE = "Size";
	private static final String KEY_MOD_TIME = "ModTime";
	private static final String KEY_METADATA = "Metadata";
	private static final String KEY_MIME_TYPE = "MimeType";
	private static final String MIME_DIRECTORY = "inode/directory";

	private static final String SITES_SELECTED = "Sites.Selected";

	/**
	 * Permissions that reach every site in the tenant. Holding any of these means a
	 * denial is not about the scope of the grant.
	 */
	private static final List<String> TENANT_WIDE_PERMISSIONS = List.of("Files.Read.All", "Files.ReadWrite.All",
			"Sites.Read.All", "Sites.ReadWrite.All", "Sites.FullControl.All");

	private static final String ACCESS_DENIED_GUIDANCE = """
			Microsoft Graph denied access to %s.

			%s""";

	private static final String NO_ADMIN_CONSENT_GUIDANCE = """
			The access token carries no application permissions, which means admin consent has not been
			granted for this app registration. An administrator can grant it at:

			  https://login.microsoftonline.com/%s/adminconsent?client_id=%s""";

	private static final String SITE_GRANT_GUIDANCE = """
			The app holds Sites.Selected, so the permission itself is correct, but this site has not been
			granted to it. Sites.Selected has no page in the Azure portal, so the grant has to be made
			through Graph.

			Using Graph Explorer (https://developer.microsoft.com/graph/graph-explorer):

			  1. Sign in with a work account in this tenant. It needs the SharePoint Administrator role
			     or higher. Being an owner of the site is not sufficient.
			  2. Consent to the delegated Sites.FullControl.All permission. Note that the Modify
			     permissions tab is empty for the request in step 4, because Graph Explorer has no
			     metadata for it, so consent this from the avatar menu at the top right under "Consent
			     to permissions" instead. To check it took, copy the token from the Access token tab
			     into https://jwt.ms and look for Sites.FullControl.All in the scp claim. If it is
			     missing, sign out and back in, since consent only lands on a newly issued token.
			  3. Find the site and copy its "id" exactly as returned, whatever shape it has. Do not
			     assemble it by hand:
			       %s
			     Pick the entry whose webUrl matches the site. This request needs only Sites.Read.All.
			  4. Grant this app access to that site. Replace SITE_ID in the url with the id from step 3,
			     and set displayName to whatever you want the grant listed as. The id inside
			     "application" is this engine's own client id and is already filled in, so leave it be:
			       POST https://graph.microsoft.com/v1.0/sites/SITE_ID/permissions
			       {"roles":["write"],"grantedToIdentities":[{"application":{"id":"%s","displayName":"APP_NAME"}}]}
			     A 403 here with an empty Modify permissions tab means either Sites.FullControl.All is
			     not consented or the account lacks the SharePoint Administrator role.
			  5. Confirm the grant:
			       GET https://graph.microsoft.com/v1.0/sites/SITE_ID/permissions

			Use "read" instead of "write" for a read only catalog. A token is cached for up to an hour,
			so restart after granting.""";

	private static final String SHAREPOINT_PERMISSIONS_GUIDANCE = """
			The token carries %s, which should be sufficient, so the denial is most likely SharePoint's
			own permissions on the target rather than the Graph permission. Check whether the library or
			folder has broken permission inheritance, and if it has, grant this app's service principal
			access to it directly.""";

	private static final String TEAMS_SITES_SELECTED_NOTE = """


			Note that Sites.Selected only authorizes requests addressed under /sites/{id}, and this
			engine resolves its root through /groups and /teams, which are not gated by it. It therefore
			cannot work with a site scoped grant at all. Use the SHAREPOINT storage type pointed at the
			team's site url instead, which stays on the site addressed route throughout.""";

	protected transient MicrosoftGraphAppTokenProvider tokenProvider;
	protected transient MicrosoftGraphDriveClient driveClient;

	protected String driveId;
	protected String rootItemId;
	protected String conflictBehavior = "replace";

	/**
	 * The drive and the item within it that a subclass resolved as its root.
	 *
	 * @param driveId    Graph drive id
	 * @param rootItemId drive item id that every storage path is relative to
	 */
	protected record DriveRoot(String driveId, String rootItemId) {
	}

	/**
	 * Builds the token provider from this engine's own credential properties.
	 *
	 * <p>
	 * Called before {@link #resolveDriveRoot}, since resolving the root needs a
	 * token. Implementations should let the provider's constructor validate the
	 * credentials, so a misconfigured engine fails on open rather than on the first
	 * file operation.
	 * </p>
	 *
	 * @param smssProp the engine's properties
	 * @return a token provider for the configured service identity
	 */
	protected abstract MicrosoftGraphAppTokenProvider createTokenProvider(Properties smssProp);

	/**
	 * Resolves the drive and root item this engine operates within.
	 *
	 * @param smssProp the engine's properties
	 * @return the resolved root
	 * @throws Exception if the configured target cannot be resolved
	 */
	protected abstract DriveRoot resolveDriveRoot(Properties smssProp) throws Exception;

	/**
	 * Describes what this engine is rooted at, for the log line on open.
	 *
	 * @param smssProp the engine's properties
	 * @return a short human readable description
	 */
	protected abstract String describeTarget(Properties smssProp);

	/**
	 * Names the request that looks up the site id of whatever this engine is
	 * configured against, for the guidance shown when a grant is missing.
	 *
	 * <p>
	 * The default is the generic form, with the hostname and path left to the
	 * reader. An engine that already knows its site should override this and return
	 * the concrete request, so the guidance can be run as it stands.
	 * </p>
	 *
	 * @param smssProp the engine's properties
	 * @return a request the reader can run to obtain the site id
	 */
	protected String describeSiteLookup(Properties smssProp) {
		return "GET https://graph.microsoft.com/v1.0/sites?search={site name}";
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		String configuredConflict = smssProp.getProperty(MS_CONFLICT_BEHAVIOR);
		if (configuredConflict != null && !configuredConflict.trim().isEmpty()) {
			this.conflictBehavior = configuredConflict.trim();
		}

		this.tokenProvider = createTokenProvider(smssProp);
		DriveRoot root;
		try {
			root = resolveDriveRoot(smssProp);
		} catch (IllegalArgumentException e) {
			// a bare 403 says nothing about which of the several possible causes it is,
			// so read the token's own roles and say which one it looks like
			if (isForbidden(e)) {
				throw new IllegalArgumentException(buildAccessDeniedGuidance(smssProp, e), e);
			}
			throw e;
		}
		this.driveId = root.driveId();
		this.rootItemId = root.rootItemId();
		this.driveClient = new MicrosoftGraphDriveClient(this.driveId, this.rootItemId,
				this.tokenProvider::getAccessToken);

		classLogger.info("Opened {} on {}, drive {} root {}", this.getClass().getSimpleName(), describeTarget(smssProp),
				this.driveId, this.rootItemId);
	}

	@Override
	public void close() throws IOException {
		// every call is a stateless https request, so there is no connection to drop.
		// Dropping the cached token would only force a needless re-acquire if the
		// engine is reopened
	}

	/*
	 * Listing
	 */

	@Override
	public List<String> list(String storagePath) throws Exception {
		List<Map<String, Object>> details = listDetails(storagePath);
		List<String> fileList = new ArrayList<>(details.size());
		for (Map<String, Object> item : details) {
			Object nameObj = item.get(KEY_NAME);
			if (nameObj == null) {
				continue;
			}
			String name = nameObj.toString();
			fileList.add(Boolean.TRUE.equals(item.get(KEY_IS_DIR)) ? name + "/" : name);
		}
		return fileList;
	}

	@Override
	public List<Map<String, Object>> listDetails(String storagePath) throws Exception {
		String requestedPath = normalizeStoragePrefixPath(storagePath);
		List<Map<String, Object>> detailsList = new ArrayList<>();

		for (Map<String, Object> child : this.driveClient.listChildren(requestedPath)) {
			Object nameObj = child.get(MicrosoftGraphDriveClient.NAME);
			if (nameObj == null) {
				continue;
			}
			String name = nameObj.toString();
			boolean isDir = MicrosoftGraphDriveClient.isFolder(child);

			Map<String, Object> itemMap = new HashMap<>();
			itemMap.put(KEY_PATH, requestedPath.isEmpty() ? "/" + name : "/" + requestedPath + "/" + name);
			itemMap.put(KEY_NAME, name);
			itemMap.put(KEY_IS_DIR, isDir);
			itemMap.put(KEY_SIZE, MicrosoftGraphDriveClient.getSize(child));
			itemMap.put(KEY_MOD_TIME, child.get(MicrosoftGraphDriveClient.LAST_MODIFIED));
			itemMap.put(KEY_MIME_TYPE, isDir ? MIME_DIRECTORY : MicrosoftGraphDriveClient.getMimeType(child));
			// Graph exposes custom columns through the listItem side of a driveItem
			// rather than as blob style metadata, so there is nothing to surface here
			itemMap.put(KEY_METADATA, Collections.emptyMap());
			detailsList.add(itemMap);
		}
		return detailsList;
	}

	@Override
	public List<Map<String, Object>> listVersions(String storagePath) throws Exception {
		String normalized = normalizeStoragePrefixPath(storagePath);
		// version history belongs to one file, so a folder is not something this can
		// answer for
		if (normalized.isEmpty()) {
			throw new IllegalArgumentException("Storage path '" + storagePath + "' does not name a file. Versions "
					+ "belong to a single file, for example myfolder/myfile.txt");
		}
		Map<String, Object> item = requireFile(normalized, storagePath);

		List<Map<String, Object>> versions = new ArrayList<>();
		List<Map<String, Object>> graphVersions = this.driveClient
				.listVersions(item.get(MicrosoftGraphDriveClient.ID).toString());
		for (int i = 0; i < graphVersions.size(); i++) {
			Map<String, Object> graphVersion = graphVersions.get(i);
			Map<String, Object> versionInfo = new LinkedHashMap<>();
			versionInfo.put("versionId", graphVersion.get(MicrosoftGraphDriveClient.ID));
			versionInfo.put("lastModified", graphVersion.get(MicrosoftGraphDriveClient.LAST_MODIFIED));
			versionInfo.put("size", graphVersion.get(MicrosoftGraphDriveClient.SIZE));
			// Graph returns versions newest first and does not support $orderby, so the
			// first entry is the current one
			versionInfo.put("isLatest", i == 0);
			versionInfo.put("key", normalized);
			versionInfo.put("lastModifiedBy", describeLastModifiedBy(graphVersion));
			versions.add(versionInfo);
		}
		return versions;
	}

	/*
	 * Transfers
	 */

	@Override
	public String copyToStorage(String localFilePath, String storageFolderPath, Map<String, Object> metadata)
			throws Exception {
		String storageFolder = normalizeStoragePrefixPath(storageFolderPath);
		Map<Path, String> filesToUpload = collectLocalFiles(localFilePath, storageFolder);
		if (filesToUpload.isEmpty()) {
			classLogger.info("No files found to copy for: {}", storageFolderPath);
			return null;
		}

		List<String> uploaded = new ArrayList<>(), failed = new ArrayList<>();
		for (TransferOutcome outcome : runTransfersInParallel(buildUploads(filesToUpload))) {
			if (outcome.failure() == null) {
				uploaded.add(outcome.fileKey());
			} else {
				failed.add(outcome.fileKey());
			}
		}
		if (!failed.isEmpty()) {
			classLogger.error("Failed to upload {} file(s) to '{}': {}", failed.size(), storageFolder, failed);
		}
		classLogger.info("Uploaded {} file(s) to '{}'", uploaded.size(), storageFolder);
		// the upload response is a driveItem, which does not carry the version it
		// created. Reading it back would cost an extra request per file, so callers
		// that need the new version ask listVersions for it
		return null;
	}

	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath, String versionId) throws Exception {
		String requestedVersionId = versionId == null ? null : versionId.trim();
		if (requestedVersionId != null && !requestedVersionId.isEmpty()) {
			copyVersionToLocal(storageFilePath, localFolderPath, requestedVersionId);
			return;
		}

		Path localDirectory = Paths.get(localFolderPath);
		Files.createDirectories(localDirectory);

		Map<String, Map<String, Object>> toDownload = new LinkedHashMap<>();
		for (String requested : parseStorageObjectPaths(storageFilePath)) {
			String normalized = normalizeStoragePrefixPath(requested);
			Map<String, Object> item = this.driveClient.getItem(normalized);
			if (item == null) {
				classLogger.error("Nothing exists at storage path: {}", normalized);
				continue;
			}
			if (MicrosoftGraphDriveClient.isFolder(item)) {
				// a folder is a request for everything beneath it, keyed by the path
				// relative to that folder so the local tree mirrors the remote one
				for (Map.Entry<String, Map<String, Object>> entry : this.driveClient.listFilesRecursively(normalized)
						.entrySet()) {
					toDownload.put(MicrosoftGraphDriveClient.joinPath(normalized, entry.getKey()), entry.getValue());
				}
			} else {
				toDownload.put(normalized, item);
			}
		}
		if (toDownload.isEmpty()) {
			classLogger.info("No files found to download for: {}", storageFilePath);
			return;
		}

		List<Callable<TransferOutcome>> transfers = new ArrayList<>(toDownload.size());
		for (Map.Entry<String, Map<String, Object>> entry : toDownload.entrySet()) {
			String remotePath = entry.getKey();
			Map<String, Object> item = entry.getValue();
			// keep the tree below what was asked for, matching the other engines
			String relative = relativeToRequest(remotePath, storageFilePath);
			transfers.add(() -> {
				try {
					byte[] bytes = this.driveClient.downloadBytes(item, remotePath);
					Path target = localDirectory.resolve(relative);
					if (target.getParent() != null) {
						Files.createDirectories(target.getParent());
					}
					Files.write(target, bytes);
					return new TransferOutcome(remotePath, null);
				} catch (Exception e) {
					classLogger.error("Failed to download file: {}", remotePath, e);
					return new TransferOutcome(remotePath, e);
				}
			});
		}

		int downloaded = 0;
		List<String> failed = new ArrayList<>();
		for (TransferOutcome outcome : runTransfersInParallel(transfers)) {
			if (outcome.failure() == null) {
				downloaded++;
			} else {
				failed.add(outcome.fileKey());
			}
		}
		if (!failed.isEmpty()) {
			classLogger.error("Failed to download {} file(s): {}", failed.size(), failed);
		}
		classLogger.info("Downloaded {} file(s) to {}", downloaded, localFolderPath);
	}

	@Override
	public StorageSyncStatus syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata)
			throws Exception {
		String storageFolder = normalizeStoragePrefixPath(storagePath);
		Path localRoot = Paths.get(localPath);
		if (!Files.exists(localRoot)) {
			throw new IllegalArgumentException("Local path does not exist: " + localPath);
		}

		// one listing up front, so deciding what changed costs no extra requests
		Map<String, Map<String, Object>> remoteFiles = this.driveClient.listFilesRecursively(storageFolder);

		Map<Path, String> candidates = new LinkedHashMap<>();
		List<String> skipped = new ArrayList<>();
		if (Files.isDirectory(localRoot)) {
			try (Stream<Path> stream = Files.walk(localRoot)) {
				for (Path file : stream.filter(Files::isRegularFile).toList()) {
					String relative = toRelativeKey(localRoot.relativize(file).toString());
					addSyncCandidate(file, relative, storageFolder, remoteFiles, candidates, skipped);
				}
			}
		} else {
			String relative = localRoot.getFileName().toString().trim();
			addSyncCandidate(localRoot, relative, storageFolder, remoteFiles, candidates, skipped);
		}

		List<String> uploaded = new ArrayList<>(), failed = new ArrayList<>();
		for (TransferOutcome outcome : runTransfersInParallel(buildUploads(candidates))) {
			if (outcome.failure() == null) {
				uploaded.add(outcome.fileKey());
			} else {
				failed.add(outcome.fileKey());
			}
		}
		classLogger.info("Sync to '{}': {} uploaded, {} skipped, {} failed", storageFolder, uploaded.size(),
				skipped.size(), failed.size());
		return StorageSyncStatus.of(storageFolder, uploaded, skipped, failed);
	}

	@Override
	public void syncStorageToLocal(String storagePath, String localPath) throws Exception {
		String storageFolder = normalizeStoragePrefixPath(storagePath);
		Path localRoot = Paths.get(localPath);
		Files.createDirectories(localRoot);

		Map<String, Map<String, Object>> remoteFiles = this.driveClient.listFilesRecursively(storageFolder);
		if (remoteFiles.isEmpty()) {
			classLogger.info("Nothing to sync down from storage path: {}", storageFolder);
			return;
		}

		List<Callable<TransferOutcome>> transfers = new ArrayList<>(remoteFiles.size());
		for (Map.Entry<String, Map<String, Object>> entry : remoteFiles.entrySet()) {
			String relative = entry.getKey();
			Map<String, Object> item = entry.getValue();
			String remotePath = MicrosoftGraphDriveClient.joinPath(storageFolder, relative);
			transfers.add(() -> {
				try {
					Path target = localRoot.resolve(relative);
					if (needsDownload(target, item)) {
						byte[] bytes = this.driveClient.downloadBytes(item, remotePath);
						if (target.getParent() != null) {
							Files.createDirectories(target.getParent());
						}
						Files.write(target, bytes);
					}
					return new TransferOutcome(relative, null);
				} catch (Exception e) {
					classLogger.error("Failed to sync down file: {}", remotePath, e);
					return new TransferOutcome(relative, e);
				}
			});
		}

		List<String> failed = new ArrayList<>();
		for (TransferOutcome outcome : runTransfersInParallel(transfers)) {
			if (outcome.failure() != null) {
				failed.add(outcome.fileKey());
			}
		}
		if (!failed.isEmpty()) {
			classLogger.error("Failed to sync down {} file(s): {}", failed.size(), failed);
		}
	}

	@Override
	public byte[] readBlobToMemory(String storagePath) throws Exception {
		return this.driveClient.downloadBytes(normalizeStoragePrefixPath(storagePath));
	}

	/*
	 * Deletes
	 */

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception {
		String normalized = normalizeStoragePrefixPath(storagePath);
		if (normalized.isEmpty()) {
			throw new IllegalArgumentException(
					"Refusing to delete the root of this engine. Name a file or folder beneath it instead.");
		}

		Map<String, Object> item = this.driveClient.getItem(normalized);
		if (item == null) {
			classLogger.info("Nothing to delete at storage path: {}", normalized);
			return;
		}
		if (leaveFolderStructure && MicrosoftGraphDriveClient.isFolder(item)) {
			this.driveClient.deleteChildren(normalized);
			classLogger.info("Emptied folder: {}", normalized);
			return;
		}
		this.driveClient.delete(normalized);
		classLogger.info("Deleted storage path: {}", normalized);
	}

	@Override
	public void deleteFolderFromStorage(String storageFolderPath) throws Exception {
		String normalized = normalizeStoragePrefixPath(storageFolderPath);
		if (normalized.isEmpty()) {
			// the root belongs to the channel or library, so removing it is not this
			// engine's call to make
			throw new IllegalArgumentException(
					"Refusing to delete the root folder of this engine. Name a folder beneath it instead.");
		}
		this.driveClient.delete(normalized);
		classLogger.info("Deleted folder: {}", normalized);
	}

	/*
	 * Transfer helpers
	 */

	/**
	 * Gathers the local files to upload and the storage key each one lands on.
	 *
	 * <p>
	 * A directory keeps its internal structure below the target folder, and a named
	 * file lands directly in it, which is the same shape the other engines use.
	 * </p>
	 */
	private Map<Path, String> collectLocalFiles(String localFilePath, String storageFolder) throws Exception {
		Map<Path, String> filesToUpload = new LinkedHashMap<>();
		for (Path localPath : parseLocalPaths(localFilePath)) {
			if (!Files.exists(localPath)) {
				classLogger.error("File not found: {}", localPath);
				continue;
			}
			if (Files.isDirectory(localPath)) {
				try (Stream<Path> stream = Files.walk(localPath)) {
					for (Path file : stream.filter(Files::isRegularFile).toList()) {
						String relative = toRelativeKey(localPath.relativize(file).toString());
						filesToUpload.put(file, MicrosoftGraphDriveClient.joinPath(storageFolder, relative));
					}
				}
			} else {
				String name = localPath.getFileName().toString().trim();
				filesToUpload.put(localPath, MicrosoftGraphDriveClient.joinPath(storageFolder, name));
			}
		}
		return filesToUpload;
	}

	private List<Callable<TransferOutcome>> buildUploads(Map<Path, String> filesToUpload) {
		List<Callable<TransferOutcome>> transfers = new ArrayList<>(filesToUpload.size());
		for (Map.Entry<Path, String> entry : filesToUpload.entrySet()) {
			Path file = entry.getKey();
			String storageKey = entry.getValue();
			transfers.add(() -> {
				try {
					this.driveClient.uploadFile(file.toFile(), storageKey, this.conflictBehavior);
					return new TransferOutcome(storageKey, null);
				} catch (Exception e) {
					classLogger.error("Failed to upload file '{}' to storage path '{}'", file, storageKey, e);
					return new TransferOutcome(storageKey, e);
				}
			});
		}
		return transfers;
	}

	/**
	 * Decides whether one local file still has to go up, comparing against the
	 * listing already taken of the remote folder.
	 */
	private void addSyncCandidate(Path file, String relativeKey, String storageFolder,
			Map<String, Map<String, Object>> remoteFiles, Map<Path, String> candidates, List<String> skipped)
			throws Exception {
		String storageKey = MicrosoftGraphDriveClient.joinPath(storageFolder, relativeKey);
		StoredObjectStat stored = toStoredObjectStat(remoteFiles.get(relativeKey));
		if (needsUpload(file, stored)) {
			candidates.put(file, storageKey);
		} else {
			skipped.add(storageKey);
		}
	}

	/**
	 * Downloads one stored version of a single file.
	 *
	 * <p>
	 * A version id identifies one version of one file, so unlike the unversioned
	 * path this does not accept a folder or a list of paths. The file is written
	 * under the name it carries in storage.
	 * </p>
	 */
	private void copyVersionToLocal(String storageFilePath, String localFolderPath, String versionId) throws Exception {
		List<String> requestedPaths = parseStorageObjectPaths(storageFilePath);
		if (requestedPaths.size() != 1 || normalizeStoragePrefixPath(requestedPaths.get(0)).isEmpty()) {
			throw new IllegalArgumentException("A version id only applies to a single file. Name the file in the "
					+ "path, for example myfolder/myfile.txt");
		}
		String normalized = normalizeStoragePrefixPath(requestedPaths.get(0));
		Map<String, Object> item = requireFile(normalized, storageFilePath);

		Path localDirectory = Paths.get(localFolderPath);
		Files.createDirectories(localDirectory);

		byte[] bytes = this.driveClient.downloadVersionBytes(item.get(MicrosoftGraphDriveClient.ID).toString(),
				versionId);
		Object itemName = item.get(MicrosoftGraphDriveClient.NAME);
		String fileName = itemName != null ? itemName.toString() : baseName(normalized);
		Path target = localDirectory.resolve(fileName);
		Files.write(target, bytes);
		classLogger.info("Downloaded version {} of '{}' to {}", versionId, normalized, target);
	}

	/**
	 * Resolves a storage path that has to name an existing file rather than a
	 * folder, which is what the version operations require.
	 */
	private Map<String, Object> requireFile(String normalizedPath, String requestedPath) throws Exception {
		Map<String, Object> item = this.driveClient.getItem(normalizedPath);
		if (item == null) {
			throw new IllegalArgumentException("Nothing exists at storage path: " + requestedPath);
		}
		if (MicrosoftGraphDriveClient.isFolder(item)) {
			throw new IllegalArgumentException("Storage path '" + requestedPath + "' names a folder. Versions belong "
					+ "to a single file, for example myfolder/myfile.txt");
		}
		if (item.get(MicrosoftGraphDriveClient.ID) == null) {
			throw new IllegalStateException("Microsoft Graph returned no id for storage path: " + requestedPath);
		}
		return item;
	}

	/**
	 * Turns a Graph drive item into the size and modified time the shared upload
	 * comparison expects, or null when the item is absent.
	 */
	private StoredObjectStat toStoredObjectStat(Map<String, Object> item) {
		if (item == null) {
			return null;
		}
		return new StoredObjectStat(MicrosoftGraphDriveClient.getSize(item),
				parseGraphTimestamp(item.get(MicrosoftGraphDriveClient.LAST_MODIFIED)));
	}

	/**
	 * Whether a remote file differs from what is already on disk. Mirrors
	 * needsUpload in the other direction.
	 */
	private boolean needsDownload(Path localFile, Map<String, Object> remoteItem) throws Exception {
		if (!Files.exists(localFile)) {
			return true;
		}
		long remoteSize = MicrosoftGraphDriveClient.getSize(remoteItem);
		if (Files.size(localFile) != remoteSize) {
			return true;
		}
		long remoteModified = parseGraphTimestamp(remoteItem.get(MicrosoftGraphDriveClient.LAST_MODIFIED));
		return remoteModified > Files.getLastModifiedTime(localFile).toMillis();
	}

	/**
	 * Parses a Graph ISO-8601 timestamp to epoch millis, returning 0 when it cannot
	 * be read so the file is treated as needing transfer rather than skipped.
	 */
	private long parseGraphTimestamp(Object timestamp) {
		if (timestamp == null) {
			return 0L;
		}
		try {
			return Instant.parse(timestamp.toString()).toEpochMilli();
		} catch (Exception e) {
			classLogger.warn("Unable to parse Microsoft Graph timestamp '{}'", timestamp);
			return 0L;
		}
	}

	/**
	 * Reduces a full remote path to the portion below what the caller asked for, so
	 * a download preserves the tree beneath the requested path and nothing above
	 * it.
	 */
	private String relativeToRequest(String remotePath, String requestedPaths) {
		for (String requested : parseStorageObjectPaths(requestedPaths)) {
			String relative = resolveRelativeStoragePath(remotePath, normalizeStoragePrefixPath(requested));
			if (relative != null) {
				return relative;
			}
		}
		return baseName(remotePath);
	}

	/**
	 * Pulls the display name out of a version's lastModifiedBy identity set, which
	 * is nested as {@code lastModifiedBy.user.displayName}.
	 */
	@SuppressWarnings("unchecked")
	private static String describeLastModifiedBy(Map<String, Object> graphVersion) {
		Object lastModifiedBy = graphVersion.get("lastModifiedBy");
		if (!(lastModifiedBy instanceof Map)) {
			return null;
		}
		Object user = ((Map<String, Object>) lastModifiedBy).get("user");
		if (!(user instanceof Map)) {
			return null;
		}
		Object displayName = ((Map<String, Object>) user).get("displayName");
		return displayName == null ? null : displayName.toString();
	}

	private static String toRelativeKey(String relativePath) {
		return relativePath.replace('\\', '/').trim();
	}

	private static String baseName(String path) {
		int slashIdx = path.lastIndexOf('/');
		return slashIdx >= 0 ? path.substring(slashIdx + 1) : path;
	}

	/*
	 * Graph helpers, for subclasses resolving their root on open
	 */

	/**
	 * Whether a failed Graph request was rejected as forbidden.
	 *
	 * @param e the exception the request helper raised
	 * @return true when the response was a 403
	 */
	protected static boolean isForbidden(IllegalArgumentException e) {
		String message = e.getMessage();
		if (message == null) {
			return false;
		}
		String lower = message.toLowerCase();
		return lower.contains("returned http 403") || lower.contains("accessdenied");
	}

	/**
	 * Explains a 403 in terms of what the token actually carries, since the causes
	 * need different fixes and Graph's own message does not distinguish them.
	 *
	 * <p>
	 * Three cases are told apart by the {@code roles} claim: no roles at all means
	 * admin consent was never granted, {@code Sites.Selected} without any tenant
	 * wide permission means the per-site grant is missing, and anything else points
	 * at SharePoint's own permissions on the target.
	 * </p>
	 *
	 * @param smssProp the engine's properties, for naming the configured target
	 * @param cause    the 403 that was raised
	 * @return a message naming the likely cause and how to fix it
	 */
	protected String buildAccessDeniedGuidance(Properties smssProp, IllegalArgumentException cause) {
		Set<String> roles = this.tokenProvider.getGrantedRoles();
		String clientId = this.tokenProvider.getClientId();

		String guidance;
		if (roles.isEmpty()) {
			guidance = NO_ADMIN_CONSENT_GUIDANCE.formatted(this.tokenProvider.getTenantId(), clientId);
		} else if (roles.contains(SITES_SELECTED) && Collections.disjoint(roles, TENANT_WIDE_PERMISSIONS)) {
			// the permission is right, so what is missing is the per site grant. There is
			// no portal page for that, so the whole procedure is spelled out rather than
			// left as a name to go and search for
			guidance = SITE_GRANT_GUIDANCE.formatted(describeSiteLookup(smssProp), clientId);
		} else {
			guidance = SHAREPOINT_PERMISSIONS_GUIDANCE.formatted(roles);
		}

		if (roles.contains(SITES_SELECTED) && this.getStorageType() == StorageTypeEnum.MICROSOFT_TEAMS) {
			guidance = guidance + TEAMS_SITES_SELECTED_NOTE;
		}
		return ACCESS_DENIED_GUIDANCE.formatted(describeTarget(smssProp), guidance);
	}

	/**
	 * Runs a Graph GET and parses the response as a JSON object.
	 *
	 * @param url absolute Graph url
	 * @return the parsed object, or null when the response is empty
	 * @throws Exception if the request fails
	 */
	protected Map<String, Object> graphGet(String url) throws Exception {
		String response = HttpHelperUtility.getRequest(url,
				MicrosoftLoginUtils.getBearerHeader(this.tokenProvider.getAccessToken()), null, null, null);
		if (response == null || response.trim().isEmpty()) {
			return null;
		}
		return GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
	}

	/**
	 * Runs a Graph GET and returns its {@code value} collection.
	 *
	 * @param url absolute Graph url
	 * @return the collection, never null
	 * @throws Exception if the request fails
	 */
	@SuppressWarnings("unchecked")
	protected List<Map<String, Object>> graphList(String url) throws Exception {
		Map<String, Object> json = graphGet(url);
		if (json == null) {
			return Collections.emptyList();
		}
		Object value = json.get("value");
		if (!(value instanceof List)) {
			return Collections.emptyList();
		}
		return (List<Map<String, Object>>) value;
	}

	/**
	 * Runs a Graph GET and returns its {@code value} collection, following
	 * {@code @odata.nextLink} to the end.
	 *
	 * @param url absolute Graph url
	 * @return every page's entries, never null
	 * @throws Exception if any page fails
	 */
	@SuppressWarnings("unchecked")
	protected List<Map<String, Object>> graphListAllPages(String url) throws Exception {
		List<Map<String, Object>> collected = new ArrayList<>();
		String nextUrl = url;
		while (nextUrl != null) {
			Map<String, Object> json = graphGet(nextUrl);
			if (json == null) {
				break;
			}
			Object value = json.get("value");
			if (value instanceof List) {
				for (Object entry : (List<?>) value) {
					if (entry instanceof Map) {
						collected.add((Map<String, Object>) entry);
					}
				}
			}
			Object next = json.get("@odata.nextLink");
			nextUrl = next == null ? null : next.toString();
		}
		return collected;
	}

	/**
	 * Reads the drive id off an item's parentReference, which is how Graph reports
	 * which drive an item belongs to.
	 *
	 * @param item a drive item
	 * @return the drive id, or null when absent
	 */
	@SuppressWarnings("unchecked")
	protected String extractDriveId(Map<String, Object> item) {
		Object parentReference = item.get("parentReference");
		if (parentReference instanceof Map) {
			Object parentDriveId = ((Map<String, Object>) parentReference).get("driveId");
			return parentDriveId == null ? null : parentDriveId.toString();
		}
		return null;
	}

	/**
	 * A single quote inside an OData string literal is escaped by doubling it.
	 *
	 * @param value raw value to place inside a literal
	 * @return the escaped value
	 */
	protected static String escapeOData(String value) {
		return value.replace("'", "''");
	}

	/**
	 * Encodes a value for use in a query string.
	 *
	 * @param value raw value
	 * @return the encoded value, with spaces as %20 rather than +
	 */
	protected static String encodeQuery(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
	}

	/**
	 * @param value candidate identifier
	 * @return true when the value has the shape of a GUID, which is how a
	 *         configured id is told apart from a display name
	 */
	protected static boolean looksLikeGuid(String value) {
		return value != null && value.length() == 36 && value.charAt(8) == '-' && value.charAt(13) == '-'
				&& value.charAt(18) == '-' && value.charAt(23) == '-';
	}

	/**
	 * @param value   value that must be present
	 * @param message what to report when it is not
	 * @return the trimmed value
	 * @throws IllegalStateException when the value is null or blank
	 */
	protected static String requireString(Object value, String message) {
		if (value == null || value.toString().trim().isEmpty()) {
			throw new IllegalStateException(message);
		}
		return value.toString().trim();
	}

	protected static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}
}
