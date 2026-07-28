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
package prerna.engine.impl.model;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.date.SemossDate;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MediaMessagePart;
import prerna.engine.impl.model.message.MessageInputMedia;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.message.MessageSchemaUpgrader;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.playground.PlaygroundUtils;
import prerna.project.api.IProject;
import prerna.redis.RedisConnectionConfig;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Utility methods for creating, loading, migrating, and querying {@link Room}
 * instances.
 * <p>
 * This class centralizes room lifecycle concerns including:
 * <ul>
 * <li>conditional room creation in persistence</li>
 * <li>loading from user cache and database</li>
 * <li>legacy message backfill and schema upgrade</li>
 * <li>message paging helpers</li>
 * <li>room-folder file presence checks</li>
 * </ul>
 */
public final class RoomUtils {

	private static final Logger classLogger = LogManager.getLogger(RoomUtils.class);

	/**
	 * Convenience overload that creates/loads a room with default optional values.
	 *
	 * @param roomId      requested room id; when null/blank the insight id is used
	 * @param insight     active insight context
	 * @param modelEngine model engine associated with the room
	 * @param question    initial user question used for default room naming
	 * @return the existing or newly created Room
	 */
	public static Room createRoomIfNotExists(String roomId, Insight insight, IModelEngine modelEngine,
			String question) {
		return createRoomIfNotExists(roomId, insight, modelEngine, question, null, null, null, null, null);
	}

	/**
	 * Creates/loads a room for a stateless model ask. This path is used when the
	 * caller is not reading or writing persisted room message history.
	 *
	 * @param roomId      requested room id; when null/blank the insight id is used
	 * @param insight     active insight context
	 * @param modelEngine model engine associated with the room
	 * @param question    initial user question used for default room naming
	 * @return the existing or newly created Room
	 */
	public static Room createRoomForStatelessAsk(String roomId, Insight insight, IModelEngine modelEngine,
			String question) {
		return createRoomForStatelessAsk(roomId, insight, modelEngine, question, null, null, null, null, null);
	}

	/**
	 * Creates/loads a room for a stateless model ask. Existing rooms are loaded
	 * without acquiring the room message mutation lock or normalizing/persisting
	 * message history. Missing rooms still lock around create-if-missing because
	 * room row creation is a shared write.
	 *
	 * @param roomId       requested room id; when null/blank the insight id is used
	 * @param insight      active insight context
	 * @param modelEngine  model engine associated with the room (optional)
	 * @param question     initial user question used for default room naming
	 * @param workspaceId  optional workspace id to associate with the room
	 * @param options      optional room options payload
	 * @param context      optional room context/system prompt
	 * @param projectId    optional project id override
	 * @param parentRoomId optional parent room id for sub-conversations
	 * @return the existing or newly created Room
	 */
	public static Room createRoomForStatelessAsk(String roomId, Insight insight, IModelEngine modelEngine,
			String question, String workspaceId, Map<String, Object> options, String context, String projectId,
			String parentRoomId) {
		roomId = resolveRoomId(roomId, insight);

		if (!ModelInferenceLogsUtils.doCheckRoomExists(roomId)) {
			try (RoomMessageStore.RoomMutationLock ignored = RoomMessageStore.acquireMutationLock(roomId)) {
				createRoomRowIfMissing(roomId, insight, modelEngine, question, workspaceId, options, context, projectId,
						parentRoomId);
			}
		}

		return getOrLoadRoomForStatelessAsk(roomId, insight);
	}

	/**
	 * Ensures a Room exists: creates it if necessary, then loads it for the given
	 * user/insight.
	 *
	 * @param roomId       requested room id; when null/blank the insight id is used
	 * @param insight      active insight context
	 * @param modelEngine  model engine associated with the room (optional)
	 * @param question     initial user question used for default room naming
	 * @param workspaceId  optional workspace id to associate with the room
	 * @param options      optional room options payload
	 * @param context      optional room context/system prompt
	 * @param projectId    optional project id override
	 * @param parentRoomId optional parent room id for sub-conversations
	 * @return the existing or newly created Room
	 */
	public static Room createRoomIfNotExists(String roomId, Insight insight, IModelEngine modelEngine, String question,
			String workspaceId, Map<String, Object> options, String context, String projectId, String parentRoomId) {
		roomId = resolveRoomId(roomId, insight);

		try (RoomMessageStore.RoomMutationLock ignored = RoomMessageStore.acquireMutationLock(roomId)) {
			createRoomRowIfMissing(roomId, insight, modelEngine, question, workspaceId, options, context, projectId,
					parentRoomId);
			return RoomUtils.getOrLoadRoom(roomId, insight);
		}
	}

	private static String resolveRoomId(String roomId, Insight insight) {
		if (roomId == null || roomId.trim().isEmpty()) {
			return insight.getInsightId();
		}
		return roomId;
	}

	private static void createRoomRowIfMissing(String roomId, Insight insight, IModelEngine modelEngine,
			String question, String workspaceId, Map<String, Object> options, String context, String projectId,
			String parentRoomId) {
		boolean roomExistsInDB = ModelInferenceLogsUtils.doCheckRoomExists(roomId);
		if (roomExistsInDB) {
			return;
		}

		String agentType = null;
		String engineId = null;
		if (modelEngine != null) {
			agentType = modelEngine.getCatalogSubType(modelEngine.getSmssProp());
			engineId = modelEngine.getEngineId();
		}
		User user = insight.getUser();
		AccessToken userToken = user.getPrimaryLoginToken();
		String userName = userToken.getName();
		String userEmail = userToken.getEmail();
		if (projectId == null) {
			projectId = insight.getContextProjectId();
		}
		if (projectId == null) {
			projectId = insight.getProjectId();
		}
		String projectName = null;
		// ignore playground project id
		if (projectId != null && !projectId.equals(PlaygroundUtils.PLAYGROUND_PROJECT_ID)) {
			IProject project = Utility.getProject(projectId);
			projectName = project != null ? project.getProjectName() : null;
		}
		String roomName = (question != null) ? question.substring(0, Math.min(question.length(), 100)) : null;
		// @formatter:off
		ModelInferenceLogsUtils.doCreateNewConversation(
				insight.getInsightId(),
				roomId,
				roomName,
				context,
				userToken.getId(),
				userName,
				userEmail,
				agentType,
				engineId,
				true,
				projectId,
				projectName,
				workspaceId,
				options,
				parentRoomId
		);
		// @formatter:on
	}

	/**
	 * Loads a Room from user room hash or database if present.
	 *
	 * @param roomId  room identifier
	 * @param insight active insight context (contains user cache and user id)
	 * @return loaded room with normalized message state
	 * @throws IllegalArgumentException if Room does not exist.
	 */
	public static Room getOrLoadRoom(String roomId, Insight insight) {
		Room room;
		// Check in user's cache (roomHash)
		if (insight.getUser().getRoomHash().containsKey(roomId)) {
			try {
				room = insight.getUser().getRoomHash().get(roomId);
				// A user's room cache outlives individual HTTP Insight instances. Always
				// attach the current caller before any room operation uses transient context.
				room.setInsight(insight);
				refreshCachedRoomMessagesIfRedisEnabled(room, insight);
				ensureRoomMessagesUpToDate(room, insight);
				symlinkRoomFolderIfNeeded(room, insight);
				return room;
			} catch (ClassCastException e) {
				insight.getUser().getRoomHash().remove(roomId); // Clear corrupted cache entry
			}
		}
		// else it may be in the DB
		boolean roomExistsInDB = ModelInferenceLogsUtils.doCheckRoomExists(roomId);
		if (!roomExistsInDB) {
			throw new IllegalArgumentException("Room ID is not valid");
		}
		room = ModelInferenceLogsUtils.getRoomById(roomId, insight.getUser().getPrimaryLoginToken().getId());
		if (room == null) {
			throw new IllegalArgumentException("Room is not valid for this user");
		}

		ensureRoomMessagesUpToDate(room, insight);

		// TODO: do we need this?
		List<AbstractMessage> messages = room.getMessages();
		if (!messages.isEmpty()) {
			// if the message id in room table does not match message ids in message table,
			// probably needs migration - this is only if we never have had a message with a
			// message_json yet!
			boolean migratedMessageIds = ModelInferenceLogsUtils.doCheckMessageIdMigration(roomId,
					messages.get(0).getMessageId());
			if (!migratedMessageIds) {
				for (AbstractMessage m : messages) {
					ModelInferenceLogsUtils.updateMessageIds(m.getTransactionId(), m.getMessageId(),
							m.getMessageType());
				}
			}
		}

		room.setInsight(insight);
		insight.getUser().getRoomHash().put(roomId, room);
		symlinkRoomFolderIfNeeded(room, insight);
		return room;
	}

	private static Room getOrLoadRoomForStatelessAsk(String roomId, Insight insight) {
		Room room;
		if (insight.getUser().getRoomHash().containsKey(roomId)) {
			try {
				room = insight.getUser().getRoomHash().get(roomId);
				room.setInsight(insight);
				symlinkRoomFolderIfNeeded(room, insight);
				return room;
			} catch (ClassCastException e) {
				insight.getUser().getRoomHash().remove(roomId);
			}
		}

		boolean roomExistsInDB = ModelInferenceLogsUtils.doCheckRoomExists(roomId);
		if (!roomExistsInDB) {
			throw new IllegalArgumentException("Room ID is not valid");
		}
		room = ModelInferenceLogsUtils.getRoomById(roomId, insight.getUser().getPrimaryLoginToken().getId());
		if (room == null) {
			throw new IllegalArgumentException("Room is not valid for this user");
		}

		room.setInsight(insight);
		insight.getUser().getRoomHash().put(roomId, room);
		symlinkRoomFolderIfNeeded(room, insight);
		return room;
	}

	private static void refreshCachedRoomMessagesIfRedisEnabled(Room room, Insight insight) {
		if (room == null || insight == null || insight.getUser() == null || !RedisConnectionConfig.isRedisEnabled()) {
			return;
		}
		RoomMessageStore.refreshFromLatestProjection(room, insight.getUser().getPrimaryLoginToken().getId());
	}

	/**
	 * Message normalization lifecycle on room load: 1) If ROOM.MESSAGES is empty,
	 * treat as legacy and backfill from MESSAGE rows. 2) Otherwise ensure in-memory
	 * messages are parsed (cached safety-net case). 3) Run schema upgrade checks
	 * and persist only when content actually changes.
	 * <p>
	 * Both cache-hit and DB-load paths call this method so legacy migration and
	 * schema upgrades are defined in one place. Ensures room messages are loaded
	 * and upgraded to the latest persisted schema (including pre-message_json
	 * legacy rooms).
	 *
	 * @param room    room to normalize
	 * @param insight insight context used for persistence/user checks
	 */
	private static void ensureRoomMessagesUpToDate(Room room, Insight insight) {
		if (room == null || insight == null || insight.getUser() == null) {
			return;
		}
		String json = room.getMessageJson();
		if (json == null || json.trim().isEmpty()) {
			updateRoom(room, insight);
			return;
		}
		// Messages should already be parsed by Room constructor. This is a safety net
		// for any cached legacy/corrupted objects.
		if (room.getMessages().isEmpty() && !"[]".equals(json.trim())) {
			room.parseMessages();
		}
		upgradeRoomMessagesIfNeeded(room, insight);
	}

	/**
	 * Ensures the room folder is symlinked into the user's chroot environment. This
	 * is needed when an existing room is loaded after re-login, since the chroot
	 * jail is destroyed on logout and recreated on the new session.
	 *
	 * @param room    room containing folder path information
	 * @param insight insight context containing user symlink helper
	 */
	private static void symlinkRoomFolderIfNeeded(Room room, Insight insight) {
		if (!Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
			return;
		}
		if (room == null || insight == null || insight.getUser() == null) {
			return;
		}
		String roomFolderPath = room.getRoomFolderPath();
		if (roomFolderPath == null || roomFolderPath.trim().isEmpty()) {
			return;
		}
		try {
			Path folderPath = Paths.get(roomFolderPath);
			Files.createDirectories(folderPath);
			insight.getUser().getUserSymlinkHelper().symlinkFolder(roomFolderPath);
		} catch (IOException e) {
			classLogger.warn("Failed to symlink room folder into chroot: " + roomFolderPath, e);
		}
	}

	/**
	 * Upgrades persisted room messages to the latest message schema when needed and
	 * persists only if content changes.
	 *
	 * @param room    room whose messages should be upgraded
	 * @param insight insight context used for persistence/user checks
	 */
	private static void upgradeRoomMessagesIfNeeded(Room room, Insight insight) {
		if (room == null || insight == null || insight.getUser() == null) {
			return;
		}
		String json = room.getMessageJson();
		boolean jsonMissingSchema = (json != null && !json.contains("\"schemaVersion\""));
		if (!jsonMissingSchema && !MessageSchemaUpgrader.needsUpgrade(room.getMessages())) {
			return;
		}

		boolean changed = MessageSchemaUpgrader.upgradeInPlace(room.getMessages());
		String upgraded = room.getMessagesAsString();
		if (!changed && upgraded.equals(json)) {
			return;
		}
		RoomMessageStore.persist(room, insight.getUser().getPrimaryLoginToken().getId());
	}

	/**
	 * Migrates a legacy room (without ROOM.MESSAGES JSON) by rebuilding message
	 * history from MESSAGE table rows and persisting normalized message JSON.
	 *
	 * @param room    room to migrate
	 * @param insight insight context used for user-scoped retrieval/persistence
	 */
	private static void updateRoom(Room room, Insight insight) {
		List<Map<String, Object>> output = ModelInferenceLogsUtils
				.doRetrieveConversation(insight.getUser().getPrimaryLoginToken().getId(), room.getId(), "ASC", -1, -1);

		// for each message, build an AbstractMessage
		List<AbstractMessage> messages = new ArrayList<>();
		for (Map<String, Object> entry : output) {
			AbstractMessage msg = convertLegacyMessage(room, entry);
			if (msg != null) {
				messages.add(msg);
			}
		}

		// set and persist the normalized messages in one pass
		room.setMessages(messages);
		RoomMessageStore.persist(room, insight.getUser().getPrimaryLoginToken().getId());
	}

	/**
	 * Retrieves parsed room options for a user-scoped room.
	 *
	 * @param roomId room identifier
	 * @param userId user identifier
	 * @return room options map, or empty map when no options are stored
	 */
	public static Map<String, Object> getRoomOptions(String roomId, String userId) {
		List<Map<String, Object>> roomOptions = ModelInferenceLogsUtils.getRoomOptions(roomId, userId);
		if (roomOptions == null || roomOptions.isEmpty()) {
			return new HashMap<String, Object>();
		}
		return roomOptions.get(0);
	}

	/**
	 * Converts a single legacy MESSAGE-table row to an in-memory room message.
	 *
	 * @param room  room context used by message builders
	 * @param entry legacy MESSAGE row
	 * @return converted message, or {@code null} for unknown message types
	 */
	private static AbstractMessage convertLegacyMessage(Room room, Map<String, Object> entry) {
		// Read type
		String type = "" + entry.get("MESSAGE_TYPE");
		// Defensive: uppercase for control
		type = (type == null ? "" : type.trim().toUpperCase());

		// Common fields
		String messageId = (String) entry.get("MESSAGE_ID");
		String data = (String) entry.get("MESSAGE_DATA");
		SemossDate dateCreated = (prerna.date.SemossDate) entry.get("DATE_CREATED");

		// Switch by type
		if ("INPUT".equals(type)) {
			InputMessage im = InputMessage.builder(room).withText(data).withType(MessageType.INPUT_TEXT).build();
			im.setDateCreated(dateCreated);
			im.setModelId(room.getModelId());
			return im;
		} else if ("RESPONSE".equals(type)) {
			ResponseMessage rm = ResponseMessage.builder().withText(data).withType(MessageType.RESPONSE_TEXT).build();
			rm.setTransactionId(messageId);
			rm.setDateCreated(dateCreated);
			rm.setModelId(room.getModelId());

			return rm;
		} else {
			// fallback; or you could throw an exception
			classLogger.error("Unknown message type: {}, row: {}", type, entry);
			return null;
		}
	}

	/**
	 * Returns a paged and sorted sub-list of messages.
	 *
	 * @param messages  The complete (unsorted) list of messages.
	 * @param sortOrder "ASC" or "DESC" (sort by dateCreated).
	 * @param offset    Number of records to skip (0-based).
	 * @param limit     Maximum records to return. If limit <= 0, returns all after
	 *                  offset.
	 * @return A new List containing the result slice in requested sort order.
	 */
	public static List<AbstractMessage> getPagedMessages(List<AbstractMessage> messages, String sortOrder, int offset,
			int limit) {
		if (messages == null || messages.isEmpty()) {
			return new ArrayList<>();
		}

		// Copy to avoid mutating the original list
		List<AbstractMessage> copy = new ArrayList<>(messages);

		Comparator<AbstractMessage> comp = Comparator.comparing(AbstractMessage::getDateCreated);

		if ("DESC".equalsIgnoreCase(sortOrder)) {
			comp = comp.reversed();
		}

		// Sort the copy
		copy.sort(comp);

		// Calculate safe offset/limit
		int startIdx = Math.max(0, offset);
		int endIdx = limit > 0 ? Math.min(copy.size(), startIdx + limit) : copy.size();

		// If offset is past end, return empty list
		if (startIdx >= copy.size()) {
			return new ArrayList<>();
		}

		// Return the requested sublist
		// new ArrayList to ensure it's not a view of the original list
		return new ArrayList<>(copy.subList(startIdx, endIdx));
	}

	/**
	 * Returns true if there are any non-hidden (not starting with .) files under
	 * the room's folder, recursively.
	 *
	 * @param room room whose folder should be scanned
	 * @return {@code true} when at least one visible file exists
	 */
	public static boolean hasFiles(Room room) {
		if (room == null) {
			return false;
		}
		String folderPath = room.getRoomFolderPath();
		if (folderPath == null) {
			return false;
		}
		File folder = new File(folderPath);
		return hasVisibleFilesRecursive(folder);
	}

	/**
	 * Recursively checks whether a directory tree contains any non-hidden file.
	 *
	 * @param folder folder to scan
	 * @return {@code true} if any visible file exists beneath {@code folder}
	 */
	private static boolean hasVisibleFilesRecursive(File folder) {
		if (folder == null || !folder.exists() || !folder.isDirectory()) {
			return false;
		}

		File[] files = folder.listFiles();
		if (files == null) {
			return false;
		}

		for (File f : files) {
			String name = f.getName();
			if (name.startsWith(".")) {
				continue; // skip hidden files/folders
			}
			if (f.isDirectory()) {
				if (hasVisibleFilesRecursive(f)) {
					return true;
				}
			} else if (f.isFile()) {
				return true; // found a non-hidden file!
			}
		}
		return false;
	}

	// ---- Image move utilities ----

	/**
	 * Persists any FILE-based {@link MediaMessagePart} in a message to the room
	 * folder.
	 * <p>
	 * This is used for model-generated media (e.g., Gemini inline images) that
	 * arrive as base64.
	 * <p>
	 * This will also persist to cloud storage.
	 *
	 * @param message message to inspect for file-based media parts
	 * @param room    room context used to resolve destination folder
	 */
	public static void persistMediaPartsToRoomFolder(AbstractMessage message, Room room) {
		if (message == null || room == null || room.getRoomFolderPath() == null) {
			return;
		}
		if (!message.hasMediaPart()) {
			return;
		}

		String roomFolder = room.getRoomFolderPath();
		try {
			Files.createDirectories(Paths.get(roomFolder));
		} catch (IOException e) {
			classLogger.warn("Unable to create room folder: " + roomFolder, e);
			return;
		}

		boolean pushToCloud = false;
		for (MessagePart part : message.getParts()) {
			if (!(part instanceof MediaMessagePart)) {
				continue;
			}
			MessageInputMedia media = ((MediaMessagePart) part).getMediaInfo();
			if (media == null || media.getMediaInputType() == null) {
				continue;
			}
			if (media.getMediaInputType() != MessageInputMedia.MEDIA_INPUT_TYPE.FILE) {
				continue;
			}

			String base64Data = media.getBase64Data();
			if (base64Data == null || base64Data.isEmpty()) {
				continue;
			}

			String fileName = media.getFileName();
			fileName = MessageInputMedia.extractFileName(fileName);
			if (fileName == null || fileName.trim().isEmpty()) {
				String ext = media.getFileFormat();
				if (ext == null || ext.trim().isEmpty()) {
					ext = "bin";
				}
				fileName = GUID.v7().toUUID().toString() + "." + ext;
			}

			Path target = Paths.get(roomFolder).resolve(fileName).normalize();
			if (!target.startsWith(Paths.get(roomFolder))) {
				classLogger.warn("Skipping unsafe media filename: " + fileName);
				continue;
			}

			if (!Files.exists(target)) {
				try {
					byte[] bytes = Base64.getDecoder().decode(base64Data);
					Files.write(target, bytes);
					// this is meant to be relative to the room
					media.setFileLocation(fileName);
					pushToCloud = true;
				} catch (Exception e) {
					classLogger.warn("Unable to persist media part to " + target, e);
					continue;
				}
			}

			media.setRoomFolder(roomFolder);
		}
		// only at end persist entire room if necessary
		if (pushToCloud) {
			ClusterUtil.pushRoomAsync(room.getId());
		}
	}

	/**
	 * Moves files from an insight folder into the room folder.
	 *
	 * @param relativePathToFiles paths relative to the insight folder
	 * @param room                destination room context
	 * @param insight             source insight context
	 * @return absolute destination paths for files that were moved
	 */
	public static List<String> moveFilesToRoomFolder(List<String> relativePathToFiles, Room room, Insight insight) {
		List<String> roomFilePaths = new ArrayList<>();
		if (relativePathToFiles == null || relativePathToFiles.isEmpty()) {
			classLogger.info("No file paths provided to move.");
			return roomFilePaths;
		}
		String insightFolder = insight.getInsightFolder(); // absolute path to insight folder
		String roomFolder = room.getRoomFolderPath(); // absolute path to room folder
		Path targetDir = Paths.get(roomFolder);
		try {
			Files.createDirectories(targetDir);
		} catch (IOException e) {
			classLogger.warn("Failed to create room folder: " + targetDir, e);
			return roomFilePaths;
		}
		for (String relPath : relativePathToFiles) {
			File srcFile = new File(insightFolder, relPath);
			if (!srcFile.exists() || !srcFile.isFile()) {
				classLogger.info("Source file does not exist in insight folder: " + srcFile.getAbsolutePath());
				continue;
			}
			String fileName = srcFile.getName();
			Path destination = targetDir.resolve(fileName);
			try {
				Files.move(srcFile.toPath(), destination, StandardCopyOption.REPLACE_EXISTING);
			} catch (IOException e) {
				classLogger.warn("Failed to move file: " + srcFile.getAbsolutePath() + " to " + destination, e);
				continue;
			}
			roomFilePaths.add(destination.toString());
		}
		return roomFilePaths;
	}

	/**
	 * Copies files (or supported base64 data URIs) into the room folder.
	 *
	 * @param relativePathToFiles source relative file paths or data URI strings
	 * @param room                destination room context
	 * @param insight             source insight context
	 * @return destination file names for successfully copied/decoded assets
	 */
	public static List<String> copyFilesToRoomFolder(List<String> relativePathToFiles, Room room, Insight insight) {
		List<String> copiedFileNames = new ArrayList<>();
		if (relativePathToFiles == null || relativePathToFiles.isEmpty()) {
			return copiedFileNames;
		}
		classLogger.info("Need to copy file paths from the insight to the room");
		String insightFolder = insight.getInsightFolder(); // absolute path to insight folder
		String roomFolder = room.getRoomFolderPath(); // absolute path to room folder
		Path targetDir = Paths.get(roomFolder);
		try {
			Files.createDirectories(targetDir);
		} catch (IOException e) {
			classLogger.warn("Failed to create room folder: " + targetDir, e);
			return copiedFileNames;
		}
		for (String relPath : relativePathToFiles) {
			if (isBase64MediaDataUri(relPath)) {
				String fileName = writeBase64ImageDataUriToDir(relPath, targetDir);
				if (fileName != null) {
					copiedFileNames.add(fileName);
				}
				continue;
			}
			File srcFile = new File(insightFolder, relPath);
			if (!srcFile.exists() || !srcFile.isFile()) {
				classLogger.info("Source file does not exist in insight folder: " + srcFile.getAbsolutePath());
				continue;
			}
			String fileName = srcFile.getName();
			Path destination = targetDir.resolve(fileName);
			try {
				Files.copy(srcFile.toPath(), destination, StandardCopyOption.REPLACE_EXISTING);
				copiedFileNames.add(fileName); // only add if copy succeeded
			} catch (IOException e) {
				classLogger.warn("Failed to copy file: " + srcFile.getAbsolutePath() + " to " + destination, e);
			}
		}
		return copiedFileNames;
	}

	/**
	 * Checks whether a value is a supported base64 media data URI.
	 *
	 * @param value input string
	 * @return {@code true} for supported image/pdf data URIs
	 */
	public static boolean isBase64MediaDataUri(String value) {
		if (value == null) {
			return false;
		}
		// e.g. data:image/jpeg;base64,/9j/4AAQ... or data:application/pdf;base64,....
		String trimmed = value.trim();
		if (!trimmed.contains(";base64,")) {
			return false;
		}
		return trimmed.startsWith("data:image/") || trimmed.startsWith("data:application/pdf");
	}

	/**
	 * Decodes a base64 image/pdf data URI and writes it to the target directory.
	 *
	 * @param dataUri   media data URI
	 * @param targetDir destination directory
	 * @return written file name, or {@code null} on validation/IO/decode failure
	 */
	public static String writeBase64ImageDataUriToDir(String dataUri, Path targetDir) {
		try {
			Files.createDirectories(targetDir);

			String trimmed = dataUri.trim();
			int commaIdx = trimmed.indexOf(',');
			if (commaIdx < 0) {
				classLogger.info("Invalid data URI (no comma separator)");
				return null;
			}

			String meta = trimmed.substring(0, commaIdx); // data:image/jpeg;base64
			String base64 = trimmed.substring(commaIdx + 1);
			if (!meta.startsWith("data:") || !meta.contains(";base64")) {
				classLogger.info("Invalid data URI meta: " + meta);
				return null;
			}

			int colonIdx = meta.indexOf(':');
			int semiIdx = meta.indexOf(';');
			if (colonIdx < 0 || semiIdx < 0 || semiIdx <= colonIdx + 1) {
				classLogger.info("Invalid data URI meta: " + meta);
				return null;
			}

			String mimeType = meta.substring(colonIdx + 1, semiIdx).trim().toLowerCase();
			if (!mimeType.startsWith("image/") && !"application/pdf".equals(mimeType)) {
				classLogger.info("Unsupported data URI mime type: " + mimeType);
				return null;
			}

			String ext = extensionFromMimeType(mimeType);
			byte[] decoded = Base64.getDecoder().decode(base64.replaceAll("\\s+", ""));

			// Content-addressed name -- identical bytes dedup to the same file.
			String fileName = "media_" + sha256Hex(decoded).substring(0, 16) + "." + ext;
			Path destination = targetDir.resolve(fileName);
			if (!Files.exists(destination)) {
				Files.write(destination, decoded);
			}
			return fileName;
		} catch (IllegalArgumentException e) {
			// base64 decoder throws IllegalArgumentException on bad input
			classLogger.warn("Failed to decode base64 data URI image", e);
			return null;
		} catch (IOException e) {
			classLogger.warn("Failed to write decoded base64 data URI image to room folder: " + targetDir, e);
			return null;
		}
	}

	private static String sha256Hex(byte[] data) {
		try {
			byte[] hash = MessageDigest.getInstance("SHA-256").digest(data);
			StringBuilder sb = new StringBuilder(hash.length * 2);
			for (byte b : hash) {
				sb.append(String.format("%02x", b));
			}
			return sb.toString();
		} catch (NoSuchAlgorithmException e) {
			// SHA-256 is required by the JRE spec; fall back to a UUID if it ever fails.
			classLogger.warn("SHA-256 unavailable; falling back to random media filename", e);
			return UUID.randomUUID().toString().replace("-", "");
		}
	}

	/**
	 * Resolves an output file extension from mime type.
	 *
	 * @param mimeType mime type string
	 * @return normalized file extension (without dot)
	 */
	private static String extensionFromMimeType(String mimeType) {
		if ("application/pdf".equals(mimeType)) {
			return "pdf";
		}
		if (mimeType == null || !mimeType.startsWith("image/")) {
			return "png";
		}
		switch (mimeType) {
		case "image/jpg":
		case "image/jpeg":
			return "jpeg";
		case "image/png":
			return "png";
		case "image/gif":
			return "gif";
		case "image/webp":
			return "webp";
		case "image/bmp":
			return "bmp";
		case "image/svg+xml":
			return "svg";
		case "image/x-icon":
		case "image/vnd.microsoft.icon":
			return "ico";
		default:
			String subtype = mimeType.substring("image/".length());
			int plusIdx = subtype.indexOf('+');
			if (plusIdx > 0) {
				subtype = subtype.substring(0, plusIdx);
			}
			subtype = subtype.replaceAll("[^a-z0-9]", "");
			return subtype.isEmpty() ? "png" : subtype;
		}
	}

	/**
	 * Utility class constructor.
	 */
	private RoomUtils() {

	}

}
