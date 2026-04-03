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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageSchemaUpgrader;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.playground.PlaygroundUtils;
import prerna.project.api.IProject;
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
		return createRoomIfNotExists(roomId, insight, modelEngine, question, null, null, null, null);
	}

	/**
	 * Ensures a Room exists: creates it if necessary, then loads it for the given
	 * user/insight.
	 *
	 * @param roomId      requested room id; when null/blank the insight id is used
	 * @param insight     active insight context
	 * @param modelEngine model engine associated with the room (optional)
	 * @param question    initial user question used for default room naming
	 * @param workspaceId optional workspace id to associate with the room
	 * @param options     optional room options payload
	 * @param context     optional room context/system prompt
	 * @param projectId   optional project id override
	 * @return the existing or newly created Room
	 */
	public static Room createRoomIfNotExists(String roomId, Insight insight, IModelEngine modelEngine, String question,
			String workspaceId, Map<String, Object> options, String context, String projectId) {
		// Use the passed roomId or fallback to the insightId if null/empty
		if (roomId == null || roomId.trim().isEmpty()) {
			roomId = insight.getInsightId();
		}

		boolean roomExistsInDB = ModelInferenceLogsUtils.doCheckRoomExists(roomId);
		if (!roomExistsInDB) {
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
                    options
            );
    		// @formatter:on

			// Always get the loaded room object (avoiding any skipping, ensures in-memory
			// cache is filled)
			return RoomUtils.getOrLoadRoom(roomId, insight);
		} else {
			return RoomUtils.getOrLoadRoom(roomId, insight);
		}
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
				room = (Room) insight.getUser().getRoomHash().get(roomId);
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
		ModelInferenceLogsUtils.llm2_updateRoomMessages(room.getId(), insight.getUser().getPrimaryLoginToken().getId(),
				upgraded);
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
		String messageJson = room.getMessagesAsString();
		ModelInferenceLogsUtils.llm2_updateRoomMessages(room.getId(), insight.getUser().getPrimaryLoginToken().getId(),
				messageJson);
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

	/**
	 * Utility class constructor.
	 */
	private RoomUtils() {

	}

}
