/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.model;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
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
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.util.Utility;

/**
 * Utility methods for fetching and managing Room objects. -
 * createRoomIfNotExists: creates (if needed) and returns a Room -
 * getOrLoadRoom: looks up or loads room to memory hash, but never creates a
 * Room
 */
public final class RoomUtils {

	private static final Logger logger = LogManager.getLogger(RoomUtils.class);

	/**
	 * Overload create room
	 *
	 * @param roomId
	 * @param insight
	 * @param modelEngine
	 * @param question
	 * @return the existing or newly created Room
	 */
	public static Room createRoomIfNotExists(String roomId, Insight insight, IModelEngine modelEngine,
			String question) {
		return createRoomIfNotExists(roomId, insight, modelEngine, question, null, null, null);
	}

	/**
	 * Ensures a Room exists: creates it if necessary, then loads it for the given
	 * user/insight.
	 *
	 * @param roomId
	 * @param insight
	 * @param modelEngine
	 * @param question
	 * @param workspaceId
	 * @param options
	 * @param context
	 * @return the existing or newly created Room
	 */
	public static Room createRoomIfNotExists(String roomId, Insight insight, IModelEngine modelEngine, String question,
			String workspaceId, Map<String, Object> options, String context) {
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
			String projectId = insight.getContextProjectId();
			if (projectId == null) {
				projectId = insight.getProjectId();
			}
			String projectName = null;
			if (projectId != null) {
				IProject project = Utility.getProject(projectId);
				projectName = project != null ? project.getProjectName() : null;
			}
			String roomName = (question != null) ? question.substring(0, Math.min(question.length(), 100)) : null;
			ModelInferenceLogsUtils.doCreateNewConversation(insight.getInsightId(), roomId, roomName, context,
					userToken.getId(), userName, userEmail, agentType, engineId, true, projectId, projectName,
					workspaceId, options);
			// Always get the loaded room object (avoiding any skipping, ensures in-memory
			// cache is
			// filled)
			return RoomUtils.getOrLoadRoom(roomId, insight);
		} else {
			return RoomUtils.getOrLoadRoom(roomId, insight);
		}
	}

	/**
	 * Loads a Room from user room hash or database if present.
	 *
	 * @throws IllegalArgumentException
	 *             if Room does not exist.
	 */
	public static Room getOrLoadRoom(String roomId, Insight insight) {
		Room room;
		// Check in user's cache (roomHash)
		if (insight.getUser().roomHash.containsKey(roomId)) {
			try {
				room = (Room) insight.getUser().roomHash.get(roomId);
				// is the message json null? if so then this is probably a legacy room
				if (room.getMessageJson() == null || room.getMessageJson().trim().isEmpty()) {
					RoomUtils.updateRoom(room, insight);
				}
				return room;
			} catch (ClassCastException e) {
				insight.getUser().roomHash.remove(roomId); // Clear corrupted cache entry
			}
		}
		// else it may be in the DB
		boolean roomExistsInDB = ModelInferenceLogsUtils.doCheckRoomExists(roomId);
		if (!roomExistsInDB)
			throw new IllegalArgumentException("User room is not valid");
		room = ModelInferenceLogsUtils.getRoomById(roomId, insight.getUser().getPrimaryLoginToken().getId());

		// is the message json null? if so then this is probably a legacy room
		if (room.getMessageJson() == null || room.getMessageJson().trim().isEmpty()) {
			RoomUtils.updateRoom(room, insight);
		}
		room.setInsight(insight);
		room.parseMessages();
		insight.getUser().roomHash.put(roomId, room);
		return room;
	}

	private static void updateRoom(Room room, Insight insight) {
		List<Map<String, Object>> output = ModelInferenceLogsUtils
				.doRetrieveConversation(insight.getUser().getPrimaryLoginToken().getId(), room.getId(), "ASC", -1, -1);

		// for each message, build an AbstractMessage
		List<AbstractMessage> messages = new ArrayList<>();
		for (Map<String, Object> entry : output) {
			AbstractMessage msg = convertLegacyMessage(room, entry);
			if (msg != null)
				messages.add(msg);
		}

		// set the messages in the room from string
		room.setMessagesJson(MessageUtils.toJsonArray(messages));
		room.setMessages(messages);

		// write the message json to db
		ModelInferenceLogsUtils.llm2_updateRoomMessages(room.getId(), insight.getUser().getPrimaryLoginToken().getId(),
				MessageUtils.toJsonArray(messages));
	}

	/** Gets the room options map */
	public static Map<String, Object> getRoomOptions(String roomId, String userId) {

		String roomOptionsString = ModelInferenceLogsUtils.getRoomOptions(roomId, userId);

		Gson gson = new Gson();
		Type type = new TypeToken<Map<String, Object>>() {
		}.getType();
		Map<String, Object> map = new HashMap<>();
		try {
			map = gson.fromJson(roomOptionsString, type);
		} catch (Exception e) {
			e.printStackTrace();
		}

		String logMessage = String.format("Found %s in room options", map.keySet());
		logger.info(logMessage);

		return map;
	}

	/**
	 * Helper method: converts a single row map to an InputMessage or
	 * ResponseMessage
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
			InputMessage im = InputMessage.builder(room).withInputUIPrompt(data).withInputPrompt(data)
					.withType(MessageType.INPUT_TEXT).build();
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
			System.err.println("Unknown message type: " + type + ", row: " + entry);
			return null;
		}
	}

	/**
	 * Returns a paged and sorted sub-list of messages.
	 *
	 * @param messages
	 *            The complete (unsorted) list of messages.
	 * @param sortOrder
	 *            "ASC" or "DESC" (sort by dateCreated).
	 * @param offset
	 *            Number of records to skip (0-based).
	 * @param limit
	 *            Maximum records to return. If limit <= 0, returns all after
	 *            offset.
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

	/*
	 * Private constructor
	 */
	private RoomUtils() {
	}
}
