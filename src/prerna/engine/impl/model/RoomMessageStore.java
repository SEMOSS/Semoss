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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ToolCallMessagePart;
import prerna.engine.impl.model.message.ToolResultMessagePart;
import prerna.engine.impl.model.message.ToolResultPart;
import prerna.redis.RedisConnectionConfig;
import prerna.redis.RedisConnectionFactory;
import prerna.util.Utility;

/**
 * Internal boundary for room-message projection reads/writes.
 * <p>
 * The durable source of truth remains {@code modellogs.ROOM.MESSAGES}. Redis is
 * optional hot coordination/cache and is disabled by default.
 */
public final class RoomMessageStore {

	private static final Logger classLogger = LogManager.getLogger(RoomMessageStore.class);

	private static final String LOCK_TTL_MS = "ROOM_MESSAGE_STORE_LOCK_TTL_MS";
	private static final String LOCK_WAIT_MS = "ROOM_MESSAGE_STORE_LOCK_WAIT_MS";

	private static final ThreadLocal<Map<String, HeldLock>> HELD_LOCKS = ThreadLocal.withInitial(HashMap::new);
	private static volatile RoomMessageRedisClient cachedRedisClient;
	private static volatile String cachedRedisClientKey;

	private RoomMessageStore() {
	}

	public static List<AbstractMessage> loadFromPersistedJson(Room room, String messagesJson) {
		String projection = messagesJson;
		if (projection == null || projection.trim().isEmpty()) {
			return new ArrayList<>();
		}
		List<AbstractMessage> loaded = MessageUtils.fromJsonArrayPreservingToolState(projection, room);
		List<AbstractMessage> messages = loaded != null ? loaded : new ArrayList<>();
		validateForPersistence(room, messages);
		warmRedisProjection(room, projection);
		return messages;
	}

	public static void refreshFromStore(Room room, String userId) {
		if (room == null || room.getId() == null || userId == null || !isRedisEnabled()) {
			return;
		}
		Room persistedRoom = ModelInferenceLogsUtils.getRoomById(room.getId(), userId);
		if (persistedRoom == null) {
			return;
		}
		String persistedJson = persistedRoom.getMessageJson();
		List<AbstractMessage> messages = loadFromPersistedJson(room, persistedJson);
		room.setMessages(messages);
		room.setMessagesJson(persistedJson);
		if (room.getRoomName() == null || room.getRoomName().trim().isEmpty()) {
			room.setRoomName(persistedRoom.getRoomName());
		}
		if (room.getModelId() == null || room.getModelId().trim().isEmpty()) {
			room.setModelId(persistedRoom.getModelId());
		}
	}

	public static void refreshFromLatestProjection(Room room, String userId) {
		if (room == null || room.getId() == null || userId == null || !isRedisEnabled()) {
			return;
		}
		if (!refreshFromHotProjection(room)) {
			refreshFromStore(room, userId);
		}
	}

	public static boolean refreshFromHotProjection(Room room) {
		if (room == null || room.getId() == null || !isRedisEnabled()) {
			return false;
		}
		try {
			String projection = redisClient().getMessages(room.getId());
			if (projection == null || projection.trim().isEmpty()) {
				return false;
			}
			List<AbstractMessage> loaded = MessageUtils.fromJsonArrayPreservingToolState(projection, room);
			List<AbstractMessage> messages = loaded != null ? loaded : new ArrayList<>();
			validateForPersistence(room, messages);
			room.setMessages(messages);
			room.setMessagesJson(projection);
			return true;
		} catch (Exception e) {
			classLogger.warn("Failed to refresh Redis room-message projection for room={}", room.getId(), e);
			return false;
		}
	}

	public static String messageHistoryWithNewMessage(Room room, AbstractMessage newMessage) {
		List<AbstractMessage> branch = MessageUtils.getMessageBranchWithNewMessage(room.getMessages(), newMessage);
		validateProviderPayload(room, branch);
		return MessageUtils.toJsonArrayWithImageData(branch);
	}

	public static String currentMessageHistory(Room room) {
		List<AbstractMessage> branch = MessageUtils.getMessageBranchWithNewMessage(room.getMessages(), null);
		validateProviderPayload(room, branch);
		return MessageUtils.toJsonArrayWithImageData(branch);
	}

	public static String providerMessageHistory(Room room, List<AbstractMessage> messages) {
		validateProviderPayload(room, messages);
		return MessageUtils.toJsonArrayWithImageData(messages);
	}

	public static void normalizeForProviderPayload(Room room) {
		List<AbstractMessage> messages = room.getMessages();
		List<AbstractMessage> sanitized = MessageUtils.sanitizeOrphanToolCalls(messages, room);
		if (sanitized != messages) {
			room.setMessages(sanitized);
		}
		validateForPersistence(room, room.getMessages());
	}

	public static boolean persist(Room room, String userId) {
		try (RoomMutationLock ignored = acquireMutationLock(room)) {
			String messageHistory = room.getMessagesAsString();
			validateSerializedProjection(room, messageHistory);
			boolean updated = ModelInferenceLogsUtils.llm2_updateRoomMessages(room.getId(), userId, messageHistory);
			updateRedisProjection(room, messageHistory);
			return updated;
		}
	}

	public static boolean persist(Room room, String userId, String roomName, String engineId) {
		try (RoomMutationLock ignored = acquireMutationLock(room)) {
			String messageHistory = room.getMessagesAsString();
			validateSerializedProjection(room, messageHistory);
			boolean updated = ModelInferenceLogsUtils.llm2_updateRoomMessages(room.getId(), userId, messageHistory,
					roomName, engineId);
			updateRedisProjection(room, messageHistory);
			return updated;
		}
	}

	public static RoomMutationLock acquireMutationLock(Room room) {
		if (room == null) {
			return RoomMutationLock.NO_OP;
		}
		return acquireMutationLock(room.getId());
	}

	public static RoomMutationLock acquireMutationLock(String roomId) {
		if (roomId == null || roomId.trim().isEmpty() || !isRedisEnabled()) {
			return RoomMutationLock.NO_OP;
		}
		roomId = roomId.trim();
		Map<String, HeldLock> heldLocks = HELD_LOCKS.get();
		HeldLock held = heldLocks.get(roomId);
		if (held != null) {
			held.count++;
			return new RoomMutationLock(roomId, held, false);
		}

		RoomMessageRedisClient redis = redisClient();
		String token = UUID.randomUUID().toString();
		long ttlMs = getLongProperty(LOCK_TTL_MS, 300000L);
		long waitMs = getLongProperty(LOCK_WAIT_MS, 5000L);
		long deadline = System.currentTimeMillis() + Math.max(0L, waitMs);
		do {
			if (redis.tryAcquireLock(roomId, token, ttlMs)) {
				HeldLock newLock = new HeldLock(redis, roomId, token, ttlMs);
				heldLocks.put(roomId, newLock);
				newLock.startRenewal();
				return new RoomMutationLock(roomId, newLock, true);
			}
			sleepQuietly(100L);
		} while (System.currentTimeMillis() < deadline);

		throw new IllegalStateException("Room is busy; another request is updating room messages. Please retry.");
	}

	private static void validateSerializedProjection(Room room, String messageHistory) {
		List<AbstractMessage> parsed = parseWithoutSanitizing(room, messageHistory);
		validateForPersistence(room, parsed);
	}

	private static void validateForPersistence(Room room, List<AbstractMessage> messages) {
		if (messages == null || messages.isEmpty()) {
			return;
		}
		Set<String> messageIds = new HashSet<>();
		for (AbstractMessage message : messages) {
			if (message == null) {
				throw new IllegalStateException("Room message list contains a null message.");
			}
			String messageId = trimToNull(message.getMessageId());
			if (messageId == null) {
				throw new IllegalStateException("Room message list contains a message without a messageId.");
			}
			if (!messageIds.add(messageId)) {
				throw new IllegalStateException("Room message list contains duplicate messageId: " + messageId);
			}
		}
		for (AbstractMessage message : messages) {
			String parentMessageId = trimToNull(message.getParentMessageId());
			if (parentMessageId == null) {
				continue;
			}
			if (parentMessageId.equals(message.getMessageId())) {
				throw new IllegalStateException("Room message cannot be its own parent: " + parentMessageId);
			}
			if (!messageIds.contains(parentMessageId)) {
				String roomId = room != null ? room.getId() : "<unknown>";
				throw new IllegalStateException(
						"Room " + roomId + " message parent does not exist: " + parentMessageId);
			}
		}
	}

	private static void validateProviderPayload(Room room, List<AbstractMessage> messages) {
		validateForPersistence(room, messages);

		Set<String> toolCallIds = new HashSet<>();
		Set<String> toolResultIds = new HashSet<>();
		for (AbstractMessage message : messages) {
			for (MessagePart part : message.getParts()) {
				if (part instanceof ToolCallMessagePart) {
					String toolCallId = toolCallId((ToolCallMessagePart) part);
					if (toolCallId != null) {
						toolCallIds.add(toolCallId);
					}
				} else if (part instanceof ToolResultMessagePart) {
					String toolResultId = toolResultId((ToolResultMessagePart) part);
					if (toolResultId != null) {
						toolResultIds.add(toolResultId);
					}
				}
			}
		}

		if (!toolCallIds.containsAll(toolResultIds)) {
			Set<String> unmatched = new HashSet<>(toolResultIds);
			unmatched.removeAll(toolCallIds);
			throw new IllegalStateException(
					"Room message payload contains tool results without tool calls: " + unmatched);
		}
		if (!toolResultIds.containsAll(toolCallIds)) {
			Set<String> unmatched = new HashSet<>(toolCallIds);
			unmatched.removeAll(toolResultIds);
			throw new IllegalStateException("Room message payload contains unresolved tool calls: " + unmatched);
		}
	}

	private static List<AbstractMessage> parseWithoutSanitizing(Room room, String jsonArrayString) {
		if (jsonArrayString == null || jsonArrayString.trim().isEmpty()) {
			return new ArrayList<>();
		}
		JsonArray array = JsonParser.parseString(jsonArrayString).getAsJsonArray();
		List<AbstractMessage> result = new ArrayList<>();
		for (JsonElement element : array) {
			AbstractMessage message = MessageUtils.fromJson(element.toString(), room);
			if (message != null) {
				result.add(message);
			}
		}
		return result;
	}

	private static String toolCallId(ToolCallMessagePart part) {
		Map<String, Object> toolCall = part.getToolCall();
		if (toolCall == null) {
			return null;
		}
		return trimToNull(String.valueOf(toolCall.get("id")));
	}

	private static String toolResultId(ToolResultMessagePart part) {
		ToolResultPart result = part.getToolResult();
		return result == null ? null : trimToNull(result.getToolCallId());
	}

	private static String trimToNull(String value) {
		if (value == null) {
			return null;
		}
		String trimmed = value.trim();
		return trimmed.isEmpty() ? null : trimmed;
	}

	private static void warmRedisProjection(Room room, String messageHistory) {
		if (room == null || room.getId() == null || !isRedisEnabled()) {
			return;
		}
		try {
			RoomMessageRedisClient redis = redisClient();
			if (redis.getMessages(room.getId()) == null) {
				redis.setMessages(room.getId(), messageHistory);
				redis.incrementVersion(room.getId());
			}
		} catch (Exception e) {
			classLogger.warn("Failed to warm Redis room-message projection for room={}", room.getId(), e);
		}
	}

	private static void updateRedisProjection(Room room, String messageHistory) {
		if (room == null || room.getId() == null || !isRedisEnabled()) {
			return;
		}
		try {
			RoomMessageRedisClient redis = redisClient();
			redis.setMessages(room.getId(), messageHistory);
			redis.incrementVersion(room.getId());
		} catch (Exception e) {
			classLogger.warn("Failed to update Redis room-message projection for room={}", room.getId(), e);
			invalidateRedisProjection(room);
		}
	}

	private static void invalidateRedisProjection(Room room) {
		if (room == null || room.getId() == null || !isRedisEnabled()) {
			return;
		}
		try {
			redisClient().deleteMessages(room.getId());
		} catch (Exception e) {
			classLogger.warn("Failed to invalidate Redis room-message projection for room={}", room.getId(), e);
		}
	}

	public static boolean isRedisEnabled() {
		return Boolean.parseBoolean(String.valueOf(Utility.getDIHelperProperty(RedisConnectionConfig.REDIS_ENABLED)));
	}

	public static RoomMessageRedisClient redisClient() {
		RedisConnectionConfig config = RedisConnectionConfig.requireFromDIHelper();
		String cacheKey = config.cacheKey();
		RoomMessageRedisClient client = cachedRedisClient;
		if (client != null && cacheKey.equals(cachedRedisClientKey)) {
			return client;
		}
		synchronized (RoomMessageStore.class) {
			client = cachedRedisClient;
			if (client != null && cacheKey.equals(cachedRedisClientKey)) {
				return client;
			}
			RoomMessageRedisClient next = new RoomMessageRedisClient(RedisConnectionFactory.getClient(config));
			cachedRedisClient = next;
			cachedRedisClientKey = cacheKey;
			return next;
		}
	}

	private static long getLongProperty(String key, long defaultValue) {
		String value = Utility.getDIHelperProperty(key);
		if (value == null || value.trim().isEmpty()) {
			return defaultValue;
		}
		try {
			return Long.parseLong(value.trim());
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	private static void sleepQuietly(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public static final class RoomMutationLock implements AutoCloseable {
		private static final RoomMutationLock NO_OP = new RoomMutationLock(null, null, false);

		private final String roomId;
		private final HeldLock heldLock;
		private final boolean owner;
		private boolean closed;

		private RoomMutationLock(String roomId, HeldLock heldLock, boolean owner) {
			this.roomId = roomId;
			this.heldLock = heldLock;
			this.owner = owner;
		}

		@Override
		public void close() {
			if (closed || heldLock == null || roomId == null) {
				return;
			}
			closed = true;
			Map<String, HeldLock> heldLocks = HELD_LOCKS.get();
			heldLock.count--;
			if (heldLock.count > 0) {
				return;
			}
			heldLocks.remove(roomId);
			if (heldLocks.isEmpty()) {
				HELD_LOCKS.remove();
			}
			if (owner) {
				heldLock.close();
			}
		}
	}

	private static final class HeldLock implements Closeable {
		private final RoomMessageRedisClient redis;
		private final String roomId;
		private final String token;
		private final long ttlMs;
		private volatile boolean closed;
		private int count = 1;
		private Thread renewalThread;

		private HeldLock(RoomMessageRedisClient redis, String roomId, String token, long ttlMs) {
			this.redis = redis;
			this.roomId = roomId;
			this.token = token;
			this.ttlMs = ttlMs;
		}

		private void startRenewal() {
			if (ttlMs <= 0L) {
				return;
			}
			renewalThread = new Thread(() -> {
				long sleepMs = Math.max(1000L, ttlMs / 3L);
				while (!closed) {
					sleepQuietly(sleepMs);
					if (!closed) {
						redis.renewLock(roomId, token, ttlMs);
					}
				}
			}, "room-message-lock-renewal");
			renewalThread.setDaemon(true);
			renewalThread.start();
		}

		@Override
		public void close() {
			closed = true;
			redis.releaseLock(roomId, token);
		}
	}
}
