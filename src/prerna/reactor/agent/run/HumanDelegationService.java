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
package prerna.reactor.agent.run;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.collaboration.CollaborationUtils;
import prerna.engine.api.ToolExecutionResult;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.notifications.NotificationDbUtils;
import prerna.notifications.NotificationService;
import prerna.om.Insight;
import prerna.reactor.agent.AgentRunContext;
import prerna.util.NotificationConstants;
import prerna.util.Utility;

/**
 * Delegates a question from an agent run to a person. The owner keeps a HUMAN
 * child run; the assignee gets one AGENT_RUN_ACTION and a room of their own. An
 * explicit response completes the child through the normal child-completion
 * path.
 */
public final class HumanDelegationService {

	private static final Logger logger = LogManager.getLogger(HumanDelegationService.class);

	private static final Gson GSON = new Gson();

	public static final String TOOL_NAME = "DelegateToPerson";
	public static final String SUBMIT_TOOL_NAME = "SubmitDelegationResponse";
	public static final String FIND_PERSON_TOOL_NAME = "FindPerson";
	private static final int MAX_PEOPLE_RESULTS = 20;
	private static final int FIND_PERSON_LIMIT = 10;
	// Rows scanned per search before the every-word filter trims them.
	private static final int PEOPLE_SCAN_LIMIT = 200;
	// Tool name on the assignee's action row; distinct from the owner's
	// DelegateToPerson approval.
	private static final String ASSIGNEE_ACTION = "DelegationRequest";
	public static final String ROOM_OPTION_ACTION_ID = CollaborationUtils.ROOM_OPTION_DELEGATION_ACTION_ID;
	private static final String STATUS_PENDING = "PENDING";
	private static final String STATUS_CANCELLED = "CANCELLED";
	private static final String STATUS_RESPONDED = "RESPONDED";
	private static final String STATUS_DECLINED = "DECLINED";
	// Child ERROR_MESSAGE prefix; the completion message keys off it.
	static final String DECLINED_PREFIX = "Declined";
	private static final int MAX_QUESTION_LENGTH = 8_000;
	private static final int MAX_CONTEXT_LENGTH = 32_000;
	private static final int MAX_RESPONSE_LENGTH = 32_000;
	private static final int LIST_LIMIT = 200;
	// Returned files land in the owner's room folder under
	// delegations/<childRunId>/.
	public static final String FILES_FOLDER = "delegations";
	// Files sent with a request land in the assignee's room folder here.
	private static final String REQUEST_FILES_FOLDER = "from-requester";
	public static final String REQUEST_ORNAMENT = "delegationRequest";
	private static final int MAX_LINKS = 20;
	private static final String MAX_FILES_PROPERTY = "COLLAB_DELEGATION_MAX_FILES";
	private static final String MAX_FILE_MB_PROPERTY = "COLLAB_DELEGATION_MAX_FILE_MB";
	private static final String MAX_TOTAL_MB_PROPERTY = "COLLAB_DELEGATION_MAX_TOTAL_MB";

	private HumanDelegationService() {
	}

	public static boolean isDelegationAction(Map<String, Object> action) {
		return action != null && ASSIGNEE_ACTION.equals(action.get("toolName"));
	}

	/**
	 * Create the assignee's room and action plus the owner's child run; returns
	 * immediately.
	 */
	public static Map<String, Object> delegate(Map<String, Object> args, String parentRunId, Insight ownerInsight) {
		AgentRunRecord parent = parentRunId == null ? null : AgentRunStore.getRun(parentRunId, ownerInsight);
		if (parent == null) {
			throw new IllegalArgumentException(TOOL_NAME + " must be called from an agent run");
		}
		if (!CollaborationUtils
				.isCollaborationRoom(ModelInferenceLogsUtils.getRoomById(parent.roomId(), ownerInsight.getUserId()))) {
			throw new IllegalStateException(TOOL_NAME + " is only available in collaboration rooms");
		}
		String question = bounded(args, "question", MAX_QUESTION_LENGTH, true);
		String context = bounded(args, "context", MAX_CONTEXT_LENGTH, false);
		String responseFormat = bounded(args, "responseFormat", MAX_QUESTION_LENGTH, false);
		String dueAt = dueDate(bounded(args, "dueAt", 100, false));
		SubAgentRunCompletionMode mode = completionMode(args);
		Person assignee = resolveAssignee(args == null ? null : args.get("assignee"));
		Person requester = Person.of(ownerInsight.getUser().getPrimaryLoginToken());
		List<Map<String, String>> links = links(args == null ? null : args.get("links"));
		List<String> files = stringList(args == null ? null : args.get("files"));
		List<Path> sources = List.of();
		if (!files.isEmpty()) {
			if (parent.roomId() == null) {
				throw new IllegalStateException("Unable to find the room to attach files from");
			}
			// The approval may land on another node than the run; pull adds, never deletes.
			ClusterUtil.pullRoom(parent.roomId());
			sources = validateFiles(Paths.get(Room.roomFolderPath(parent.roomId())), files);
		}

		String childRunId = GUID.v7().toUUID().toString();
		String actionId = GUID.v7().toUUID().toString();
		String assigneeRoomId = deterministicId("semoss:delegation-room:" + actionId);
		Map<String, Object> packet = new LinkedHashMap<>();
		packet.put("question", question);
		packet.put("context", context);
		packet.put("responseFormat", responseFormat);
		packet.put("dueAt", dueAt);
		if (!sources.isEmpty()) {
			packet.put("files", copyFiles(sources,
					Paths.get(Room.roomFolderPath(assigneeRoomId)).resolve(REQUEST_FILES_FOLDER), assigneeRoomId));
		}
		if (!links.isEmpty()) {
			packet.put("links", links);
		}

		// Room, then action, then child run: a failure never leaves a child for the
		// owner's room to report.
		createAssigneeRoom(assignee, assigneeRoomId, actionId, requester, packet);
		// Both people by exact principal; label fields are display only.
		Map<String, Object> meta = new HashMap<>();
		meta.put("requester", requester.toMap());
		meta.put("assignee", assignee.toMap());
		Map<String, Object> action = new HashMap<>();
		action.put("actionId", actionId);
		action.put("toolName", ASSIGNEE_ACTION);
		action.put("toolArgs", packet);
		action.put("toolMeta", meta);
		AgentRunActionStore.insertPendingActions(childRunId, assigneeRoomId, assignee.userId(), List.of(action));

		AgentRunRequest childRequest = new AgentRunRequest(null, question, null, null, null,
				AgentRunContext.DEFAULT_MAX_TURNS, AgentRunContext.DEFAULT_MAX_REFLECTIONS, null, null, ownerInsight)
				.withParentRunId(parent.runId()).withCompletionMode(mode).withHumanExecutor(assignee.label());
		try {
			AgentRunStore.insertInputRequired(childRunId, childRequest, parent.userId());
		} catch (RuntimeException e) {
			AgentRunActionStore.cancelPendingForRun(childRunId);
			throw e;
		}
		logger.info("Delegated childRunId={} parentRunId={} actionId={}", childRunId, parent.runId(), actionId);
		notifyAssignee(actionId, assigneeRoomId, requester, assignee);

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("runId", childRunId);
		out.put("status", AgentRunStatus.INPUT_REQUIRED.name());
		out.put("assignee", assignee.label());
		out.put("assigneePerson", assignee.toMap());
		out.put("completionMode", mode.name());
		out.put("message", "Sent a new request to " + assignee.label() + ". They work on it in their own room; "
				+ "their response will be posted in this room when they send it. Do not wait or poll for it.");
		return out;
	}

	/** Delegations assigned to the caller, newest first; status is optional. */
	public static List<Map<String, Object>> listAssigned(Insight insight, String status) {
		User user = requireUser(insight);
		String statusFilter = status == null || status.isBlank() ? null : status.trim().toUpperCase(Locale.ROOT);
		List<Map<String, Object>> out = new ArrayList<>();
		for (AuthProvider provider : user.getLogins()) {
			AccessToken token = user.getAccessToken(provider);
			if (token == null || token.getId() == null) {
				continue;
			}
			for (Map<String, Object> action : AgentRunActionStore.getAssignedActions(token.getId(), ASSIGNEE_ACTION,
					statusFilter, LIST_LIMIT)) {
				if (assignedTo(action, provider)) {
					out.add(view(action));
				}
			}
		}
		return out;
	}

	/**
	 * Decline from "Assigned to you" without opening the room; the reason goes back
	 * to the requester.
	 */
	public static Map<String, Object> decline(Insight insight, String actionId, String reason) {
		return respond(insight, actionId, null, true, reason, null);
	}

	/**
	 * Record the assignee's one answer or decline, then complete the owner's child
	 * run.
	 */
	private static Map<String, Object> respond(Insight insight, String actionId, String response, boolean decline,
			String reason, List<String> files) {
		Map<String, Object> action = findAssigned(requireUser(insight), actionId);
		actionId = (String) action.get("actionId");
		String runId = (String) action.get("runId");
		String userId = (String) action.get("userId");
		if (STATUS_CANCELLED.equals(action.get("status"))) {
			dismissAssigneeNotification(action);
			throw new IllegalStateException(
					requesterLabel(parseJson(action.get("toolMeta"))) + " withdrew this request, so nothing was sent.");
		}
		if (STATUS_PENDING.equals(action.get("status"))) {
			String result = decline ? trimToNull(reason) : trimToNull(response);
			if (!decline && result == null) {
				throw new IllegalArgumentException("response is required");
			}
			if (result != null && result.length() > MAX_RESPONSE_LENGTH) {
				throw new IllegalArgumentException("response exceeds " + MAX_RESPONSE_LENGTH + " characters");
			}
			if (!decline && files != null && !files.isEmpty()) {
				// Files reach cloud storage before the answer counts, so delivery always finds
				// them.
				copyFilesToOwnerRoom((String) action.get("roomId"), runId, files);
			}
			// Only one submission can move the action off PENDING.
			AgentRunActionStore.markDecided(actionId, runId, userId, null, result,
					decline ? STATUS_DECLINED : STATUS_RESPONDED, null);
			action = AgentRunActionStore.getActionById(actionId, userId);
		}
		// Re-applied on duplicates, so a crash between the two writes heals on retry.
		String status = (String) action.get("status");
		String result = (String) action.get("result");
		if (STATUS_RESPONDED.equals(status)) {
			AgentRunStore.markCompletedIfInputRequired(runId, result);
		} else if (STATUS_DECLINED.equals(status)) {
			AgentRunStore.markFailedIfInputRequired(runId,
					result == null ? DECLINED_PREFIX : DECLINED_PREFIX + ": " + result);
		}
		AgentRunService.get().queueChildCompletion(runId);
		dismissAssigneeNotification(action);
		return view(action);
	}

	/**
	 * Inbox notice for the assignee. The text is fixed and names only the
	 * requester; the request itself stays in the assignee's room.
	 */
	private static void notifyAssignee(String actionId, String roomId, Person requester, Person assignee) {
		notifyPerson(NotificationConstants.Type.DELEGATION_REQUEST, notificationId(actionId), actionId, roomId,
				requester, assignee, requester.shortName() + " sent you a request",
				"Open the request to review it and respond.");
	}

	/**
	 * Tells the other person how a delegation ended, once the requester's room has
	 * the result: the requester hears about an answer or a decline, the assignee
	 * about a withdrawal. The assignee's request notice is cleared either way.
	 * Delivery retries call this again; the notification ids keep it to one each.
	 *
	 * @param outcome RESPONDED, DECLINED, CANCELLED, or UNANSWERED, as posted to
	 *                the requester's room
	 */
	static void notifySettled(String childRunId, String requesterRoomId, String outcome) {
		if (!NotificationDbUtils.isInitalized()) {
			return;
		}
		try {
			Map<String, Object> action = AgentRunActionStore.getActionsForRun(childRunId).stream()
					.filter(HumanDelegationService::isDelegationAction).findFirst().orElse(null);
			if (action == null) {
				return;
			}
			dismissAssigneeNotification(action);
			Map<String, Object> meta = parseJson(action.get("toolMeta"));
			// Rows from before the Person meta cannot be addressed.
			if (!(meta.get("requester") instanceof Map<?, ?> from) || !(meta.get("assignee") instanceof Map<?, ?> to)) {
				return;
			}
			Person requester = Person.fromMap(from);
			Person assignee = Person.fromMap(to);
			String actionId = (String) action.get("actionId");
			switch (outcome) {
			case STATUS_RESPONDED -> notifyPerson(NotificationConstants.Type.DELEGATION_RESPONSE,
					outcomeNotificationId(actionId, outcome), actionId, requesterRoomId, requester, assignee,
					assignee.shortName() + " responded to your request",
					"Open the conversation to read their response.");
			case STATUS_DECLINED -> notifyPerson(NotificationConstants.Type.DELEGATION_DECLINED,
					outcomeNotificationId(actionId, outcome), actionId, requesterRoomId, requester, assignee,
					assignee.shortName() + " declined your request", "Open the conversation for details.");
			case STATUS_CANCELLED -> notifyPerson(NotificationConstants.Type.DELEGATION_WITHDRAWN,
					outcomeNotificationId(actionId, outcome), actionId, (String) action.get("roomId"), requester,
					assignee, requester.shortName() + " withdrew their request",
					"You no longer need to respond to it.");
			default -> {
				// Nothing reached either person, so there is nothing to tell them.
			}
			}
		} catch (RuntimeException e) {
			logger.warn("Unable to send delegation notifications for childRunId={}", childRunId, e);
		}
	}

	/**
	 * One Collaboration inbox notice about a delegation. It goes to the assignee for
	 * a request or a withdrawal, and to the requester for an answer or a decline.
	 * Best effort: the delegation is already committed either way.
	 */
	private static void notifyPerson(String type, String notificationId, String actionId, String roomId,
			Person requester, Person assignee, String title, String message) {
		if (!NotificationDbUtils.isInitalized()) {
			return;
		}
		boolean toRequester = NotificationConstants.Type.DELEGATION_RESPONSE.equals(type)
				|| NotificationConstants.Type.DELEGATION_DECLINED.equals(type);
		Person recipient = toRequester ? requester : assignee;
		Person sender = toRequester ? assignee : requester;
		try {
			Map<String, Object> metadata = new LinkedHashMap<>();
			metadata.put("actionId", actionId);
			metadata.put("roomId", roomId);
			metadata.put("requester", requester.toMap());
			metadata.put("assignee", assignee.toMap());
			NotificationService.createCollaborationNotification(notificationId, type, recipient.userId(),
					recipient.provider(), StringUtils.abbreviate(title, 255), message, sender.userId(), roomId,
					GSON.toJson(metadata));
		} catch (RuntimeException e) {
			logger.warn("Unable to send the {} notification for actionId={}", type, actionId, e);
		}
	}

	private static void dismissAssigneeNotification(Map<String, Object> action) {
		if (!NotificationDbUtils.isInitalized()
				|| !(parseJson(action.get("toolMeta")).get("assignee") instanceof Map<?, ?> person)) {
			return;
		}
		try {
			Person assignee = Person.fromMap(person);
			NotificationService.dismissUserNotification(notificationId((String) action.get("actionId")),
					assignee.userId(), assignee.provider());
		} catch (RuntimeException e) {
			logger.warn("Unable to clear the delegation notification for actionId={}", action.get("actionId"), e);
		}
	}

	private static String notificationId(String actionId) {
		return deterministicId("semoss:delegation-notification:" + actionId);
	}

	private static String outcomeNotificationId(String actionId, String outcome) {
		return deterministicId("semoss:delegation-notification:" + outcome + ":" + actionId);
	}

	/** The delegation a room was created for, or null for ordinary rooms. */
	public static String delegationActionId(Room room) {
		Object value = room == null || room.getOptionsMap() == null ? null
				: room.getOptionsMap().get(ROOM_OPTION_ACTION_ID);
		return trimToNull(value);
	}

	/**
	 * What the model sees when the user turns down one of the delegation cards;
	 * null for other tools.
	 */
	public static String rejectedResult(String toolName) {
		if (SUBMIT_TOOL_NAME.equals(toolName)) {
			return "Not sent. The user chose to keep working, and nothing reached the requester. Stop here and "
					+ "wait for the user; send again only when they ask.";
		}
		if (TOOL_NAME.equals(toolName)) {
			return "Not sent. The user chose not to send this request, and no one was contacted. Ask the user "
					+ "what to change, or continue without it.";
		}
		return null;
	}

	/**
	 * Who asked for the delegation a room was made for, or null if it cannot be
	 * read.
	 */
	public static String requesterName(Insight insight, String actionId) {
		try {
			return requesterLabel(parseJson(findAssigned(insight.getUser(), actionId).get("toolMeta")));
		} catch (RuntimeException e) {
			return null;
		}
	}

	/**
	 * Runs an approved SubmitDelegationResponse call; the room, not the model,
	 * names the delegation.
	 */
	public static ToolExecutionResult submitFromTool(Insight insight, Room room, Map<String, Object> params) {
		String actionId = delegationActionId(room);
		if (actionId == null) {
			return ToolExecutionResult.error(null, "This room is not a delegation room");
		}
		try {
			boolean decline = Boolean.parseBoolean(String.valueOf(params == null ? null : params.get("decline")));
			Map<String, Object> outcome = respond(insight, actionId,
					trimToNull(params == null ? null : params.get("response")), decline,
					trimToNull(params == null ? null : params.get("reason")),
					stringList(params == null ? null : params.get("files")));
			Map<String, Object> summary = new LinkedHashMap<>();
			summary.put("status", outcome.get("status"));
			summary.put("sentTo", outcome.get("requesterName"));
			summary.put("message", (decline ? "Declined the request from " : "Sent your answer to ")
					+ outcome.get("requesterName") + ". This request is now closed.");
			return ToolExecutionResult.success(GSON.toJson(summary));
		} catch (RuntimeException e) {
			return ToolExecutionResult.error(null, e.getMessage());
		}
	}

	/**
	 * Runs an approved DelegateToPerson call with the arguments the owner
	 * confirmed.
	 */
	public static ToolExecutionResult delegateFromTool(Insight insight, String parentRunId,
			Map<String, Object> params) {
		try {
			return ToolExecutionResult.success(GSON.toJson(delegate(params, parentRunId, insight)));
		} catch (RuntimeException e) {
			return ToolExecutionResult.error(null, e.getMessage());
		}
	}

	/**
	 * Files a person sent back, relative to the owner's room folder; empty when
	 * none.
	 */
	public static List<Map<String, Object>> returnedFiles(String parentRoomId, String childRunId) {
		List<Map<String, Object>> out = new ArrayList<>();
		Path roomRoot = Paths.get(Room.roomFolderPath(parentRoomId));
		Path dir = roomRoot.resolve(FILES_FOLDER).resolve(childRunId);
		if (!Files.isDirectory(dir)) {
			return out;
		}
		try (Stream<Path> paths = Files.list(dir)) {
			paths.filter(Files::isRegularFile).sorted().forEach(path -> {
				Map<String, Object> file = new LinkedHashMap<>();
				file.put("path", roomRoot.relativize(path).toString().replace('\\', '/'));
				file.put("name", path.getFileName().toString());
				try {
					file.put("size", Files.size(path));
				} catch (IOException e) {
					file.put("size", null);
				}
				out.add(file);
			});
		} catch (IOException e) {
			logger.warn("Unable to list returned files for childRunId={}: {}", childRunId, e.getMessage());
		}
		return out;
	}

	// Validate every file first, then replace delegations/<childRunId>/ in the
	// owner's room and push it.
	private static void copyFilesToOwnerRoom(String assigneeRoomId, String childRunId, List<String> files) {
		String parentRoomId = AgentRunStore.getParentRoomId(childRunId);
		if (assigneeRoomId == null || parentRoomId == null) {
			throw new IllegalStateException("Unable to find the rooms for this delegation");
		}
		List<Path> sources = validateFiles(Paths.get(Room.roomFolderPath(assigneeRoomId)), files);

		ClusterUtil.pullRoom(parentRoomId);
		copyFiles(sources, Paths.get(Room.roomFolderPath(parentRoomId)).resolve(FILES_FOLDER).resolve(childRunId),
				parentRoomId);
	}

	// Replace target with copies of sources, then push the room; returns paths
	// relative to the room folder.
	private static List<String> copyFiles(List<Path> sources, Path target, String roomId) {
		Path roomRoot = Paths.get(Room.roomFolderPath(roomId));
		List<String> copied = new ArrayList<>();
		try {
			// A retry after a failed push starts from a clean folder.
			deleteRecursively(target);
			Files.createDirectories(target);
			for (Path source : sources) {
				Path dest = uniqueTarget(target, source.getFileName().toString());
				Files.copy(source, dest, StandardCopyOption.COPY_ATTRIBUTES);
				copied.add(roomRoot.relativize(dest).toString().replace('\\', '/'));
			}
		} catch (IOException e) {
			throw new IllegalStateException("Unable to copy files: " + e.getMessage(), e);
		}
		// Blocking: the other side must find the files as soon as this returns.
		ClusterUtil.pushRoom(roomId);
		return copied;
	}

	// Links stay links: access to Microsoft 365 or other documents is controlled at
	// their source.
	@SuppressWarnings("unchecked")
	private static List<Map<String, String>> links(Object value) {
		List<Map<String, String>> out = new ArrayList<>();
		if (!(value instanceof List<?> list)) {
			return out;
		}
		for (Object item : list) {
			String url = item instanceof Map<?, ?> map ? trimToNull(map.get("url")) : trimToNull(item);
			String title = item instanceof Map<?, ?> map ? trimToNull(map.get("title")) : null;
			if (url == null) {
				continue;
			}
			String lower = url.toLowerCase(Locale.ROOT);
			if (!lower.startsWith("https://") && !lower.startsWith("http://")) {
				throw new IllegalArgumentException("Links must be http or https URLs: " + url);
			}
			if (url.length() > 2000 || (title != null && title.length() > 300)) {
				throw new IllegalArgumentException("Link is too long: " + url);
			}
			Map<String, String> link = new LinkedHashMap<>();
			link.put("url", url);
			link.put("title", title);
			out.add(link);
		}
		if (out.size() > MAX_LINKS) {
			throw new IllegalArgumentException("At most " + MAX_LINKS + " links can be sent");
		}
		return out;
	}

	private static List<Path> validateFiles(Path roomFolder, List<String> files) {
		int maxFiles = intProperty(MAX_FILES_PROPERTY, 10);
		long maxFileBytes = intProperty(MAX_FILE_MB_PROPERTY, 25) * 1024L * 1024L;
		long maxTotalBytes = intProperty(MAX_TOTAL_MB_PROPERTY, 100) * 1024L * 1024L;
		if (files.size() > maxFiles) {
			throw new IllegalArgumentException("At most " + maxFiles + " files can be sent");
		}
		Path root;
		try {
			root = roomFolder.toRealPath();
		} catch (IOException e) {
			throw new IllegalArgumentException("This room has no files to send");
		}
		List<Path> sources = new ArrayList<>();
		long total = 0;
		for (String file : files) {
			Path real;
			try {
				real = root.resolve(file).normalize().toRealPath();
			} catch (IOException e) {
				throw new IllegalArgumentException("File not found in this room: " + file
						+ ". Use the path relative to the room folder, as a file listing shows it.");
			}
			// Real paths defeat ../ and symlinks that point outside the room.
			if (!real.startsWith(root) || !Files.isRegularFile(real)) {
				throw new IllegalArgumentException("Not a file in this room: " + file);
			}
			long size;
			try {
				size = Files.size(real);
			} catch (IOException e) {
				throw new IllegalArgumentException("Unable to read file: " + file);
			}
			if (size > maxFileBytes) {
				throw new IllegalArgumentException(file + " is larger than " + (maxFileBytes / 1024 / 1024) + " MB");
			}
			total += size;
			if (total > maxTotalBytes) {
				throw new IllegalArgumentException(
						"Files are larger than " + (maxTotalBytes / 1024 / 1024) + " MB in total");
			}
			sources.add(real);
		}
		return sources;
	}

	// Two files with the same name from different subfolders keep both copies.
	private static Path uniqueTarget(Path dir, String name) {
		Path candidate = dir.resolve(name);
		int dot = name.lastIndexOf('.');
		String base = dot > 0 ? name.substring(0, dot) : name;
		String ext = dot > 0 ? name.substring(dot) : "";
		for (int i = 2; Files.exists(candidate); i++) {
			candidate = dir.resolve(base + "-" + i + ext);
		}
		return candidate;
	}

	private static void deleteRecursively(Path path) throws IOException {
		if (!Files.exists(path)) {
			return;
		}
		try (Stream<Path> paths = Files.walk(path)) {
			for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
				Files.delete(p);
			}
		}
	}

	private static int intProperty(String key, int fallback) {
		String value = Utility.getDIHelperProperty(key);
		try {
			return value == null || value.isBlank() ? fallback : Math.max(0, Integer.parseInt(value.trim()));
		} catch (NumberFormatException e) {
			return fallback;
		}
	}

	// Canonical is ["a.md"]; also read [{"path": "a.md"}] and {"a.md": "a.md"},
	// which models send.
	private static List<String> stringList(Object value) {
		List<Object> items = new ArrayList<>();
		if (value instanceof List<?> list) {
			items.addAll(list);
		} else if (value instanceof Map<?, ?> map) {
			items.addAll(map.values());
		} else if (value != null) {
			items.add(value);
		}
		List<String> out = new ArrayList<>();
		for (Object item : items) {
			String text = trimToNull(item instanceof Map<?, ?> map ? map.get("path") : item);
			if (text != null) {
				// Room file listings return "/a.md"; paths here are always room-relative.
				out.add(text.replaceFirst("^[/\\\\]+", ""));
			}
		}
		return out;
	}

	private static void createAssigneeRoom(Person assignee, String roomId, String actionId, Person requester,
			Map<String, Object> packet) {
		Insight assigneeInsight = AgentRunService.createBackgroundExecutionInsight(assignee.userId(),
				assignee.provider(), null);
		Map<String, Object> options = new HashMap<>();
		options.put(ROOM_OPTION_ACTION_ID, actionId);
		// Appended to the agent's prompt so the task stays system-level, not only in history.
		options.put("instructions", roomInstructions(assignee, requester, packet));
		options.put("overrideSystemPrompt", false);
		RoomUtils.createRoomIfNotExists(roomId, assigneeInsight, null,
				"Request from " + requester.shortName() + ": " + packet.get("question"), null, options, null,
				CollaborationUtils.COLLABORATION_PROJECT_ID, null);
		RoomMessageStore.appendPlatformMessageIfAbsent(roomId, assignee.userId(),
				deterministicId("semoss:delegation-packet:" + actionId), packetText(requester, packet), null,
				Map.of(REQUEST_ORNAMENT, requestOrnament(roomId, requester, packet)), null);
	}

	// Structured copy of the packet so clients show a request card; the model still
	// reads the text.
	private static Map<String, Object> requestOrnament(String roomId, Person requester, Map<String, Object> packet) {
		Map<String, Object> ornament = new LinkedHashMap<>(packet);
		ornament.put("requester", requester.label());
		if (packet.get("files") instanceof List<?> paths) {
			Path folder = Paths.get(Room.roomFolderPath(roomId));
			List<Map<String, Object>> files = new ArrayList<>();
			for (Object item : paths) {
				String path = String.valueOf(item);
				Map<String, Object> file = new LinkedHashMap<>();
				file.put("path", path);
				file.put("name", Paths.get(path).getFileName().toString());
				try {
					file.put("size", Files.size(folder.resolve(path)));
				} catch (IOException e) {
					// Size is display only.
				}
				files.add(file);
			}
			ornament.put("files", files);
		}
		return ornament;
	}

	private static String packetText(Person requester, Map<String, Object> packet) {
		StringBuilder text = new StringBuilder(requester.label()).append(" asked you for help.\n\nQuestion:\n")
				.append(packet.get("question"));
		appendSection(text, "Context", packet.get("context"));
		appendSection(text, "Requested response format", packet.get("responseFormat"));
		appendSection(text, "Due", packet.get("dueAt"));
		if (packet.get("files") instanceof List<?> files && !files.isEmpty()) {
			text.append("\n\nAttached files (your own copies, in this room's folder):");
			files.forEach(file -> text.append("\n- ").append(file));
		}
		if (packet.get("links") instanceof List<?> links && !links.isEmpty()) {
			text.append("\n\nLinked documents (open at the source; access is controlled there, not by SEMOSS):");
			for (Object item : links) {
				Map<?, ?> link = (Map<?, ?>) item;
				Object title = link.get("title");
				text.append("\n- ").append(title == null ? "" : title + ": ").append(link.get("url"));
			}
		}
		return text.append("\n\nWork on it here with your agent for as long as you need. When you are ready, ask ")
				.append("it to send your answer back to ").append(requester.shortName())
				.append(". It does that with SubmitDelegationResponse, which answers this request; you confirm ")
				.append("exactly what is sent, and nothing else in this room is shared. Files you want to return ")
				.append("must be attached to that answer.").toString();
	}

	private static String roomInstructions(Person assignee, Person requester, Map<String, Object> packet) {
		String from = requester.shortName();
		StringBuilder text = new StringBuilder("## Delegated request\n\nThis room was created for one request from ")
				.append(requester.label()).append(" to ").append(assignee.label())
				.append(", who is the user you are working with. ").append(from)
				.append(" cannot see this room; only the answer sent with ").append(SUBMIT_TOOL_NAME)
				.append(" reaches them.\n\nRequest:\n").append(packet.get("question"));
		appendSection(text, "Context from " + from, packet.get("context"));
		appendSection(text, "Requested response format", packet.get("responseFormat"));
		appendSection(text, "Due", packet.get("dueAt"));
		if (packet.get("files") instanceof List<?> files && !files.isEmpty()) {
			text.append("\n\nFiles ").append(from).append(" sent (copies in this room's folder):");
			files.forEach(file -> text.append("\n- ").append(file));
		}
		if (packet.get("links") instanceof List<?> links && !links.isEmpty()) {
			text.append("\n\nLinked documents (access is controlled at the source):");
			for (Object item : links) {
				Map<?, ?> link = (Map<?, ?>) item;
				Object title = link.get("title");
				text.append("\n- ").append(title == null ? "" : title + ": ").append(link.get("url"));
			}
		}
		return text.append("\n\nHow to help:\n- Treat the user's messages as being about this request unless they ")
				.append("say otherwise.\n- When a message answers the request, call ").append(SUBMIT_TOOL_NAME)
				.append(" right away with that answer written for ").append(from)
				.append(". The user confirms or edits it in the card, so do not ask follow-up questions first.")
				.append("\n- When they ask for research, a draft, or file changes, do that work here and offer to send ")
				.append("it when it is ready.\n- Do not contact anyone else about this request.").toString();
	}

	private static void appendSection(StringBuilder text, String title, Object value) {
		if (value != null) {
			text.append("\n\n").append(title).append(":\n").append(value);
		}
	}

	// Load by each of the caller's logins; a wrong user and an unknown id look the
	// same.
	private static Map<String, Object> findAssigned(User user, String actionId) {
		if (actionId != null && !actionId.isBlank()) {
			for (AuthProvider provider : user.getLogins()) {
				AccessToken token = user.getAccessToken(provider);
				if (token == null || token.getId() == null) {
					continue;
				}
				Map<String, Object> action = AgentRunActionStore.getActionById(actionId.trim(), token.getId());
				if (isDelegationAction(action) && assignedTo(action, provider)) {
					return action;
				}
			}
		}
		throw new IllegalArgumentException("No delegation found for actionId=" + actionId);
	}

	@SuppressWarnings("unchecked")
	private static boolean assignedTo(Map<String, Object> action, AuthProvider provider) {
		Map<String, Object> meta = parseJson(action.get("toolMeta"));
		Object authType = meta.get("assignee") instanceof Map<?, ?> person ? person.get("provider")
				: meta.get("assigneeAuthType");
		return authType != null && (provider.name().equalsIgnoreCase(String.valueOf(authType))
				|| provider.getLabel().equalsIgnoreCase(String.valueOf(authType)));
	}

	private static Map<String, Object> view(Map<String, Object> action) {
		Map<String, Object> out = new LinkedHashMap<>(parseJson(action.get("toolArgs")));
		out.put("actionId", action.get("actionId"));
		out.put("status", action.get("status"));
		out.put("roomId", action.get("roomId"));
		Map<String, Object> meta = parseJson(action.get("toolMeta"));
		out.put("requester", meta.get("requester"));
		out.put("requesterName", requesterLabel(meta));
		out.put("response", action.get("result"));
		out.put("dateCreated", action.get("dateCreated"));
		out.put("decidedAt", action.get("decidedAt"));
		return out;
	}

	/**
	 * FindPerson tool: names and emails only, so the model can pass one to
	 * DelegateToPerson.
	 */
	public static ToolExecutionResult findPersonFromTool(Insight insight, Room room, Map<String, Object> params) {
		try {
			if (!CollaborationUtils.isCollaborationRoom(room) || delegationActionId(room) != null) {
				throw new IllegalStateException(FIND_PERSON_TOOL_NAME + " is only available in collaboration rooms");
			}
			requireUser(insight);
			String query = bounded(params, "query", 200, true);
			List<Map<String, Object>> people = new ArrayList<>();
			for (PersonMatch match : matchPeople(query, FIND_PERSON_LIMIT)) {
				Map<String, Object> entry = new LinkedHashMap<>();
				entry.put("name", match.person().name());
				entry.put("email", match.person().email());
				people.add(entry);
			}
			Map<String, Object> out = new LinkedHashMap<>();
			out.put("query", query);
			out.put("people", people);
			out.put("note", people.isEmpty() ? "No active user matches. Ask the user who they mean."
					: "Pass the name or email to " + TOOL_NAME + "; the user confirms the exact person in the card.");
			return ToolExecutionResult.success(GSON.toJson(out));
		} catch (RuntimeException e) {
			return ToolExecutionResult.error(null, e.getMessage());
		}
	}

	// The card sends an exact {userId, provider}; a bare hint must match exactly
	// one person.
	private static Person resolveAssignee(Object value) {
		if (value instanceof Map<?, ?> map) {
			Person picked = Person.fromMap(map);
			Map<String, Object> row = picked.userId() == null || picked.provider() == null ? null
					: SecurityQueryUtils.getUnlockedUser(picked.userId(), picked.provider());
			if (row == null) {
				throw new IllegalArgumentException("The selected person does not have an active account");
			}
			return Person.fromRow(row);
		}
		String hint = trimToNull(value);
		if (hint == null) {
			throw new IllegalArgumentException("assignee is required for " + TOOL_NAME);
		}
		if (hint.length() > 320) {
			throw new IllegalArgumentException("assignee exceeds 320 characters");
		}
		List<PersonMatch> matches = matchPeople(hint, MAX_PEOPLE_RESULTS);
		List<PersonMatch> exact = matches.stream().filter(m -> m.rank() == 0).toList();
		List<PersonMatch> pool = exact.isEmpty() ? matches : exact;
		if (pool.size() == 1) {
			return pool.getFirst().person();
		}
		throw new IllegalArgumentException(pool.isEmpty() ? "No active user matches assignee " + hint
				: "Assignee " + hint + " matches " + pool.size() + " people; pick one in the request card");
	}

	// Every word must appear in the name, email, or username; rank 0 is an exact
	// match.
	private static List<PersonMatch> matchPeople(String query, int limit) {
		String q = trimToNull(query);
		if (q == null) {
			return List.of();
		}
		String lower = q.toLowerCase(Locale.ROOT);
		List<String> words = List.of(lower.split("[\\s,]+"));
		String longest = words.stream().max(Comparator.comparingInt(String::length)).orElse(lower);
		List<PersonMatch> out = new ArrayList<>();
		for (Map<String, Object> row : SecurityQueryUtils.searchUnlockedUsers(longest, PEOPLE_SCAN_LIMIT)) {
			String name = lowerOrEmpty(row.get("name"));
			String email = lowerOrEmpty(row.get("email"));
			String username = lowerOrEmpty(row.get("username"));
			String id = lowerOrEmpty(row.get("id"));
			String haystack = String.join(" ", name, email, username, id);
			if (!words.stream().allMatch(haystack::contains)) {
				continue;
			}
			int rank = lower.equals(email) || lower.equals(username) || lower.equals(id) || lower.equals(name) ? 0
					: name.startsWith(lower) || email.startsWith(lower) ? 1 : 2;
			out.add(new PersonMatch(Person.fromRow(row), rank));
		}
		out.sort(Comparator.comparingInt(PersonMatch::rank));
		return out.size() > limit ? out.subList(0, limit) : out;
	}

	private static String lowerOrEmpty(Object value) {
		String text = trimToNull(value);
		return text == null ? "" : text.toLowerCase(Locale.ROOT);
	}

	private record PersonMatch(Person person, int rank) {
	}

	private static SubAgentRunCompletionMode completionMode(Map<String, Object> args) {
		String value = trimToNull(args == null ? null : args.get("completionMode"));
		if (value == null) {
			return SubAgentRunCompletionMode.POST_AND_CONTINUE;
		}
		SubAgentRunCompletionMode mode;
		try {
			mode = SubAgentRunCompletionMode.fromExternalValue(value);
		} catch (IllegalArgumentException e) {
			mode = null;
		}
		// WAIT would hold the run open for as long as a person takes to answer.
		if (mode == null || mode == SubAgentRunCompletionMode.WAIT) {
			throw new IllegalArgumentException("completionMode must be POST or POST_AND_CONTINUE");
		}
		return mode;
	}

	// Models invent deadlines; a date already past is dropped, free text is kept
	// for display.
	static String dueDate(String value) {
		if (value == null) {
			return null;
		}
		LocalDate date;
		try {
			date = value.length() == 10 ? LocalDate.parse(value) : OffsetDateTime.parse(value).toLocalDate();
		} catch (DateTimeParseException e) {
			return value;
		}
		if (date.isBefore(LocalDate.now())) {
			logger.info("Dropped past dueAt={}", value);
			return null;
		}
		return value;
	}

	private static String bounded(Map<String, Object> args, String key, int maxLength, boolean required) {
		String value = trimToNull(args == null ? null : args.get(key));
		if (value == null && required) {
			throw new IllegalArgumentException(key + " is required for " + TOOL_NAME);
		}
		if (value != null && value.length() > maxLength) {
			throw new IllegalArgumentException(key + " exceeds " + maxLength + " characters");
		}
		return value;
	}

	private static User requireUser(Insight insight) {
		User user = insight == null ? null : insight.getUser();
		if (user == null || user.getLogins() == null || user.getLogins().isEmpty()) {
			throw new SecurityException("Must be logged in to work with delegations");
		}
		return user;
	}

	// Rows from before the Person meta only stored a name.
	private static String requesterLabel(Map<String, Object> meta) {
		if (meta.get("requester") instanceof Map<?, ?> person) {
			return Person.fromMap(person).label();
		}
		return trimToNull(meta.get("requesterName"));
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> parseJson(Object value) {
		if (!(value instanceof String json) || json.isBlank()) {
			return new HashMap<>();
		}
		Map<String, Object> map = GSON.fromJson(json, Map.class);
		return map == null ? new HashMap<>() : map;
	}

	private static String deterministicId(String seed) {
		return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
	}

	private static String trimToNull(Object value) {
		if (value == null) {
			return null;
		}
		String text = String.valueOf(value).trim();
		return text.isEmpty() ? null : text;
	}

	/**
	 * A SEMOSS principal: userId plus provider is the identity, name and email are
	 * for display.
	 */
	private record Person(String userId, String provider, String name, String email) {
		static Person of(AccessToken token) {
			if (token == null) {
				throw new SecurityException("Must be logged in to work with delegations");
			}
			return new Person(token.getId(), token.getProvider() == null ? null : token.getProvider().name(),
					trimToNull(token.getName()), trimToNull(token.getEmail()));
		}

		static Person fromRow(Map<String, Object> row) {
			return new Person(trimToNull(row.get("id")), trimToNull(row.get("type")), trimToNull(row.get("name")),
					trimToNull(row.get("email")));
		}

		static Person fromMap(Map<?, ?> map) {
			return new Person(trimToNull(map.get("userId")), trimToNull(map.get("provider")),
					trimToNull(map.get("name")), trimToNull(map.get("email")));
		}

		String shortName() {
			return name != null ? name : label();
		}

		/** "Name (email)", falling back to whichever part exists. */
		String label() {
			if (name != null && email != null && !name.equalsIgnoreCase(email)) {
				return name + " (" + email + ")";
			}
			return name != null ? name : email != null ? email : userId;
		}

		Map<String, Object> toMap() {
			Map<String, Object> map = new LinkedHashMap<>();
			map.put("userId", userId);
			map.put("provider", provider);
			map.put("name", name);
			map.put("email", email);
			return map;
		}
	}
}
