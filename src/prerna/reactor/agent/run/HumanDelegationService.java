package prerna.reactor.agent.run;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.ToolExecutionResult;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.om.Insight;
import prerna.reactor.agent.AgentRunContext;
import prerna.util.Utility;

/**
 * Delegates a question from an agent run to a person. The owner keeps a HUMAN
 * child run; the assignee gets one AGENT_RUN_ACTION and a room of their own.
 * An explicit response completes the child through the normal child-completion
 * path.
 */
public final class HumanDelegationService {

	private static final Logger logger = LogManager.getLogger(HumanDelegationService.class);
	private static final Gson GSON = new Gson();

	public static final String TOOL_NAME = "DelegateToPerson";
	public static final String SUBMIT_TOOL_NAME = "SubmitDelegationResponse";
	public static final String ROOM_OPTION_ACTION_ID = "delegation_action_id";
	private static final String ENABLED_PROPERTY = "AGENT_HUMAN_DELEGATION_ENABLED";
	private static final String STATUS_PENDING = "PENDING";
	private static final String STATUS_RESPONDED = "RESPONDED";
	private static final String STATUS_DECLINED = "DECLINED";
	// Child ERROR_MESSAGE prefix; the completion message keys off it.
	static final String DECLINED_PREFIX = "Declined";
	private static final int MAX_QUESTION_LENGTH = 8_000;
	private static final int MAX_CONTEXT_LENGTH = 32_000;
	private static final int MAX_RESPONSE_LENGTH = 32_000;
	private static final int LIST_LIMIT = 200;

	private HumanDelegationService() {
	}

	public static boolean isEnabled() {
		return Boolean.parseBoolean(Utility.getDIHelperProperty(ENABLED_PROPERTY));
	}

	public static boolean isDelegationAction(Map<String, Object> action) {
		return action != null && TOOL_NAME.equals(action.get("toolName"));
	}

	/** Create the assignee's room and action plus the owner's child run; returns immediately. */
	public static Map<String, Object> delegate(Map<String, Object> args, String parentRunId, Insight ownerInsight) {
		if (!isEnabled()) {
			throw new IllegalStateException("Delegating to a person is not enabled");
		}
		AgentRunRecord parent = parentRunId == null ? null : AgentRunStore.getRun(parentRunId, ownerInsight);
		if (parent == null) {
			throw new IllegalArgumentException(TOOL_NAME + " must be called from an agent run");
		}
		String question = bounded(args, "question", MAX_QUESTION_LENGTH, true);
		String context = bounded(args, "context", MAX_CONTEXT_LENGTH, false);
		String responseFormat = bounded(args, "responseFormat", MAX_QUESTION_LENGTH, false);
		String dueAt = bounded(args, "dueAt", 100, false);
		SubAgentRunCompletionMode mode = completionMode(args);
		Assignee assignee = resolveAssignee(bounded(args, "assignee", 320, true));
		String requesterName = displayName(ownerInsight.getUser().getPrimaryLoginToken());

		String childRunId = GUID.v7().toUUID().toString();
		String actionId = GUID.v7().toUUID().toString();
		String assigneeRoomId = deterministicId("semoss:delegation-room:" + actionId);
		Map<String, Object> packet = new LinkedHashMap<>();
		packet.put("question", question);
		packet.put("context", context);
		packet.put("responseFormat", responseFormat);
		packet.put("dueAt", dueAt);

		// Room, then action, then child run: a failure never leaves a child for the owner's room to report.
		createAssigneeRoom(assignee, assigneeRoomId, actionId, requesterName, packet);
		Map<String, Object> meta = new HashMap<>();
		meta.put("assigneeAuthType", assignee.authType());
		meta.put("requesterName", requesterName);
		Map<String, Object> action = new HashMap<>();
		action.put("actionId", actionId);
		action.put("toolName", TOOL_NAME);
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

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("runId", childRunId);
		out.put("status", AgentRunStatus.INPUT_REQUIRED.name());
		out.put("assignee", assignee.label());
		out.put("completionMode", mode.name());
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
			for (Map<String, Object> action : AgentRunActionStore.getAssignedActions(token.getId(), TOOL_NAME,
					statusFilter, LIST_LIMIT)) {
				if (assignedTo(action, provider)) {
					out.add(view(action));
				}
			}
		}
		return out;
	}

	/** Record the assignee's one answer or decline, then complete the owner's child run. */
	private static Map<String, Object> respond(Insight insight, String actionId, String response, boolean decline,
			String reason) {
		Map<String, Object> action = findAssigned(requireUser(insight), actionId);
		actionId = (String) action.get("actionId");
		String runId = (String) action.get("runId");
		String userId = (String) action.get("userId");
		if (STATUS_PENDING.equals(action.get("status"))) {
			String result = decline ? trimToNull(reason) : trimToNull(response);
			if (!decline && result == null) {
				throw new IllegalArgumentException("response is required");
			}
			if (result != null && result.length() > MAX_RESPONSE_LENGTH) {
				throw new IllegalArgumentException("response exceeds " + MAX_RESPONSE_LENGTH + " characters");
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
		return view(action);
	}

	/** The delegation a room was created for, or null for ordinary rooms. */
	public static String delegationActionId(Room room) {
		Object value = room == null || room.getOptionsMap() == null ? null
				: room.getOptionsMap().get(ROOM_OPTION_ACTION_ID);
		return trimToNull(value);
	}

	/** Runs an approved SubmitDelegationResponse call; the room, not the model, names the delegation. */
	public static ToolExecutionResult submitFromTool(Insight insight, Room room, Map<String, Object> params) {
		String actionId = delegationActionId(room);
		if (actionId == null) {
			return ToolExecutionResult.error(null, "This room is not a delegation room");
		}
		try {
			boolean decline = Boolean.parseBoolean(String.valueOf(params == null ? null : params.get("decline")));
			Map<String, Object> outcome = respond(insight, actionId,
					trimToNull(params == null ? null : params.get("response")), decline,
					trimToNull(params == null ? null : params.get("reason")));
			Map<String, Object> summary = new LinkedHashMap<>();
			summary.put("status", outcome.get("status"));
			summary.put("sentTo", outcome.get("requesterName"));
			return ToolExecutionResult.success(GSON.toJson(summary));
		} catch (RuntimeException e) {
			return ToolExecutionResult.error(null, e.getMessage());
		}
	}

	private static void createAssigneeRoom(Assignee assignee, String roomId, String actionId, String requesterName,
			Map<String, Object> packet) {
		Insight assigneeInsight = AgentRunService.createBackgroundExecutionInsight(assignee.userId(),
				assignee.authType(), null);
		Map<String, Object> options = new HashMap<>();
		options.put(ROOM_OPTION_ACTION_ID, actionId);
		RoomUtils.createRoomIfNotExists(roomId, assigneeInsight, null,
				"Request from " + requesterName + ": " + packet.get("question"), null, options, null, null, null);
		RoomMessageStore.appendPlatformMessageIfAbsent(roomId, assignee.userId(),
				deterministicId("semoss:delegation-packet:" + actionId), packetText(requesterName, packet),
				null);
	}

	private static String packetText(String requesterName, Map<String, Object> packet) {
		StringBuilder text = new StringBuilder(requesterName).append(" asked you for help.\n\nQuestion:\n")
				.append(packet.get("question"));
		appendSection(text, "Context", packet.get("context"));
		appendSection(text, "Requested response format", packet.get("responseFormat"));
		appendSection(text, "Due", packet.get("dueAt"));
		return text.append("\n\nWork on it here with your agent for as long as you need. When you are ready, ask ")
				.append("it to send your answer back; you will confirm exactly what is sent. Nothing else in this ")
				.append("room is shared.").toString();
	}

	private static void appendSection(StringBuilder text, String title, Object value) {
		if (value != null) {
			text.append("\n\n").append(title).append(":\n").append(value);
		}
	}

	// Load by each of the caller's logins; a wrong user and an unknown id look the same.
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
		Object authType = meta.get("assigneeAuthType");
		return authType != null && (provider.name().equalsIgnoreCase(String.valueOf(authType))
				|| provider.getLabel().equalsIgnoreCase(String.valueOf(authType)));
	}

	private static Map<String, Object> view(Map<String, Object> action) {
		Map<String, Object> out = new LinkedHashMap<>(parseJson(action.get("toolArgs")));
		out.put("actionId", action.get("actionId"));
		out.put("status", action.get("status"));
		out.put("roomId", action.get("roomId"));
		out.put("requesterName", parseJson(action.get("toolMeta")).get("requesterName"));
		out.put("response", action.get("result"));
		out.put("dateCreated", action.get("dateCreated"));
		out.put("decidedAt", action.get("decidedAt"));
		return out;
	}

	// Exactly one unlocked account must match; every miss reports the same error.
	private static Assignee resolveAssignee(String email) {
		List<Object[]> matches = SecurityQueryUtils.getUnlockedUsersByEmail(email.trim());
		if (matches.size() != 1) {
			throw new IllegalArgumentException("No single active user matches assignee " + email);
		}
		Object[] row = matches.getFirst();
		return new Assignee(String.valueOf(row[0]), String.valueOf(row[1]), trimToNull(row[2]), trimToNull(row[3]));
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

	private static String displayName(AccessToken token) {
		if (token == null) {
			return "Someone";
		}
		String name = trimToNull(token.getName());
		return name != null ? name : trimToNull(token.getEmail()) != null ? token.getEmail() : token.getId();
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

	private record Assignee(String userId, String authType, String name, String email) {
		String label() {
			return name != null && !name.isBlank() ? name : email;
		}
	}
}
