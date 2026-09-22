package prerna.reactor.agent.run;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.AgentRunMessageContext;
import prerna.om.Insight;
import prerna.util.Utility;

/** Delivers detached child results and submits their optional continuation. */
public final class ChildRunCompletionService {

	private static final Logger logger = LogManager.getLogger(ChildRunCompletionService.class);
	private static final Gson GSON = new Gson();
	private static final String MAX_CONTINUATION_DEPTH = "AGENT_RUN_MAX_CONTINUATION_DEPTH";
	private static final int DEFAULT_MAX_CONTINUATION_DEPTH = 5;
	private static final int MAX_ERROR_TEXT_LENGTH = 2000;
	private static final int RECOVERY_SCAN_LIMIT = 1000;

	private ChildRunCompletionService() {
	}

	/** Load the durable delivery payload for one terminal child. */
	static Delivery load(String childRunId) {
		List<Map<String, Object>> completions = AgentRunStore.getTerminalChildCompletions(childRunId, null, null, null, 1);
		return completions.isEmpty() ? null : deliveryFrom(completions.getFirst());
	}

	/** Find terminal children of runs in this room whose delivery can be re-queued. */
	static List<String> findUndeliveredChildIdsForRoom(String parentRoomId, String userId) {
		return findUndelivered(AgentRunStore.getTerminalChildCompletions(null, null, parentRoomId, userId, 0));
	}

	/** Bounded scan of the most recent terminal children, used once per JVM to repair lost queue items. */
	static List<String> findRecentUndeliveredChildIds() {
		return findUndelivered(AgentRunStore.getTerminalChildCompletions(null, null, null, null, RECOVERY_SCAN_LIMIT));
	}

	private static List<String> findUndelivered(List<Map<String, Object>> completions) {
		// Rooms are keyed by (roomId, userId); load each projection once.
		Map<String, List<Delivery>> byRoom = new LinkedHashMap<>();
		for (Map<String, Object> completion : completions) {
			Delivery delivery;
			try {
				delivery = deliveryFrom(completion);
			} catch (RuntimeException e) {
				logger.warn("Skipping malformed child completion row: {}", e.getMessage());
				continue;
			}
			if (delivery.mode() != SubAgentRunCompletionMode.JOIN) {
				byRoom.computeIfAbsent(delivery.parentRoomId() + "\n" + delivery.userId(), ignored -> new ArrayList<>())
						.add(delivery);
			}
		}
		List<String> childRunIds = new ArrayList<>();
		for (List<Delivery> deliveries : byRoom.values()) {
			Delivery first = deliveries.getFirst();
			Room room;
			try {
				room = ModelInferenceLogsUtils.getRoomById(first.parentRoomId(), first.userId());
			} catch (RuntimeException e) {
				logger.warn("Skipping child completions for parent roomId={}: {}", first.parentRoomId(), e.getMessage());
				continue;
			}
			if (room == null) {
				logger.warn("Skipping child completions for missing parent roomId={}", first.parentRoomId());
				continue;
			}
			Set<String> deliveredMessageIds = new HashSet<>();
			for (AbstractMessage message : room.getMessages()) {
				deliveredMessageIds.add(message.getMessageId());
			}
			for (Delivery delivery : deliveries) {
				boolean messageMissing = !deliveredMessageIds.contains(deterministicMessageId(delivery.childRunId()));
				if (messageMissing || continuationMissing(delivery)) {
					childRunIds.add(delivery.childRunId());
				}
			}
		}
		return childRunIds;
	}

	private static boolean continuationMissing(Delivery delivery) {
		if (delivery.mode() != SubAgentRunCompletionMode.CONTINUE) {
			return false;
		}
		AgentRunRequest parentRequest = parentRequestOrNull(delivery);
		return parentRequest != null && withinDepthLimit(parentRequest)
				&& !AgentRunStore.runExists(deterministicContinuationRunId(delivery.childRunId()));
	}

	/** Append exactly one result and submit exactly one continuation when requested. */
	static void deliver(Delivery delivery) {
		boolean continueRun = delivery.mode() == SubAgentRunCompletionMode.CONTINUE;
		AgentRunRequest parentRequest = continueRun ? parentRequestOrNull(delivery) : null;
		boolean depthExceeded = parentRequest != null && !withinDepthLimit(parentRequest);
		AgentRunMessageContext agentRun = new AgentRunMessageContext(delivery.parentRunId(), "subagent_completion");
		agentRun.setOriginatingRunId(delivery.parentRunId());
		agentRun.setChildRunId(delivery.childRunId());
		agentRun.setCompletionMode(delivery.mode().name());
		agentRun.setChildStatus(delivery.status().name());

		boolean appended = RoomMessageStore.appendPlatformMessageIfAbsent(delivery.parentRoomId(), delivery.userId(),
				deterministicMessageId(delivery.childRunId()),
				messageText(delivery.childRunId(), delivery.status(), delivery.finalText(), delivery.errorMessage(),
						depthExceeded),
				agentRun);
		if (appended) {
			logger.info("Delivered child completion runId={} parentRunId={} mode={}", delivery.childRunId(),
					delivery.parentRunId(), delivery.mode());
		}
		if (!continueRun) {
			return;
		}
		if (parentRequest == null) {
			logger.warn("Skipped child continuation childRunId={} parentRunId={}: parent request is missing",
					delivery.childRunId(), delivery.parentRunId());
		} else if (depthExceeded) {
			logger.warn("Skipped child continuation childRunId={} parentRunId={}: continuation depth limit reached",
					delivery.childRunId(), delivery.parentRunId());
		} else {
			submitContinuation(delivery, parentRequest);
		}
	}

	private static void submitContinuation(Delivery delivery, AgentRunRequest parentRequest) {
		String input = "[SEMOSS continuation] Delegated task " + delivery.childRunId() + " is now "
				+ delivery.status().name() + ". Continue the original task using the result delivered immediately "
				+ "before this message. Do not wait for or repeat that delegated task.";
		Insight continuationInsight = AgentRunService.createBackgroundExecutionInsight(delivery.userId(), parentRequest);
		AgentRunRequest continuation = parentRequest.forContinuation(delivery.parentRoomId(), delivery.childRunId(),
				input, continuationInsight);
		String continuationRunId = deterministicContinuationRunId(delivery.childRunId());
		boolean submitted = AgentRunService.get().runBackgroundWithIdIfAbsent(continuationRunId, continuation);
		if (submitted) {
			logger.info("Submitted child continuation runId={} childRunId={} parentRunId={}", continuationRunId,
					delivery.childRunId(), delivery.parentRunId());
		}
	}

	// Continuations inherit depth from the run that spawned the child, so chains stop at the cap.
	private static boolean withinDepthLimit(AgentRunRequest parentRequest) {
		return parentRequest.getContinuationDepth() < maxContinuationDepth();
	}

	@SuppressWarnings("unchecked")
	private static AgentRunRequest parentRequestOrNull(Delivery delivery) {
		try {
			Map<String, Object> persisted = delivery.parentRequestJson() == null ? null
					: GSON.fromJson(delivery.parentRequestJson(), Map.class);
			return AgentRunRequest.fromPersistedMap(persisted, null);
		} catch (RuntimeException e) {
			return null;
		}
	}

	private static int maxContinuationDepth() {
		String value = Utility.getDIHelperProperty(MAX_CONTINUATION_DEPTH);
		if (value == null || value.isBlank()) {
			return DEFAULT_MAX_CONTINUATION_DEPTH;
		}
		try {
			return Math.max(0, Integer.parseInt(value.trim()));
		} catch (NumberFormatException e) {
			return DEFAULT_MAX_CONTINUATION_DEPTH;
		}
	}

	@SuppressWarnings("unchecked")
	private static Delivery deliveryFrom(Map<String, Object> completion) {
		Map<String, Object> request = GSON.fromJson(stringValue(completion.get("requestJson")), Map.class);
		SubAgentRunCompletionMode mode = SubAgentRunCompletionMode
				.fromPersistedValue(request == null ? null : request.get("completionMode"));
		return new Delivery(required(completion, "childRunId"), required(completion, "parentRunId"),
				required(completion, "parentRoomId"), required(completion, "userId"),
				AgentRunStatus.valueOf(required(completion, "status")), mode,
				stringValue(completion.get("finalText")), stringValue(completion.get("errorMessage")),
				stringValue(completion.get("parentRequestJson")));
	}

	static record Delivery(String childRunId, String parentRunId, String parentRoomId, String userId,
			AgentRunStatus status, SubAgentRunCompletionMode mode, String finalText, String errorMessage,
			String parentRequestJson) {
	}

	private static String messageText(String childRunId, AgentRunStatus status, String finalText, String errorMessage,
			boolean depthExceeded) {
		String text;
		if (status == AgentRunStatus.COMPLETED) {
			text = "Subagent " + childRunId + " completed:\n\n" + defaultText(finalText, "(no output)");
		} else if (status == AgentRunStatus.CANCELLED) {
			text = "Subagent " + childRunId + " was cancelled.";
		} else {
			// Error text becomes model context, so keep it bounded.
			text = "Subagent " + childRunId + " failed:\n\n"
					+ truncate(defaultText(errorMessage, "Unknown failure"), MAX_ERROR_TEXT_LENGTH);
		}
		if (depthExceeded) {
			text += "\n\nAutomatic continuation was skipped because the continuation limit was reached.";
		}
		return text;
	}

	private static String truncate(String value, int maxLength) {
		return value.length() <= maxLength ? value : value.substring(0, maxLength) + "... [truncated]";
	}

	private static String deterministicMessageId(String childRunId) {
		return UUID.nameUUIDFromBytes(("semoss:child-completion:" + childRunId).getBytes(StandardCharsets.UTF_8))
				.toString();
	}

	private static String deterministicContinuationRunId(String childRunId) {
		return UUID.nameUUIDFromBytes(("semoss:child-continuation:" + childRunId).getBytes(StandardCharsets.UTF_8))
				.toString();
	}

	private static String required(Map<String, Object> values, String key) {
		String value = stringValue(values.get(key));
		if (value == null) {
			throw new IllegalStateException("Missing child completion field: " + key);
		}
		return value;
	}

	private static String defaultText(String value, String fallback) {
		return value == null || value.isBlank() ? fallback : value;
	}

	private static String stringValue(Object value) {
		if (value == null) {
			return null;
		}
		String text = String.valueOf(value).trim();
		return text.isEmpty() ? null : text;
	}
}
