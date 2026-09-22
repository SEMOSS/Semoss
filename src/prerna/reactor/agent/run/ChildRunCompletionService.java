package prerna.reactor.agent.run;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
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

/** Delivers detached child-run results to the durable parent room. */
public final class ChildRunCompletionService {

	private static final Logger logger = LogManager.getLogger(ChildRunCompletionService.class);
	private static final Gson GSON = new Gson();

	private ChildRunCompletionService() {
	}

	/** Load the durable delivery payload for one terminal child. */
	static Delivery load(String childRunId) {
		List<Map<String, Object>> completions = AgentRunStore.getTerminalChildCompletions(childRunId, null, 1);
		return completions.isEmpty() ? null : deliveryFrom(completions.getFirst());
	}

	/** Find terminal children whose deterministic delivery can be re-queued. */
	static List<String> findTerminalChildIds(String parentRunId) {
		List<Map<String, Object>> completions = AgentRunStore.getTerminalChildCompletions(null, parentRunId, 0);
		List<String> childRunIds = new ArrayList<>();
		if (completions.isEmpty()) {
			return childRunIds;
		}
		Delivery first = deliveryFrom(completions.getFirst());
		Room room = ModelInferenceLogsUtils.getRoomById(first.parentRoomId(), first.userId());
		if (room == null) {
			throw new IllegalStateException("Unable to find parent room for runId=" + parentRunId);
		}
		Set<String> deliveredMessageIds = new HashSet<>();
		for (AbstractMessage message : room.getMessages()) {
			deliveredMessageIds.add(message.getMessageId());
		}
		for (Map<String, Object> completion : completions) {
			Delivery delivery = deliveryFrom(completion);
			if (delivery.mode() != SubAgentRunCompletionMode.JOIN
					&& !deliveredMessageIds.contains(deterministicMessageId(delivery.childRunId()))) {
				childRunIds.add(delivery.childRunId());
			}
		}
		return childRunIds;
	}

	/** Append exactly one platform message; callers retain and retry failed work. */
	static void deliver(Delivery delivery) {
		AgentRunMessageContext agentRun = new AgentRunMessageContext(delivery.parentRunId(), "subagent_completion");
		agentRun.setOriginatingRunId(delivery.parentRunId());
		agentRun.setChildRunId(delivery.childRunId());
		agentRun.setCompletionMode(delivery.mode().name());
		agentRun.setChildStatus(delivery.status().name());

		boolean appended = RoomMessageStore.appendPlatformMessageIfAbsent(delivery.parentRoomId(), delivery.userId(),
				deterministicMessageId(delivery.childRunId()),
				messageText(delivery.childRunId(), delivery.status(), delivery.finalText(), delivery.errorMessage()),
				agentRun);
		if (appended) {
			logger.info("Delivered child completion runId={} parentRunId={} mode={}", delivery.childRunId(),
					delivery.parentRunId(), delivery.mode());
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
				stringValue(completion.get("finalText")), stringValue(completion.get("errorMessage")));
	}

	static record Delivery(String childRunId, String parentRunId, String parentRoomId, String userId,
			AgentRunStatus status, SubAgentRunCompletionMode mode, String finalText, String errorMessage) {
	}

	private static String messageText(String childRunId, AgentRunStatus status, String finalText, String errorMessage) {
		if (status == AgentRunStatus.COMPLETED) {
			return "Subagent " + childRunId + " completed:\n\n" + defaultText(finalText, "(no output)");
		}
		if (status == AgentRunStatus.CANCELLED) {
			return "Subagent " + childRunId + " was cancelled.";
		}
		return "Subagent " + childRunId + " failed:\n\n" + defaultText(errorMessage, "Unknown failure");
	}

	private static String deterministicMessageId(String childRunId) {
		return UUID.nameUUIDFromBytes(("semoss:child-completion:" + childRunId).getBytes(StandardCharsets.UTF_8))
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
