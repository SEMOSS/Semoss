package prerna.reactor.playwright;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reactor that returns the full prompt/response history for a single room
 * formatted both as structured data and as a ready-to-use context string.
 */
public class GetRoomConversationHistoryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetRoomConversationHistoryReactor.class);

	public GetRoomConversationHistoryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.SORT.getKey(), "includePartial" };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		if (roomId == null || roomId.trim().isEmpty()) {
			throw new IllegalArgumentException("Room id is required");
		}

		int offset = parseInt(this.keyValue.get(ReactorKeysEnum.OFFSET.getKey()), 0);
		int limit = parseInt(this.keyValue.get(ReactorKeysEnum.LIMIT.getKey()), -1);
		String sortDir = normalizeSort(this.keyValue.get(ReactorKeysEnum.SORT.getKey()));
		boolean includePartial = Boolean.parseBoolean(this.keyValue.getOrDefault("includePartial", "false"));

		Room room;
		try {
			room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		} catch (Exception e) {
			classLogger.error("Unable to load room history for {}", roomId, e);
			throw new IllegalArgumentException("Unable to load room history for the requested room id");
		}

		List<AbstractMessage> chronologicalMessages = RoomUtils.getPagedMessages(room.getMessages(), "ASC", 0, -1);
		ConversationHistory conversationHistory = collectConversationHistory(chronologicalMessages);
		List<Map<String, Object>> allPairs = conversationHistory.pairs;

		List<Map<String, Object>> orderedPairs = new ArrayList<>(allPairs);
		if ("DESC".equals(sortDir)) {
			Collections.reverse(orderedPairs);
		}

		List<Map<String, Object>> pagedPairs = applyPaging(orderedPairs, offset, limit);
		String historyString = buildHistoryString(pagedPairs);
		StringBuilder historyBuilder = new StringBuilder(historyString);

		if (includePartial && conversationHistory.pendingQuestion != null) {
			String pendingQuestion = (String) conversationHistory.pendingQuestion.get("question");
			if (pendingQuestion != null && !pendingQuestion.trim().isEmpty()) {
				if (historyBuilder.length() > 0) {
					historyBuilder.append("\n---\n");
				}
				historyBuilder.append("User: ").append(pendingQuestion).append("\nAssistant: ");
			}
		}

		return new NounMetadata(historyBuilder.toString(), PixelDataType.CONST_STRING);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.ROOM_ID.getKey().equals(key)) {
			return "Conversation/room identifier that the history should be pulled from.";
		} else if (ReactorKeysEnum.LIMIT.getKey().equals(key)) {
			return "Maximum number of question/answer pairs to return (default is all).";
		} else if (ReactorKeysEnum.OFFSET.getKey().equals(key)) {
			return "Number of question/answer pairs to skip before collecting history.";
		} else if (ReactorKeysEnum.SORT.getKey().equals(key)) {
			return "Sort direction for the returned pairs. Accepts ASC (oldest first) or DESC (newest first).";
		} else if ("includePartial".equals(key)) {
			return "Set to true to include the most recent unanswered question, if one exists.";
		}

		return super.getDescriptionForKey(key);
	}

	private static int parseInt(String value, int defaultValue) {
		if (value == null || value.trim().isEmpty()) {
			return defaultValue;
		}
		try {
			return Integer.parseInt(value.trim());
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	private static String normalizeSort(String value) {
		if (value == null) {
			return "ASC";
		}
		String normalized = value.trim().toUpperCase();
		return "DESC".equals(normalized) ? "DESC" : "ASC";
	}

	private static List<Map<String, Object>> applyPaging(List<Map<String, Object>> pairs, int offset, int limit) {
		if (pairs.isEmpty()) {
			return pairs;
		}
		int start = Math.max(0, offset);
		if (start >= pairs.size()) {
			return new ArrayList<>();
		}
		int end = pairs.size();
		if (limit > -1) {
			end = Math.min(start + Math.max(0, limit), pairs.size());
		}
		return new ArrayList<>(pairs.subList(start, end));
	}

	private static ConversationHistory collectConversationHistory(List<AbstractMessage> messages) {
		List<Map<String, Object>> pairs = new ArrayList<>();
		InputMessage pendingInput = null;

		for (AbstractMessage message : messages) {
			if (message == null || !message.isVisible()) {
				continue;
			}
			if (message instanceof InputMessage inputMessage) {
				pendingInput = inputMessage;
			} else if (message instanceof ResponseMessage responseMessage) {
				if (pendingInput == null) {
					continue;
				}
				pairs.add(buildPair(pendingInput, responseMessage, true));
				pendingInput = null;
			}
		}

		Map<String, Object> pendingQuestion = null;
		if (pendingInput != null) {
			pendingQuestion = buildPair(pendingInput, null, false);
		}

		return new ConversationHistory(pairs, pendingQuestion);
	}

	private static Map<String, Object> buildPair(InputMessage question, ResponseMessage answer, boolean complete) {
		Map<String, Object> pair = new LinkedHashMap<>();
		String questionText = question != null ? firstNonBlank(question.getInputUIPrompt(), question.getInputPrompt())
				: null;
		pair.put("question", questionText);
		pair.put("questionId", question != null ? question.getMessageId() : null);
		pair.put("questionDate", question != null ? question.getDateCreated() : null);
		pair.put("questionType",
				question != null && question.getMessageType() != null ? question.getMessageType().name() : null);

		String answerText = answer != null ? extractAnswerText(answer) : null;
		pair.put("answer", answerText);
		pair.put("answerId", answer != null ? answer.getMessageId() : null);
		pair.put("answerDate", answer != null ? answer.getDateCreated() : null);
		pair.put("answerType",
				answer != null && answer.getMessageType() != null ? answer.getMessageType().name() : null);
		if (answer != null && answer.getThinking() != null && !answer.getThinking().isEmpty()) {
			pair.put("thinking", answer.getThinking());
		}
		if (answer != null && answer.hasToolResponses()) {
			pair.put("toolResponses", answer.getToolResponses());
		}
		pair.put("complete", complete);
		return pair;
	}

	private static String firstNonBlank(String first, String second) {
		if (first != null && !first.trim().isEmpty()) {
			return first;
		}
		if (second != null && !second.trim().isEmpty()) {
			return second;
		}
		return null;
	}

	private static String extractAnswerText(ResponseMessage response) {
		String answer = response.getContent();
		if (answer == null || answer.trim().isEmpty()) {
			answer = response.getThinking();
		}
		if ((answer == null || answer.trim().isEmpty()) && response.hasToolResponses()) {
			answer = response.getToolResponses().toString();
		}
		return answer;
	}

	private static String buildHistoryString(List<Map<String, Object>> pairs) {
		if (pairs == null || pairs.isEmpty()) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < pairs.size(); i++) {
			Map<String, Object> pair = pairs.get(i);
			String question = (String) pair.get("question");
			String answer = (String) pair.get("answer");
			sb.append("User: ").append(question != null ? question : "");
			sb.append("\nAssistant: ").append(answer != null ? answer : "");
			if (i < pairs.size() - 1) {
				sb.append("\n---\n");
			}
		}
		return sb.toString();
	}

	private static final class ConversationHistory {
		private final List<Map<String, Object>> pairs;
		private final Map<String, Object> pendingQuestion;

		private ConversationHistory(List<Map<String, Object>> pairs, Map<String, Object> pendingQuestion) {
			this.pairs = pairs;
			this.pendingQuestion = pendingQuestion;
		}
	}
}
