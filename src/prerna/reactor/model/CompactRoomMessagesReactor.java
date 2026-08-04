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
package prerna.reactor.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.message.ToolCallMessagePart;
import prerna.engine.impl.model.message.ToolResultMessagePart;
import prerna.engine.impl.model.message.ToolResultPart;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class CompactRoomMessagesReactor extends AbstractReactor {

	private static Logger classLogger = LogManager.getLogger(CompactRoomMessagesReactor.class);
	private static final String COMPACTION_TYPES_KEY = "compactionTypes";
	private static final Integer KEEP_N_TRANSACTIONS = 2;

	/**
	 * Fraction of the context window consumed by tool-result tokens that triggers
	 * tool pruning in auto-detect mode. If tool tokens exceed this fraction the
	 * TOOLS strategy is chosen; otherwise SUMMARY is used.
	 */
	private static final double TOOL_TOKEN_RATIO_THRESHOLD = 0.25;

	private static final Set<String> VALID_COMPACTION_TYPES = new HashSet<>(Arrays.asList("TOOL_PRUNE", "SUMMARY"));

	public CompactRoomMessagesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.PARENT_MESSAGE_ID.getKey(),
				COMPACTION_TYPES_KEY, };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();

		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		if (roomId == null || roomId.isEmpty()) {
			throw new IllegalArgumentException("Room ID is required");
		}
		String userId = user.getPrimaryLoginToken().getId();
		ModelInferenceLogsUtils.validUserRoom(roomId, userId);

		String parentMessageId = this.keyValue.get(ReactorKeysEnum.PARENT_MESSAGE_ID.getKey());
		if (parentMessageId == null || parentMessageId.isEmpty()) {
			throw new IllegalArgumentException("Parent Message ID is required");
		}

		// consider validating the parentMessageId is a leaf node - FE blocks for now

		Set<String> requestedTypes = getCompactionTypes();
		for (String type : requestedTypes) {
			if (!VALID_COMPACTION_TYPES.contains(type)) {
				throw new IllegalArgumentException(
						"Invalid compaction type: '" + type + "'. Valid types: " + VALID_COMPACTION_TYPES);
			}
		}

		boolean autoDetect = requestedTypes.isEmpty();

		Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		if (room == null) {
			throw new IllegalArgumentException("Room not found for id: " + roomId);
		}

		List<AbstractMessage> messages = room.getMessages();
		if (messages == null || messages.isEmpty()) {
			Map<String, Object> emptyResult = new HashMap<>();
			emptyResult.put("success", false);
			emptyResult.put("types", new ArrayList<>());
			return new NounMetadata(emptyResult, PixelDataType.MAP);
		}

		String modelId = null;
		boolean roomHasViewableModel = false;
		IModelEngine modelEngine = null;

		Map<String, Object> optionsMap = room.getOptionsMap();
		if (optionsMap.containsKey("modelId")) {
			modelId = (String) optionsMap.get("modelId");
		}

		if (modelId != null && SecurityEngineUtils.userCanViewEngine(user, modelId)) {
			roomHasViewableModel = true;
			modelEngine = Utility.getModel(modelId);
		}

		List<Map<String, Object>> typeResults = new ArrayList<>();

		List<AbstractMessage> branch = MessageUtils.getMessageBranchFromParent(messages, parentMessageId);

		// Prevent compaction on invalid message states
		if (!branch.isEmpty() && (branch.getLast() instanceof InputMessage || branch.getLast().hasToolCallPart())) {
			throw new IllegalArgumentException("Cannot compact: message " + parentMessageId
					+ " is an input message. Compact after the assistant has responded.");
		}

		if (!branch.isEmpty() && branch.getLast().hasToolCallPart()) {
			throw new IllegalArgumentException("Cannot compact: message " + parentMessageId
					+ " has unanswered tool calls. Compact once the tool call has been resolved.");
		}

		if (autoDetect) {
			if (!roomHasViewableModel) {
				classLogger.warn("No model id attached to room - attempting to clear out tool calls and responses");
				typeResults.add(addToolPruneKeyToMessage(branch, parentMessageId, room, 0));
			} else {

				Map<String, Object> detectResult = detectAndCompact(room, parentMessageId, modelEngine, branch);
				if (detectResult != null) {
					typeResults.add(detectResult);
				}
			}
		} else {
			if (requestedTypes.contains("TOOL_PRUNE")) {
				typeResults.add(addToolPruneKeyToMessage(branch, parentMessageId, room, 0));
			}
			if (requestedTypes.contains("SUMMARY")) {
				int txCount = countTransactions(branch);
				typeResults.add(
						summarizeMessages(room, parentMessageId, branch, txCount, KEEP_N_TRANSACTIONS, modelEngine));
			}
		}

		return new NounMetadata(typeResults, PixelDataType.VECTOR);
	}

	/**
	 * Examines the branch ending at {@code messageId} and chooses a compaction
	 * strategy automatically:
	 * <ul>
	 * <li>If tool tokens account for more than {@value #TOOL_TOKEN_RATIO_THRESHOLD}
	 * of the current context window, apply {@code TOOLS} pruning (cheap,
	 * lossless).</li>
	 * <li>Otherwise apply {@code SUMMARY} compaction to reduce overall history size
	 * via LLM summarization.</li>
	 * </ul>
	 */
	private Map<String, Object> detectAndCompact(Room room, String messageId, IModelEngine modelEngine,
			List<AbstractMessage> branch) {

		int toolTokens = computeToolTokens(branch);
		int currTokenCount = getLastMessageTokens(branch);

		boolean useToolPruning = currTokenCount > 0
				&& (double) toolTokens / currTokenCount >= TOOL_TOKEN_RATIO_THRESHOLD;

		if (useToolPruning) {
			return addToolPruneKeyToMessage(branch, messageId, room, toolTokens);
		} else {
			int txCount = countTransactions(branch);
			if (txCount > KEEP_N_TRANSACTIONS) {
				return summarizeMessages(room, messageId, branch, txCount, KEEP_N_TRANSACTIONS, modelEngine);
			} else {
				return null;
			}
		}
	}

	private Map<String, Object> addToolPruneKeyToMessage(List<AbstractMessage> branch, String parentMessageId,
			Room room, int toolTokens) {
		Map<String, Object> result = new HashMap<>();
		result.put("type", "TOOL_PRUNE");

		if (branch.size() < 2) {
			result.put("success", false);
			result.put("error", "Not enough messages in room to apply tool pruning");
			return result;
		}

		if (toolTokens == 0) {
			toolTokens = computeToolTokens(branch);
		}

		int branchTokens = getLastMessageTokens(branch);

		InputMessage toolPruneMessage = InputMessage.builder(room).withText("Pruning Tools For Future Messages")
				.build();

		// Inherit the parent of the oldest pruned message so other branches stay intact
		toolPruneMessage.setParentMessageId(branch.getLast().getMessageId());
		toolPruneMessage.setVisible(false);

		// Set token count to last input + response - expected tokens pruned
		// Will be off by a few tokens but it will get fixed anyway on the next message
		toolPruneMessage.setTokensInMessage(branchTokens - toolTokens);
		toolPruneMessage.setPruneToolsAbove(true);
		// Set the prune flag on the message itself for the FE to render the UI

		List<AbstractMessage> messages = room.getMessages();
		String branchLeafMessageId = branch.getLast().getMessageId();
		AbstractMessage leafToFlag = null;
		for (AbstractMessage m : messages) {
			if (m.getMessageId().equals(branchLeafMessageId)) {
				leafToFlag = m;
				break;
			}
		}
		boolean priorLeafFlag = leafToFlag != null && leafToFlag.getPruneToolsAbove();
		if (leafToFlag != null) {
			leafToFlag.setPruneToolsAbove(true);
		}

		// Pair the compacted input with a response message so the branch is complete
		ResponseMessage toolPruneResponse = ResponseMessage.builder()
				.withText("This conversation was compacted at the user's request - "
						+ "intermediate tool call results have been pruned from the context "
						+ "to free up space. If asked whether the conversation was compacted, "
						+ "confirm that it was. Use the term \"compacted\" by default (matching "
						+ "what the user sees in the UI), but feel free to explain that tool "
						+ "results were pruned if the user asks for specifics.")
				.build();
		toolPruneResponse.setParentMessageId(toolPruneMessage.getMessageId());
		toolPruneResponse.setVisible(false);

		// summary response tokens + last n messages tokens - original tokens in that
		// span (to avoid double counting)
		toolPruneResponse.setTokensInMessage(65);

		messages.add(toolPruneMessage);
		messages.add(toolPruneResponse);

		try {
			RoomMessageStore.persist(room, this.insight.getUser().getPrimaryLoginToken().getId());
		} catch (Exception e) {
			// Roll back the in-memory mutations so the cached Room stays in sync with the
			// DB.
			// Room objects are process-cached (RoomUtils), so leaking dirty state here
			// would
			// corrupt future requests for this room until the cache is evicted.
			messages.remove(toolPruneResponse);
			messages.remove(toolPruneMessage);
			if (leafToFlag != null) {
				leafToFlag.setPruneToolsAbove(priorLeafFlag);
			}
			classLogger.error("Failed to persist TOOL_PRUNE compaction for room {}; rolled back in-memory changes",
					room.getId(), e);
			result.put("success", false);
			result.put("error", "Failed to persist compaction: " + e.getMessage());
			return result;
		}

		result.put("success", true);
		result.put("inputMessage", toolPruneMessage);
		result.put("responseMessage", toolPruneResponse);
		return result;
	}

	private Map<String, Object> summarizeMessages(Room room, String messageId, List<AbstractMessage> branch,
			int transactionCount, int keepNTransactions, IModelEngine modelEngine) {
		Map<String, Object> result = new HashMap<>();
		result.put("type", "SUMMARY");

		if (modelEngine == null) {
			result.put("success", false);
			result.put("error", "No model engine found for room");
			return result;
		}

		if (transactionCount <= keepNTransactions) {
			result.put("success", false);
			result.put("error",
					"Not enough transactions in chat to summarize - need at least " + (keepNTransactions + 1));
			return result;
		}

		int splitPoint = findTransactionSplitPoint(branch, keepNTransactions, transactionCount);
		if (splitPoint < 0) {
			result.put("success", false);
			result.put("error", "Could not determine transaction split point");
			return result;
		}
		List<AbstractMessage> toSummarize = new ArrayList<>(branch.subList(0, splitPoint));
		List<AbstractMessage> toKeep = new ArrayList<>(branch.subList(splitPoint, branch.size()));

		// Build a human-readable transcript of the messages to summarize
		StringBuilder summaryTranscript = new StringBuilder();
		for (AbstractMessage m : toSummarize) {
			appendMessageToTranscript(m, summaryTranscript);
		}

		int beforeSummaryTokenCount = getLastMessageTokens(toSummarize);

		if (summaryTranscript.isEmpty()) {
			result.put("success", false);
			result.put("error", "No text content found in messages to summarize");
			return result;
		}

		// Ask the LLM to summarize using a throw-away room (no history pollution)
		String summarizationPrompt = """
				Summarize the following conversation history. Your summary will be used to
				continue this conversation in a new context window, so preserve:

				- The user's current goal and any sub-tasks
				- Decisions made and the reasoning behind them
				- User preferences, constraints, or requirements stated
				- Any unresolved questions or open threads
				- Critical specifics (names, values, code, file paths, etc.) - quote these verbatim
				- Outcomes of any tool calls or actions taken (search results used, files
				read, APIs called, etc.)
				- Overall communication style (tone, formality, verbosity)

				Be concise but do not omit anything that would be needed to seamlessly
				continue the conversation. Write in present tense as if briefing someone
				taking over the conversation. Only summarize up to the point the conversation
				ends - do not speculate about what comes next.

				Begin your response with the literal line "[SUMMARY]" before any other text.

				""" + summaryTranscript.toString().trim();

		Room throwawayRoom = RoomUtils.createRoomIfNotExists(null, this.insight, modelEngine, null);
		InputMessage summarizationMsg = InputMessage.builder(throwawayRoom).withText(summarizationPrompt)
				.withModelType(modelEngine.getModelType()).withParamMap(new HashMap<>()).build();
		ResponseMessage summaryResponse = throwawayRoom.ask(summarizationMsg, modelEngine);
		String summaryText = summaryResponse != null ? summaryResponse.getContent() : null;
		if (summaryText == null || summaryText.isBlank()) {
			result.put("success", false);
			result.put("error", "Model could not generate a summary");
			return result;
		}

		// Build a human-readable transcript of the messages to keep verbatim
		StringBuilder keepTranscript = new StringBuilder();

		for (AbstractMessage m : toKeep) {
			appendMessageToTranscript(m, keepTranscript);
		}

		int lastMessagesTokenCount = getLastMessageTokens(toKeep); // need this to determine tokens used by verbatim
																	// messages
		String compactedTextMessage = """
				You are continuing an ongoing conversation. The user compacted this
				conversation - earlier messages have been condensed and replaced with a
				summary below to free up context space.

				If asked whether this conversation was compacted, always confirm that it
				was, since the user initiated it. When describing what happened, use the
				term "compacted" by default (matching what the user sees in the UI), but
				feel free to elaborate - that earlier messages were summarized - if the
				user asks for more detail.

				For questions about specific details from before the compaction: only
				confirm them if they appear explicitly in the summary. If a detail is
				absent, be honest that you cannot verify it since those earlier messages
				are no longer available to you.

				Continue in the tone and style described in the summary and demonstrated
				by the verbatim messages below.

				[SUMMARY]
				""" + summaryText.trim() + """


				The following are the most recent messages verbatim:

				""" + keepTranscript;

		// Build the summary placeholder as a new user message at the root of the kept
		// chain
		InputMessage compactedMessage = InputMessage.builder(room).withText("Summarizing Conversation")
				.withModelType(modelEngine.getModelType()).build();

		// Inherit the parent of the oldest pruned message so other branches stay intact
		compactedMessage.setParentMessageId(null);
		compactedMessage.setSummaryLeafMessageId(messageId);
		compactedMessage.setVisible(false);

		// Disclaimer:
		// UI needs a non-zero token count
		// Can't pull token count for this message without sending to llm
		// Token count will get updated regardless in the next ask call
		compactedMessage.setTokensInMessage(175);

		// Pair the compacted input with a response message so the branch is complete
		ResponseMessage compactedResponse = ResponseMessage.builder().withText(compactedTextMessage).build();
		compactedResponse.setParentMessageId(compactedMessage.getMessageId());
		compactedResponse.setVisible(false);

		// summary response tokens + last n messages tokens - original tokens in that
		// span (to avoid double counting)
		compactedResponse.setTokensInMessage(
				summaryResponse.getTokensInMessage() + lastMessagesTokenCount - beforeSummaryTokenCount);

		List<AbstractMessage> messages = room.getMessages();
		messages.add(compactedMessage);
		messages.add(compactedResponse);

		try {
			RoomMessageStore.persist(room, this.insight.getUser().getPrimaryLoginToken().getId());
		} catch (Exception e) {
			// Roll back the in-memory mutations so the cached Room stays in sync with the
			// DB.
			// Room objects are process-cached (RoomUtils), so leaking dirty state here
			// would
			// corrupt future requests for this room until the cache is evicted.
			messages.remove(compactedResponse);
			messages.remove(compactedMessage);
			classLogger.error("Failed to persist SUMMARY compaction for room {}; rolled back in-memory changes",
					room.getId(), e);
			result.put("success", false);
			result.put("error", "Failed to persist compaction: " + e.getMessage());
			return result;
		}

		result.put("success", true);
		result.put("inputMessage", compactedMessage);
		result.put("responseMessage", compactedResponse);
		return result;
	}

	private static void appendMessageToTranscript(AbstractMessage m, StringBuilder transcript) {
		String text = getMessageText(m);
		if (text != null && !text.isBlank()) {
			transcript.append((m instanceof InputMessage) ? "User" : "Assistant").append(": ").append(text)
					.append("\n\n");
		}
		if (m.hasToolCallPart()) {
			for (MessagePart part : m.getParts()) {
				if (part instanceof ToolCallMessagePart) {
					Map<String, Object> toolCall = ((ToolCallMessagePart) part).getToolCall();
					transcript.append("Assistant calls tool: ").append(toolCall).append("\n\n");
				}
			}
		}
		if (m.hasToolResultPart()) {
			for (MessagePart part : m.getParts()) {
				if (part instanceof ToolResultMessagePart) {
					ToolResultPart result = ((ToolResultMessagePart) part).getToolResult();
					if (result == null) {
						continue;
					}
					String toolName = result.getToolName();
					String output = result.getOutput();
					transcript.append("Tool result").append(toolName != null ? " for " + toolName : "").append(": ")
							.append(output != null ? output : "").append("\n\n");
				}
			}
		}
	}

	private static String getMessageText(AbstractMessage m) {
		if (m instanceof InputMessage) {
			InputMessage input = (InputMessage) m;
			String text = input.getInputPrompt();
			if (text == null || text.isBlank()) {
				text = input.getInputUIPrompt();
			}
			return text;
		} else if (m instanceof ResponseMessage) {
			return ((ResponseMessage) m).getContent();
		}
		return null;
	}

	private Set<String> getCompactionTypes() {
		Set<String> types = new HashSet<>();
		GenRowStruct grs = this.store.getGenRowStruct(COMPACTION_TYPES_KEY);
		if (grs != null && grs.size() > 0) {
			List<String> values = grs.getAllStrValues();
			for (String val : values) {
				if (val != null && !val.trim().isEmpty()) {
					types.add(val.trim().toUpperCase());
				}
			}
		}
		return types;
	}

	/**
	 * A transaction ends at a ResponseMessage that is not a tool-call response
	 * (i.e., the final assistant reply, not an intermediate RESPONSE_TOOL).
	 */
	private static boolean isTransactionEnd(AbstractMessage m) {
		return m instanceof ResponseMessage && !m.hasToolCallPart();
	}

	private static int countTransactions(List<AbstractMessage> branch) {
		int count = 0;
		for (AbstractMessage m : branch) {
			if (isTransactionEnd(m)) {
				count++;
			}
		}
		return count;
	}

	/**
	 * Returns the index of the first message that belongs to the last
	 * {@code keepNTransactions} transactions. Messages before this index are
	 * summarized; messages from this index onward are kept verbatim.
	 */
	private static int findTransactionSplitPoint(List<AbstractMessage> branch, int keepNTransactions, int total) {
		int transactionsToSummarize = total - keepNTransactions;
		int endsSeen = 0;
		for (int i = 0; i < branch.size(); i++) {
			if (isTransactionEnd(branch.get(i))) {
				endsSeen++;
				if (endsSeen == transactionsToSummarize) {
					return i + 1;
				}
			}
		}
		return -1;
	}

	/**
	 * Counts tokens contributed by tool-call/result cycles that are still in the
	 * live context. Walks leaf-to-root and stops at the latest pruneToolsAbove
	 * marker since everything at/above that point has already been stripped.
	 */
	private static int computeToolTokens(List<AbstractMessage> branch) {
		int toolTokens = 0;
		AbstractMessage laterInput = null;
		for (int i = branch.size() - 1; i >= 0; i--) {
			AbstractMessage m = branch.get(i);
			if (m instanceof InputMessage) {
				if (laterInput != null && laterInput.hasToolResultPart()) {
					int delta = laterInput.getTokensInMessage() - m.getTokensInMessage();
					if (delta > 0) {
						toolTokens += delta;
					}
				}
				laterInput = m;
			}
			if (m.getPruneToolsAbove()) {
				break;
			}
		}
		return toolTokens;
	}

	private int getLastMessageTokens(List<AbstractMessage> branch) {
		if (branch == null || branch.size() < 2) {
			return 0;
		}
		AbstractMessage lastMessageResponse = branch.getLast();
		AbstractMessage lastMessageInput = branch.get(branch.size() - 2);
		return lastMessageResponse.getTokensInMessage() + lastMessageInput.getTokensInMessage();
	}

	@Override
	public String getReactorDescription() {
		return "Compact room messages by stripping tool results, tool call arguments, and other bulky content. "
				+ "Supported compaction types: TOOL_PRUNE, SUMMARY. "
				+ "This tool can be called at the LLMs discretion when the context window is at risk of being exceeded, or manually by the user. "
				+ "If no compaction type is specified, the reactor will attempt to auto-detect the best strategy based on the content of the messages. "
				+ "Note that compaction by summarization is a lossy operation and should be used with care.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "The room id for which to compact messages";
		} else if (key.equals(COMPACTION_TYPES_KEY)) {
			return "Method(s) to apply for compacting messages. Valid values: " + VALID_COMPACTION_TYPES
					+ ". If not provided, compaction type will be auto-detected based on message content.";
		}
		return super.getDescriptionForKey(key);
	}
}
