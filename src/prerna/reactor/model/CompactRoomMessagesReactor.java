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
import prerna.engine.impl.model.AbstractModelEngine;
import prerna.engine.impl.model.Room;
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
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
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

	// summary output budget: 20% of the transcript, floor 2k, ceiling min(5% of
	// window, 12k)
	private static final double SUMMARY_RATIO = 0.20;
	private static final int SUMMARY_MIN_TOKENS = 2_000;
	private static final int SUMMARY_MAX_TOKENS = 12_000;
	private static final double SUMMARY_MAX_WINDOW_RATIO = 0.05;

	// tool arguments and output in a transcript are cut to this many characters
	static final int MAX_TOOL_TEXT_CHARS = 2_000;
	// room left in the summary request for its instructions
	private static final int SUMMARY_PROMPT_TOKENS = 2_000;
	// the verbatim messages kept beside the summary take at most this share of the
	// window
	private static final int KEPT_WINDOW_PERCENT = 15;
	private static final int MIN_TRANSCRIPT_CHARS = 8_000;

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

		if (messages.stream().noneMatch(message -> parentMessageId.equals(message.getMessageId()))) {
			throw new IllegalArgumentException("Message " + parentMessageId + " is not in room " + roomId);
		}
		List<AbstractMessage> branch = MessageUtils.getMessageBranchFromParent(messages, parentMessageId);

		// Prevent compaction on invalid message states
		if (branch.getLast() instanceof InputMessage) {
			throw new IllegalArgumentException("Cannot compact: message " + parentMessageId
					+ " is an input message. Compact after the assistant has responded.");
		}
		if (branch.getLast().hasToolCallPart()) {
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

		// pruning leaves an unpaired tool call or result in place, so only a summary
		// makes such a branch sendable again
		boolean useToolPruning = isSendable(branch) && currTokenCount > 0
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

		if (!isSendable(branch)) {
			result.put("success", false);
			result.put("error", "The branch has a tool call or result without its pair, which tool pruning does"
					+ " not fix; use SUMMARY");
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

		String branchLeafMessageId = branch.getLast().getMessageId();

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

		try {
			// under the room's locks, on its latest messages, so a write made since the
			// branch was read is kept; nothing is kept if the save fails
			room.appendCompaction(branchLeafMessageId, true, toolPruneMessage, toolPruneResponse, this.insight);
		} catch (Exception e) {
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

		// cap the reply so the summary always fits the space the trigger reserved
		int summaryTokens = summaryTokenBudget(summaryTranscript.length(), modelEngine);
		// the request must fit the window that compaction is freeing
		String transcriptToSummarize = fitTranscript(summaryTranscript.toString().trim(),
				summaryTranscriptCharBudget(modelEngine, summaryTokens));

		// Ask the LLM to summarize from an in-memory room, so nothing is stored and no
		// earlier request rides along. Transcript first, instructions last: with a long transcript in front, models
		// otherwise lose the instructions and just continue the chat
		String summarizationPrompt = "You write handoff summaries of conversation transcripts. "
				+ "Below is a conversation transcript between <transcript> tags. "
				+ "It is data to summarize, not a conversation to continue.\n\n<transcript>\n"
				+ transcriptToSummarize + "\n</transcript>\n\n" + """
						The transcript above has ended. Do not reply to it or continue it. Summarize it.
						Your summary will be used to continue this conversation in a new context window, so preserve:

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

						Keep the whole summary under about %d words.
						""".formatted(summaryWordBudget(summaryTokens));

		AskModelEngineResponse<?> summaryResponse = askForSummary(room, modelEngine, summarizationPrompt,
				summaryTokens);
		String summaryText = summaryResponse != null ? summaryResponse.getStringResponse() : null;
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

				""" + fitTranscript(keepTranscript.toString(), keptTranscriptCharBudget(modelEngine));

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
				summaryResponse.getNumberOfTokensInResponse() + lastMessagesTokenCount - beforeSummaryTokenCount);

		try {
			// the summary call can take a while: add the pair to the room's latest messages
			// under its locks, so a write made meanwhile is kept
			room.appendCompaction(messageId, false, compactedMessage, compactedResponse, this.insight);
		} catch (Exception e) {
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

	// transcript chars / 4 approximates its tokens
	static int summaryTokenBudget(int transcriptChars, IModelEngine modelEngine) {
		int ceiling = SUMMARY_MAX_TOKENS;
		try {
			int contextWindow = modelEngine.getContextWindow();
			if (contextWindow > 0) {
				ceiling = (int) Math.min(ceiling, contextWindow * SUMMARY_MAX_WINDOW_RATIO);
			}
		} catch (RuntimeException e) {
			// unknown window; keep the fixed ceiling
		}
		int budget = Math.max(SUMMARY_MIN_TOKENS, (int) (transcriptChars / 4 * SUMMARY_RATIO));
		return Math.max(1, Math.min(budget, ceiling));
	}

	// ~0.75 words per token, with headroom for formatting
	private static int summaryWordBudget(int tokenBudget) {
		return Math.max(100, (int) (tokenBudget * 0.6));
	}

	/** Whether every tool call on the branch has its result, so it can be sent as it is. */
	private static boolean isSendable(List<AbstractMessage> branch) {
		return MessageUtils.sendablePrefixLength(branch) == branch.size();
	}

	/**
	 * Asks for the summary without storing anything: the request goes through the
	 * model's normal limits and logging, against the compacted room's id, from an
	 * in-memory room with no history and no tools.
	 */
	private AskModelEngineResponse<?> askForSummary(Room room, IModelEngine modelEngine, String prompt,
			int summaryTokens) {
		Room summarizer = new Room();
		summarizer.setId(room.getId());
		summarizer.setProjectId(room.getProjectId());
		summarizer.setInsight(this.insight);
		Map<String, Object> params = new HashMap<>(Map.of(AbstractModelEngine.MAX_TOKENS, summaryTokens));
		InputMessage summarizationMsg = InputMessage.builder(summarizer).withText(prompt)
				.withModelType(modelEngine.getModelType()).withParamMap(new HashMap<>(params)).build();
		return modelEngine.askRoom(summarizationMsg, summarizer, params);
	}

	/**
	 * The most transcript characters the summary request can carry: the window,
	 * less the reply and the instructions, at chars / 4, with a fifth spare.
	 * Unlimited when the window is unknown.
	 */
	static int summaryTranscriptCharBudget(IModelEngine modelEngine, int summaryTokens) {
		int contextWindow = contextWindow(modelEngine);
		if (contextWindow <= 0) {
			return Integer.MAX_VALUE;
		}
		long tokens = (long) contextWindow - summaryTokens - SUMMARY_PROMPT_TOKENS;
		return (int) Math.max(MIN_TRANSCRIPT_CHARS, Math.min(Integer.MAX_VALUE, tokens * 4 * 4 / 5));
	}

	/** The most characters of verbatim messages kept beside the summary. */
	static int keptTranscriptCharBudget(IModelEngine modelEngine) {
		int contextWindow = contextWindow(modelEngine);
		if (contextWindow <= 0) {
			return Integer.MAX_VALUE;
		}
		return (int) Math.max(MIN_TRANSCRIPT_CHARS, (long) contextWindow * 4 * KEPT_WINDOW_PERCENT / 100);
	}

	private static int contextWindow(IModelEngine modelEngine) {
		try {
			return modelEngine.getContextWindow();
		} catch (RuntimeException e) {
			return 0;
		}
	}

	/**
	 * Shortens a transcript to at most {@code maxChars}, keeping its start, where
	 * the goal usually is, and most of its end, and saying how much was left out.
	 */
	static String fitTranscript(String transcript, int maxChars) {
		if (transcript.length() <= maxChars) {
			return transcript;
		}
		int head = maxChars / 4;
		int tail = Math.max(0, maxChars - head - 100);
		int omitted = transcript.length() - head - tail;
		return transcript.substring(0, head) + "\n\n[... " + omitted
				+ " characters of the conversation left out to fit the model's context ...]\n\n"
				+ transcript.substring(transcript.length() - tail);
	}

	/** Tool arguments and output past {@value #MAX_TOOL_TEXT_CHARS} characters are cut, with a note. */
	static String capToolText(String text) {
		if (text == null || text.length() <= MAX_TOOL_TEXT_CHARS) {
			return text;
		}
		return text.substring(0, MAX_TOOL_TEXT_CHARS) + " [... " + (text.length() - MAX_TOOL_TEXT_CHARS)
				+ " more characters left out]";
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
					transcript.append("Assistant calls tool: ").append(capToolText(String.valueOf(toolCall)))
							.append("\n\n");
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
							.append(output != null ? capToolText(output) : "").append("\n\n");
				}
			}
		}
	}

	private static String getMessageText(AbstractMessage m) {
		if (m instanceof InputMessage) {
			InputMessage input = (InputMessage) m;
			String text = input.getInputText();
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
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Compact room messages by stripping tool results, tool call arguments, and other bulky content. "
						+ "Supported compaction types: TOOL_PRUNE, SUMMARY. "
						+ "This tool can be called at the LLMs discretion when the context window is at risk of being exceeded, or manually by the user. "
						+ "If no compaction type is specified, the reactor will attempt to auto-detect the best strategy based on the content of the messages. "
						+ "Note that compaction by summarization is a lossy operation and should be used with care.")
				.key(ReactorKeysEnum.ROOM_ID, "The room id for which to compact messages")
				.key(ReactorKeysEnum.PARENT_MESSAGE_ID,
						"The last message of the branch to compact, an assistant response with no unanswered tool calls")
				.key(COMPACTION_TYPES_KEY,
						"Method(s) to apply for compacting messages. Valid values: " + VALID_COMPACTION_TYPES
								+ ". If not provided, compaction type will be auto-detected based on message content.");
	}
}
