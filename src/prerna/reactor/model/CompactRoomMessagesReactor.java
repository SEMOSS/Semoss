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
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class CompactRoomMessagesReactor extends AbstractReactor {

    private static Logger classLogger = LogManager.getLogger(CompactRoomMessagesReactor.class);
    private static final String COMPACTION_TYPES_KEY = "compactionTypes";
    private static final String AUTO_DETECT_KEY = "autoDetect";
    private static final Integer KEEP_N_TRANSACTIONS = 2;

    /**
     * Fraction of the context window consumed by tool-result tokens that triggers
     * tool pruning in auto-detect mode. If tool tokens exceed this fraction the
     * TOOLS strategy is chosen; otherwise SUMMARY is used.
     */
    private static final double TOOL_TOKEN_RATIO_THRESHOLD = 0.25;

    private static final Set<String> VALID_COMPACTION_TYPES = new HashSet<>(
            Arrays.asList("TOOL_PRUNE", "SUMMARY"));

    public CompactRoomMessagesReactor() {
        this.keysToGet = new String[] {
                ReactorKeysEnum.ROOM_ID.getKey(),
                ReactorKeysEnum.PARENT_MESSAGE_ID.getKey(),
                COMPACTION_TYPES_KEY,
                AUTO_DETECT_KEY
        };
        this.keyRequired = new int[] { 1, 1, 0, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        User user = this.insight.getUser();
        String userId = user.getPrimaryLoginToken().getId();
        String autoDetectVal = this.keyValue.get(AUTO_DETECT_KEY);
        boolean autoDetect = "true".equalsIgnoreCase(autoDetectVal);

        String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
        if (roomId == null || roomId.isEmpty()) {
            throw new IllegalArgumentException("Room ID is required");
        }

        String parentMessageId = this.keyValue.get(ReactorKeysEnum.PARENT_MESSAGE_ID.getKey());
        if (parentMessageId == null || parentMessageId.isEmpty()) {
            throw new IllegalArgumentException("Parent Message ID is required");
        }

        // parse compaction types
        Set<String> requestedTypes = getCompactionTypes();

        // validate all requested types
        for (String type : requestedTypes) {
            if (!VALID_COMPACTION_TYPES.contains(type)) {
                throw new IllegalArgumentException(
                        "Invalid compaction type: '" + type + "'. Valid types: " + VALID_COMPACTION_TYPES);
            }
        }

        if (requestedTypes.isEmpty() && !autoDetect) {
            throw new IllegalArgumentException(
                    "At least one compaction type is required. Valid types: " + VALID_COMPACTION_TYPES);
        }

        // load room
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

        if (autoDetect) {
            if (!roomHasViewableModel) {
                classLogger.warn("No model id attached to room - attempting to clear out tool calls and responses");
                typeResults.add(addToolPruneKeyToMessage(messages, parentMessageId));
            } else {
                List<AbstractMessage> branch = MessageUtils.getMessageBranchFromParent(messages, parentMessageId);
                Map<String, Object> detectResult = detectAndCompact(room, parentMessageId, modelEngine, branch);
                if (detectResult != null) {
                    typeResults.add(detectResult);
                }
            }
        } else {
            if (requestedTypes.contains("TOOL_PRUNE")) {
                typeResults.add(addToolPruneKeyToMessage(messages, parentMessageId));
            }
            if (requestedTypes.contains("SUMMARY")) {
                typeResults.add(summarizeMessages(room, parentMessageId,
                        KEEP_N_TRANSACTIONS, modelEngine));
            }
        }

        return new NounMetadata(typeResults, PixelDataType.VECTOR);
    }

    /**
     * Examines the branch ending at {@code messageId} and chooses a compaction
     * strategy automatically:
     * <ul>
     * <li>If tool-result tokens account for more than
     * {@value #TOOL_TOKEN_RATIO_THRESHOLD} of the current context window,
     * apply {@code TOOLS} pruning (cheap, lossless).</li>
     * <li>Otherwise apply {@code SUMMARY} compaction to reduce overall history
     * size via LLM summarization.</li>
     * </ul>
     */
    private Map<String, Object> detectAndCompact(Room room, String messageId, IModelEngine modelEngine,
            List<AbstractMessage> branch) {
        List<AbstractMessage> messages = room.getMessages();

        // InputMessages carry a cumulative token count (full history up to and
        // including themselves). The tokens added by each tool-call/result cycle
        // equal the delta between an INPUT_TOOL_EXEC's cumulative count and the
        // preceding InputMessage's cumulative count.
        int toolTokens = 0;
        int prevInputCumulative = 0;
        for (AbstractMessage m : branch) {
            if (m instanceof InputMessage) {
                if (m.hasToolResultPart()) {
                    int delta = m.getTokensInMessage() - prevInputCumulative;
                    if (delta > 0) {
                        toolTokens += delta;
                    }
                }
                prevInputCumulative = m.getTokensInMessage();
            }
        }

        // The total token count can be found from summing the last two messages
        int currTokenCount = getLastMessageTokens(branch);

        boolean useToolPruning = currTokenCount > 0
                && (double) toolTokens / currTokenCount >= TOOL_TOKEN_RATIO_THRESHOLD;

        if (useToolPruning) {
            return addToolPruneKeyToMessage(messages, messageId);
        } else if (countTransactions(branch) > KEEP_N_TRANSACTIONS) {
            return summarizeMessages(room, messageId, KEEP_N_TRANSACTIONS, modelEngine);
        } else {
            return null;
        }
    }

    private Map<String, Object> addToolPruneKeyToMessage(List<AbstractMessage> messages, String parentMessageId) {
        Map<String, Object> result = new HashMap<>();
        result.put("type", "TOOL_PRUNE");

        for (AbstractMessage message : messages) {
            if (parentMessageId.equals(message.getMessageId())) {
                message.setPruneToolsAbove(true);
                result.put("success", true);
                return result;
            }
        }

        result.put("success", false);
        result.put("error", "No message found with id: " + parentMessageId);
        return result;
    }

    private Map<String, Object> summarizeMessages(Room room, String messageId, int keepNTransactions,
            IModelEngine modelEngine) {
        Map<String, Object> result = new HashMap<>();
        result.put("type", "SUMMARY");

        if (modelEngine == null) {
            result.put("success", false);
            result.put("error", "No model engine found for room");
            return result;
        }

        List<AbstractMessage> messages = room.getMessages();

        // Walk the parent chain from messageId to root, producing an ordered branch
        List<AbstractMessage> branch = MessageUtils.getMessageBranchFromParent(messages, messageId);
        if (countTransactions(branch) <= keepNTransactions) {
            result.put("success", false);
            result.put("error",
                    "Not enough transactions in chat to summarize - need at least " + (keepNTransactions + 1));
            return result;
        }

        int splitPoint = findTransactionSplitPoint(branch, keepNTransactions);
        if (splitPoint < 0) {
            result.put("success", false);
            result.put("error", "Could not determine transaction split point");
            return result;
        }
        List<AbstractMessage> toSummarize = new ArrayList<>(branch.subList(0, splitPoint));
        List<AbstractMessage> toKeep = new ArrayList<>(branch.subList(splitPoint, branch.size()));

        // Build a human-readable transcript of only the text-bearing messages
        StringBuilder summaryTranscript = new StringBuilder();
        for (AbstractMessage m : toSummarize) {
            String text = getMessageText(m);
            if (text == null || text.isBlank()) {
                continue;
            }
            String role = (m instanceof InputMessage) ? "User" : "Assistant";
            summaryTranscript.append(role).append(": ").append(text).append("\n\n");
        }

        int beforeSummaryTokenCount = getLastMessageTokens(toSummarize);

        if (summaryTranscript.isEmpty()) {
            result.put("success", false);
            result.put("error", "No text content found in messages to summarize");
            return result;
        }

        // Ask the LLM to summarize using a throw-away room (no history pollution)
        String summarizationPrompt = "Summarize the following conversation history concisely, "
                + "preserving all key facts, decisions, and context:\n\n"
                + summaryTranscript.toString().trim();

        Room throwawayRoom = RoomUtils.createRoomIfNotExists(null, this.insight, modelEngine, null);
        InputMessage summarizationMsg = InputMessage.builder(throwawayRoom)
                .withText(summarizationPrompt)
                .withModelType(modelEngine.getModelType())
                .withParamMap(new HashMap<>())
                .build();
        ResponseMessage summaryResponse = throwawayRoom.ask(summarizationMsg, modelEngine);
        String summaryText = summaryResponse != null ? summaryResponse.getContent() : null;
        if (summaryText == null || summaryText.isBlank()) {
            result.put("success", false);
            result.put("error", "Model could not generate a summary");
            return result;
        }

        // Build the summary placeholder as a new user message at the root of the kept
        // chain
        String summaryMessageText = "The following is a summary of the earlier conversation:\n\n" + summaryText;

        // Build a human-readable transcript of only the text-bearing messages
        StringBuilder keepTranscript = new StringBuilder();
        for (AbstractMessage m : toKeep) {
            String text = getMessageText(m);
            if (text == null || text.isBlank()) {
                continue;
            }
            String role = (m instanceof InputMessage) ? "User" : "Assistant";
            keepTranscript.append(role).append(": ").append(text).append("\n\n");
        }

        int lastMessagesTokenCount = getLastMessageTokens(toKeep);

        String lastNMessagesText = "The following is the last n messages verbatim:\n\n" + keepTranscript;

        String compactedTextMessage = summaryMessageText + "\n\n" + lastNMessagesText;

        InputMessage compactedMessage = InputMessage.builder(room)
                .withText("Summarizing Conversation")
                .withModelType(modelEngine.getModelType())
                .build();

        // Inherit the parent of the oldest pruned message so other branches stay intact
        compactedMessage.setParentMessageId(null);
        compactedMessage.setSummaryLeafMessageId(messageId);
        compactedMessage.setVisibile(false);

        // Disclaimer:
        // UI needs a non-zero token count
        // Can't pull token count for this message without sending to llm
        // Token count will get updated regardless in the next ask call
        compactedMessage.setTokensInMessage(2);

        // Pair the compacted input with a response message so the branch is complete
        ResponseMessage compactedResponse = ResponseMessage.builder()
                .withText(compactedTextMessage)
                .build();
        compactedResponse.setParentMessageId(compactedMessage.getMessageId());
        compactedResponse.setVisibile(false);
        // summary response tokens + last n messages tokens - original tokens in that
        // span (to avoid double counting)
        compactedResponse.setTokensInMessage(
                summaryResponse.getTokensInMessage() + lastMessagesTokenCount - beforeSummaryTokenCount);

        messages.add(compactedMessage);
        messages.add(compactedResponse);

        // room.setMessages(messages);
        ModelInferenceLogsUtils.llm2_updateRoomMessages(
                room.getId(),
                this.insight.getUser().getPrimaryLoginToken().getId(),
                room.getMessagesAsString());

        result.put("success", true);
        result.put("inputMessage", compactedMessage);
        result.put("responseMessage", compactedResponse);
        return result;
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

        // try named key first
        GenRowStruct grs = this.store.getNoun(COMPACTION_TYPES_KEY);
        if (grs != null && grs.size() > 0) {
            List<String> values = grs.getAllStrValues();
            for (String val : values) {
                if (val != null && !val.trim().isEmpty()) {
                    types.add(val.trim().toUpperCase());
                }
            }
        }

        // fall back to curRow if named key not found
        if (types.isEmpty() && this.curRow != null && this.curRow.size() > 1) {
            // index 0 is roomId, rest are compaction types
            for (int i = 1; i < this.curRow.size(); i++) {
                String val = this.curRow.get(i).toString();
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
    private static int findTransactionSplitPoint(List<AbstractMessage> branch, int keepNTransactions) {
        int total = countTransactions(branch);
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

    @Override
    public String getReactorDescription() {
        return "Compact room messages by stripping tool results, tool call arguments, and other bulky content. "
                + "Supported compaction types: TOOL_PRUNE, SUMMARY. ";
    }

    private int getLastMessageTokens(List<AbstractMessage> branch) {
        AbstractMessage lastMessageResponse = branch.getLast();
        AbstractMessage lastMessageInput = branch.get(branch.size() - 2);
        return lastMessageResponse.getTokensInMessage() + lastMessageInput.getTokensInMessage();
    }

}
