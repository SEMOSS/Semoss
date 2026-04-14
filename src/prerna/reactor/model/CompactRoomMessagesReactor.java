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
    private static final Integer KEEP_N_TRANSACTIONS = 3;

    /**
     * Fraction of the context window consumed by tool-result tokens that triggers
     * tool pruning in auto-detect mode. If tool tokens exceed this fraction the
     * TOOLS strategy is chosen; otherwise SUMMARY is used.
     */
    private static final double TOOL_TOKEN_RATIO_THRESHOLD = 0.25;

    private static final Set<String> VALID_COMPACTION_TYPES = new HashSet<>(
            Arrays.asList("TOOLS", "SUMMARY"));

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
                Map<String, Object> typeResult = addToolPruneKeyToMessage(messages, parentMessageId);
                if (typeResult != null)
                    typeResults.add(typeResult);
            } else {
                Map<String, Object> typeResult = detectAndCompact(room, parentMessageId, modelEngine);
                if (typeResult != null)
                    typeResults.add(typeResult);
            }
        } else {
            if (requestedTypes.contains("TOOLS")) {
                Map<String, Object> typeResult = addToolPruneKeyToMessage(messages, parentMessageId);
                if (typeResult != null)
                    typeResults.add(typeResult);
            }
            if (requestedTypes.contains("SUMMARY")) {
                Map<String, Object> typeResult = summarizeMessages(room, parentMessageId, KEEP_N_TRANSACTIONS * 2,
                        modelEngine);
                if (typeResult != null)
                    typeResults.add(typeResult);
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("success", (typeResults != null && !typeResults.isEmpty()));
        result.put("types", typeResults);
        return new NounMetadata(result, PixelDataType.MAP);
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
    private Map<String, Object> detectAndCompact(Room room, String messageId, IModelEngine modelEngine) {
        List<AbstractMessage> messages = room.getMessages();
        List<AbstractMessage> branch = MessageUtils.getMessageBranchFromParent(messages, messageId);

        int toolTokens = 0;
        for (AbstractMessage m : branch) {
            if (m.hasToolResultPart() || m.hasToolCallPart()) {
                toolTokens += m.getTokensInMessage();
            }
        }

        AbstractMessage lastMessage = branch.getLast();
        AbstractMessage lastMessageParent = branch.get(branch.size() - 2);
        int currTokenCount = lastMessage.getTokensInMessage() + lastMessageParent.getTokensInMessage();

        boolean useToolPruning = currTokenCount > 0
                && (double) toolTokens / currTokenCount >= TOOL_TOKEN_RATIO_THRESHOLD;

        if (useToolPruning) {
            return addToolPruneKeyToMessage(messages, messageId);
        } else {
            return summarizeMessages(room, messageId, KEEP_N_TRANSACTIONS * 2, modelEngine);
        }
    }

    private Map<String, Object> addToolPruneKeyToMessage(List<AbstractMessage> messages, String parentMessageId) {
        for (AbstractMessage message : messages) {
            if (parentMessageId.equals(message.getMessageId())) {
                message.setPruneToolsAbove(true);
                Map<String, Object> result = new HashMap<>();
                result.put("type", "tool_pruning");
                return result;
            }
        }
        return null;
    }

    private Map<String, Object> summarizeMessages(Room room, String messageId, int keepN, IModelEngine modelEngine) {
        if (modelEngine == null) {
            throw new IllegalArgumentException("No model engine found for room");
        }

        List<AbstractMessage> messages = room.getMessages();

        // Walk the parent chain from messageId to root, producing an ordered branch
        List<AbstractMessage> branch = MessageUtils.getMessageBranchFromParent(messages, messageId);
        if (branch.size() <= keepN) {
            return null; // not enough messages to warrant summarization
        }

        int splitPoint = branch.size() - keepN;
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

        if (summaryTranscript.isEmpty()) {
            return null;
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
            return null; // model couldn't summarize; leave messages untouched
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

        String lastNMessagesText = "The following is the last n messages verbatim:\n\n" + keepTranscript;

        String compactedTextMessage = summaryMessageText + "\n\n" + lastNMessagesText;

        InputMessage compactedMessage = InputMessage.builder(room)
                .withText("Summarizing Conversation")
                .withModelType(modelEngine.getModelType())
                .build();

        // Inherit the parent of the oldest pruned message so other branches stay intact
        compactedMessage.setParentMessageId(null);
        compactedMessage.setSiblingMessageId(messageId);
        compactedMessage.setVisibile(false);

        // Pair the compacted input with a response message so the branch is complete
        ResponseMessage compactedResponse = ResponseMessage.builder()
                .withText(compactedTextMessage)
                .build();
        compactedResponse.setParentMessageId(compactedMessage.getMessageId());
        compactedResponse.setVisibile(false);

        messages.add(compactedMessage);
        messages.add(compactedResponse);

        // room.setMessages(messages);
        ModelInferenceLogsUtils.llm2_updateRoomMessages(
                room.getId(),
                this.insight.getUser().getPrimaryLoginToken().getId(),
                room.getMessagesAsString());

        Map<String, Object> result = new HashMap<>();
        result.put("type", "summarization");
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

    @Override
    public String getReactorDescription() {
        return "Compact room messages by stripping tool results, tool call arguments, and other bulky content. "
                + "Supported compaction types: TOOLS, SUMMARY. ";
    }

}
