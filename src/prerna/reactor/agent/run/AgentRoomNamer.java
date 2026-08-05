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

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.util.Utility;

/**
 * Fire-and-forget background naming for agent-run rooms, mirroring the
 * playground's GenerateRoomName flow. Generates a short LLM title from the
 * run's initial user request and falls back to the truncated request text
 * when the model call fails.
 *
 * The rename runs on a daemon thread and must never block, delay, or fail
 * the agent run:
 * - the title ask uses a DETACHED Room instance loaded straight from the DB
 *   row (never the cached instance in the user's room hash), so it cannot
 *   contend for the run's room lock;
 * - persistence is a single conditional UPDATE that only replaces an unset
 *   name or the auto-derived truncated-input default - a name set by the
 *   user (e.g. via RenameRoom) is never overwritten;
 * - every failure is logged and swallowed.
 */
public final class AgentRoomNamer {

    private static final Logger logger = LogManager.getLogger(AgentRoomNamer.class);

    /** Must match the room-creation default in RoomUtils.createRoomRowIfMissing. */
    private static final int DEFAULT_NAME_CHAR_LIMIT = 100;

    /** Truncate the request before sending it to the model (same as GenerateRoomNameReactor). */
    private static final int PROMPT_CHAR_LIMIT = 500;

    private static final String TITLE_INSTRUCTION =
            "Generate a concise 3-5 word title summarizing the topic of the following user message. "
            + "Return ONLY the title. No punctuation, no quotes, no explanation.";

    private AgentRoomNamer() {
        /* static utility */
    }

    /**
     * Names the room from the run's initial user input on a background daemon
     * thread. Returns immediately; the agent run proceeds regardless of the
     * naming outcome.
     *
     * @param roomId  room to name
     * @param input   the user's original request for this run
     * @param modelId resolved model engine id used for title generation; when
     *                null/blank the name falls back to the truncated input
     * @param userId  owner user id on the ROOM row
     * @param insight caller insight used for the one-off model ask
     */
    public static void nameRoomAsync(String roomId, String input, String modelId, String userId, Insight insight) {
        if (roomId == null || roomId.trim().isEmpty()
                || input == null || input.trim().isEmpty()
                || userId == null || userId.trim().isEmpty()) {
            return;
        }
        Thread namer = new Thread(() -> {
            try {
                nameRoom(roomId, input, modelId, userId, insight);
            } catch (Exception e) {
                logger.warn("AgentRoomNamer: room rename failed for room='{}' - run unaffected: {}",
                        roomId, e.getMessage(), e);
            }
        }, "agent-room-namer-" + roomId);
        namer.setDaemon(true);
        namer.start();
    }

    private static void nameRoom(String roomId, String input, String modelId, String userId, Insight insight) {
        String defaultName = truncate(input.trim(), DEFAULT_NAME_CHAR_LIMIT);

        // Skip the model call entirely when the room already carries a custom name.
        String currentName = ModelInferenceLogsUtils.doGetRoomName(userId, roomId);
        if (currentName != null && !currentName.trim().isEmpty() && !currentName.equals(defaultName)) {
            return;
        }

        String title = generateTitle(roomId, input, modelId, userId, insight);
        if (title == null || title.trim().isEmpty()) {
            title = defaultName;
        }

        boolean updated = ModelInferenceLogsUtils.doSetNameForRoomIfDefault(userId, roomId, title, defaultName);
        if (updated) {
            syncCachedRoomName(roomId, title, insight);
            logger.info("AgentRoomNamer: room='{}' named '{}'", roomId, title);
        }
    }

    /**
     * One-off title generation mirroring GenerateRoomNameReactor: use_history
     * off and appendToHistory=false so nothing is written back to the room.
     * Runs against a detached Room instance so the shared room lock held by
     * the run's own asks is never touched.
     *
     * @return cleaned title, or {@code null} when generation is unavailable or fails
     */
    private static String generateTitle(String roomId, String input, String modelId, String userId, Insight insight) {
        if (modelId == null || modelId.trim().isEmpty()) {
            return null;
        }
        try {
            IModelEngine modelEngine = Utility.getModel(modelId);
            if (modelEngine == null) {
                return null;
            }
            Room detached = ModelInferenceLogsUtils.getRoomById(roomId, userId);
            if (detached == null) {
                return null;
            }
            detached.setInsight(insight);

            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("use_history", false);

            InputMessage inputMsg = InputMessage.builder(detached)
                    .withText(TITLE_INSTRUCTION + "\n\n" + truncate(input, PROMPT_CHAR_LIMIT))
                    .withModelType(modelEngine.getModelType())
                    .withParamMap(paramMap)
                    .build();

            ResponseMessage response = detached.ask(inputMsg, modelEngine, null, false);
            String raw = response == null ? null : response.getContent();
            if (raw == null || raw.trim().isEmpty()) {
                return null;
            }
            String title = raw.trim()
                    .replaceAll("^[\"']+|[\"']+$", "")
                    .replaceAll("\\s+", " ")
                    .trim();
            return truncate(title, DEFAULT_NAME_CHAR_LIMIT);
        } catch (Exception e) {
            logger.warn("AgentRoomNamer: title generation failed for room='{}', falling back to truncated input: {}",
                    roomId, e.getMessage());
            return null;
        }
    }

    /**
     * Best-effort sync of the cached in-memory Room so a later
     * persist(room, userId, roomName, engineId) from Room.ask's name backfill
     * does not resurrect the old name.
     */
    private static void syncCachedRoomName(String roomId, String title, Insight insight) {
        try {
            User user = insight == null ? null : insight.getUser();
            if (user == null) {
                return;
            }
            Room cached = user.getRoomHash().get(roomId);
            if (cached != null) {
                cached.setRoomName(title);
            }
        } catch (Exception e) {
            logger.debug("AgentRoomNamer: cached room name sync skipped for room='{}': {}", roomId, e.getMessage());
        }
    }

    private static String truncate(String value, int max) {
        if (value == null) {
            return null;
        }
        return value.substring(0, Math.min(value.length(), max));
    }
}
