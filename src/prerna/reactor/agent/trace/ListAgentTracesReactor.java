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
package prerna.reactor.agent.trace;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.AgentTraceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists agent trace summaries for a given room.
 *
 * <p>Pixel: {@code ListAgentTraces(roomId=["room1"], limit=[50])}
 *
 * <p>Security: the requesting user must own or have access to the specified room, OR be an admin
 * to list across all rooms. Trace summaries do not include content — only IDs, timing, iteration
 * counts, and termination reasons.
 */
public class ListAgentTracesReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(ListAgentTracesReactor.class);

    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT     = 500;

    public ListAgentTracesReactor() {
        this.keysToGet  = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.LIMIT.getKey() };
        this.keyRequired = new int[] { 0, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        User user = this.insight.getUser();
        String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());

        int limit = DEFAULT_LIMIT;
        String limitStr = this.keyValue.get(ReactorKeysEnum.LIMIT.getKey());
        if (limitStr != null && !limitStr.isEmpty()) {
            try {
                limit = Math.min(Integer.parseInt(limitStr), MAX_LIMIT);
            } catch (NumberFormatException e) {
                classLogger.warn("ListAgentTracesReactor: invalid limit value '{}', using default.", limitStr);
            }
        }

        List<Map<String, Object>> traces;

        if (roomId == null || roomId.trim().isEmpty()) {
            // No roomId — admin-only: return traces across all rooms
            if (user == null || !SecurityAdminUtils.userIsAdmin(user)) {
                throw new IllegalArgumentException(
                        "roomId is required, or the caller must be an admin to list all traces");
            }
            traces = AgentTraceLogsUtils.listAllTraces(limit);
            classLogger.info("ListAgentTracesReactor: admin all-rooms query returned {} traces (limit={})",
                    traces.size(), limit);
        } else {
            // Room-scoped: verify user access
            Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
            if (room == null) {
                throw new IllegalArgumentException("Room not found: " + roomId);
            }
            traces = AgentTraceLogsUtils.listTraces(roomId, limit);
            classLogger.info("ListAgentTracesReactor: returned {} traces for room {} (limit={})",
                    traces.size(), roomId, limit);
        }

        return new NounMetadata(traces, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
    }

    @Override
    public String getReactorDescription() {
        return "Lists agent trace summaries for a given room, ordered by most recent first. "
                + "Returns metadata only — no tool call content. Use GetAgentTrace for a full trace.";
    }
}
