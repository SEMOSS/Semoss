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
package prerna.reactor.agent.hooks;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.Room;
import prerna.om.ThreadStore;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.IToolHook;

// Reference implementation of IToolHook. Logs name/iteration/param keys before dispatch and
// name/duration/success/result size after. Param values are intentionally not logged (PII / size).
// Enable in CONFIG_JSON: {"hooks": [{"kind": "log_tools"}]}
public final class LoggingToolHook implements IToolHook {

    private static final Logger logger = LogManager.getLogger(LoggingToolHook.class);

    // Cap how much of the result content we log per call.
    private static final int RESULT_PREVIEW_CHARS = 200;

    @Override
    public void beforeTool(AgentRunContext ctx,
                           String toolName,
                           String toolCallId,
                           Map<String, Object> params,
                           int iteration) {
        Room room = ctx == null ? null : ctx.getRoom();
        String roomId = room == null ? null : room.getId();
        String jobId  = ThreadStore.getJobId();
        logger.info(
                "[log_tools] BEFORE name={} callId={} iter={} room={} job={} paramKeys={}",
                toolName, toolCallId, iteration, roomId, jobId,
                params == null ? "[]" : params.keySet());
    }

    @Override
    public void afterTool(AgentRunContext ctx,
                          String toolName,
                          String toolCallId,
                          Map<String, Object> params,
                          String resultContent,
                          long durationMs,
                          boolean success,
                          int iteration) {
        Room room = ctx == null ? null : ctx.getRoom();
        String roomId = room == null ? null : room.getId();
        String jobId  = ThreadStore.getJobId();
        int resultLen = resultContent == null ? 0 : resultContent.length();
        String preview = preview(resultContent);
        logger.info(
                "[log_tools] AFTER  name={} callId={} iter={} success={} durMs={} resultLen={} room={} job={} preview={}",
                toolName, toolCallId, iteration, success, durationMs, resultLen, roomId, jobId, preview);
    }

    private static String preview(String s) {
        if (s == null) return "";
        String oneLine = s.replace('\n', ' ').replace('\r', ' ');
        return oneLine.length() <= RESULT_PREVIEW_CHARS
                ? oneLine
                : oneLine.substring(0, RESULT_PREVIEW_CHARS) + "...";
    }
}
