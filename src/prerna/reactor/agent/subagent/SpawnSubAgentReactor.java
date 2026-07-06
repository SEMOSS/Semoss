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
package prerna.reactor.agent.subagent;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.AgentHarnessRegistry;
import prerna.reactor.agent.AgentRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Platform reactor for spawning a subagent from Pixel - same code path the
 * semoss harness's built-in {@code SpawnSubAgent} / named-subagent tools use,
 * exposed as a first-class Pixel operation so frontends and scripts can drive
 * subagents directly.
 *
 * <h3>Pixel syntax</h3>
 * <pre>{@code
 * SpawnSubAgent(prompt='write a haiku')                                 -- anonymous
 * SpawnSubAgent(prompt='plan eastern leg', workspaceId='<uuid>')        -- named-by-workspace
 * }</pre>
 *
 * <p>Returns a JSON string {@code {"jobId":..., "roomId":..., "alias":...}}.
 * The caller uses {@code jobId} with {@link WaitSubAgentReactor} or
 * {@link CheckSubAgentReactor} to collect / monitor the run.
 */
public class SpawnSubAgentReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(SpawnSubAgentReactor.class);

    private static final String ALIAS_KEY                  = "alias";
    private static final String CONTEXT_KEY                = "context";
    private static final String HARNESS_TYPE_KEY           = "harnessType";
    /** When true, the child's working directory is the parent's room folder (shared
     *  filesystem). Stream + history isolation is preserved (child still gets its own
     *  roomId / jobId). Default: false -> child uses its own room folder. */
    private static final String INHERIT_PARENT_WORKDIR_KEY = "inherit_parent_workdir";
    private static final Gson GSON = new Gson();

    public SpawnSubAgentReactor() {
        this.keysToGet = new String[] {
                ReactorKeysEnum.PROMPT.getKey(),       // 0 required
                ALIAS_KEY,                              // 1 optional
                ReactorKeysEnum.WORKSPACE_ID.getKey(), // 2 optional
                CONTEXT_KEY,                            // 3 optional
                ReactorKeysEnum.ENGINE.getKey(),       // 4 optional
                HARNESS_TYPE_KEY,                       // 5 optional
                ReactorKeysEnum.ROOM_ID.getKey(),      // 6 optional - defaults to insight's current room
                INHERIT_PARENT_WORKDIR_KEY              // 7 optional
        };
        this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0, 0, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String prompt      = this.keyValue.get(ReactorKeysEnum.PROMPT.getKey());
        String alias       = StringUtils.trimToNull(this.keyValue.get(ALIAS_KEY));
        String workspaceId = StringUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey()));
        String context     = StringUtils.trimToNull(this.keyValue.get(CONTEXT_KEY));
        String engine      = StringUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.ENGINE.getKey()));
        String harnessType = StringUtils.trimToNull(this.keyValue.get(HARNESS_TYPE_KEY));
        String roomId      = StringUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey()));
        boolean inheritParentWorkdir = Boolean.parseBoolean(
                StringUtils.trimToEmpty(this.keyValue.get(INHERIT_PARENT_WORKDIR_KEY)));

        if (prompt == null || prompt.trim().isEmpty()) {
            throw new IllegalArgumentException("prompt is required for SpawnSubAgent");
        }
        if (roomId == null) {
            roomId = this.insight.getRoomId();
        }
        if (roomId == null || roomId.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "SpawnSubAgent requires roomId, either as a parameter or via the calling insight's current room");
        }
        if (workspaceId != null) {
            User user = this.insight != null ? this.insight.getUser() : null;
            if (user != null && !SecurityProjectUtils.userCanViewProject(user, workspaceId)) {
                throw new IllegalArgumentException(
                        "Workspace " + workspaceId + " does not exist or user does not have access to the workspace");
            }
        }
        if (harnessType != null && AgentHarnessRegistry.get(harnessType) == null) {
            throw new IllegalArgumentException("Unknown harnessType: " + harnessType);
        }

        SpawnRequest req = new SpawnRequest();
        req.parentJobId       = ThreadStore.getJobId();
        req.parentRoomId      = roomId;
        req.alias             = alias;
        req.workspaceId       = workspaceId;
        req.prompt            = prompt;
        req.additionalContext = context;
        req.engine            = engine;
        req.harnessType       = harnessType;
        req.callerInsight     = this.insight;
        if (inheritParentWorkdir) {
            prerna.engine.impl.model.Room parentRoom =
                    prerna.engine.impl.model.RoomUtils.getOrLoadRoom(roomId, this.insight);
            Object wd = parentRoom.getOptionsMap() != null
                    ? parentRoom.getOptionsMap().get(AgentRunner.ROOM_OPTION_WORKING_DIR)
                    : null;
            req.workingDirOverride = (wd != null && !String.valueOf(wd).trim().isEmpty())
                    ? String.valueOf(wd).trim()
                    : parentRoom.getRoomFolderPath();
        }

        SpawnResult result = AgentSubAgentRegistry.getManager().spawn(req);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("jobId",  result.getJobId());
        out.put("runId",  result.getRunId());
        out.put("roomId", result.getRoomId());
        out.put("status", result.getStatus() == null ? null : result.getStatus().name());
        if (result.getAlias() != null) {
            out.put("alias", result.getAlias());
        }
        logger.info("SpawnSubAgentReactor: spawned jobId={} roomId={} alias={}",
                result.getJobId(), result.getRoomId(), result.getAlias());
        return new NounMetadata(GSON.toJson(out), PixelDataType.CONST_STRING);
    }

    @Override
    public String getReactorDescription() {
        return "Spawn a subagent AgentRun and return {runId, jobId, roomId, status, alias} as a JSON string. "
                + "Use jobId with WaitSubAgent / CheckSubAgentStatus.";
    }
}
