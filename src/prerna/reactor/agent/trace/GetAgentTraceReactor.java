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

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.model.inferencetracking.AgentTraceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns the full decision trace for a given trace ID.
 *
 * <p>Pixel: {@code GetAgentTrace(traceId=["abc123"])}
 *
 * <p>Security: the trace record contains a {@code userId} column. This reactor verifies the
 * requesting user matches the stored userId before returning trace content. Admins may access
 * any trace.
 */
public class GetAgentTraceReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(GetAgentTraceReactor.class);

    private static final String KEY_TRACE_ID = "traceId";

    public GetAgentTraceReactor() {
        this.keysToGet  = new String[] { KEY_TRACE_ID };
        this.keyRequired = new int[] { 1 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        String traceId = this.keyValue.get(KEY_TRACE_ID);
        if (traceId == null || traceId.trim().isEmpty()) {
            throw new IllegalArgumentException("traceId is required");
        }

        Map<String, Object> trace = AgentTraceLogsUtils.getTrace(traceId);

        if (!trace.containsKey("header")) {
            throw new IllegalArgumentException("No trace found with id: " + traceId);
        }

        // Security: verify requesting user owns this trace or is an admin.
        User user = this.insight.getUser();
        if (user == null || user.getPrimaryLoginToken() == null) {
            throw new IllegalArgumentException("User not authenticated");
        }
        String requestingUserId = user.getPrimaryLoginToken().getId();

        Map<String, Object> header = (Map<String, Object>) trace.get("header");
        if (header != null) {
            String traceUserId = (String) header.get("userId");
            boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
            if (!isAdmin && traceUserId != null && !traceUserId.equals(requestingUserId)) {
                throw new IllegalArgumentException("Access denied to trace: " + traceId);
            }
            // Do not expose userId in the returned payload
            header.remove("userId");
        }

        classLogger.info("GetAgentTraceReactor: returned trace {} for user {}", traceId, requestingUserId);
        return new NounMetadata(trace, PixelDataType.MAP, PixelOperationType.OPERATION);
    }

    @Override
    public String getReactorDescription() {
        return "Returns the full decision trace for a given agent trace ID. "
                + "Requires the requesting user to own the trace or be an admin.";
    }
}