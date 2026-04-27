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
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns aggregate dashboard metrics across all agent runs. Admin-only.
 *
 * <p>Pixel: {@code AgentSummaryMetrics();}
 *
 * <p>Returns a map with:
 * <ul>
 *   <li>{@code totalRuns}       — all-time run count
 *   <li>{@code runsLast24h}     — runs in the last 24 hours
 *   <li>{@code avgIterations}   — average agent iterations per run
 *   <li>{@code avgDurationMs}   — average run duration in milliseconds
 *   <li>{@code successRate}     — % of runs that terminated with a text response
 *   <li>{@code totalToolCalls}  — sum of all tool calls made
 *   <li>{@code topModels}       — [{modelEngineId, runCount}] top 10 by usage
 *   <li>{@code runsByDay}       — [{date, count}] for the last 7 days
 * </ul>
 */
public class AgentSummaryMetricsReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(AgentSummaryMetricsReactor.class);

    public AgentSummaryMetricsReactor() {
        this.keysToGet  = new String[] {};
        this.keyRequired = new int[] {};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        User user = this.insight.getUser();
        if (user == null) {
            throw new SemossPixelException(NounMetadata.getErrorNounMessage("You are not properly logged in"));
        }
        if (!SecurityAdminUtils.userIsAdmin(user)) {
            throw new SemossPixelException(NounMetadata.getErrorNounMessage(
                    "AgentSummaryMetrics is restricted to admin users"));
        }

        Map<String, Object> metrics = AgentTraceLogsUtils.getMetrics();

        classLogger.info("AgentSummaryMetricsReactor: returning metrics (totalRuns={})",
                metrics.get("totalRuns"));

        return new NounMetadata(metrics, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
    }

    @Override
    public String getReactorDescription() {
        return "Returns aggregate agent observability metrics across all rooms. Admin only.";
    }
}
