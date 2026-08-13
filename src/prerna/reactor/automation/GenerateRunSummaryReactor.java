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
package prerna.reactor.automation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Generates a plain-English summary of a completed automation run using an LLM.
 * Reads per-step output previews from the database and asks the model to describe
 * what happened in 2-3 sentences suitable for a non-technical user.
 *
 * <p>Pixel: {@code GenerateRunSummary(project=["appId"], runId=["runId"])}
 */
public class GenerateRunSummaryReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(GenerateRunSummaryReactor.class);

    private static final String KEY_RUN_ID = "runId";

    private static final String SYSTEM_PROMPT =
        "You summarize completed automation workflow runs for non-technical users. "
        + "Given the run status and a list of steps with their outputs, write a 2-3 sentence "
        + "plain-English summary of what happened: what the automation did, what it found or produced, "
        + "and (if applicable) what went wrong. Be specific about data where available. "
        + "Do not use technical jargon, node IDs, or implementation details. "
        + "Do not start with 'The automation'. Respond with only the summary - no preamble, no headers.";

    public GenerateRunSummaryReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), KEY_RUN_ID };
        this.keyRequired = new int[] { 1, 1 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in.");
        }

        String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
        String runId = this.keyValue.get(KEY_RUN_ID);

        projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
        if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
            throw new IllegalArgumentException("Project does not exist or user does not have access.");
        }

        Map<String, Object> runDetail = AutomationDatabaseUtility.getRunDetail(runId);
        if (runDetail == null) {
            throw new IllegalArgumentException("Run not found: " + runId);
        }

        // Guard against summarizing an in-progress run - the result would be incomplete.
        Object runStatus = runDetail.get(AutomationConstants.STATUS);
        if (AutomationConstants.STATUS_RUNNING.equals(runStatus)) {
            throw new IllegalArgumentException("Run " + runId + " is still in progress. Wait for it to complete.");
        }

        List<Map<String, Object>> nodeOutputs = AutomationDatabaseUtility.getNodeOutputsForRun(runId);
        if (nodeOutputs.isEmpty()) {
            classLogger.warn("GenerateRunSummary: no node outputs found for run {}", runId);
        }

        String engineId = AutomationExecutionUtils.findFirstModelEngine(user);
        if (engineId == null || engineId.isBlank()) {
            throw new IllegalArgumentException(
                "No AI model engine is available. Add a model engine connection to use this feature.");
        }
        if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
            throw new IllegalArgumentException(
                "Model engine " + engineId + " does not exist or user does not have access.");
        }

        IModelEngine modelEngine = Utility.getModel(engineId);
        if (modelEngine == null) {
            throw new IllegalArgumentException("Model engine " + engineId + " could not be loaded.");
        }

        String userMessage = AutomationExecutionUtils.buildRunSummaryPrompt(runDetail, nodeOutputs);

        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("use_history", false);

        Map<String, Object> response;
        try {
            response = modelEngine.ask(SYSTEM_PROMPT + "\n\n" + userMessage,
                    null, this.insight, paramMap).toMap();
        } catch (Exception e) {
            classLogger.error("LLM call failed for GenerateRunSummary on run {}", runId, e);
            throw new RuntimeException("Summary generation failed: " + e.getMessage(), e);
        }

        String summary = AutomationExecutionUtils.extractResponseText(response);
        if (summary == null || summary.isBlank()) {
            throw new IllegalStateException("The AI model did not return a summary. Try again.");
        }

        classLogger.info("GenerateRunSummary completed: project={}, runId={}", projectId, runId);
        return new NounMetadata(summary.strip(), PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
    }

    @Override
    public String getReactorDescription() {
        return "Generates a plain-English 2-3 sentence summary of a completed automation run "
            + "using an LLM. Reads per-step results from the database. Returns the summary string.";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        return switch (key) {
            case "project" -> "The project ID the automation belongs to.";
            case "runId" -> "The run ID to summarize.";
            default -> super.getDescriptionForKey(key);
        };
    }
}
