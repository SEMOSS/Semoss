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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns detail for a single automation run including per-node results.
 *
 * <p>Pixel: {@code GetAutomationRun(app=["appId"], runId=["uuid"])}
 *
 * <p>Reads from AUTOMATION_RUNS and AUTOMATION_NODE_OUTPUTS in the scheduler DB.
 * Includes for-each progress for batch nodes.
 */
public class GetAutomationRunReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetAutomationRunReactor.class);

	public GetAutomationRunReactor() {
		this.keysToGet = new String[]{ "project", "runId" };
		this.keyRequired = new int[]{ 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String runId = this.keyValue.get(this.keysToGet[1]);

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access");
		}

		Map<String, Object> runDetail = AutomationDatabaseUtility.getRunDetail(runId);
		if (runDetail == null) {
			Map<String, Object> notFound = new HashMap<>();
			notFound.put(AutomationConstants.RUN_ID, runId);
			notFound.put("nodeResults", new ArrayList<>());
			return new NounMetadata(notFound, PixelDataType.MAP, PixelOperationType.OPERATION);
		}

		// Build node results with for-each progress and while-loop iteration data
		List<Map<String, Object>> nodeOutputs = AutomationDatabaseUtility.getNodeOutputsForRun(runId);
		List<Map<String, Object>> nodeResults = new ArrayList<>();
		Gson gson = new Gson();

		for (Map<String, Object> nodeOutput : nodeOutputs) {
			Map<String, Object> nodeResult = new HashMap<>();
			nodeResult.put(AutomationConstants.NODE_ID, nodeOutput.get(AutomationConstants.NODE_ID));
			nodeResult.put(AutomationConstants.NODE_LABEL, nodeOutput.get(AutomationConstants.NODE_LABEL));
			nodeResult.put(AutomationConstants.STATUS, nodeOutput.get(AutomationConstants.STATUS));
			nodeResult.put(AutomationConstants.DURATION_MS, nodeOutput.get(AutomationConstants.DURATION_MS));
			nodeResult.put(AutomationConstants.OUTPUT_PREVIEW, nodeOutput.get(AutomationConstants.OUTPUT_PREVIEW));
			nodeResult.put(AutomationConstants.ERROR_MESSAGE, nodeOutput.get(AutomationConstants.ERROR_MESSAGE));

			// Include for-each progress if this node has a row count
			Object rowCount = nodeOutput.get(AutomationConstants.ROW_COUNT);
			if (rowCount != null) {
				nodeResult.put(AutomationConstants.ROW_COUNT, rowCount);
				String nodeId = (String) nodeOutput.get(AutomationConstants.NODE_ID);
				Map<String, Integer> progress = AutomationDatabaseUtility.getForEachProgress(runId, nodeId);
				if (!progress.isEmpty()) {
					nodeResult.put("forEachProgress", progress);
				}
			}

			// Parse while-loop iteration data stored in OUTPUT_VALUE
			Object outputValue = nodeOutput.get(AutomationConstants.OUTPUT_VALUE);
			if (outputValue instanceof String) {
				String outputStr = (String) outputValue;
				if (outputStr.contains("\"__whileResult\":true")) {
					try {
						@SuppressWarnings("unchecked")
						Map<String, Object> wr = gson.fromJson(outputStr, Map.class);
						Object iterations = wr.get("iterations");
						if (iterations != null) {
							nodeResult.put("iterationResults", iterations);
						}
					} catch (Exception ignored) {
						// malformed JSON - skip
					}
				}
			}

			nodeResults.add(nodeResult);
		}

		runDetail.put("nodeResults", nodeResults);
		return new NounMetadata(runDetail, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
}
