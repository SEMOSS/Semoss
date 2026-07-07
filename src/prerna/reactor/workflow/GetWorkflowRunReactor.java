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
package prerna.reactor.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns detail for a single workflow run including per-node results.
 *
 * <p>Pixel: {@code GetWorkflowRun(app=["appId"], runId=["uuid"])}
 *
 * <p>Reads from WORKFLOW_RUNS and WORKFLOW_NODE_OUTPUTS in the scheduler DB.
 * Includes for-each progress for batch nodes.
 */
public class GetWorkflowRunReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetWorkflowRunReactor.class);

	public GetWorkflowRunReactor() {
		this.keysToGet = new String[]{ "app", "runId" };
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

		Map<String, Object> runDetail = WorkflowDatabaseUtility.getRunDetail(runId);
		if (runDetail == null) {
			Map<String, Object> notFound = new HashMap<>();
			notFound.put(WorkflowConstants.RUN_ID, runId);
			notFound.put("nodeResults", new ArrayList<>());
			return new NounMetadata(notFound, PixelDataType.MAP, PixelOperationType.OPERATION);
		}

		// Build node results with for-each progress
		List<Map<String, Object>> nodeOutputs = WorkflowDatabaseUtility.getNodeOutputsForRun(runId);
		List<Map<String, Object>> nodeResults = new ArrayList<>();

		for (Map<String, Object> nodeOutput : nodeOutputs) {
			Map<String, Object> nodeResult = new HashMap<>();
			nodeResult.put(WorkflowConstants.NODE_ID, nodeOutput.get(WorkflowConstants.NODE_ID));
			nodeResult.put(WorkflowConstants.NODE_LABEL, nodeOutput.get(WorkflowConstants.NODE_LABEL));
			nodeResult.put(WorkflowConstants.STATUS, nodeOutput.get(WorkflowConstants.STATUS));
			nodeResult.put(WorkflowConstants.DURATION_MS, nodeOutput.get(WorkflowConstants.DURATION_MS));
			nodeResult.put(WorkflowConstants.OUTPUT_PREVIEW, nodeOutput.get(WorkflowConstants.OUTPUT_PREVIEW));
			nodeResult.put(WorkflowConstants.ERROR_MESSAGE, nodeOutput.get(WorkflowConstants.ERROR_MESSAGE));

			// Include for-each progress if this node has a row count
			Object rowCount = nodeOutput.get(WorkflowConstants.ROW_COUNT);
			if (rowCount != null) {
				nodeResult.put(WorkflowConstants.ROW_COUNT, rowCount);
				String nodeId = (String) nodeOutput.get(WorkflowConstants.NODE_ID);
				Map<String, Integer> progress = WorkflowDatabaseUtility.getForEachProgress(runId, nodeId);
				if (!progress.isEmpty()) {
					nodeResult.put("forEachProgress", progress);
				}
			}
			nodeResults.add(nodeResult);
		}

		runDetail.put("nodeResults", nodeResults);
		return new NounMetadata(runDetail, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
}
