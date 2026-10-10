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
package prerna.reactor.automation.run;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Returns one authorized, bounded page from retained Automation node data. */
public class GetAutomationRunNodeDataReactor extends AbstractReactor {

	private static final String RUN_ID_KEY = "runId";
	private static final String NODE_ID_KEY = "nodeId";
	private static final String OFFSET_KEY = "offset";
	private static final String LIMIT_KEY = "limit";

	public GetAutomationRunNodeDataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), RUN_ID_KEY, NODE_ID_KEY, OFFSET_KEY,
				LIMIT_KEY };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = required(this.keyValue.get(ReactorKeysEnum.PROJECT.getKey()), "project");
		String runId = required(this.keyValue.get(RUN_ID_KEY), RUN_ID_KEY);
		String nodeId = required(this.keyValue.get(NODE_ID_KEY), NODE_ID_KEY);
		long offset = parseLong(this.keyValue.get(OFFSET_KEY), 0L, OFFSET_KEY);
		int limit = Math.toIntExact(parseLong(this.keyValue.get(LIMIT_KEY),
				AutomationConstants.RUN_DATA_DEFAULT_PAGE_SIZE, LIMIT_KEY));

		projectId = AutomationProjectService.getViewableAutomationProject(this.insight.getUser(), projectId)
				.getProjectId();
		Map<String, Object> run = AutomationRunStore.getRunDetail(runId);
		if (run == null || !projectId.equals(run.get(AutomationConstants.PROJECT_ID))) {
			throw new IllegalArgumentException("Automation run data was not found.");
		}

		AutomationFrameHistory.Page page = AutomationFrameHistory.page(runId, nodeId, offset, limit);
		Map<String, Object> output = new LinkedHashMap<>();
		if (page == null) {
			output.put("available", false);
			output.put("runId", runId);
			output.put("nodeId", nodeId);
			return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
		}

		AutomationFrameHistory.Snapshot snapshot = page.snapshot();
		output.put("available", true);
		output.put("referenceId", snapshot.referenceId());
		output.put("runId", runId);
		output.put("nodeId", nodeId);
		output.put("outputVariable", snapshot.outputVariable());
		output.put("headers", snapshot.headers());
		output.put("types", snapshot.types());
		output.put("rows", page.rows());
		output.put("total", snapshot.rowCount());
		output.put("offset", offset);
		output.put("limit", limit);
		output.put("hasMore", offset + page.rows().size() < snapshot.rowCount());
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static String required(String value, String key) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Must provide " + key + ".");
		}
		return value;
	}

	private static long parseLong(String value, long defaultValue, String key) {
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		try {
			return Long.parseLong(value);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(key + " must be an integer.", e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Returns one bounded page of retained tabular data for an authorized Automation run node.";
	}
}
