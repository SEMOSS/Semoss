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
 ******************************************************************************/
package prerna.reactor.automation;

import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns one bounded page from a node value retained by an Automation run's
 * execution Insight.
 *
 * <p>
 * Pixel: {@code GetAutomationRunNodeData(project=["appId"], runId=["uuid"],
 * nodeId=["node"], offset=["0"], limit=["50"])}
 *
 * <p>
 * The opaque reference never crosses the client boundary. Project permission,
 * run ownership, node membership, and execution-Insight ownership are all
 * re-established for every page request.
 */
public class GetAutomationRunNodeDataReactor extends AbstractReactor {

	private static final String RUN_ID_KEY = "runId";
	private static final String NODE_ID_KEY = "nodeId";

	public GetAutomationRunNodeDataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), RUN_ID_KEY, NODE_ID_KEY,
				ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = requiredValue(ReactorKeysEnum.PROJECT.getKey(), "project id");
		String runId = requiredValue(RUN_ID_KEY, "run id");
		String nodeId = requiredValue(NODE_ID_KEY, "node id");
		int offset = integerValue(ReactorKeysEnum.OFFSET.getKey(), 0, 0, Integer.MAX_VALUE);
		int limit = integerValue(ReactorKeysEnum.LIMIT.getKey(), AutomationConstants.DEFAULT_DATA_PAGE_LIMIT, 1,
				AutomationConstants.MAX_DATA_PAGE_LIMIT);

		Map<String, Object> result = AutomationRunDataService.getNodeDataPage(this.insight.getUser(), projectId, runId,
				nodeId, offset, limit);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private String requiredValue(String key, String label) {
		String value = this.keyValue.get(key);
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Must provide a " + label);
		}
		return value;
	}

	private int integerValue(String key, int defaultValue, int minimum, int maximum) {
		String value = this.keyValue.get(key);
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		try {
			int parsed = Integer.parseInt(value);
			if (parsed < minimum || parsed > maximum) {
				throw new IllegalArgumentException(key + " must be between " + minimum + " and " + maximum + ".");
			}
			return parsed;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(key + " must be an integer.", e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Returns a bounded page from data retained by one Automation run node.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (RUN_ID_KEY.equals(key)) {
			return "Run identifier returned when the automation was triggered.";
		}
		if (NODE_ID_KEY.equals(key)) {
			return "Node identifier from the immutable run definition.";
		}
		return super.getDescriptionForKey(key);
	}
}
