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
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists automation run history for a project.
 *
 * <p>Pixel: {@code ListAutomationRuns(app=["appId"], limit=["25"])}
 *
 * <p>Reads from AUTOMATION_RUNS in the scheduler DB.
 */
public class ListAutomationRunsReactor extends AbstractReactor {

	private static final int MAXIMUM_LIMIT = 100;

	public ListAutomationRunsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		String projectId = getString(ReactorKeysEnum.PROJECT.getKey());
		int limit = getInt(ReactorKeysEnum.LIMIT.getKey(), AutomationConstants.DEFAULT_LIST_RUNS_LIMIT);

		if (projectId == null || projectId.isBlank()) {
			throw new IllegalArgumentException("Must provide a project id");
		}
		if (limit <= 0) {
			throw new IllegalArgumentException("Limit must be greater than 0");
		}
		limit = Math.min(limit, MAXIMUM_LIMIT);

		projectId = AutomationProjectUtils.getViewableAutomationProject(this.insight.getUser(), projectId)
				.getProjectId();

		List<Map<String, Object>> runs = AutomationDatabaseUtility.getRunsForProject(projectId, limit);
		if (runs == null) {
			runs = new ArrayList<>();
		}

		return new NounMetadata(runs, PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Lists automation run history for a project, newest first.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.LIMIT.getKey().equals(key)) {
			return "Optional maximum number of runs to return. Must be positive and is capped at "
					+ MAXIMUM_LIMIT + ".";
		}
		return super.getDescriptionForKey(key);
	}
}
