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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists automation run history for a project.
 *
 * <p>Pixel: {@code ListAutomationRuns(app=["appId"], limit=["25"])}
 *
 * <p>Reads from AUTOMATION_RUNS in the scheduler DB.
 */
public class ListAutomationRunsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ListAutomationRunsReactor.class);

	public ListAutomationRunsReactor() {
		this.keysToGet = new String[]{ "project", "limit" };
		this.keyRequired = new int[]{ 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String limitStr = this.keyValue.get(this.keysToGet[1]);
		int limit = parseLimit(limitStr);

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access");
		}

		List<Map<String, Object>> runs = AutomationDatabaseUtility.getRunsForProject(projectId, limit);
		if (runs == null) {
			runs = new ArrayList<>();
		}

		return new NounMetadata(runs, PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

	private int parseLimit(String limitStr) {
		if (limitStr == null || limitStr.isEmpty()) return 25;
		try {
			return Integer.parseInt(limitStr.trim());
		} catch (NumberFormatException e) {
			return 25;
		}
	}
}
