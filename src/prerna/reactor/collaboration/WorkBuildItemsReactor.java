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
package prerna.reactor.collaboration;

import prerna.auth.User;
import prerna.collaboration.WorkPipeline;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkBuildItems(days=[14], replaceOld=[true]); WorkBuildItems(status=[true]); WorkBuildItems(tidy=[true]);
public class WorkBuildItemsReactor extends AbstractCollaborationReactor {

	private static final String DAYS = "days";
	private static final String REPLACE_OLD = "replaceOld";
	private static final String STATUS = "status";
	private static final String TIDY = "tidy";
	private static final int DEFAULT_DAYS = 14;

	public WorkBuildItemsReactor() {
		this.keysToGet = new String[] { DAYS, REPLACE_OLD, STATUS, TIDY };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		if (Boolean.TRUE.equals(getBoolean(STATUS))) {
			return mapResult(WorkPipeline.status(user));
		}
		if (Boolean.TRUE.equals(getBoolean(TIDY))) {
			return mapResult(WorkPipeline.tidyAll(user));
		}
		String days = getString(DAYS);
		int n = days == null || days.isBlank() ? DEFAULT_DAYS : (int) Double.parseDouble(days.trim());
		return mapResult(WorkPipeline.backfill(user, n, Boolean.TRUE.equals(getBoolean(REPLACE_OLD))));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description(
						"Builds work items from the owner's recent threads in the background; status reports progress")
				.key(DAYS, "How many days of threads to read (default 14)")
				.key(REPLACE_OLD, "true to retire the steps threads made on their own and read every thread again")
				.key(STATUS, "true to report the last build instead of starting one")
				.key(TIDY, "true to fold duplicate steps on every open item instead of building");
	}
}
