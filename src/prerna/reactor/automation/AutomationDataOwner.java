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
 * -----------------------------------------------------------------------------
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

import java.util.Map;

/**
 * Immutable ownership boundary for data retained by one Automation execution.
 *
 * <p>
 * A reference is never an authorization token. Resolvers first authorize the
 * project and run, then use this complete owner tuple to prevent a reference
 * from being replayed across projects, runs, or execution principals.
 *
 * @param projectId project containing the Automation
 * @param runId durable Automation run identifier
 * @param userId immutable principal captured when the run was submitted
 */
record AutomationDataOwner(String projectId, String runId, String userId) {

	AutomationDataOwner {
		if (projectId == null || projectId.isBlank()) {
			throw new IllegalArgumentException("Automation data owner requires a project id.");
		}
		if (runId == null || runId.isBlank()) {
			throw new IllegalArgumentException("Automation data owner requires a run id.");
		}
		if (userId == null || userId.isBlank()) {
			throw new IllegalArgumentException("Automation data owner requires a user id.");
		}
	}

	/** @return JSON-shaped owner passed to the run-local provider */
	Map<String, String> toMap() {
		return Map.of("projectId", this.projectId, "runId", this.runId, "userId", this.userId);
	}
}
