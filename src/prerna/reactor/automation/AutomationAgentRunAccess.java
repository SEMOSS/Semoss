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

import prerna.auth.User;
import prerna.om.Insight;
import prerna.project.api.IProject;

/**
 * Permission boundary for Automation's trace-linked agent-run APIs.
 *
 * <p>This class is the only Automation path that may access an agent run without
 * the generic agent-run owner's identity. It first uses the normal project ACL,
 * then requires an exact persisted Automation run/node/agent-run relationship.
 */
final class AutomationAgentRunAccess {

	private AutomationAgentRunAccess() {
	}

	static Access authorizeView(Insight insight, String projectId, String automationRunId, String nodeId,
			String agentRunId) {
		User user = authenticatedUser(insight);
		IProject project = AutomationProjectUtils.getViewableAutomationProject(user, required(projectId, "project"));
		requireTrace(project.getProjectId(), automationRunId, nodeId, agentRunId);
		return new Access(project.getProjectId(), canControl(user, project.getProjectId()));
	}

	static Access authorizeEdit(Insight insight, String projectId, String automationRunId, String nodeId,
			String agentRunId) {
		User user = authenticatedUser(insight);
		IProject project = AutomationProjectUtils.getEditableAutomationProject(user, required(projectId, "project"));
		requireTrace(project.getProjectId(), automationRunId, nodeId, agentRunId);
		return new Access(project.getProjectId(), true);
	}

	private static boolean canControl(User user, String projectId) {
		try {
			AutomationProjectUtils.getEditableAutomationProject(user, projectId);
			return true;
		} catch (IllegalArgumentException e) {
			return false;
		}
	}

	private static void requireTrace(String projectId, String automationRunId, String nodeId, String agentRunId) {
		if (!AutomationDatabaseUtility.hasAgentRunTrace(projectId, required(automationRunId, "automation run"),
				required(nodeId, "node"), required(agentRunId, "agent run"))) {
			throw new SecurityException("The requested agent run is not trace-linked to this Automation run and node.");
		}
	}

	private static User authenticatedUser(Insight insight) {
		if (insight == null || insight.getUser() == null || insight.getUser().getPrimaryLoginToken() == null
				|| insight.getUser().getPrimaryLoginToken().getId() == null
				|| insight.getUser().getPrimaryLoginToken().getId().isBlank()
				|| "-1".equals(insight.getUser().getPrimaryLoginToken().getId())) {
			throw new SecurityException("Automation agent-run access requires an authenticated user.");
		}
		return insight.getUser();
	}

	private static String required(String value, String label) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(label + " id is required.");
		}
		return value.trim();
	}

	static final class Access {
		private final String projectId;
		private final boolean canControl;

		private Access(String projectId, boolean canControl) {
			this.projectId = projectId;
			this.canControl = canControl;
		}

		String projectId() {
			return projectId;
		}

		boolean canControl() {
			return canControl;
		}
	}
}
