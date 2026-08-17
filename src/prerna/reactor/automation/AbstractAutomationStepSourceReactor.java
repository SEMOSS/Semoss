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
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.Utility;

/**
 * Common authenticated validation and generation flow for automation Python source reactors.
 */
public abstract class AbstractAutomationStepSourceReactor extends AbstractReactor {

	protected static final String NODE_ID_KEY = "nodeId";
	protected static final String NODE_TYPE_KEY = "nodeType";
	protected static final String CONFIG_KEY = "config";

	protected final PreparedStep prepareStep() {
		User user = requireAuthenticatedUser();
		String projectId = requireNonblank(this.keyValue.get(ReactorKeysEnum.PROJECT.getKey()), "project");
		String nodeId = requireNonblank(this.keyValue.get(NODE_ID_KEY), NODE_ID_KEY);
		String nodeType = requireNonblank(this.keyValue.get(NODE_TYPE_KEY), NODE_TYPE_KEY);
		String config = requireNonblank(this.keyValue.get(CONFIG_KEY), CONFIG_KEY);

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		validateAutomationProject(user, projectId);
		validateNodeId(nodeId);
		return new PreparedStep(projectId, stepRef(nodeId),
				AutomationStepGenerationService.generate(user, nodeType, config));
	}

	protected final PreparedStep prepareStepForAction(String actionId) {
		User user = requireAuthenticatedUser();
		String projectId = requireNonblank(this.keyValue.get(ReactorKeysEnum.PROJECT.getKey()), "project");
		String nodeId = requireNonblank(this.keyValue.get(NODE_ID_KEY), NODE_ID_KEY);
		String config = requireNonblank(this.keyValue.get(CONFIG_KEY), CONFIG_KEY);
		AutomationStepTemplateRegistry.ActionDefinition action =
				AutomationStepTemplateRegistry.getAction(actionId);

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		validateAutomationProject(user, projectId);
		validateNodeId(nodeId);
		return new PreparedStep(projectId, stepRef(nodeId), action.getNodeType(),
				AutomationStepGenerationService.generateForAction(user, action.getActionId(), config));
	}

	protected final String requireNonblank(String value, String key) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Must provide " + key + ".");
		}
		return value;
	}

	protected final String readCurrentSource(PreparedStep step) {
		return AutomationStepSourceService.readSource(step.getProjectId(), step.getStepRef());
	}

	protected final void saveSource(PreparedStep step, String source, String comment) {
		AutomationStepSourceService.saveSource(this.insight, step.getProjectId(), step.getStepRef(), source, comment);
	}

	protected final boolean sourceExists(PreparedStep step) {
		return AutomationStepSourceService.sourceExists(step.getProjectId(), step.getStepRef());
	}

	private User requireAuthenticatedUser() {
		User user = this.insight.getUser();
		if (user == null || user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("You must be signed in to manage an automation step.");
		}
		return user;
	}

	private static void validateAutomationProject(User user, String projectId) {
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have edit access.");
		}
		IProject project = Utility.getProject(projectId);
		if (project == null || project.getProjectType() != IProject.PROJECT_TYPE.AUTOMATION) {
			throw new IllegalArgumentException("Project is not an automation project: " + projectId);
		}
	}

	private static void validateNodeId(String nodeId) {
		if (!AutomationStepTemplateRegistry.isSafeStepNodeId(nodeId)) {
			throw new IllegalArgumentException("nodeId must use only letters, numbers, periods, underscores, and hyphens "
					+ "and must start with a letter or number.");
		}
	}

	private static String stepRef(String nodeId) {
		return AutomationConstants.AUTOMATION_STEPS_FOLDER + "/" + nodeId + ".py";
	}

	/**
	 * Validated source location and its in-memory generated replacement.
	 */
	protected static final class PreparedStep {
		private final String projectId;
		private final String stepRef;
		private final String nodeType;
		private final AutomationStepGenerationService.GeneratedStep generated;

		private PreparedStep(String projectId, String stepRef,
				AutomationStepGenerationService.GeneratedStep generated) {
			this(projectId, stepRef, null, generated);
		}

		private PreparedStep(String projectId, String stepRef, String nodeType,
				AutomationStepGenerationService.GeneratedStep generated) {
			this.projectId = projectId;
			this.stepRef = stepRef;
			this.nodeType = nodeType;
			this.generated = generated;
		}

		String getProjectId() {
			return projectId;
		}

		String getStepRef() {
			return stepRef;
		}

		String getNodeType() {
			return nodeType;
		}

		AutomationStepGenerationService.GeneratedStep getGenerated() {
			return generated;
		}
	}
}
