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
package prerna.reactor.automation.project;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.AutomationRuntime;
import prerna.reactor.automation.definition.AutomationDefinitionService;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;
import prerna.reactor.automation.definition.AutomationScopeCatalog;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

/**
 * Restores an automation project's definition to the state captured at a
 * specific git commit,
 * then saves it as a new version.
 *
 * <p>
 * Pixel:
 * {@code RollbackAutomation(project=["<projectId>"], revision=["<commitId>"])}
 */
public class RollbackAutomationReactor extends AbstractReactor {

	private static final String PROJECT_KEY = "project";
	private static final String REVISION_KEY = "revision";

	private static final String WORKFLOW_GIT_PATH = "assets/"
			+ AutomationConstants.AUTOMATION_PYTHON_DEFINITION_FILE_NAME;
	private static final String NODES_GIT_PREFIX = "assets/" + AutomationConstants.AUTOMATION_NODE_SOURCES_FOLDER_NAME
			+ "/";

	public RollbackAutomationReactor() {
		this.keysToGet = new String[] { PROJECT_KEY, REVISION_KEY };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to restore an automation version",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException error = new SemossPixelException(noun);
			error.setContinueThreadOfExecution(false);
			throw error;
		}

		String projectIdOrAlias = this.keyValue.get(PROJECT_KEY);
		String revision = this.keyValue.get(REVISION_KEY);
		if (revision == null || revision.isBlank()) {
			throw new IllegalArgumentException("revision must be provided.");
		}

		// Edit-level ACL — restoring writes a new commit, so the user needs edit access
		String projectId = AutomationProjectService.getEditableAutomationProject(user, projectIdOrAlias).getProjectId();
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalArgumentException("Project not found: " + projectId);
		}

		String versionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);

		String definitionJson = GitRepoUtils.getFile(revision, WORKFLOW_GIT_PATH, versionFolder);
		if (definitionJson == null || definitionJson.isBlank()) {
			throw new IllegalArgumentException("No automation definition found at revision: " + revision);
		}

		// Reassemble node sources from the same commit
		AutomationDefinitionValidator.ValidatedDefinition validated = AutomationDefinitionValidator
				.parseAndValidateForAuthoring(definitionJson);

		Map<String, String> nodeSources = new LinkedHashMap<>();
		for (Map<String, Object> node : AutomationRuntime.allNodes(validated)) {
			if (!AutomationConstants.NODE_DEVELOPER_PYTHON.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				continue;
			}
			String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
			String gitPath = NODES_GIT_PREFIX + AutomationDefinitionService.safeNodeFileName(node) + ".py";
			String source = GitRepoUtils.getFile(revision, gitPath, versionFolder);
			if (source != null) {
				nodeSources.put(nodeId, source);
			}
		}

		// Save as a new commit — null expectedRevision skips optimistic concurrency
		// check
		AutomationDefinitionService.DefinitionFiles files = AutomationProjectService.saveDefinition(
				projectId, definitionJson, nodeSources, null, user);

		// Return the same shape as RollbackAutomation so the frontend can reuse the
		// same handler
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("saved", true);
		result.put(AutomationConstants.DOC_NODE_SOURCES, files.nodeSources());
		result.put(AutomationConstants.RESULT_REVISION,
				AutomationDefinitionService.calculateRevision(files.definition(), files.nodeSources()));
		AutomationDefinitionValidator.ValidatedDefinition savedValidated = AutomationDefinitionValidator
				.parseAndValidateForAuthoring(files.definition());
		result.put(AutomationConstants.DOC_GLOBALS, AutomationRuntime.declaredGlobals(savedValidated));
		result.put(AutomationConstants.RESULT_SCOPE_VARIABLES, AutomationScopeCatalog.variablesByNode(savedValidated));

		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Restores an automation project's definition to the state at the given git revision and saves it as a new version.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (PROJECT_KEY.equals(key)) {
			return "ID or alias of the automation project.";
		}
		if (REVISION_KEY.equals(key)) {
			return "Git commit id (full or short hash) returned by ListAutomationVersions.";
		}
		return super.getDescriptionForKey(key);
	}
}
