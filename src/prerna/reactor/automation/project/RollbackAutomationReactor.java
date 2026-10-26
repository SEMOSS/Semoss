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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.UserAuditTrailUtils;

/**
 * Restores an automation project's definition to the state captured at a
 * specific Git commit, then saves it as a new version.
 *
 * <p>
 * Pixel:
 * {@code RollbackAutomation(project=["<projectId>"], revision=["<commitId>"], expectedRevision=["<definitionRevision>"])}
 */
public class RollbackAutomationReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RollbackAutomationReactor.class);

	private static final String REVISION_KEY = "revision";

	public RollbackAutomationReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), REVISION_KEY,
				AutomationConstants.EXPECTED_REVISION_KEY };
		this.keyRequired = new int[] { 1, 1, 1 };
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

		String projectIdOrAlias = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String revision = this.keyValue.get(REVISION_KEY);
		String expectedRevision = this.keyValue.get(AutomationConstants.EXPECTED_REVISION_KEY);
		if (revision == null || revision.isBlank()) {
			throw new IllegalArgumentException("revision must be provided.");
		}

		// Edit-level ACL — restoring writes a new commit, so the user needs edit access
		IProject project = AutomationProjectService.getEditableAutomationProject(user, projectIdOrAlias);
		String projectId = project.getProjectId();
		AutomationDefinitionService.DefinitionFiles files = AutomationVersionService.restore(project, revision,
				expectedRevision, user);

		String savedRevision = AutomationDefinitionService.calculateRevision(files.definition(), files.nodeSources());
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("saved", true);
		result.put(AutomationConstants.DOC_NODE_SOURCES, files.nodeSources());
		result.put(AutomationConstants.RESULT_REVISION, savedRevision);
		AutomationDefinitionValidator.ValidatedDefinition savedValidated = AutomationDefinitionValidator
				.parseAndValidateForAuthoring(files.definition());
		result.put(AutomationConstants.DOC_GLOBALS, AutomationRuntime.declaredGlobals(savedValidated));
		result.put(AutomationConstants.RESULT_SCOPE_VARIABLES, AutomationScopeCatalog.variablesByNode(savedValidated));

		UserAuditTrailUtils.recordAutomationEvent(user, AuditEventType.AUTOMATION_UPDATE, projectId, null, null,
				Map.of("revision", savedRevision, "restoredFrom", revision, "nodeSourceCount",
						files.nodeSources() == null ? 0 : files.nodeSources().size()));
		classLogger.info("Restored automation project {} from revision {} as revision {}", projectId, revision,
				savedRevision);

		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Restores an automation project's definition to the state at the given Git revision and saves it as a new version.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "ID or alias of the automation project.";
		}
		if (REVISION_KEY.equals(key)) {
			return "Full Git commit SHA returned by ListAutomationVersions.";
		}
		if (AutomationConstants.EXPECTED_REVISION_KEY.equals(key)) {
			return "Current definition revision returned by GetAutomation. The restore is rejected if the automation "
					+ "changed.";
		}
		return super.getDescriptionForKey(key);
	}
}
