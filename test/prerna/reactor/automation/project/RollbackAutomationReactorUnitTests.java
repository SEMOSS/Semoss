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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionService;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.UserAuditTrailUtils;

/** Covers rollback authorization, concurrency forwarding, and audit behavior. */
class RollbackAutomationReactorUnitTests {

	private static final String TARGET_REVISION = "a".repeat(40);
	private static final String CURRENT_REVISION = "current-revision";
	private static final String DEFINITION = """
			{
			  "formatVersion": 2,
			  "triggerBindings": [{ "id": "manual", "type": "manual" }],
			  "graph": {
			    "nodes": [{ "id": "start", "type": "trigger.start", "label": "Start",
			      "codeMode": "generated", "config": {} }],
			    "edges": []
			  }
			}
			""";

	private RollbackAutomationReactor reactor;
	private Insight insight;
	private User user;

	@BeforeEach
	void setup() {
		reactor = new RollbackAutomationReactor();
		insight = mock(Insight.class);
		user = mock(User.class);
		reactor.setInsight(insight);
		when(insight.getUser()).thenReturn(user);
		reactor.keyValue.put("project", "project-1");
		reactor.keyValue.put("revision", TARGET_REVISION);
		reactor.keyValue.put(AutomationConstants.EXPECTED_REVISION_KEY, CURRENT_REVISION);
	}

	@Test
	void requiresEditAccess() {
		try (var projects = mockStatic(AutomationProjectService.class)) {
			projects.when(() -> AutomationProjectService.getEditableAutomationProject(user, "project-1"))
					.thenThrow(new IllegalArgumentException("Project does not exist or user does not have edit access."));

			IllegalArgumentException error = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Project does not exist or user does not have edit access.", error.getMessage());
		}
	}

	@Test
	void requiresCurrentRevision() {
		reactor.keyValue.remove(AutomationConstants.EXPECTED_REVISION_KEY);

		IllegalArgumentException error = assertThrows(IllegalArgumentException.class, reactor::execute);
		assertEquals("Required input(s) missing: expectedRevision", error.getMessage());
	}

	@Test
	void restoresWithConcurrencyTokenAndAuditsAfterSuccess() {
		IProject project = project();
		AutomationDefinitionService.DefinitionFiles files = new AutomationDefinitionService.DefinitionFiles(DEFINITION,
				Map.of());

		try (var projects = mockStatic(AutomationProjectService.class);
				var versions = mockStatic(AutomationVersionService.class);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			projects.when(() -> AutomationProjectService.getEditableAutomationProject(user, "project-1"))
					.thenReturn(project);
			versions.when(() -> AutomationVersionService.restore(project, TARGET_REVISION, CURRENT_REVISION, user))
					.thenReturn(files);

			NounMetadata result = reactor.execute();

			@SuppressWarnings("unchecked")
			Map<String, Object> output = (Map<String, Object>) result.getValue();
			assertEquals(true, output.get("saved"));
			versions.verify(() -> AutomationVersionService.restore(project, TARGET_REVISION, CURRENT_REVISION, user));
			audit.verify(() -> UserAuditTrailUtils.recordAutomationEvent(eq(user),
					eq(AuditEventType.AUTOMATION_UPDATE), eq("project-1"), eq(null), eq(null), any(Map.class)));
		}
	}

	@Test
	void failedRestoreDoesNotWriteAuditEvent() {
		IProject project = project();
		try (var projects = mockStatic(AutomationProjectService.class);
				var versions = mockStatic(AutomationVersionService.class);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			projects.when(() -> AutomationProjectService.getEditableAutomationProject(user, "project-1"))
					.thenReturn(project);
			versions.when(() -> AutomationVersionService.restore(project, TARGET_REVISION, CURRENT_REVISION, user))
					.thenThrow(new IllegalArgumentException("Automation changed since it was loaded."));

			assertThrows(IllegalArgumentException.class, reactor::execute);
			audit.verifyNoInteractions();
		}
	}

	private static IProject project() {
		IProject project = mock(IProject.class);
		when(project.getProjectId()).thenReturn("project-1");
		return project;
	}
}
