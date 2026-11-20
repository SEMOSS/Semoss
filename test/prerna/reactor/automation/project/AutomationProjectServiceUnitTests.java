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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/** Covers the owner-scoped run scope used by runs of inline definition JSON. */
public class AutomationProjectServiceUnitTests {

	private static User user(String id) {
		AccessToken token = new AccessToken();
		token.setId(id);
		token.setProvider(AuthProvider.NATIVE);
		User user = new User();
		user.setAccessToken(token);
		return user;
	}

	@Test
	void inlineRunScopeIsStablePerLoginAndDistinctAcrossLogins() {
		String scope = AutomationProjectService.inlineRunScope(user("owner"));

		assertTrue(scope.startsWith(AutomationConstants.INLINE_RUN_SCOPE_PREFIX));
		assertTrue(AutomationProjectService.isInlineRunScope(scope));
		assertEquals(scope, AutomationProjectService.inlineRunScope(user("owner")));
		assertNotEquals(scope, AutomationProjectService.inlineRunScope(user("other")));
		assertFalse(AutomationProjectService.isInlineRunScope("project-id"));
		assertFalse(AutomationProjectService.isInlineRunScope(null));
	}

	@Test
	void blankProjectResolvesToTheCallersOwnScopeWithoutAnAclCall() {
		User owner = user("owner");
		try (MockedStatic<SecurityProjectUtils> security = mockStatic(SecurityProjectUtils.class)) {
			assertEquals(AutomationProjectService.inlineRunScope(owner),
					AutomationProjectService.resolveRunScope(owner, null, false));
			assertEquals(AutomationProjectService.inlineRunScope(owner),
					AutomationProjectService.resolveRunScope(owner, " ", true));
			security.verifyNoInteractions();
		}
	}

	@Test
	void acceptsOnlyTheCallersOwnInlineScope() {
		User owner = user("owner");
		String ownScope = AutomationProjectService.inlineRunScope(owner);
		String otherScope = AutomationProjectService.inlineRunScope(user("other"));
		try (MockedStatic<SecurityProjectUtils> security = mockStatic(SecurityProjectUtils.class)) {
			assertEquals(ownScope, AutomationProjectService.resolveRunScope(owner, ownScope, true));
			IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
					() -> AutomationProjectService.resolveRunScope(owner, otherScope, false));
			assertEquals("Run does not exist or user does not have access.", error.getMessage());
			security.verifyNoInteractions();
		}
	}

	@Test
	void projectLookupRejectsInlinePrefixedIds() {
		User owner = user("owner");
		String otherScope = AutomationProjectService.inlineRunScope(user("other"));
		try (MockedStatic<SecurityProjectUtils> security = mockStatic(SecurityProjectUtils.class)) {
			// Even a project registered under that id must not expose the other user's runs.
			security.when(() -> SecurityProjectUtils.userCanViewProject(any(), anyString())).thenReturn(true);
			security.when(() -> SecurityProjectUtils.userCanEditProject(any(), anyString())).thenReturn(true);
			security.when(() -> SecurityProjectUtils.testUserProjectIdForAlias(any(), anyString()))
					.thenReturn(otherScope);

			IllegalArgumentException direct = assertThrows(IllegalArgumentException.class,
					() -> AutomationProjectService.getViewableAutomationProject(owner, otherScope));
			assertEquals("Project does not exist or user does not have access.", direct.getMessage());
			IllegalArgumentException alias = assertThrows(IllegalArgumentException.class,
					() -> AutomationProjectService.getEditableAutomationProject(owner, "alias"));
			assertEquals("Project does not exist or user does not have edit access.", alias.getMessage());
			security.verify(() -> SecurityProjectUtils.userCanViewProject(any(), anyString()), never());
			security.verify(() -> SecurityProjectUtils.userCanEditProject(any(), anyString()), never());
		}
	}

	@Test
	void namedProjectStillUsesTheProjectAcl() {
		User owner = user("owner");
		try (MockedStatic<SecurityProjectUtils> security = mockStatic(SecurityProjectUtils.class)) {
			security.when(() -> SecurityProjectUtils.testUserProjectIdForAlias(owner, "project-id"))
					.thenReturn("project-id");

			IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
					() -> AutomationProjectService.resolveRunScope(owner, "project-id", true));
			assertEquals("Project does not exist or user does not have edit access.", error.getMessage());
			security.verify(() -> SecurityProjectUtils.userCanEditProject(owner, "project-id"));
		}
	}

	@Test
	void agentRunWithoutAWorkspaceSkipsOnlyTheWorkspaceCheck() {
		User owner = user("owner");
		try (MockedStatic<SecurityProjectUtils> security = mockStatic(SecurityProjectUtils.class);
				MockedStatic<SecurityEngineUtils> engines = mockStatic(SecurityEngineUtils.class)) {
			engines.when(() -> SecurityEngineUtils.userCanViewEngine(owner, "engine-1")).thenReturn(true);
			engines.when(() -> SecurityEngineUtils.getEngineType("engine-1")).thenReturn(IEngine.CATALOG_TYPE.MODEL);

			AutomationProjectService.validateDefinitionReferences(agentDefinition(null), owner);
			security.verifyNoInteractions();

			IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
					() -> AutomationProjectService.validateDefinitionReferences(agentDefinition("workspace-1"), owner));
			assertTrue(error.getMessage().contains("workspaceId 'workspace-1'"));
		}
	}

	private static AutomationDefinitionValidator.ValidatedDefinition agentDefinition(String workspaceId) {
		Map<String, Object> config = new LinkedHashMap<>();
		config.put(AutomationConstants.CONFIG_ENGINE_ID, "engine-1");
		config.put(AutomationConstants.CONFIG_COMMAND, "Summarize");
		if (workspaceId != null) {
			config.put(AutomationConstants.CONFIG_WORKSPACE_ID, workspaceId);
		}
		Map<String, Object> start = Map.of(AutomationConstants.NODE_FIELD_ID, "start",
				AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_START,
				AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED,
				AutomationConstants.NODE_FIELD_CONFIG, Map.of());
		Map<String, Object> agent = Map.of(AutomationConstants.NODE_FIELD_ID, "agent",
				AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_AGENT_RUN,
				AutomationConstants.NODE_FIELD_OUTPUT_VAR, "brief",
				AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED,
				AutomationConstants.NODE_FIELD_CONFIG, config);
		Map<String, Object> edge = Map.of(AutomationConstants.NODE_FIELD_ID, "e1",
				AutomationConstants.EDGE_FIELD_KIND, AutomationConstants.EDGE_KIND_CONTROL,
				AutomationConstants.EDGE_FIELD_SOURCE, "start",
				AutomationConstants.EDGE_FIELD_SOURCE_PORT, AutomationConstants.CONTROL_PORT_OUT,
				AutomationConstants.EDGE_FIELD_TARGET, "agent",
				AutomationConstants.EDGE_FIELD_TARGET_PORT, AutomationConstants.CONTROL_PORT_IN);
		return AutomationDefinitionValidator.parseAndValidate(AutomationRuntimeUtils.GSON.toJson(Map.of(
				AutomationConstants.DOC_FORMAT_VERSION, AutomationConstants.PYTHON_DOC_CURRENT_VERSION,
				AutomationConstants.DOC_TRIGGER_BINDINGS, List.of(Map.of("id", "manual", "type", "manual")),
				AutomationConstants.DOC_GRAPH,
				Map.of(AutomationConstants.DOC_NODES, List.of(start, agent), AutomationConstants.DOC_EDGES,
						List.of(edge)))));
	}

	@Test
	void rejectsAnonymousAndMissingUsers() {
		User anonymous = new User();
		anonymous.setAnonymous(true);

		assertThrows(IllegalArgumentException.class, () -> AutomationProjectService.inlineRunScope(anonymous));
		assertThrows(IllegalArgumentException.class, () -> AutomationProjectService.inlineRunScope(new User()));
		assertThrows(IllegalArgumentException.class, () -> AutomationProjectService.inlineRunScope(null));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationProjectService.resolveRunScope(anonymous, null, false));
	}
}
