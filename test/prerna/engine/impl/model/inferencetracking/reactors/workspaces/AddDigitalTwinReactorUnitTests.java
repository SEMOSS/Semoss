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
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserAuditTrailUtils;

/**
 * Checks what making a Digital Twin adds to making an agent: a user has one,
 * and the agent that was made is marked, so it can only be shared with named
 * people. The project, workspace and security writes are static mocks.
 */
class AddDigitalTwinReactorUnitTests {

	private User user;
	private MockedStatic<SecurityProjectUtils> security;
	private MockedStatic<ProjectHelper> projects;
	private MockedStatic<ModelInferenceLogsUtils> workspaces;
	private MockedStatic<UserAuditTrailUtils> audit;

	@BeforeEach
	void setup() {
		user = new User();
		user.setPrimaryLogin(AuthProvider.NATIVE);
		AccessToken token = new AccessToken();
		token.setId("ownerid");
		token.setProvider(AuthProvider.NATIVE);
		user.setAccessToken(token);

		security = mockStatic(SecurityProjectUtils.class);
		projects = mockStatic(ProjectHelper.class);
		workspaces = mockStatic(ModelInferenceLogsUtils.class);
		audit = mockStatic(UserAuditTrailUtils.class);
	}

	@AfterEach
	void cleanup() {
		audit.close();
		workspaces.close();
		projects.close();
		security.close();
	}

	@Test
	void theNewAgentIsMarkedAsATwin() {
		NounMetadata created = twinNamed("My Twin").execute();

		assertEquals(PixelDataType.CONST_STRING, created.getNounType());
		String twinId = (String) created.getValue();
		// made as any agent is: a private workspace project
		projects.verify(() -> ProjectHelper.createWorkspaceProject(eq(twinId), eq("My Twin"),
				eq(IProject.PROJECT_TYPE.WORKSPACE), eq(false), isNull(), isNull(), eq(user), any()));
		security.verify(() -> SecurityProjectUtils.markDigitalTwin(twinId));
	}

	@Test
	void aSecondTwinIsRefusedBeforeAnythingIsMade() {
		// a twin the user made and owns, the same list the web client reads
		security.when(() -> SecurityProjectUtils.getUserProjectList(eq(user), eq(List.of("WORKSPACE")), isNull(),
				eq(false), eq(Map.of(SecurityProjectUtils.DIGITAL_TWIN_METAKEY, List.of("true"))), isNull(), isNull(),
				isNull(), isNull(), isNull(), eq(false), eq(List.of(1)), eq(User.getUserIdAndType(user))))
				.thenReturn(List.of(Map.of("project_id", "first-twin")));

		NounMetadata refused = twinNamed("Another Twin").execute();

		assertEquals(PixelDataType.ERROR, refused.getNounType());
		assertEquals("You already have a Digital Twin", refused.getValue());
		projects.verifyNoInteractions();
		security.verify(() -> SecurityProjectUtils.markDigitalTwin(any()), never());
	}

	@Test
	void anAgentThatCouldNotBeMadeIsNotMarked() {
		projects.when(() -> ProjectHelper.createWorkspaceProject(any(), any(), any(), anyBoolean(), any(), any(),
				any(), any())).thenThrow(new IllegalStateException("No room left"));

		NounMetadata failed = twinNamed("My Twin").execute();

		assertEquals(PixelDataType.ERROR, failed.getNounType());
		security.verify(() -> SecurityProjectUtils.markDigitalTwin(any()), never());
	}

	private AddDigitalTwinReactor twinNamed(String name) {
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		NounStore store = new NounStore("add-digital-twin-test");
		store.makeNoun(AbstractWorkspaceReactor.NAME).addLiteral(name);
		AddDigitalTwinReactor reactor = new AddDigitalTwinReactor();
		reactor.setInsight(insight);
		reactor.setNounStore(store);
		return reactor;
	}
}
