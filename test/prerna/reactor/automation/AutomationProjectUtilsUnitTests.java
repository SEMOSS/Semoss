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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.util.Utility;

class AutomationProjectUtilsUnitTests {

	private static final String PROJECT_ALIAS = "automation-alias";
	private static final String PROJECT_ID = "automation-id";

	@Test
	void resolvesViewableAutomationProjectAlias() {
		User user = mock(User.class);
		IProject project = automationProject();

		try (MockedStatic<SecurityProjectUtils> security = Mockito.mockStatic(SecurityProjectUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
			security.when(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, PROJECT_ALIAS))
					.thenReturn(PROJECT_ID);
			security.when(() -> SecurityProjectUtils.userCanViewProject(user, PROJECT_ID)).thenReturn(true);
			utility.when(() -> Utility.getProject(PROJECT_ID)).thenReturn(project);

			assertSame(project, AutomationProjectUtils.getViewableAutomationProject(user, PROJECT_ALIAS));
			security.verify(() -> SecurityProjectUtils.userCanViewProject(user, PROJECT_ID));
		}
	}

	@Test
	void requiresEditPermissionForEditableProject() {
		User user = mock(User.class);
		IProject project = automationProject();

		try (MockedStatic<SecurityProjectUtils> security = Mockito.mockStatic(SecurityProjectUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
			security.when(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, PROJECT_ALIAS))
					.thenReturn(PROJECT_ID);
			security.when(() -> SecurityProjectUtils.userCanEditProject(user, PROJECT_ID)).thenReturn(true);
			utility.when(() -> Utility.getProject(PROJECT_ID)).thenReturn(project);

			assertSame(project, AutomationProjectUtils.getEditableAutomationProject(user, PROJECT_ALIAS));
			security.verify(() -> SecurityProjectUtils.userCanEditProject(user, PROJECT_ID));
		}
	}

	@Test
	void rejectsProjectWithoutRequiredAccess() {
		User user = mock(User.class);

		try (MockedStatic<SecurityProjectUtils> security = Mockito.mockStatic(SecurityProjectUtils.class)) {
			security.when(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, PROJECT_ALIAS))
					.thenReturn(PROJECT_ID);
			security.when(() -> SecurityProjectUtils.userCanEditProject(user, PROJECT_ID)).thenReturn(false);

			IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
					() -> AutomationProjectUtils.getEditableAutomationProject(user, PROJECT_ALIAS));
			assertEquals("Project does not exist or user does not have edit access.", exception.getMessage());
		}
	}

	@Test
	void rejectsNonAutomationProject() {
		User user = mock(User.class);
		IProject project = mock(IProject.class);
		when(project.getProjectType()).thenReturn(IProject.PROJECT_TYPE.BLOCKS);

		try (MockedStatic<SecurityProjectUtils> security = Mockito.mockStatic(SecurityProjectUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
			security.when(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, PROJECT_ALIAS))
					.thenReturn(PROJECT_ID);
			security.when(() -> SecurityProjectUtils.userCanViewProject(user, PROJECT_ID)).thenReturn(true);
			utility.when(() -> Utility.getProject(PROJECT_ID)).thenReturn(project);

			IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
					() -> AutomationProjectUtils.getViewableAutomationProject(user, PROJECT_ALIAS));
			assertEquals("Project is not an automation project: " + PROJECT_ID, exception.getMessage());
		}
	}

	private static IProject automationProject() {
		IProject project = mock(IProject.class);
		when(project.getProjectType()).thenReturn(IProject.PROJECT_TYPE.AUTOMATION);
		when(project.getProjectId()).thenReturn(PROJECT_ID);
		return project;
	}
}
