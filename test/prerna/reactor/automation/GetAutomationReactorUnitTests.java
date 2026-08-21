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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class GetAutomationReactorUnitTests {

	private static final String PROJECT_ID = "automation-id";
	private static final String DEFINITION = """
			{"formatVersion":2,"graph":{"nodes":[
			  {"id":"start","type":"trigger.start","config":{}}
			],"edges":[]}}
			""";

	@Test
	void readsDefinitionWithoutMutatingProjectOrMcpAssets() {
		GetAutomationReactor reactor = new GetAutomationReactor();
		Insight insight = mock(Insight.class);
		User user = mock(User.class);
		IProject project = mock(IProject.class);
		when(insight.getUser()).thenReturn(user);
		when(project.getProjectId()).thenReturn(PROJECT_ID);
		reactor.setInsight(insight);
		reactor.setNounStore(new NounStore("test"));
		reactor.keyValue.put(ReactorKeysEnum.PROJECT.getKey(), PROJECT_ID);

		AutomationDefinitionService.DefinitionFiles files =
				new AutomationDefinitionService.DefinitionFiles(DEFINITION, Map.of());
		try (MockedStatic<AutomationProjectUtils> projects = Mockito.mockStatic(AutomationProjectUtils.class);
				MockedStatic<AutomationDefinitionService> definitions =
						Mockito.mockStatic(AutomationDefinitionService.class);
				MockedStatic<AutomationMcpSync> mcpSync = Mockito.mockStatic(AutomationMcpSync.class)) {
			projects.when(() -> AutomationProjectUtils.getViewableAutomationProject(user, PROJECT_ID))
					.thenReturn(project);
			definitions.when(() -> AutomationDefinitionService.load(PROJECT_ID)).thenReturn(files);

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			definitions.verify(() -> AutomationDefinitionService.load(PROJECT_ID));
			mcpSync.verifyNoInteractions();
			verify(project, never()).requirePublish(true);
		}
	}
}
