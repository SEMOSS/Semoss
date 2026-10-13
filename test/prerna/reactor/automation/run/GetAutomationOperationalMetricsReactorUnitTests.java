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
 *******************************************************************************/
package prerna.reactor.automation.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class GetAutomationOperationalMetricsReactorUnitTests {

	@Test
	void authorizesAliasAndScopesStoreToResolvedProject() {
		GetAutomationOperationalMetricsReactor reactor = reactor("alias");
		IProject project = mock(IProject.class);
		when(project.getProjectId()).thenReturn("canonical-project");
		Map<String, Object> metrics = Map.of(
				"projectId", "canonical-project",
				"collectedAt", "2026-10-12T23:12:39Z",
				"runs", Map.of("submitted", 1L),
				"leases", Map.of("fresh", 1L),
				"durableItems", Map.of("retry", 0L),
				"effects", Map.of("unknown", 0L),
				"retention", Map.of("retainedBytes", 0L),
				"limits", Map.of("projectAdmissionQuota", 100));

		try (var projects = mockStatic(AutomationProjectService.class);
				var store = mockStatic(AutomationRunStore.class)) {
			projects.when(() -> AutomationProjectService.getViewableAutomationProject(any(User.class), eq("alias")))
					.thenReturn(project);
			store.when(() -> AutomationRunStore.operationalMetrics("canonical-project")).thenReturn(metrics);

			NounMetadata result = reactor.execute();

			assertEquals(PixelDataType.MAP, result.getNounType());
			assertEquals(metrics, result.getValue());
			store.verify(() -> AutomationRunStore.operationalMetrics("canonical-project"));
		}
	}

	@Test
	void deniedProjectNeverReachesMetricsStore() {
		GetAutomationOperationalMetricsReactor reactor = reactor("denied");
		try (var projects = mockStatic(AutomationProjectService.class);
				var store = mockStatic(AutomationRunStore.class)) {
			projects.when(() -> AutomationProjectService.getViewableAutomationProject(any(User.class), eq("denied")))
					.thenThrow(new IllegalArgumentException("Project does not exist or user does not have access."));

			assertThrows(IllegalArgumentException.class, reactor::execute);
			store.verifyNoInteractions();
		}
	}

	@Test
	void missingProjectFailsExplicitlyBeforeAuthorization() {
		GetAutomationOperationalMetricsReactor reactor = reactor(" ");
		try (var projects = mockStatic(AutomationProjectService.class);
				var store = mockStatic(AutomationRunStore.class)) {
			IllegalArgumentException error = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must provide a project id", error.getMessage());
			projects.verifyNoInteractions();
			store.verifyNoInteractions();
		}
	}

	private static GetAutomationOperationalMetricsReactor reactor(String projectId) {
		GetAutomationOperationalMetricsReactor reactor = new GetAutomationOperationalMetricsReactor();
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(mock(User.class));
		NounStore store = mock(NounStore.class);
		when(store.size()).thenReturn(1);
		when(store.getGenRowStruct("project")).thenReturn(row(projectId));
		reactor.setInsight(insight);
		reactor.setNounStore(store);
		return reactor;
	}

	private static GenRowStruct row(String value) {
		GenRowStruct row = new GenRowStruct();
		row.add(new NounMetadata(value, PixelDataType.CONST_STRING));
		return row;
	}
}
