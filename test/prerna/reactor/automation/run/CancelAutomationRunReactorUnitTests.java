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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class CancelAutomationRunReactorUnitTests {

	@Test
	void submittedObservationStillCancelsAndSignalsAfterWorkerClaim() {
		CancelAutomationRunReactor reactor = reactor("project", "run");
		IProject project = mock(IProject.class);
		when(project.getProjectId()).thenReturn("project");

		try (var projects = mockStatic(AutomationProjectService.class);
				var store = mockStatic(AutomationRunStore.class);
				var registry = mockStatic(AutomationRunRegistry.class)) {
			projects.when(() -> AutomationProjectService.getEditableAutomationProject(
					org.mockito.ArgumentMatchers.any(User.class), org.mockito.ArgumentMatchers.eq("project")))
					.thenReturn(project);
			store.when(() -> AutomationRunStore.getRunDetail("run"))
					.thenReturn(Map.of(AutomationConstants.PROJECT_ID, "project",
							AutomationConstants.STATUS, AutomationConstants.STATUS_SUBMITTED));
			store.when(() -> AutomationRunStore.cancelQueuedOrRunningRun("run", "project"))
					.thenReturn(new AutomationRunStore.CancellationResult(
							AutomationConstants.STATUS_RUNNING, true));
			registry.when(() -> AutomationRunRegistry.requestCancellation("run")).thenReturn(true);

			@SuppressWarnings("unchecked")
			Map<String, Object> result = (Map<String, Object>) reactor.execute().getValue();

			assertEquals(AutomationConstants.STATUS_RUNNING, result.get(AutomationConstants.STATUS));
			assertEquals(true, result.get(AutomationConstants.RESULT_CANCEL_REQUESTED));
			assertEquals(true, result.get(AutomationConstants.RESULT_SIGNALLED_LOCALLY));
			verify(project).getProjectId();
			registry.verify(() -> AutomationRunRegistry.requestCancellation("run"));
		}
	}

	@Test
	void waitingForItemsReachesStoreCancellationAndReturnsCancelled() {
		CancelAutomationRunReactor reactor = reactor("project", "run");
		IProject project = mock(IProject.class);
		when(project.getProjectId()).thenReturn("project");

		try (var projects = mockStatic(AutomationProjectService.class);
				var store = mockStatic(AutomationRunStore.class);
				var registry = mockStatic(AutomationRunRegistry.class)) {
			projects.when(() -> AutomationProjectService.getEditableAutomationProject(
					org.mockito.ArgumentMatchers.any(User.class), org.mockito.ArgumentMatchers.eq("project")))
					.thenReturn(project);
			store.when(() -> AutomationRunStore.getRunDetail("run"))
					.thenReturn(Map.of(AutomationConstants.PROJECT_ID, "project",
							AutomationConstants.STATUS, AutomationConstants.STATUS_WAITING_FOR_ITEMS));
			store.when(() -> AutomationRunStore.cancelQueuedOrRunningRun("run", "project"))
					.thenReturn(new AutomationRunStore.CancellationResult(
							AutomationConstants.STATUS_CANCELLED, true));
			registry.when(() -> AutomationRunRegistry.requestCancellation("run")).thenReturn(false);

			@SuppressWarnings("unchecked")
			Map<String, Object> result = (Map<String, Object>) reactor.execute().getValue();

			assertEquals(AutomationConstants.STATUS_CANCELLED, result.get(AutomationConstants.STATUS));
			assertEquals(true, result.get(AutomationConstants.RESULT_CANCEL_REQUESTED));
			assertEquals(false, result.get(AutomationConstants.RESULT_SIGNALLED_LOCALLY));
			store.verify(() -> AutomationRunStore.cancelQueuedOrRunningRun("run", "project"));
		}
	}

	private static CancelAutomationRunReactor reactor(String projectId, String runId) {
		CancelAutomationRunReactor reactor = new CancelAutomationRunReactor();
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(mock(User.class));
		NounStore store = mock(NounStore.class);
		when(store.size()).thenReturn(2);
		when(store.getGenRowStruct("project")).thenReturn(row(projectId));
		when(store.getGenRowStruct("runId")).thenReturn(row(runId));
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
