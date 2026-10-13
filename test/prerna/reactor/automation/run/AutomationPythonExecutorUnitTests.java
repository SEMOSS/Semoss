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
package prerna.reactor.automation.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.ds.py.PyTranslator;
import prerna.om.Insight;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.AutomationRuntime;

public class AutomationPythonExecutorUnitTests {

	@Test
	void rejectsTypedNodesOutsideLegacyContracts() {
		AutomationPythonExecutor executor = new AutomationPythonExecutor(mock(Insight.class), "run-1");
		assertThrows(IllegalStateException.class, () -> executor.executeNode(
				Map.of(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_DATABASE_QUERY),
				"def run(scope): return []", Map.of(),
				"result", "/assets", "/assets/py"));
	}

	@Test
	void acquiresAndAttachesPythonOnlyOnFirstInvocation() {
		String runId = UUID.randomUUID().toString();
		Insight insight = mock(Insight.class);
		PyTranslator translator = mock(PyTranslator.class);
		when(insight.getInsightId()).thenReturn("automation-" + runId);
		when(insight.getPyTranslator()).thenReturn(translator);
		when(translator.runScriptWithExplicitAssetPaths(eq(insight), eq("node-script"), eq("/assets"),
				any(String[].class))).thenReturn("raw");

		AutomationRunRegistry.register(runId, insight, null);
		clearInvocations(insight);
		try (MockedStatic<AutomationRuntime> runtime = mockStatic(AutomationRuntime.class)) {
			AutomationRuntime.NodeResult decoded = new AutomationRuntime.NodeResult(Map.of("ready", true));
			runtime.when(() -> AutomationRuntime.buildNodeInvocationScript("source", Map.of("input", 1),
					"result")).thenReturn("node-script");
			runtime.when(() -> AutomationRuntime.decodeNodeResult("raw")).thenReturn(decoded);
			AutomationPythonExecutor executor = new AutomationPythonExecutor(insight, runId);
			verifyNoInteractions(insight);

			Map<String, Object> descriptor = Map.of(
					AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_DEVELOPER_PYTHON);
			assertEquals(decoded, executor.executeNode(descriptor,
					"source", Map.of("input", 1), "result", "/assets", "/assets/py"));
			assertEquals(decoded, executor.executeNode(descriptor,
					"source", Map.of("input", 1), "result", "/assets", "/assets/py"));

			verify(insight, times(1)).getPyTranslator();
			verify(translator, times(2)).runScriptWithExplicitAssetPaths(eq(insight), eq("node-script"),
					eq("/assets"), any(String[].class));
		} finally {
			AutomationRunRegistry.unregister(runId);
		}
	}
}
