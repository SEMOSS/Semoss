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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.om.Insight;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.sablecc2.om.NounStore;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Covers the Py() switches RunAutomationDefinition applies to caller-written
 * Python.
 */
class RunAutomationDefinitionReactorUnitTests {

	private static final String DEFINITION = """
			{
			  "formatVersion": 2,
			  "triggerBindings": [{ "id": "manual", "type": "manual" }],
			  "graph": {
			    "nodes": [
			      { "id": "start", "type": "trigger.start", "codeMode": "generated", "config": {} },
			      { "id": "work", "type": "%s", "outputVar": "work_result", "codeMode": "%s", "config": %s }
			    ],
			    "edges": [{ "id": "e1", "kind": "control", "source": "start", "sourcePort": "out",
			      "target": "work", "targetPort": "in" }]
			  }
			}
			""";

	private static RunAutomationDefinitionReactor reactor(Insight insight, String json) {
		return reactor(insight, json, false);
	}

	private static RunAutomationDefinitionReactor reactor(Insight insight, String json, boolean withPythonSource) {
		RunAutomationDefinitionReactor reactor = new RunAutomationDefinitionReactor();
		reactor.setInsight(insight);
		NounStore store = new NounStore("all");
		store.makeNoun("json").addLiteral(json);
		if (withPythonSource) {
			store.makeNoun("nodeSources").addLiteral("{\"work\":\"def run(scope):\\n    return 1\\n\"}");
		}
		reactor.setNounStore(store);
		return reactor;
	}

	private static Insight insight() {
		AccessToken token = new AccessToken();
		token.setId("owner");
		token.setProvider(AuthProvider.NATIVE);
		User user = new User();
		user.setAccessToken(token);
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		return insight;
	}

	@Test
	void refusesCallerPythonWhenThePythonTerminalIsDisabled() {
		String custom = DEFINITION.formatted(AutomationConstants.NODE_DEVELOPER_PYTHON,
				AutomationConstants.NODE_CODE_MODE_CUSTOM, "{}");
		try (var utility = mockStatic(Utility.class); var store = mockStatic(AutomationRunStore.class)) {
			utility.when(() -> Utility.getDIHelperProperty(Constants.DISABLE_PY_TERMINAL)).thenReturn("true");

			IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
					() -> reactor(insight(), custom, true).execute());

			assertEquals("Python terminal has been disabled.", error.getMessage());
			store.verifyNoInteractions();
		}
	}

	@Test
	void runsAGeneratedOnlyDefinitionAsTheCallerWhenThePythonTerminalIsDisabled() {
		String generated = DEFINITION.formatted(AutomationConstants.NODE_CONTROL_WAIT,
				AutomationConstants.NODE_CODE_MODE_GENERATED, "{ \"durationSeconds\": 1 }");
		Insight insight = insight();
		String ownScope = AutomationProjectService.inlineRunScope(insight.getUser());
		try (var utility = mockStatic(Utility.class); var store = mockStatic(AutomationRunStore.class)) {
			utility.when(() -> Utility.getDIHelperProperty(Constants.DISABLE_PY_TERMINAL)).thenReturn("true");
			// Stop at the durable start: reaching it proves the switches did not apply.
			store.when(() -> AutomationRunStore.initializeRun(anyString(), eq(ownScope),
					eq(AutomationConstants.DEFAULT_AUTOMATION_ID), anyInt(), any(), any(), any(),
					eq(AutomationConstants.TRIGGER_MANUAL), any(), any(), any(), any()))
					.thenThrow(new IllegalStateException("initialized"));

			IllegalStateException stop = assertThrows(IllegalStateException.class,
					() -> reactor(insight, generated).execute());

			assertEquals("initialized", stop.getMessage());
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = { true, false })
	void auditsStartOnlyAfterDurableInitialization(boolean initializationSucceeds) {
		String generated = DEFINITION.formatted(AutomationConstants.NODE_CONTROL_WAIT,
				AutomationConstants.NODE_CODE_MODE_GENERATED, "{ \"durationSeconds\": 1 }");
		Insight insight = insight();
		String ownScope = AutomationProjectService.inlineRunScope(insight.getUser());
		AtomicBoolean initialized = new AtomicBoolean();
		try (var utility = mockStatic(Utility.class);
				var store = mockStatic(AutomationRunStore.class);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			store.when(() -> AutomationRunStore.initializeRun(anyString(), eq(ownScope),
					eq(AutomationConstants.DEFAULT_AUTOMATION_ID), anyInt(), any(), any(), any(),
					eq(AutomationConstants.TRIGGER_MANUAL), any(), any(), any(), any())).thenAnswer(call -> {
						if (!initializationSucceeds) {
							throw new IllegalStateException("initialization failed");
						}
						initialized.set(true);
						return null;
					});
			audit.when(() -> UserAuditTrailUtils.recordAutomationEvent(eq(insight.getUser()),
					eq(AuditEventType.AUTOMATION_RUN_START), eq(ownScope), anyString(), isNull(), any()))
					.thenAnswer(call -> {
						assertTrue(initialized.get());
						Map<?, ?> details = call.getArgument(5);
						assertEquals(AutomationConstants.TRIGGER_MANUAL, details.get("triggerType"));
						assertEquals(2, details.get("nodeCount"));
						// Stop here to verify start ordering independently of the execution engine.
						throw new IllegalStateException("audited start");
					});
			IllegalStateException stop = assertThrows(IllegalStateException.class,
					() -> reactor(insight, generated).execute());
			assertEquals(initializationSucceeds ? "audited start" : "initialization failed", stop.getMessage());
			if (!initializationSucceeds) {
				audit.verifyNoInteractions();
			}
		}
	}
}
