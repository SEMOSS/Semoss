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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class AutomationDefinitionValidatorTest {

	@Test
	void validatesTriggerOnlyDefinitionWithoutRequiringRunnableSteps() {
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.parseAndValidate(document(
						"[{\"id\":\"trigger\",\"type\":\"trigger\"}]", "[]"));

		assertEquals(AutomationConstants.DOC_CURRENT_VERSION, definition.getVersion());
		assertEquals(1, definition.getNodes().size());
	}

	@Test
	void canonicalizesEquivalentDocumentsToTheSameSnapshotAndHash() {
		String first = """
				{"version":1,"graph":{"nodes":[{"id":"trigger","type":"trigger"},{"id":"step","type":"python-step",
				"config":{"stepRef":"automation/steps/step.py","purpose":"Run the step"}}],
				"edges":[{"source":"trigger","target":"step"}]}}""";
		String second = """
				{"graph":{"edges":[{"target":"step","source":"trigger"}],
				"nodes":[{"type":"trigger","id":"trigger"},{"config":{"purpose":"Run the step",
				"stepRef":"automation/steps/step.py"},"type":"python-step","id":"step"}]},"version":1}""";

		AutomationDefinitionValidator.ValidatedDefinition firstDefinition =
				AutomationDefinitionValidator.parseAndValidate(first);
		AutomationDefinitionValidator.ValidatedDefinition secondDefinition =
				AutomationDefinitionValidator.parseAndValidate(second);

		assertEquals(firstDefinition.getSnapshot(), secondDefinition.getSnapshot());
		assertEquals(firstDefinition.getHash(), secondDefinition.getHash());
	}

	@Test
	void rejectsInvalidVersionsAndNodes() {
		assertThrows(IllegalArgumentException.class, () ->
				AutomationDefinitionValidator.parseAndValidate(
						"{\"version\":2,\"graph\":{\"nodes\":[],\"edges\":[]}}"));
		assertThrows(IllegalArgumentException.class, () ->
				AutomationDefinitionValidator.parseAndValidate(document(
						"[{\"id\":\"trigger\",\"type\":\"trigger\"},{\"id\":\"trigger\",\"type\":\"wait\"}]", "[]")));
		assertThrows(IllegalArgumentException.class, () ->
				AutomationDefinitionValidator.parseAndValidate(document(
						"[{\"id\":\"trigger\",\"type\":\"trigger\"},{\"id\":\"unknown\",\"type\":\"unknown\","
								+ "\"config\":{\"stepRef\":\"automation/steps/unknown.py\"}}]", "[]")));
		assertThrows(IllegalArgumentException.class, () ->
				AutomationDefinitionValidator.parseAndValidate(document(
						"[{\"id\":\"trigger\",\"type\":\"trigger\"},{\"id\":\"python\",\"type\":\"python-step\","
								+ "\"config\":{\"stepRef\":\"../../not-a-step.py\"}}]", "[]")));
	}

	@Test
	void validatesPythonStepReferencesAndInputMappings() {
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.parseAndValidate(document(
						"[{\"id\":\"trigger\",\"type\":\"trigger\"},{\"id\":\"python\",\"type\":\"python-step\","
								+ "\"config\":{\"stepRef\":\"automation/steps/notify_slack.py\","
								+ "\"purpose\":\"Post a ticket summary\","
								+ "\"outputDescription\":\"The posted message ID\","
								+ "\"inputs\":{\"summary\":\"${summary}\"}}}]",
						"[{\"source\":\"trigger\",\"target\":\"python\"}]"));

		assertEquals(2, definition.getNodes().size());
	}

	@Test
	void rejectsPythonStepWithoutSetupPurpose() {
		assertThrows(IllegalArgumentException.class, () ->
				AutomationDefinitionValidator.parseAndValidate(document(
						"[{\"id\":\"trigger\",\"type\":\"trigger\"},{\"id\":\"python\",\"type\":\"python-step\","
								+ "\"config\":{\"stepRef\":\"automation/steps/normalize.py\",\"inputs\":{}}}]",
						"[{\"source\":\"trigger\",\"target\":\"python\"}]")));
	}

	@Test
	void rejectsInvalidEdgesAndCycles() {
		assertThrows(IllegalArgumentException.class, () ->
				AutomationDefinitionValidator.parseAndValidate(document(
						"[{\"id\":\"trigger\",\"type\":\"trigger\"}]", "[{\"source\":\"trigger\",\"target\":\"missing\"}]")));
		assertThrows(IllegalArgumentException.class, () ->
				AutomationDefinitionValidator.parseAndValidate(document(
						"[{\"id\":\"trigger\",\"type\":\"trigger\"}]", "[{\"source\":\"trigger\",\"target\":\"trigger\"}]")));
		assertThrows(IllegalArgumentException.class, () ->
				AutomationDefinitionValidator.parseAndValidate(document(
						"[{\"id\":\"trigger\",\"type\":\"trigger\"},{\"id\":\"one\",\"type\":\"python-step\","
								+ "\"config\":{\"stepRef\":\"automation/steps/one.py\",\"purpose\":\"One\"}},"
								+ "{\"id\":\"two\",\"type\":\"python-step\","
								+ "\"config\":{\"stepRef\":\"automation/steps/two.py\",\"purpose\":\"Two\"}}]",
						"[{\"source\":\"trigger\",\"target\":\"one\"},{\"source\":\"one\",\"target\":\"two\"},{\"source\":\"two\",\"target\":\"one\"}]")));
	}

	@Test
	void ordersNodesByDependenciesWhilePreservingReadyNodeDocumentOrder() {
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.parseAndValidate(document(
						"[{\"id\":\"trigger\",\"type\":\"trigger\"},{\"id\":\"second\",\"type\":\"python-step\","
								+ "\"config\":{\"stepRef\":\"automation/steps/second.py\",\"purpose\":\"Second\"}},"
								+ "{\"id\":\"first\",\"type\":\"python-step\","
								+ "\"config\":{\"stepRef\":\"automation/steps/first.py\",\"purpose\":\"First\"}},"
								+ "{\"id\":\"join\",\"type\":\"python-step\","
								+ "\"config\":{\"stepRef\":\"automation/steps/join.py\",\"purpose\":\"Join\"}}]",
						"[{\"source\":\"trigger\",\"target\":\"first\"},{\"source\":\"trigger\",\"target\":\"second\"},"
								+ "{\"source\":\"first\",\"target\":\"join\"},{\"source\":\"second\",\"target\":\"join\"}]"));

		List<String> nodeIds = definition.getExecutionOrder().stream()
				.map(node -> (String) node.get(AutomationConstants.NODE_FIELD_ID))
				.toList();

		assertEquals(List.of("trigger", "second", "first", "join"), nodeIds);
	}

	private static String document(String nodes, String edges) {
		return "{\"version\":1,\"graph\":{\"nodes\":" + nodes + ",\"edges\":" + edges + "}}";
	}
}
