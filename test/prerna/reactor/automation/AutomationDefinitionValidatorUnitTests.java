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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/**
 * Covers the authoring and execution contracts for an Automation graph. Drafts
 * may be incomplete, while anything accepted for execution must be runnable.
 */
public class AutomationDefinitionValidatorUnitTests {

	private static Map<String, Object> startNode(Map<String, Object> config) {
		Map<String, Object> node = new LinkedHashMap<>();
		node.put(AutomationConstants.NODE_FIELD_ID, "start");
		node.put(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_START);
		node.put(AutomationConstants.NODE_FIELD_LABEL, "Start");
		node.put("position", Map.of("x", 0, "y", 0));
		node.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED);
		node.put(AutomationConstants.NODE_FIELD_CONFIG, config);
		return node;
	}

	private static Map<String, Object> workNode(String type, Map<String, Object> config) {
		Map<String, Object> node = new LinkedHashMap<>();
		node.put(AutomationConstants.NODE_FIELD_ID, "work");
		node.put(AutomationConstants.NODE_FIELD_TYPE, type);
		node.put(AutomationConstants.NODE_FIELD_LABEL, "Work");
		node.put(AutomationConstants.NODE_FIELD_OUTPUT_VAR, "work_1");
		node.put("position", Map.of("x", 300, "y", 0));
		node.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED);
		node.put(AutomationConstants.NODE_FIELD_CONFIG, config);
		return node;
	}

	private static Map<String, Object> controlEdge() {
		Map<String, Object> edge = new LinkedHashMap<>();
		edge.put(AutomationConstants.NODE_FIELD_ID, "e-start-work");
		edge.put(AutomationConstants.EDGE_FIELD_KIND, AutomationConstants.EDGE_KIND_CONTROL);
		edge.put(AutomationConstants.EDGE_FIELD_SOURCE, "start");
		edge.put(AutomationConstants.EDGE_FIELD_SOURCE_PORT, AutomationConstants.CONTROL_PORT_OUT);
		edge.put(AutomationConstants.EDGE_FIELD_TARGET, "work");
		edge.put(AutomationConstants.EDGE_FIELD_TARGET_PORT, AutomationConstants.CONTROL_PORT_IN);
		return edge;
	}

	private static String definition(Map<String, Object> startConfig) {
		return document(startConfig, null, AutomationConstants.PYTHON_DOC_CURRENT_VERSION, true);
	}

	private static String definition(Map<String, Object> startConfig, Map<String, Object> workNode) {
		return document(startConfig, workNode, AutomationConstants.PYTHON_DOC_CURRENT_VERSION, true);
	}

	/**
	 * @param startConfig trigger node configuration under test
	 * @param workNode    optional second node, connected to the trigger when linked
	 * @param version     document format version
	 * @param linked      whether the work node is wired to the trigger
	 * @return serialized definition
	 */
	private static String document(Map<String, Object> startConfig, Map<String, Object> workNode, int version,
			boolean linked) {
		List<Map<String, Object>> nodes = new ArrayList<>();
		nodes.add(startNode(startConfig));
		List<Map<String, Object>> edges = new ArrayList<>();
		if (workNode != null) {
			nodes.add(workNode);
			if (linked) {
				edges.add(controlEdge());
			}
		}

		Map<String, Object> graph = new LinkedHashMap<>();
		graph.put(AutomationConstants.DOC_NODES, nodes);
		graph.put(AutomationConstants.DOC_EDGES, edges);

		Map<String, Object> definition = new LinkedHashMap<>();
		definition.put(AutomationConstants.DOC_FORMAT_VERSION, version);
		definition.put(AutomationConstants.DOC_DESCRIPTION, "");
		definition.put(AutomationConstants.DOC_TRIGGER_BINDINGS, List.of(Map.of("id", "manual", "type", "manual")));
		definition.put(AutomationConstants.DOC_GRAPH, graph);
		return AutomationRuntimeUtils.GSON.toJson(definition);
	}

	private static Map<String, Object> databaseConfig(String query) {
		Map<String, Object> config = new LinkedHashMap<>();
		config.put("engineId", "engine-1");
		config.put("query", query);
		return config;
	}

	private static Map<String, Object> jevConfig() {
		Map<String, Object> config = new LinkedHashMap<>();
		config.put(AutomationConstants.CONFIG_ENGINE_ID, "typesafe-engine");
		config.put(AutomationConstants.CONFIG_STATE, "${request}");
		config.put(AutomationConstants.CONFIG_QUESTION, "Which route best matches this request?");
		config.put(AutomationConstants.CONFIG_CLAUSES,
				List.of(Map.of(AutomationConstants.CONFIG_CLAUSE_ID, "research",
						AutomationConstants.CONFIG_DESCRIPTION, "Research and summarize information")));
		config.put(AutomationConstants.CONFIG_CONFIDENCE_THRESHOLD, 0.6);
		config.put(AutomationConstants.CONFIG_PARAM_VALUES, Map.of());
		return config;
	}

	private static Map<String, Object> jevNoulConfig() {
		Map<String, Object> config = jevConfig();
		config.put(AutomationConstants.CONFIG_QUESTION_TYPE, AutomationConstants.JEV_QUESTION_TYPE_NOUL);
		config.put(AutomationConstants.CONFIG_CLAUSES,
				List.of(
						Map.of(AutomationConstants.CONFIG_CLAUSE_ID, "yes-route",
								AutomationConstants.CONFIG_DESCRIPTION, "Continue automatically",
								AutomationConstants.CONFIG_ANSWER, true),
						Map.of(AutomationConstants.CONFIG_CLAUSE_ID, "no-route",
								AutomationConstants.CONFIG_DESCRIPTION, "Send for review",
								AutomationConstants.CONFIG_ANSWER, false)));
		return config;
	}

	/** The statement a generated node of this type is required to carry. */
	private static String statementFor(String type) {
		if (AutomationConstants.NODE_DATABASE_INSERT.equals(type)) {
			return "INSERT INTO CLAIMS (ID) VALUES (1)";
		}
		if (AutomationConstants.NODE_DATABASE_UPDATE.equals(type)) {
			return "UPDATE CLAIMS SET STATUS = 'OPEN' WHERE ID = 1";
		}
		return "DELETE FROM CLAIMS WHERE ID = 1";
	}

	@Test
	void acceptsATriggerOnlyGraph() {
		AutomationDefinitionValidator.ValidatedDefinition validated = AutomationDefinitionValidator
				.parseAndValidateForAuthoring(definition(Map.of()));
		assertEquals(1, validated.nodes().size());
	}

	@Test
	void acceptsEveryDatabaseWriteNode() {
		for (String type : new String[] { AutomationConstants.NODE_DATABASE_INSERT,
				AutomationConstants.NODE_DATABASE_UPDATE, AutomationConstants.NODE_DATABASE_DELETE }) {
			AutomationDefinitionValidator.ValidatedDefinition validated = AutomationDefinitionValidator
					.parseAndValidateForAuthoring(
							definition(Map.of(), workNode(type, databaseConfig(statementFor(type)))));
			assertEquals(2, validated.nodes().size(), type + " should validate");
		}
	}

	@Test
	void acceptsAJevDecisionDraft() {
		Map<String, Object> node = workNode(AutomationConstants.NODE_CONTROL_JEV, jevConfig());
		node.remove(AutomationConstants.NODE_FIELD_OUTPUT_VAR);
		AutomationDefinitionValidator.ValidatedDefinition validated = AutomationDefinitionValidator
				.parseAndValidateForAuthoring(definition(Map.of(), node));
		assertEquals(2, validated.nodes().size());
	}

	@Test
	void acceptsAJevNoulDecisionWithExplicitYesAndNoRoutes() {
		Map<String, Object> node = workNode(AutomationConstants.NODE_CONTROL_JEV, jevNoulConfig());
		node.remove(AutomationConstants.NODE_FIELD_OUTPUT_VAR);
		AutomationDefinitionValidator.ValidatedDefinition validated = AutomationDefinitionValidator
				.parseAndValidateForAuthoring(definition(Map.of(), node));
		assertEquals(2, validated.nodes().size());
	}

	@Test
	void jevDecisionRequiresDescribedRoutesAndBoundedConfidence() {
		Map<String, Object> missingDescription = jevConfig();
		missingDescription.put(AutomationConstants.CONFIG_CLAUSES,
				List.of(Map.of(AutomationConstants.CONFIG_CLAUSE_ID, "research",
						AutomationConstants.CONFIG_DESCRIPTION, "")));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionValidator.parseAndValidateForAuthoring(definition(Map.of(),
						workNode(AutomationConstants.NODE_CONTROL_JEV, missingDescription))));

		Map<String, Object> invalidConfidence = jevConfig();
		invalidConfidence.put(AutomationConstants.CONFIG_CONFIDENCE_THRESHOLD, 1.1);
		assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionValidator.parseAndValidateForAuthoring(definition(Map.of(),
						workNode(AutomationConstants.NODE_CONTROL_JEV, invalidConfidence))));
	}

	@Test
	void jevNoulDecisionRequiresOneYesRouteOneNoRouteAndMajorityConfidence() {
		Map<String, Object> duplicateAnswers = jevNoulConfig();
		duplicateAnswers.put(AutomationConstants.CONFIG_CLAUSES,
				List.of(
						Map.of(AutomationConstants.CONFIG_CLAUSE_ID, "first",
								AutomationConstants.CONFIG_DESCRIPTION, "First path",
								AutomationConstants.CONFIG_ANSWER, true),
						Map.of(AutomationConstants.CONFIG_CLAUSE_ID, "second",
								AutomationConstants.CONFIG_DESCRIPTION, "Second path",
								AutomationConstants.CONFIG_ANSWER, true)));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionValidator.parseAndValidateForAuthoring(definition(Map.of(),
						workNode(AutomationConstants.NODE_CONTROL_JEV, duplicateAnswers))));

		Map<String, Object> lowThreshold = jevNoulConfig();
		lowThreshold.put(AutomationConstants.CONFIG_CONFIDENCE_THRESHOLD, 0.49);
		assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionValidator.parseAndValidateForAuthoring(definition(Map.of(),
						workNode(AutomationConstants.NODE_CONTROL_JEV, lowThreshold))));
	}

	@Test
	void jevDecisionRejectsUnknownQuestionType() {
		Map<String, Object> config = jevConfig();
		config.put(AutomationConstants.CONFIG_QUESTION_TYPE, "score");
		assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionValidator.parseAndValidateForAuthoring(
						definition(Map.of(), workNode(AutomationConstants.NODE_CONTROL_JEV, config))));
	}

	/** A write with no statement would render a call against Python None. */
	@Test
	void databaseWritesRequireAStatement() {
		for (String type : new String[] { AutomationConstants.NODE_DATABASE_INSERT,
				AutomationConstants.NODE_DATABASE_UPDATE, AutomationConstants.NODE_DATABASE_DELETE }) {
			assertThrows(IllegalArgumentException.class,
					() -> AutomationDefinitionValidator.parseAndValidateForAuthoring(
							definition(Map.of(), workNode(type, Map.of("engineId", "engine-1")))),
					type + " must require a query");
		}
	}

	/** A generated node must carry the statement its operation actually performs. */
	@Test
	void databaseNodesRejectTheWrongStatementType() {
		assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionValidator.parseAndValidateForAuthoring(definition(Map.of(), workNode(
						AutomationConstants.NODE_DATABASE_DELETE, databaseConfig("SELECT * FROM CLAIMS")))));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionValidator.parseAndValidateForAuthoring(definition(Map.of(),
						workNode(AutomationConstants.NODE_DATABASE_QUERY,
								databaseConfig("DELETE FROM CLAIMS WHERE ID = 1")))));
	}

	/**
	 * An unqualified DELETE or UPDATE empties or rewrites the whole table, and an
	 * automation runs unattended, so a WHERE clause is required.
	 */
	@Test
	void destructiveWritesRequireAWhereClause() {
		assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionValidator.parseAndValidateForAuthoring(definition(Map.of(), workNode(
						AutomationConstants.NODE_DATABASE_DELETE, databaseConfig("DELETE FROM CLAIMS")))));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionValidator.parseAndValidateForAuthoring(definition(Map.of(),
						workNode(AutomationConstants.NODE_DATABASE_UPDATE,
								databaseConfig("UPDATE CLAIMS SET STATUS = 'OPEN'")))));
	}

	@Test
	void triggerSetupSourceMustBeAString() {
		assertThrows(IllegalArgumentException.class, () -> AutomationDefinitionValidator
				.parseAndValidateForAuthoring(definition(Map.of(AutomationConstants.CONFIG_PYTHON_SOURCE, 42))));
	}

	@Test
	void triggerAcceptsSetupSourceAndGlobals() {
		Map<String, Object> config = new LinkedHashMap<>();
		config.put(AutomationConstants.CONFIG_PYTHON_SOURCE, "def run(scope):\n    return {}\n");
		config.put(AutomationConstants.CONFIG_GLOBALS,
				List.of(Map.of("name", "lookback_days", AutomationConstants.CONFIG_DEFAULT_VALUE, "7")));
		AutomationDefinitionValidator.parseAndValidateForAuthoring(definition(config));
	}

	/** Globals become Python identifiers in scope, so the name has to be one. */
	@Test
	void triggerGlobalNamesMustBeIdentifiers() {
		Map<String, Object> config = Map.of(AutomationConstants.CONFIG_GLOBALS,
				List.of(Map.of("name", "not a name", AutomationConstants.CONFIG_DEFAULT_VALUE, "7")));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionValidator.parseAndValidateForAuthoring(definition(config)));
	}

	/** Editors may persist a detached draft, but the runtime cannot execute it. */
	@Test
	void detachedNodesAreDraftOnly() {
		String json = document(Map.of(),
				workNode(AutomationConstants.NODE_DATABASE_DELETE,
						databaseConfig(statementFor(AutomationConstants.NODE_DATABASE_DELETE))),
				AutomationConstants.PYTHON_DOC_CURRENT_VERSION, false);

		AutomationDefinitionValidator.parseAndValidateForAuthoring(json);
		assertThrows(IllegalArgumentException.class, () -> AutomationDefinitionValidator.parseAndValidate(json));
	}

	@Test
	void rejectsAnUnknownFormatVersion() {
		String json = document(Map.of(), null, AutomationConstants.PYTHON_DOC_CURRENT_VERSION + 1, true);
		assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionValidator.parseAndValidateForAuthoring(json));
	}
}
