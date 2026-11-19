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
package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;

import prerna.auth.utils.SecurityCatalogDependencyUtils.Dependency;

/**
 * Working out the automatic dependencies of catalog items from their saved
 * configuration, without a security database.
 */
public class CatalogDependencyDerivationUnitTests {

	/** Every id names a model, except guardrails, vectors, storage and projects named so */
	private static final UnaryOperator<String> TYPES = id -> switch (id) {
	case "missing" -> null;
	case "guard1", "guard2" -> "GUARDRAIL";
	case "kb1" -> "VECTOR";
	case "bucket1" -> "STORAGE";
	case "agent2" -> "WORKSPACE";
	case "skill1" -> "SKILL";
	case "app1" -> "CODE";
	default -> "MODEL";
	};

	@Test
	void guardrailPipelines_giveEachGuardrailOnceWithEveryPlaceItChecks() {
		Map<String, Object> pipelines = Map.of("chat",
				Map.of("input", List.of(check("guard1"), check("guard2")), "output", List.of(check("guard1"))));

		List<Dependency> dependencies = CatalogDependencyDerivation.fromGuardrailPipelines(pipelines, TYPES);

		assertEquals(2, dependencies.size());
		Dependency first = dependencies.stream().filter(d -> d.id().equals("guard1")).findFirst().orElseThrow();
		assertEquals(CatalogDependencyRole.GUARDRAIL, first.role());
		assertEquals("GUARDRAIL", first.type());
		assertTrue(first.description().contains("chat input"));
		assertTrue(first.description().contains("chat output"));
		assertTrue(CatalogDependencyDerivation.fromGuardrailPipelines(Map.of(), TYPES).isEmpty());
	}

	@Test
	void smss_givesAGuardrailsJudgeAndVectorAndAVectorsEmbedder() {
		List<Dependency> guardrail = CatalogDependencyDerivation.fromSmss("GUARDRAIL",
				Map.of("MODEL_ENGINE_ID", " judge1 ", "VECTOR_ENGINE_ID", "kb1"), TYPES);
		assertEquals(List.of(CatalogDependencyRole.JUDGE_MODEL, CatalogDependencyRole.TOPIC_VECTOR),
				guardrail.stream().map(Dependency::role).toList());
		assertEquals("judge1", guardrail.get(0).id());

		List<Dependency> vector = CatalogDependencyDerivation.fromSmss("VECTOR",
				Map.of("EMBEDDER_ENGINE_ID", "embedder1"), TYPES);
		assertEquals(CatalogDependencyRole.EMBEDDER, vector.get(0).role());

		List<Dependency> function = CatalogDependencyDerivation.fromSmss("FUNCTION",
				Map.of("GOOGLE_BUCKET_ENGINEID", "bucket1"), TYPES);
		assertEquals("STORAGE", function.get(0).type());

		// a key that names nothing, or a key of another engine type, is no dependency
		assertTrue(CatalogDependencyDerivation.fromSmss("GUARDRAIL", Map.of("MODEL_ENGINE_ID", "missing"), TYPES)
				.isEmpty());
		assertTrue(CatalogDependencyDerivation.fromSmss("MODEL", Map.of("EMBEDDER_ENGINE_ID", "embedder1"), TYPES)
				.isEmpty());
	}

	@Test
	void routerConfig_givesRoutesClassifierEmbeddingsAndFallbacks() {
		String json = """
				{"mode":"llm","classifier_engine":"classifier1","embeddings_engine":"embed1",
				 "fallbacks":["fallback1"],
				 "routes":[{"name":"code","engine_id":"model1"},{"name":"math","engine_id":"model1"},
				           {"name":"chat","engine_id":"model2"}]}
				""";

		List<Dependency> dependencies = CatalogDependencyDerivation.fromRouterConfigJson(json, TYPES);

		assertEquals(5, dependencies.size());
		Dependency model1 = dependencies.get(0);
		assertEquals(CatalogDependencyRole.ROUTE_MODEL, model1.role());
		assertTrue(model1.description().contains("'code', 'math' routes"));
		assertTrue(dependencies.stream().anyMatch(d -> d.role() == CatalogDependencyRole.ROUTER_CLASSIFIER));
		assertTrue(dependencies.stream().anyMatch(d -> d.role() == CatalogDependencyRole.ROUTER_EMBEDDINGS));
		assertTrue(dependencies.stream().anyMatch(d -> d.role() == CatalogDependencyRole.ROUTER_FALLBACK));
		assertTrue(CatalogDependencyDerivation.fromRouterConfigJson("not json", TYPES).isEmpty());
	}

	@Test
	void automationNodes_giveEachEngineAppAndAgentOnce() {
		List<Map<String, Object>> nodes = List.of(
				Map.of("id", "n1", "label", "Summarize", "config", Map.of("engineId", "model1")),
				Map.of("id", "n2", "label", "Classify", "config", Map.of("engineId", "model1")),
				Map.of("id", "n3", "label", "Ask agent", "config", Map.of("workspaceId", "agent2", "engineId", "")),
				Map.of("id", "n4", "config", Map.of("appId", "app1")), Map.of("id", "n5", "type", "start"));

		List<Dependency> dependencies = CatalogDependencyDerivation.fromAutomationNodes(nodes, TYPES);

		assertEquals(List.of("model1", "agent2", "app1"), dependencies.stream().map(Dependency::id).toList());
		assertTrue(dependencies.get(0).description().contains("'Summarize', 'Classify'"));
		assertEquals("WORKSPACE", dependencies.get(1).type());
		assertTrue(dependencies.get(2).description().contains("'n4'"));
	}

	@Test
	void workspace_givesToolsKnowledgeSkillsModelAndSubagents() {
		List<Map<String, Object>> resources = List.of(Map.of("resource_id", "kb1", "resource_type", "ENGINE"),
				Map.of("resource_id", "tool1", "resource_type", "ENGINE"),
				Map.of("resource_id", "skill1", "resource_type", "SKILL"),
				Map.of("resource_id", "prompt1", "resource_type", "PROMPT"));

		List<Dependency> dependencies = CatalogDependencyDerivation.fromWorkspace(resources, "model9",
				List.of("agent2"), TYPES);

		assertEquals(List.of(CatalogDependencyRole.KNOWLEDGE, CatalogDependencyRole.TOOLBOX, CatalogDependencyRole.SKILL,
				CatalogDependencyRole.AGENT_MODEL, CatalogDependencyRole.SUBAGENT),
				dependencies.stream().map(Dependency::role).toList());
		assertTrue(dependencies.stream().allMatch(d -> Boolean.TRUE.equals(d.automatic())));
	}

	private static Map<String, Object> check(String guardrailId) {
		return Map.of("reactorClass", "Guardrail", "params", Map.of("guardrailEngineId", guardrailId));
	}
}
