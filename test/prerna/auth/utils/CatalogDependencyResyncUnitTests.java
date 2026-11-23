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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.SecurityCatalogDependencyUtils.Dependency;
import prerna.engine.api.IEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.model.ModelRouterEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.pipeline.EngineProxyFactory;
import prerna.project.api.IProject;
import prerna.reactor.automation.AutomationConstants;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/**
 * Resyncing an item's automatic dependencies never clears them because its
 * saved configuration could not be read, and an engine's are written once.
 */
class CatalogDependencyResyncUnitTests {

	private final User user = mock(User.class);
	private MockedStatic<SecurityCatalogDependencyUtils> writes;
	private MockedStatic<CatalogTypes> types;

	@BeforeEach
	void mockTheSecurityDatabase() {
		writes = mockStatic(SecurityCatalogDependencyUtils.class);
		types = mockStatic(CatalogTypes.class);
		types.when(() -> CatalogTypes.resolve(anyString())).thenAnswer(call -> switch ((String) call.getArgument(0)) {
		case "guard1" -> "GUARDRAIL";
		case "kb1" -> "VECTOR";
		default -> "MODEL";
		});
	}

	@AfterEach
	void closeMocks() {
		types.close();
		writes.close();
	}

	private void verifyNothingWritten() {
		writes.verify(() -> SecurityCatalogDependencyUtils.syncAutomaticDependencies(any(), any(), any(),
				anyCollection(), anyCollection()), never());
		writes.verify(() -> SecurityCatalogDependencyUtils.syncAutomaticDependencyIds(any(), any(), any(), any(),
				any(), anyCollection()), never());
	}

	// ---------------------------------------------------------------- agents

	@Test
	void anAgentWhoseConfigCannotBeParsedThrowsAndWritesNothing() {
		try (MockedStatic<ModelInferenceLogsUtils> logs = mockStatic(ModelInferenceLogsUtils.class)) {
			logs.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry("agent1"))
					.thenReturn(Map.of("workspace_id", "agent1", "config_json", "{not json"));
			logs.when(() -> ModelInferenceLogsUtils.getWorkspaceResourcesByType("agent1", null))
					.thenReturn(List.of(Map.of("resource_id", "kb1", "resource_type", "VECTOR")));

			IllegalStateException e = assertThrows(IllegalStateException.class,
					() -> CatalogDependencyResync.resyncWorkspace(user, "agent1"));
			assertTrue(e.getMessage().contains("agent1"), e.getMessage());
		}
		verifyNothingWritten();
	}

	@Test
	void anAgentWithNoWorkspaceRowThrowsAndWritesNothing() {
		try (MockedStatic<ModelInferenceLogsUtils> logs = mockStatic(ModelInferenceLogsUtils.class)) {
			logs.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry("agent1")).thenReturn(null);

			assertThrows(IllegalStateException.class, () -> CatalogDependencyResync.resyncWorkspace(user, "agent1"));
		}
		verifyNothingWritten();
	}

	@Test
	@SuppressWarnings("unchecked")
	void anAgentWithNoConfigRecordsOnlyItsResources() {
		Map<String, Object> row = new HashMap<>();
		row.put("workspace_id", "agent1");
		row.put("config_json", null);
		try (MockedStatic<ModelInferenceLogsUtils> logs = mockStatic(ModelInferenceLogsUtils.class)) {
			logs.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry("agent1")).thenReturn(row);
			logs.when(() -> ModelInferenceLogsUtils.getWorkspaceResourcesByType("agent1", null))
					.thenReturn(List.of(Map.of("resource_id", "kb1", "resource_type", "VECTOR")));

			CatalogDependencyResync.resyncWorkspace(user, "agent1");
		}
		ArgumentCaptor<Collection<Dependency>> written = ArgumentCaptor.forClass(Collection.class);
		writes.verify(() -> SecurityCatalogDependencyUtils.syncAutomaticDependencies(eq(user), eq("agent1"),
				eq("WORKSPACE"), eq(CatalogDependencyDerivation.WORKSPACE_ROLES), written.capture()), times(1));
		assertEquals(List.of(CatalogDependencyRole.KNOWLEDGE),
				written.getValue().stream().map(Dependency::role).toList());
	}

	// ----------------------------------------------------------- automations

	@Test
	void anAutomationWhoseDefinitionCannotBeParsedThrowsAndWritesNothing(@TempDir Path assets) throws Exception {
		Files.writeString(assets.resolve(AutomationConstants.AUTOMATION_PYTHON_DEFINITION_FILE_NAME), "{not json");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class);
				MockedStatic<AssetUtility> folders = mockStatic(AssetUtility.class)) {
			stubAutomation(utility, folders, assets);

			IllegalStateException e = assertThrows(IllegalStateException.class,
					() -> CatalogDependencyResync.resyncAutomation(user, "auto1"));
			assertTrue(e.getMessage().contains("auto1"), e.getMessage());
		}
		verifyNothingWritten();
	}

	@Test
	void anAutomationWithNoSavedDefinitionThrowsAndWritesNothing(@TempDir Path assets) {
		try (MockedStatic<Utility> utility = mockStatic(Utility.class);
				MockedStatic<AssetUtility> folders = mockStatic(AssetUtility.class)) {
			stubAutomation(utility, folders, assets);

			assertThrows(IllegalStateException.class, () -> CatalogDependencyResync.resyncAutomation(user, "auto1"));
		}
		verifyNothingWritten();
	}

	@Test
	void aMissingAutomationThrowsAndWritesNothing() {
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(() -> Utility.getProject("auto1")).thenReturn(null);

			assertThrows(IllegalStateException.class, () -> CatalogDependencyResync.resyncAutomation(user, "auto1"));
		}
		verifyNothingWritten();
	}

	private static void stubAutomation(MockedStatic<Utility> utility, MockedStatic<AssetUtility> folders,
			Path assets) {
		IProject project = mock(IProject.class);
		when(project.getProjectName()).thenReturn("Flow");
		utility.when(() -> Utility.getProject("auto1")).thenReturn(project);
		folders.when(() -> AssetUtility.getProjectAssetsFolder("auto1")).thenReturn(assets.toString());
		folders.when(() -> AssetUtility.getProjectAssetsFolder("Flow", "auto1")).thenReturn(assets.toString());
	}

	// ----------------------------------------------------------- blocks apps

	@Test
	void aBlocksAppWhoseBlocksCannotBeParsedThrowsAndWritesNothing(@TempDir Path portals) throws Exception {
		Files.writeString(portals.resolve(IProject.BLOCK_FILE_NAME), "{not json");
		IProject project = mock(IProject.class);
		when(project.getProjectName()).thenReturn("Blocks");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class);
				MockedStatic<AssetUtility> folders = mockStatic(AssetUtility.class)) {
			utility.when(() -> Utility.getProject("app1")).thenReturn(project);
			folders.when(() -> AssetUtility.getProjectPortalsFolder("Blocks", "app1")).thenReturn(portals.toString());

			assertThrows(IllegalStateException.class, () -> CatalogDependencyResync.resyncBlocks(user, "app1"));
		}
		verifyNothingWritten();
	}

	// ---------------------------------------------------------------- engines

	@Test
	@SuppressWarnings("unchecked")
	void anEngineIsWrittenOnceWithItsSmssAndGuardrailRoles(@TempDir Path folder) throws Exception {
		File pipeline = folder.resolve("pipeline.json").toFile();
		Files.writeString(pipeline.toPath(), """
				{"pipelines":{"chat":{"input":[{"reactorClass":"Guardrail","params":{"guardrailEngineId":"guard1"}}]}}}
				""");
		Properties smss = new Properties();
		smss.setProperty("EMBEDDER_ENGINE_ID", "embedder1");
		smss.setProperty(IEngine.PIPELINE, "pipeline.json");
		IEngine engine = engine(IEngine.class, "vector1", IEngine.CATALOG_TYPE.VECTOR, smss);

		try (MockedStatic<EngineProxyFactory> proxies = mockStatic(EngineProxyFactory.class)) {
			proxies.when(() -> EngineProxyFactory.pipelineFile(engine, "pipeline.json")).thenReturn(pipeline);
			CatalogDependencyResync.resyncEngine(user, engine);
		}

		ArgumentCaptor<Collection<CatalogDependencyRole>> roles = ArgumentCaptor.forClass(Collection.class);
		ArgumentCaptor<Collection<Dependency>> written = ArgumentCaptor.forClass(Collection.class);
		writes.verify(() -> SecurityCatalogDependencyUtils.syncAutomaticDependencies(eq(user), eq("vector1"),
				eq("VECTOR"), roles.capture(), written.capture()), times(1));
		Set<CatalogDependencyRole> expected = new java.util.LinkedHashSet<>(CatalogDependencyDerivation.SMSS_ROLES);
		expected.add(CatalogDependencyRole.GUARDRAIL);
		assertEquals(expected, Set.copyOf(roles.getValue()));
		assertEquals(List.of(CatalogDependencyRole.EMBEDDER, CatalogDependencyRole.GUARDRAIL),
				written.getValue().stream().map(Dependency::role).toList());
	}

	@Test
	@SuppressWarnings("unchecked")
	void aModelRouterIsWrittenOnceWithItsSmssGuardrailAndRouterRoles(@TempDir Path folder) throws Exception {
		File router = folder.resolve("router.json").toFile();
		Files.writeString(router.toPath(), """
				{"mode":"llm","classifier_engine":"classifier1","routes":[{"name":"code","engine_id":"model1"}]}
				""");
		ModelRouterEngine engine = engine(ModelRouterEngine.class, "router1", IEngine.CATALOG_TYPE.MODEL,
				new CaseInsensitiveProperties());
		when(engine.resolveConfigFile()).thenReturn(router);

		CatalogDependencyResync.resyncEngine(user, engine);

		ArgumentCaptor<Collection<CatalogDependencyRole>> roles = ArgumentCaptor.forClass(Collection.class);
		writes.verify(() -> SecurityCatalogDependencyUtils.syncAutomaticDependencies(eq(user), eq("router1"),
				eq("MODEL"), roles.capture(), anyCollection()), times(1));
		assertTrue(roles.getValue().containsAll(CatalogDependencyDerivation.SMSS_ROLES));
		assertTrue(roles.getValue().containsAll(CatalogDependencyDerivation.ROUTER_ROLES));
		assertTrue(roles.getValue().contains(CatalogDependencyRole.GUARDRAIL));
	}

	@Test
	void aGuardrailLeavesTheGuardrailRoleAlone() throws Exception {
		Properties smss = new Properties();
		smss.setProperty("MODEL_ENGINE_ID", "judge1");
		IEngine engine = engine(IEngine.class, "guard1", IEngine.CATALOG_TYPE.GUARDRAIL, smss);

		CatalogDependencyResync.resyncEngine(user, engine);

		ArgumentCaptor<Collection<CatalogDependencyRole>> roles = rolesCaptor();
		writes.verify(() -> SecurityCatalogDependencyUtils.syncAutomaticDependencies(eq(user), eq("guard1"),
				eq("GUARDRAIL"), roles.capture(), anyCollection()), times(1));
		// only the smss roles: a guardrail's own pipeline is not a guardrail it depends on
		assertEquals(Set.copyOf(CatalogDependencyDerivation.SMSS_ROLES), Set.copyOf(roles.getValue()));
	}

	@Test
	void aMalformedPipelineThrowsBeforeAnythingIsWritten(@TempDir Path folder) throws Exception {
		File pipeline = folder.resolve("pipeline.json").toFile();
		Files.writeString(pipeline.toPath(), "{\"pipelines\": [not json");
		Properties smss = new Properties();
		smss.setProperty("EMBEDDER_ENGINE_ID", "embedder1");
		smss.setProperty(IEngine.PIPELINE, "pipeline.json");
		IEngine engine = engine(IEngine.class, "vector1", IEngine.CATALOG_TYPE.VECTOR, smss);

		try (MockedStatic<EngineProxyFactory> proxies = mockStatic(EngineProxyFactory.class)) {
			proxies.when(() -> EngineProxyFactory.pipelineFile(engine, "pipeline.json")).thenReturn(pipeline);
			IllegalStateException e = assertThrows(IllegalStateException.class,
					() -> CatalogDependencyResync.resyncEngine(user, engine));
			assertTrue(e.getMessage().contains("vector1"), e.getMessage());
		}
		verifyNothingWritten();
	}

	@Test
	void aMalformedRouterConfigThrowsBeforeAnythingIsWritten(@TempDir Path folder) throws Exception {
		File router = folder.resolve("router.json").toFile();
		Files.writeString(router.toPath(), "not json {");
		ModelRouterEngine engine = engine(ModelRouterEngine.class, "router1", IEngine.CATALOG_TYPE.MODEL,
				new CaseInsensitiveProperties());
		when(engine.resolveConfigFile()).thenReturn(router);

		assertThrows(IllegalStateException.class, () -> CatalogDependencyResync.resyncEngine(user, engine));
		verifyNothingWritten();
	}

	@SuppressWarnings("unchecked")
	private static ArgumentCaptor<Collection<CatalogDependencyRole>> rolesCaptor() {
		return ArgumentCaptor.forClass(Collection.class);
	}

	private static <T extends IEngine> T engine(Class<T> kind, String id, IEngine.CATALOG_TYPE type,
			Properties smss) {
		T engine = mock(kind);
		when(engine.getEngineId()).thenReturn(id);
		when(engine.getEngineName()).thenReturn(id);
		when(engine.getCatalogType()).thenReturn(type);
		when(engine.getSmssProp()).thenReturn(smss);
		return engine;
	}
}
