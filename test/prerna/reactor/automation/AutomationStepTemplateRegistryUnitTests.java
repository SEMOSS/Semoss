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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.engine.api.IEngine;

class AutomationStepTemplateRegistryUnitTests {

	@Test
	void acceptsOnlySafeStepNodeIds() {
		assertTrue(AutomationStepTemplateRegistry.isSafeStepNodeId("step_01.v2"));
		assertTrue(AutomationStepTemplateRegistry.isSafeStepNodeId("A-file"));
		assertFalse(AutomationStepTemplateRegistry.isSafeStepNodeId("../escape"));
		assertFalse(AutomationStepTemplateRegistry.isSafeStepNodeId("step/name"));
		assertFalse(AutomationStepTemplateRegistry.isSafeStepNodeId(".hidden"));
		assertFalse(AutomationStepTemplateRegistry.isSafeStepNodeId(""));
	}

	@Test
	void generatesManagedWrapperSourcesWithMappedRuntimeFallbacks() {
		AutomationStepTemplateRegistry.GeneratedStep model = AutomationStepTemplateRegistry.generate(
				AutomationConstants.NODE_MODEL_ENGINE, Map.of(
						AutomationConstants.CONFIG_ENGINE_ID, "model-id",
						AutomationConstants.CONFIG_OPERATION, AutomationConstants.OP_LLM,
						AutomationConstants.CONFIG_COMMAND, "Summarize the report",
						AutomationConstants.CONFIG_INPUTS, Map.of(AutomationConstants.CONFIG_COMMAND, "${prompt}")));

		assertEquals("model.llm", model.getActionId());
		assertTrue(model.getSource().contains("from ai_server import ModelEngine"));
		assertTrue(model.getSource().contains("inputs.get("));
		assertTrue(model.getSource().contains("inputs.get(json.loads(\"\\\"command\\\"\"),"));
		assertTrue(model.getSource().contains("json.loads("));
		assertFalse(model.getSource().contains("ServerProxy"));

		AutomationStepTemplateRegistry.GeneratedStep vector = AutomationStepTemplateRegistry.generate(
				AutomationConstants.NODE_VECTOR_ENGINE, Map.of(
						AutomationConstants.CONFIG_ENGINE_ID, "vector-id",
						AutomationConstants.CONFIG_OPERATION, AutomationConstants.OP_ADD_CSV,
						AutomationConstants.CONFIG_FILE_PATH, "documents.csv"));

		assertEquals("vector.add-csv", vector.getActionId());
		assertTrue(vector.getSource().contains("from ai_server import VectorEngine"));
		assertTrue(vector.getSource().contains("addVectorCSVFile"));
	}

	@Test
	void createsAnIncompleteModelDraftUntilAnEngineIsSelected() {
		AutomationStepTemplateRegistry.GeneratedStep model = AutomationStepTemplateRegistry.generate(
				AutomationConstants.NODE_MODEL_ENGINE, Map.of(
						AutomationConstants.CONFIG_OPERATION, AutomationConstants.OP_LLM,
						AutomationConstants.CONFIG_COMMAND, "Summarize ${github_activity}"));

		assertEquals("model.llm", model.getActionId());
		assertTrue(model.getSource().contains("Select an AI Engine"));
		assertTrue(model.getSource().contains("ModelEngine(engine_id=_engine_id)"));
	}

	@Test
	void usesInsightOnlyForActionsWithoutManagedTypedMethods() {
		AutomationStepTemplateRegistry.GeneratedStep vectorDownload = AutomationStepTemplateRegistry.generate(
				AutomationConstants.NODE_VECTOR_ENGINE, Map.of(
						AutomationConstants.CONFIG_ENGINE_ID, "vector-id",
						AutomationConstants.CONFIG_OPERATION, AutomationConstants.OP_DOWNLOAD,
						AutomationConstants.CONFIG_FILE_NAMES, "source.pdf"));
		AutomationStepTemplateRegistry.GeneratedStep storageBase64 = AutomationStepTemplateRegistry.generate(
				AutomationConstants.NODE_STORAGE_ENGINE, Map.of(
						AutomationConstants.CONFIG_ENGINE_ID, "storage-id",
						AutomationConstants.CONFIG_OPERATION, AutomationConstants.OP_READ_BASE64,
						AutomationConstants.CONFIG_STORAGE_PATH, "/reports/source.pdf"));

		assertTrue(vectorDownload.getSource().contains("from semoss import Insight"));
		assertTrue(vectorDownload.getSource().contains("VectorFileDownload"));
		assertTrue(storageBase64.getSource().contains("from semoss import Insight"));
		assertTrue(storageBase64.getSource().contains("GetStorageFileAsBase64"));
	}

	@Test
	void rejectsUnsupportedActionsAndMismatchedEngineCatalogs() {
		assertEquals(AutomationConstants.NODE_PYTHON_STEP,
				AutomationStepTemplateRegistry.getAction("python-step.skeleton").getNodeType());
		assertEquals(AutomationConstants.OP_SKELETON,
				AutomationStepTemplateRegistry.getAction("python-step.skeleton").getOperation());
		assertThrows(IllegalArgumentException.class,
				() -> AutomationStepTemplateRegistry.getAction("python-step.python-step.skeleton"));
		assertThrows(IllegalArgumentException.class, () -> AutomationStepTemplateRegistry.selectAction(
				AutomationConstants.NODE_MODEL_ENGINE,
				Map.of(AutomationConstants.CONFIG_OPERATION, AutomationConstants.OP_VISION)));
		assertThrows(IllegalArgumentException.class, () -> AutomationStepTemplateRegistry.selectAction(
				AutomationConstants.NODE_FUNCTION_ENGINE,
				Map.of(AutomationConstants.CONFIG_OPERATION, "streaming")));

		AutomationStepTemplateRegistry.ActionDefinition modelAction =
				AutomationStepTemplateRegistry.selectAction(AutomationConstants.NODE_MODEL_ENGINE,
						Map.of(AutomationConstants.CONFIG_OPERATION, AutomationConstants.OP_LLM));
		assertThrows(IllegalArgumentException.class, () -> AutomationStepTemplateRegistry.validateEngineCatalog(
				modelAction, IEngine.CATALOG_TYPE.DATABASE));
	}
}
