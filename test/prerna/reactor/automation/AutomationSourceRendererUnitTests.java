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

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Covers the Python each generated node renders. The renderer is the only place
 * a node's behavior is defined, so a wrong SDK call here is a silent data bug at
 * run time rather than a failure the validator can catch.
 */
public class AutomationSourceRendererUnitTests {

	private static Map<String, Object> node(String type, Map<String, Object> config) {
		Map<String, Object> node = new LinkedHashMap<>();
		node.put(AutomationConstants.NODE_FIELD_ID, "n1");
		node.put(AutomationConstants.NODE_FIELD_TYPE, type);
		node.put(AutomationConstants.NODE_FIELD_CONFIG, config);
		return node;
	}

	private static Map<String, Object> databaseConfig() {
		Map<String, Object> config = new LinkedHashMap<>();
		config.put("engineId", "engine-1");
		config.put("query", "SELECT 1");
		config.put("limit", 50);
		return config;
	}

	private static Map<String, Object> storageTransferConfig() {
		Map<String, Object> config = new LinkedHashMap<>();
		config.put("engineId", "storage-1");
		config.put("path", "remote/report.pdf");
		config.put("destination", "downloads");
		return config;
	}

	@Test
	void readsGoThroughExecQuery() {
		String source = AutomationSourceRenderer.renderNode(
				node(AutomationConstants.NODE_DATABASE_QUERY, databaseConfig()));
		assertTrue(source.contains("def run(scope):"), "every node source defines the run entry point");
		assertFalse(source.contains("insertData"));
		assertFalse(source.contains("removeData"));
	}

	/**
	 * execQuery only runs SELECT, so each write operation has to render the method
	 * that matches it.
	 */
	@Test
	void writesUseTheMethodMatchingTheOperation() {
		assertTrue(AutomationSourceRenderer
				.renderNode(node(AutomationConstants.NODE_DATABASE_INSERT, databaseConfig()))
				.contains("database.insertData(query=scope.resolve(QUERY))"));
		assertTrue(AutomationSourceRenderer
				.renderNode(node(AutomationConstants.NODE_DATABASE_UPDATE, databaseConfig()))
				.contains("database.updateData(query=scope.resolve(QUERY))"));
		assertTrue(AutomationSourceRenderer
				.renderNode(node(AutomationConstants.NODE_DATABASE_DELETE, databaseConfig()))
				.contains("database.removeData(query=scope.resolve(QUERY))"));
	}

	@Test
	void writesNeverFallBackToExecQuery() {
		for (String type : new String[] { AutomationConstants.NODE_DATABASE_INSERT,
				AutomationConstants.NODE_DATABASE_UPDATE, AutomationConstants.NODE_DATABASE_DELETE }) {
			assertFalse(AutomationSourceRenderer.renderNode(node(type, databaseConfig())).contains("execQuery"),
					type + " must not render a read call");
		}
	}

	@Test
	void storageDownloadReturnsReusableInsightFileMetadata() {
		String source = AutomationSourceRenderer.renderNode(
				node(AutomationConstants.NODE_STORAGE_DOWNLOAD, storageTransferConfig()));

		assertTrue(source.contains("success = storage.copyToLocal(storagePath=storage_path, localPath=workspace_folder)"));
		assertTrue(source.contains("workspace_root = os.path.abspath(ROOT)"));
		assertTrue(source.contains("os.path.join(workspace_root, str(workspace_folder), file_name)"));
		assertTrue(source.contains("os.path.commonpath([workspace_root, local_path]) != workspace_root"));
		assertTrue(source.contains("if not success or not os.path.exists(local_path):"));
		assertTrue(source.contains("Storage download did not create the expected local path"));
		assertTrue(source.contains("\"success\": bool(success)"));
		assertTrue(source.contains("\"storagePath\": storage_path"));
		assertTrue(source.contains("\"workspaceFolder\": workspace_folder"));
		assertTrue(source.contains("\"localPath\": local_path"));
		assertTrue(source.contains("\"fileName\": file_name"));
		assertTrue(source.contains("\"space\": \"insight\""));
	}

	@Test
	void storageUploadReturnsReusableMetadataAndRejectsSdkErrors() {
		String source = AutomationSourceRenderer.renderNode(
				node(AutomationConstants.NODE_STORAGE_UPLOAD, storageTransferConfig()));

		assertTrue(source.contains("result = storage.copyToStorage(storagePath=storage_path, localPath=local_path)"));
		assertTrue(source.contains("if result is True:"));
		assertTrue(source.contains("isinstance(result, dict) and result.get(\"success\") is True"));
		assertTrue(source.contains("raise RuntimeError(str(result or \"Storage upload failed.\"))"));
		assertTrue(source.contains("metadata.update({"));
		assertTrue(source.contains("return metadata"));
		assertTrue(source.contains("\"success\": True"));
		assertTrue(source.contains("\"storagePath\": storage_path"));
		assertTrue(source.contains("\"localPath\": local_path"));
		assertTrue(source.contains("\"space\": \"insight\""));
	}

	/**
	 * The canvas seeds its trigger editor with an identical template, and the save
	 * path compares the persisted source against this string to decide whether the
	 * trigger is still generated. Drift on either side marks every untouched
	 * trigger custom, so the exact bytes are the contract.
	 */
	@Test
	void triggerTemplateIsStable() {
		String expected = """
				# Declare globals in trigger.start config.globals.
				# Define optional setup here; return a map only for additional runtime values.
				def run(scope):
				    return {}
				""";
		assertEquals(expected, AutomationSourceRenderer.renderNode(node(AutomationConstants.NODE_START, Map.of())));
	}

	@Test
	void customPythonStartsWithAnEditableRunScopeTemplate() {
		String source = AutomationSourceRenderer.defaultDeveloperSource();

		assertTrue(source.contains("def run(scope):"));
		assertTrue(source.contains("scope[\"outputVar\"]"));
		assertTrue(AutomationDefinitionService.definesRunEntryPoint(source));
		assertEquals(source,
				AddAutomationStepReactor.customSource(AutomationConstants.NODE_DEVELOPER_PYTHON,
						new LinkedHashMap<>()));
	}

	/**
	 * resolve is a method on the scope mapping the runtime passes in, not a builtin.
	 * A module-level resolve(...) call raises NameError the moment the node runs, so
	 * every occurrence has to be reached through scope.
	 */
	@Test
	void everyNodeSourceResolvesThroughScope() {
		for (AutomationNodeType type : AutomationNodeType.values()) {
			if (type == AutomationNodeType.CONTROL_IF || type == AutomationNodeType.CONTROL_JEV) {
				continue;
			}
			String source = AutomationSourceRenderer.renderNode(node(type.getType(), databaseConfig()));
			assertEquals(countOccurrences(source, "resolve("), countOccurrences(source, "scope.resolve("),
					type.getType() + " must call scope.resolve rather than a module-level resolve");
		}
	}

	private static int countOccurrences(String haystack, String needle) {
		int count = 0;
		for (int index = haystack.indexOf(needle); index >= 0; index = haystack.indexOf(needle, index + 1)) {
			count++;
		}
		return count;
	}

	@Test
	void decisionNodesHaveNoPythonSource() {
		assertThrows(IllegalArgumentException.class, () -> AutomationSourceRenderer
				.renderNode(node(AutomationConstants.NODE_CONTROL_IF, Map.of())));
		assertThrows(IllegalArgumentException.class, () -> AutomationSourceRenderer
				.renderNode(node(AutomationConstants.NODE_CONTROL_JEV, Map.of())));
	}
}
