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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;

class AutomationPixelExpressionRendererUnitTests {

	@Test
	void rendersDatabaseQueryThroughTheStandardPixelCommand() {
		assertEquals(
				"SqlQuery(database=\"database-\\\";\",query=\"SELECT 'line\\n\\\";'\",limit=50000);",
				AutomationPixelExpressionRenderer.databaseQuery("database-\";", "SELECT 'line\n\";'", 50_000));
	}

	@Test
	void rendersFunctionArgumentsAsTypedJson() {
		Map<String, Object> arguments = new LinkedHashMap<>();
		arguments.put("count", "${payload.count}");
		arguments.put("enabled", true);
		arguments.put("items", List.of("line\none", "quote \" ; \u03bb"));
		arguments.put("text", "${payload.text}");
		Map<String, Object> config = Map.of(
				"engineId", "${engine}",
				"arguments", arguments);
		Map<String, Object> scope = Map.of(
				"engine", "engine-\";\u03bb",
				"payload", Map.of("text", "value\";\n\u03bb", "count", 7));

		assertEquals(
				"ExecuteFunctionEngine(engine=\"engine-\\\";\u03bb\", map=[{\"count\":7,\"enabled\":true,"
						+ "\"items\":[\"line\\none\",\"quote \\\" ; \u03bb\"],\"text\":\"value\\\";\\n\u03bb\"}]);",
				AutomationPixelExpressionRenderer.functionExecute(config, scope, "function"));
	}

	@Test
	void rendersAndFiltersStorageListWithReservedCharacters() {
		Map<String, Object> scope = Map.of(
				"engine", "storage-\";\u03bb",
				"path", "folder/line\n\";");
		Map<String, Object> config = Map.of(
				"engineId", "${engine}",
				"path", "${path}",
				"extensions", List.of("CSV", ".json"));

		assertEquals(
				"Storage(\"storage-\\\";\u03bb\")|ListStoragePathDetails(storagePath=\"folder/line\\n\\\";\");",
				AutomationPixelExpressionRenderer.storageList(config, scope, "storage"));
		assertEquals(List.of("a.CSV", "b.json"),
				AutomationPixelExpressionRenderer.storagePaths(
						List.of(Map.of("Path", "a.CSV"), Map.of("Path", "folder/"),
								Map.of("Path", "b.json"), Map.of("Other", "ignored")),
						config.get("extensions"), scope, "storage"));
	}

	@Test
	void rendersDatabaseWriteWithoutPixelTextInjection() {
		Map<String, Object> config = Map.of(
				"engineId", "${engine}",
				"query", "${query}");
		Map<String, Object> scope = Map.of(
				"engine", "database-\";",
				"query", "UPDATE T SET V='line\\n\"; \u03bb' WHERE ID=1;");

		assertEquals(
				"Database(\"database-\\\";\")|Query(\"UPDATE T SET V='line\\\\n\\\"; \u03bb' WHERE ID=1;\")"
						+ "|ExecQuery(commit=true);",
				AutomationPixelExpressionRenderer.databaseWrite(config, scope, "write"));
	}

	@Test
	void rendersVectorWritesWithTypedListsAndMaps() {
		Map<String, Object> addConfig = Map.of(
				"engineId", "vector-id",
				"value", "${files}",
				"space", "project-\";",
				"paramValues", Map.of("chunk", "${chunk}"));
		Map<String, Object> scope = Map.of("files", List.of("a.csv", "line\n\";.pdf"), "chunk", 512);

		assertEquals(
				"CreateEmbeddingsFromDocuments(engine=\"vector-id\",filePaths=[\"a.csv\",\"line\\n\\\";.pdf\"],"
						+ "space=[\"project-\\\";\"],paramValues=[{\"chunk\":512}]);",
				AutomationPixelExpressionRenderer.vectorWrite(
						prerna.reactor.automation.AutomationConstants.NODE_VECTOR_ADD, addConfig, scope, "vector"));

		Map<String, Object> deleteConfig = Map.of(
				"engineId", "vector-id",
				"value", "a.csv, b.pdf",
				"paramValues", Map.of());
		assertEquals(
				"RemoveDocumentFromVectorDatabase(engine=\"vector-id\",fileNames=[\"a.csv\",\"b.pdf\"],"
						+ "paramValues=[{}]);",
				AutomationPixelExpressionRenderer.vectorWrite(
						prerna.reactor.automation.AutomationConstants.NODE_VECTOR_DELETE, deleteConfig, Map.of(),
						"vector"));
	}

	@Test
	void rendersAndAdaptsModelCallsWithTraceMetadata() {
		Map<String, Object> config = Map.of(
				"engineId", "model-id",
				"prompt", "${prompt}",
				"systemPrompt", "system\n\";",
				"paramValues", Map.of("temperature", 0.2));
		String expression = AutomationPixelExpressionRenderer.model(
				prerna.reactor.automation.AutomationConstants.NODE_MODEL_CHAT, config,
				Map.of("prompt", "hello \u03bb\";"), "model", "room-id");

		assertEquals(
				"LLM(engine=\"model-id\",command=\"hello \u03bb\\\";\",context=[\"system\\n\\\";\"],"
						+ "useHistory=true,paramValues=[{\"temperature\":0.2}],roomId=\"room-id\");",
				expression);
		assertEquals(Map.of(
				prerna.reactor.automation.AutomationConstants.INTERNAL_RESULT_VALUE, "answer",
				prerna.reactor.automation.AutomationConstants.INTERNAL_RESULT_METADATA,
						Map.of("roomId", "room-id", "messageId", "message-id")),
				AutomationPixelExpressionRenderer.modelResult(
						prerna.reactor.automation.AutomationConstants.NODE_MODEL_CHAT,
						Map.of("response", "answer", "roomId", "room-id", "messageId", "message-id"), "model"));
	}

	@Test
	void delegatesLargePromptValidationToTheModelReactor() {
		String prompt = "x".repeat(300 * 1024);

		String expression = AutomationPixelExpressionRenderer.model(
				prerna.reactor.automation.AutomationConstants.NODE_MODEL_CHAT,
				Map.of("engineId", "model", "prompt", "${prompt}", "systemPrompt", "",
						"paramValues", Map.of()),
				Map.of("prompt", prompt), "model", null);

		assertTrue(expression.length() > 300 * 1024);
		assertTrue(expression.contains(prompt.substring(prompt.length() - 100)));
	}

	@Test
	void rendersCompleteFrameRowsInExactAndEmbeddedModelPrompts() throws Exception {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		OwlTemporalEngineMeta metadata = mock(OwlTemporalEngineMeta.class);
		when(frame.size("query_result")).thenReturn(2L);
		when(frame.getQsHeaders()).thenReturn(new String[] { "QUERY_RESULT__ID" });
		when(frame.getColumnHeaders()).thenReturn(new String[] { "ID" });
		when(frame.getMetaData()).thenReturn(metadata);
		when(metadata.getHeaderTypeAsString("QUERY_RESULT__ID")).thenReturn("INT");
		when(frame.getFrameType()).thenReturn(DataFrameTypeEnum.PYTHON);
		when(frame.query(any(SelectQueryStruct.class))).thenAnswer(invocation -> {
			IRawSelectWrapper wrapper = mock(IRawSelectWrapper.class);
			when(wrapper.hasNext()).thenReturn(true, true, false);
			when(wrapper.next()).thenReturn(
					new HeadersDataRow(new String[] { "ID" }, new Object[] { 1 }),
					new HeadersDataRow(new String[] { "ID" }, new Object[] { 2 }));
			return wrapper;
		});
		AutomationFrameOutput.RegisteredFrame output =
				AutomationFrameOutput.register(new prerna.om.Insight(), "query_result", frame);

		String expression = AutomationPixelExpressionRenderer.model(
				prerna.reactor.automation.AutomationConstants.NODE_MODEL_CHAT,
				Map.of("engineId", "model", "prompt", "${query_result}", "systemPrompt", "",
						"paramValues", Map.of()),
				Map.of("query_result", output), "model", null);

		assertTrue(expression.contains("[{\\\"ID\\\":1},{\\\"ID\\\":2}]"));

		String embedded = AutomationPixelExpressionRenderer.model(
				prerna.reactor.automation.AutomationConstants.NODE_MODEL_CHAT,
				Map.of("engineId", "model", "prompt", "Summarize these heroes: ${query_result}",
						"systemPrompt", "", "paramValues", Map.of()),
				Map.of("query_result", output), "model", null);
		assertTrue(embedded.contains("Summarize these heroes:"));
		assertTrue(embedded.contains("[{\\\"ID\\\":1},{\\\"ID\\\":2}]"));

		String function = AutomationPixelExpressionRenderer.functionExecute(
				Map.of("engineId", "function", "arguments",
						Map.of("payload", Map.of("id", "${query_result[0].ID}"))),
				Map.of("query_result", output), "function");
		assertTrue(function.contains("\\\"payload\\\":{\\\"id\\\":1}"));
	}

	@Test
	void rejectsModelErrorAndMissingVisionMedia() {
		assertThrows(IllegalStateException.class,
				() -> AutomationPixelExpressionRenderer.modelResult(
						prerna.reactor.automation.AutomationConstants.NODE_MODEL_NER,
						Map.of("response", Map.of("status", "error", "message", "failed")), "ner"));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationPixelExpressionRenderer.model(
						prerna.reactor.automation.AutomationConstants.NODE_MODEL_VISION,
						Map.of("engineId", "model", "prompt", "look", "systemPrompt", "",
								"paramValues", Map.of(), "image", "", "urls", List.of()),
						Map.of(), "vision", "room"));
	}

	@Test
	void rendersStorageOperationsWithTypedArguments() {
		Map<String, Object> scope = Map.of("path", "folder/\";\n\u03bb", "file", "/input/a.csv");
		assertEquals(
				"GetStorageFileAsBase64(storage=\"storage\",storagePath=\"folder/\\\";\\n\u03bb\",convertToPdf=true);",
				AutomationPixelExpressionRenderer.storageOperation(
						prerna.reactor.automation.AutomationConstants.NODE_STORAGE_READ,
						Map.of("engineId", "storage", "path", "${path}", "convertToPdf", true), scope, "read"));
		assertEquals(
				"Storage(\"storage\")|PushToStorage(storagePath=\"folder/\\\";\\n\u03bb\","
						+ "filePath=\"/input/a.csv\",metadata=[{\"owner\":\"Ada\"}]);",
				AutomationPixelExpressionRenderer.storageOperation(
						prerna.reactor.automation.AutomationConstants.NODE_STORAGE_UPLOAD,
						Map.of("engineId", "storage", "path", "${path}", "destination", "${file}",
								"metadata", Map.of("owner", "Ada")),
						scope, "upload"));
		assertEquals(
				"Storage(\"storage\")|DeleteFromStorage(storagePath=\"folder/\\\";\\n\u03bb\","
						+ "leaveFolderStructure=false);",
				AutomationPixelExpressionRenderer.storageOperation(
						prerna.reactor.automation.AutomationConstants.NODE_STORAGE_DELETE,
						Map.of("engineId", "storage", "path", "${path}", "leaveFolderStructure", false),
						scope, "delete"));
	}

	@Test
	void rendersAndAdaptsDurableAgentRun() {
		Map<String, Object> config = Map.of(
				prerna.reactor.automation.AutomationConstants.CONFIG_COMMAND, "${command}",
				prerna.reactor.automation.AutomationConstants.CONFIG_ENGINE_ID, "model-id",
				prerna.reactor.automation.AutomationConstants.CONFIG_HARNESS_TYPE, "native",
				prerna.reactor.automation.AutomationConstants.CONFIG_WORKSPACE_ID, "workspace-id",
				prerna.reactor.automation.AutomationConstants.CONFIG_MAX_TURNS, 8,
				prerna.reactor.automation.AutomationConstants.CONFIG_MAX_REFLECTIONS, 2,
				prerna.reactor.automation.AutomationConstants.CONFIG_WAIT_TIMEOUT_MS, 1000,
				"paramMap", Map.of("text", "${text}"));
		String expression = AutomationPixelExpressionRenderer.agent(config,
				Map.of("command", "Do \"this\";\n\u03bb", "text", "value"), "agent", "room-id");

		assertEquals("RunAgent(roomId=\"room-id\",command=\"Do \\\"this\\\";\\n\u03bb\",engine=\"model-id\","
				+ "harnessType=\"native\",workspaceId=\"workspace-id\",maxTurns=8,maxReflections=2,wait=false,"
				+ "waitTimeoutMs=1000,paramValues={\"text\":\"value\"});", expression);
		assertEquals(Map.of(
				prerna.reactor.automation.AutomationConstants.INTERNAL_RESULT_VALUE, "done",
				prerna.reactor.automation.AutomationConstants.INTERNAL_RESULT_METADATA,
						Map.of("runId", "child", "roomId", "room-id", "workspaceId", "workspace-id",
								"status", "SUCCESS", "waitTimedOut", false, "errorMessage", "")),
				AutomationPixelExpressionRenderer.agentResult(
						Map.of("finalText", "done", "runId", "child", "roomId", "room-id",
								"workspaceId", "workspace-id", "status", "SUCCESS", "waitTimedOut", false,
								"errorMessage", ""),
						"agent"));
	}

	@Test
	void interpolatesEmbeddedScopeValuesInAgentCommands() {
		String expression = AutomationPixelExpressionRenderer.agent(
				Map.of(prerna.reactor.automation.AutomationConstants.CONFIG_COMMAND,
						"Review this response: ${model_response}"),
				Map.of("model_response", "The test passed."), "agent", "room-id");

		assertEquals("RunAgent(roomId=\"room-id\",command=\"Review this response: The test passed.\",wait=false);",
				expression);
	}

	@Test
	void rendersVectorSearchAndRejectsFilterSyntaxInjection() {
		Map<String, Object> config = Map.of(
				"engineId", "vector",
				"value", "${query}",
				"limit", 7,
				"filters", Map.of("category", List.of("a\";", "b")),
				"paramValues", Map.of("threshold", 0.4));
		assertEquals(
				"VectorDatabaseQuery(engine=\"vector\",command=[\"find \\\"this\\\";\\n\u03bb\"],limit=[7],"
						+ "filters=[Filter(Category == [\"a\\\";\",\"b\"])],paramValues=[{\"threshold\":0.4}]);",
				AutomationPixelExpressionRenderer.vectorSearch(config,
						Map.of("query", "find \"this\";\n\u03bb"), "search"));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationPixelExpressionRenderer.vectorSearch(
						Map.of("engineId", "vector", "value", "find", "filters",
								Map.of("x)]; DeleteProject(", "bad")),
						Map.of(), "search"));
	}

	@Test
	void rendersAndAdaptsStorageDownload() {
		AutomationPixelExpressionRenderer.StorageDownload request =
				AutomationPixelExpressionRenderer.storageDownload(
						Map.of("engineId", "storage", "path", "${path}", "destination", "/target",
								"version", "v\";\n\u03bb"),
						Map.of("path", "source/file.csv"), "download");
		assertEquals(
				"PullFromStorage(storage=\"storage\",storagePath=\"source/file.csv\",filePath=\"/target\","
						+ "version=\"v\\\";\\n\u03bb\");",
				request.transferExpression());
		assertEquals("SearchInsightAssets(filePath=\"/target\",search=\".*\",options=\"regex\");",
				request.searchExpression());
		assertEquals(Map.of(
				"success", true,
				"storagePath", "source/file.csv",
				"destination", "/target",
				"space", "INSIGHT",
				"files", List.of("/target/file.csv"),
				"filePath", "/target/file.csv"),
				AutomationPixelExpressionRenderer.storageDownloadResult(true,
						List.of(Map.of("type", "directory", "path", "/target"),
								Map.of("type", "file", "path", "/target/file.csv")),
						request, "download"));
	}

	@Test
	void identifiesTheCurrentDownloadAmongExistingInsightFiles() {
		AutomationPixelExpressionRenderer.StorageDownload request =
				AutomationPixelExpressionRenderer.storageDownload(
						Map.of("engineId", "storage", "path", "source/tracking-numbers-5000.csv",
								"destination", "/"),
						Map.of(), "download");

		assertEquals(Map.of(
				"success", true,
				"storagePath", "source/tracking-numbers-5000.csv",
				"destination", "/",
				"space", "INSIGHT",
				"files", List.of("/tracking-numbers-5000.csv"),
				"filePath", "/tracking-numbers-5000.csv"),
				AutomationPixelExpressionRenderer.storageDownloadResult(true,
						List.of(Map.of("type", "file", "path", "/appointment-esr-map-5000.csv"),
								Map.of("type", "file", "path", "/tracking-numbers-5000.csv")),
						request, "download"));
	}
}
