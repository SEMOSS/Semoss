package prerna.reactor.agent.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.util.Utility;

class MCPToolDiscoveryServiceTest {

	private MCPToolDiscoveryService service;
	private User user;

	@BeforeEach
	void setUp() {
		service = new MCPToolDiscoveryService();
		user = mock(User.class);
		MCPToolDiscoveryService.invalidate(null);
	}

	@AfterEach
	void tearDown() {
		clearCache();
	}

	@Test
	void normalizeHandlesCamelCase() throws Exception {
		Object result = invokeNormalize("sendEmailNotification");
		String text = getNormalizedText(result);
		assertTrue(text.contains("send"));
		assertTrue(text.contains("email"));
		assertTrue(text.contains("notification"));
	}

	@Test
	void normalizeHandlesSnakeCase() throws Exception {
		Object result = invokeNormalize("get_database_structure");
		String text = getNormalizedText(result);
		assertTrue(text.contains("get"));
		assertTrue(text.contains("database"));
		assertTrue(text.contains("structure"));
	}

	@Test
	void normalizeHandlesLetterDigitBoundary() throws Exception {
		Object result = invokeNormalize("vector3d");
		String text = getNormalizedText(result);
		assertTrue(text.contains("vector"));
		assertTrue(text.contains("3"));
		assertTrue(text.contains("d"));
	}

	@Test
	void normalizeHandlesNullAndEmpty() throws Exception {
		Object resultNull = invokeNormalize(null);
		assertEquals("", getNormalizedText(resultNull));

		Object resultEmpty = invokeNormalize("");
		assertEquals("", getNormalizedText(resultEmpty));

		Object resultWhitespace = invokeNormalize("   ");
		assertEquals("", getNormalizedText(resultWhitespace));
	}

	@Test
	void searchExactNameMatchRanksHighest() {
		try (AutoCloseable mocks = mockToolIndex(List.of(
				tool("sendEmail", "Send an email to a recipient"),
				tool("sendSlackMessage", "Send a message via Slack"),
				tool("readEmail", "Read emails from inbox")))) {

			Map<String, Object> results = service.search(user, "sendEmail", Collections.emptyList(), 10, 0);
			List<Map<String, Object>> items = getResults(results);

			assertFalse(items.isEmpty());
			assertEquals("sendEmail", items.get(0).get("toolName"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void searchNameContainsQueryRanksAboveDescriptionMatch() {
		try (AutoCloseable mocks = mockToolIndex(List.of(
				tool("queryDatabase", "Run SQL queries against a database"),
				tool("uploadFile", "Upload a file to query later"),
				tool("sqlQueryBase64", "Execute a SQL query and return base64")))) {

			Map<String, Object> results = service.search(user, "query", Collections.emptyList(), 10, 0);
			List<Map<String, Object>> items = getResults(results);

			assertFalse(items.isEmpty());
			// Tools with "query" in name should rank above tools with "query" only in description
			String firstName = (String) items.get(0).get("toolName");
			assertTrue("queryDatabase".equals(firstName) || "sqlQueryBase64".equals(firstName));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void searchMultiTokenQueryMatchesBroadly() {
		try (AutoCloseable mocks = mockToolIndex(List.of(
				tool("sendEmail", "Send an email notification to a user"),
				tool("listStoragePath", "List files in a storage path"),
				tool("createEmbeddings", "Create vector embeddings from documents")))) {

			Map<String, Object> results = service.search(user, "send email notification", Collections.emptyList(), 10,
					0);
			List<Map<String, Object>> items = getResults(results);

			assertFalse(items.isEmpty());
			assertEquals("sendEmail", items.get(0).get("toolName"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void searchNoMatchReturnsEmptyResults() {
		try (AutoCloseable mocks = mockToolIndex(List.of(
				tool("sendEmail", "Send an email"),
				tool("queryDatabase", "Run SQL queries")))) {

			Map<String, Object> results = service.search(user, "xyznonexistent", Collections.emptyList(), 10, 0);
			List<Map<String, Object>> items = getResults(results);

			assertTrue(items.isEmpty());
			assertEquals(0, results.get("total"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void searchIsCaseInsensitive() {
		try (AutoCloseable mocks = mockToolIndex(List.of(
				tool("SendEmail", "Send an email to a recipient")))) {

			Map<String, Object> results = service.search(user, "sendemail", Collections.emptyList(), 10, 0);
			List<Map<String, Object>> items = getResults(results);

			assertFalse(items.isEmpty());
			assertEquals("SendEmail", items.get(0).get("toolName"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void searchRespectsLimitAndOffset() {
		try (AutoCloseable mocks = mockToolIndex(List.of(
				tool("emailTool1", "Email tool one"),
				tool("emailTool2", "Email tool two"),
				tool("emailTool3", "Email tool three"),
				tool("emailTool4", "Email tool four"),
				tool("emailTool5", "Email tool five")))) {

			Map<String, Object> results = service.search(user, "email tool", Collections.emptyList(), 2, 0);
			List<Map<String, Object>> items = getResults(results);

			assertEquals(2, items.size());
			assertEquals(5, results.get("total"));
			assertEquals(2, results.get("limit"));
			assertEquals(0, results.get("offset"));

			// Second page
			Map<String, Object> page2 = service.search(user, "email tool", Collections.emptyList(), 2, 2);
			List<Map<String, Object>> page2Items = getResults(page2);

			assertEquals(2, page2Items.size());
			assertEquals(5, page2.get("total"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void searchOffsetBeyondTotalReturnsEmpty() {
		try (AutoCloseable mocks = mockToolIndex(List.of(
				tool("sendEmail", "Send an email")))) {

			Map<String, Object> results = service.search(user, "email", Collections.emptyList(), 10, 100);
			List<Map<String, Object>> items = getResults(results);

			assertTrue(items.isEmpty());
			assertEquals(1, results.get("total"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void searchIndexesToolsFromEnginesAndProjects() {
		IEngine engine = mock(IEngine.class);
		when(engine.getEngineId()).thenReturn("engine-1");
		when(engine.getEngineName()).thenReturn("TestEngine");
		when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.FUNCTION);

		IProject project = mock(IProject.class);
		when(project.getProjectId()).thenReturn("project-1");
		when(project.getEngineId()).thenReturn("project-1");
		when(project.getEngineName()).thenReturn("TestProject");
		when(project.getProjectName()).thenReturn("TestProject");
		when(project.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.PROJECT);

		JSONObject engineTools = buildToolsJson(List.of(tool("engineFunc", "From engine")));
		JSONObject projectTools = buildToolsJson(List.of(tool("projectFunc", "From project")));

		try (MockedStatic<SecurityEngineUtils> secEngine = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<SecurityProjectUtils> secProject = Mockito.mockStatic(SecurityProjectUtils.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<MCPUtility> mcpUtility = Mockito.mockStatic(MCPUtility.class)) {

			secEngine.when(() -> SecurityEngineUtils.getUserEngineList(any(), isNull(), isNull(), anyBoolean(),
					isNull(), isNull(), isNull(), isNull(), isNull()))
					.thenReturn(List.of(Map.of("engine_id", "engine-1")));
			secEngine.when(() -> SecurityEngineUtils.getAggregateEngineMetadata(any(), anyList(), anyBoolean()))
					.thenReturn(Collections.emptyMap());

			secProject.when(() -> SecurityProjectUtils.getUserProjectList(any(), isNull(), isNull(), anyBoolean(),
					anyBoolean(), isNull(), isNull(), isNull(), isNull(), isNull()))
					.thenReturn(List.of(Map.of("project_id", "project-1")));
			secProject.when(() -> SecurityProjectUtils.getAggregateProjectMetadata(any(), anyList(), anyBoolean()))
					.thenReturn(Collections.emptyMap());

			utility.when(() -> Utility.getEngine("engine-1")).thenReturn(engine);
			utility.when(() -> Utility.getProject("project-1")).thenReturn(project);

			mcpUtility.when(() -> MCPUtility.getAggregatedTools(engine)).thenReturn(engineTools);
			mcpUtility.when(() -> MCPUtility.getAggregatedTools(project)).thenReturn(projectTools);

			Map<String, Object> results = service.search(user, "func", Collections.emptyList(), 10, 0);
			List<Map<String, Object>> items = getResults(results);

			assertEquals(2, items.size());
			// Verify both engine types are represented
			boolean hasEngine = items.stream().anyMatch(i -> "TestEngine".equals(i.get("engineName")));
			boolean hasProject = items.stream().anyMatch(i -> "TestProject".equals(i.get("engineName")));
			assertTrue(hasEngine);
			assertTrue(hasProject);
		}
	}

	@Test
	void searchSkipsDisabledTools() {
		try (AutoCloseable mocks = mockToolIndexWithDisabled()) {

			Map<String, Object> results = service.search(user, "tool", Collections.emptyList(), 10, 0);
			List<Map<String, Object>> items = getResults(results);

			assertEquals(1, items.size());
			assertEquals("enabledTool", items.get(0).get("toolName"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void invalidateRemovesCachedEngine() {
		// First search populates cache
		try (AutoCloseable mocks = mockToolIndex(List.of(
				tool("cachedTool", "A cached tool")))) {
			Map<String, Object> results = service.search(user, "cached", Collections.emptyList(), 10, 0);
			assertEquals(1, getResults(results).size());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		// Invalidate the cached engine
		MCPToolDiscoveryService.invalidate("engine-1");

		// Second search should rebuild (now returns different tool because mock returns new data)
		try (AutoCloseable mocks = mockToolIndex(List.of(
				tool("newTool", "A new tool after invalidation")))) {
			Map<String, Object> results = service.search(user, "new", Collections.emptyList(), 10, 0);
			List<Map<String, Object>> items = getResults(results);
			assertFalse(items.isEmpty());
			assertEquals("newTool", items.get(0).get("toolName"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void invalidateWithNullIsNoOp() {
		// Should not throw
		MCPToolDiscoveryService.invalidate(null);
	}

	@Test
	void searchResponseContainsRequiredFields() {
		try (AutoCloseable mocks = mockToolIndex(List.of(
				tool("testTool", "A test tool description")))) {

			Map<String, Object> response = service.search(user, "test", Collections.emptyList(), 10, 0);

			assertNotNull(response.get("query"));
			assertNotNull(response.get("limit"));
			assertNotNull(response.get("offset"));
			assertNotNull(response.get("total"));
			assertNotNull(response.get("results"));

			assertEquals("test", response.get("query"));
			assertEquals(10, response.get("limit"));
			assertEquals(0, response.get("offset"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void searchResultEntryContainsAllFields() {
		try (AutoCloseable mocks = mockToolIndex(List.of(
				tool("testTool", "A test tool description")))) {

			Map<String, Object> response = service.search(user, "test", Collections.emptyList(), 10, 0);
			List<Map<String, Object>> items = getResults(response);

			assertFalse(items.isEmpty());
			Map<String, Object> entry = items.get(0);
			assertTrue(entry.containsKey("toolName"));
			assertTrue(entry.containsKey("description"));
			assertTrue(entry.containsKey("inputSchema"));
			assertTrue(entry.containsKey("engineType"));
			assertTrue(entry.containsKey("engineName"));
			assertTrue(entry.containsKey("tags"));
			assertTrue(entry.containsKey("engineId"));
			assertTrue(entry.containsKey("roomOptionMcpEntry"));

			@SuppressWarnings("unchecked")
			Map<String, Object> mcpEntry = (Map<String, Object>) entry.get("roomOptionMcpEntry");
			assertNotNull(mcpEntry.get("id"));
			assertNotNull(mcpEntry.get("toolName"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void recommendDelegatesToSearchWithOffsetZero() {
		try (AutoCloseable mocks = mockToolIndex(List.of(
				tool("sendEmail", "Send an email notification")))) {

			Map<String, Object> results = service.recommend(user, "email", Collections.emptyList(), 5);
			List<Map<String, Object>> items = getResults(results);

			assertFalse(items.isEmpty());
			assertEquals(0, results.get("offset"));
			assertEquals(5, results.get("limit"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void clearCache() {
		try {
			var field = MCPToolDiscoveryService.class.getDeclaredField("toolCache");
			field.setAccessible(true);
			@SuppressWarnings("unchecked")
			var cache = (java.util.concurrent.ConcurrentMap<String, ?>) field.get(null);
			cache.clear();
		} catch (Exception e) {
			throw new RuntimeException("Failed to clear tool cache", e);
		}
	}

	private Object invokeNormalize(String value) throws Exception {
		Method method = MCPToolDiscoveryService.class.getDeclaredMethod("normalize", String.class);
		method.setAccessible(true);
		return method.invoke(service, value);
	}

	private String getNormalizedText(Object normalizedTextRecord) throws Exception {
		Method textMethod = normalizedTextRecord.getClass().getMethod("text");
		return (String) textMethod.invoke(normalizedTextRecord);
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getResults(Map<String, Object> response) {
		return (List<Map<String, Object>>) response.get("results");
	}

	private Map<String, String> tool(String name, String description) {
		return Map.of("name", name, "description", description);
	}

	private JSONObject buildToolsJson(List<Map<String, String>> tools) {
		JSONArray toolsArray = new JSONArray();
		for (Map<String, String> tool : tools) {
			JSONObject toolObj = new JSONObject();
			toolObj.put("name", tool.get("name"));
			toolObj.put("description", tool.get("description"));
			toolObj.put("inputSchema", new JSONObject().put("type", "object"));
			toolsArray.put(toolObj);
		}
		JSONObject result = new JSONObject();
		result.put("tools", toolsArray);
		return result;
	}

	private AutoCloseable mockToolIndex(List<Map<String, String>> tools) {
		IEngine engine = mock(IEngine.class);
		when(engine.getEngineId()).thenReturn("engine-1");
		when(engine.getEngineName()).thenReturn("TestEngine");
		when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.FUNCTION);

		JSONObject toolsJson = buildToolsJson(tools);

		MockedStatic<SecurityEngineUtils> secEngine = Mockito.mockStatic(SecurityEngineUtils.class);
		MockedStatic<SecurityProjectUtils> secProject = Mockito.mockStatic(SecurityProjectUtils.class);
		MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
		MockedStatic<MCPUtility> mcpUtility = Mockito.mockStatic(MCPUtility.class);

		secEngine.when(() -> SecurityEngineUtils.getUserEngineList(any(), isNull(), isNull(), anyBoolean(), isNull(),
				isNull(), isNull(), isNull(), isNull())).thenReturn(List.of(Map.of("engine_id", "engine-1")));
		secEngine.when(() -> SecurityEngineUtils.getAggregateEngineMetadata(any(), anyList(), anyBoolean()))
				.thenReturn(Collections.emptyMap());

		secProject.when(() -> SecurityProjectUtils.getUserProjectList(any(), isNull(), isNull(), anyBoolean(),
				anyBoolean(), isNull(), isNull(), isNull(), isNull(), isNull())).thenReturn(Collections.emptyList());

		utility.when(() -> Utility.getEngine("engine-1")).thenReturn(engine);

		mcpUtility.when(() -> MCPUtility.getAggregatedTools(engine)).thenReturn(toolsJson);

		return () -> {
			mcpUtility.close();
			utility.close();
			secProject.close();
			secEngine.close();
		};
	}

	private AutoCloseable mockToolIndexWithDisabled() {
		IEngine engine = mock(IEngine.class);
		when(engine.getEngineId()).thenReturn("engine-1");
		when(engine.getEngineName()).thenReturn("TestEngine");
		when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.FUNCTION);

		// Build tools with one disabled
		JSONArray toolsArray = new JSONArray();

		JSONObject enabledTool = new JSONObject();
		enabledTool.put("name", "enabledTool");
		enabledTool.put("description", "An enabled tool");
		enabledTool.put("inputSchema", new JSONObject().put("type", "object"));
		toolsArray.put(enabledTool);

		JSONObject disabledTool = new JSONObject();
		disabledTool.put("name", "disabledTool");
		disabledTool.put("description", "A disabled tool");
		disabledTool.put("inputSchema", new JSONObject().put("type", "object"));
		JSONObject meta = new JSONObject();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.DISABLED.getValue());
		disabledTool.put("_meta", meta);
		toolsArray.put(disabledTool);

		JSONObject toolsJson = new JSONObject();
		toolsJson.put("tools", toolsArray);

		MockedStatic<SecurityEngineUtils> secEngine = Mockito.mockStatic(SecurityEngineUtils.class);
		MockedStatic<SecurityProjectUtils> secProject = Mockito.mockStatic(SecurityProjectUtils.class);
		MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
		MockedStatic<MCPUtility> mcpUtility = Mockito.mockStatic(MCPUtility.class);

		secEngine.when(() -> SecurityEngineUtils.getUserEngineList(any(), isNull(), isNull(), anyBoolean(), isNull(),
				isNull(), isNull(), isNull(), isNull())).thenReturn(List.of(Map.of("engine_id", "engine-1")));
		secEngine.when(() -> SecurityEngineUtils.getAggregateEngineMetadata(any(), anyList(), anyBoolean()))
				.thenReturn(Collections.emptyMap());

		secProject.when(() -> SecurityProjectUtils.getUserProjectList(any(), isNull(), isNull(), anyBoolean(),
				anyBoolean(), isNull(), isNull(), isNull(), isNull(), isNull())).thenReturn(Collections.emptyList());

		utility.when(() -> Utility.getEngine("engine-1")).thenReturn(engine);

		mcpUtility.when(() -> MCPUtility.getAggregatedTools(engine)).thenReturn(toolsJson);

		return () -> {
			mcpUtility.close();
			utility.close();
			secProject.close();
			secEngine.close();
		};
	}
}
