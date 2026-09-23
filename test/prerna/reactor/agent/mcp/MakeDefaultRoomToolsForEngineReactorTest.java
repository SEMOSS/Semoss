package prerna.reactor.agent.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRDFDatabase;
import prerna.om.Insight;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.sql.RdbmsTypeEnum;

class MakeDefaultRoomToolsForEngineReactorTest {

	private static final String ENGINE_ID = "active-database";

	private <T extends IDatabaseEngine> T database(Class<T> implementation, DATABASE_TYPE type) {
		T engine = mock(implementation);
		when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.DATABASE);
		when(engine.getEngineId()).thenReturn(ENGINE_ID);
		when(engine.getDatabaseType()).thenReturn(type);
		return engine;
	}

	private static JSONObject tool(JSONArray tools, String name) {
		for (int i = 0; i < tools.length(); i++) {
			JSONObject tool = tools.getJSONObject(i);
			if (name.equals(tool.getString("name"))) {
				return tool;
			}
		}
		throw new AssertionError("Missing tool: " + name);
	}

	private static Set<String> names(JSONArray tools) {
		Set<String> names = new HashSet<>();
		for (int i = 0; i < tools.length(); i++) {
			names.add(tools.getJSONObject(i).getString("name"));
		}
		return names;
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void relationalToolsIncludeDialectAndKeepQueriesApprovedForBothPermissionLevels(boolean canEdit) {
		IRDBMSEngine engine = database(IRDBMSEngine.class, DATABASE_TYPE.RDBMS);
		when(engine.getDbType()).thenReturn(RdbmsTypeEnum.POSTGRES);
		JSONArray tools = MakeDefaultRoomToolsForEngineReactor.generateTools(engine, canEdit);
		JSONObject query = tool(tools, "SqlQuery");
		assertFalse(names(tools).contains("SparqlQuery"));
		assertEquals(canEdit, names(tools).contains("SaveEngineAssets"));
		assertTrue(query.getString("description").contains("dialect: POSTGRES"));
		assertTrue(tool(tools, "GetDatabaseTableStructure").getString("description").contains("SqlQuery"));
		assertEquals("ask", query.getJSONObject("_meta").getString(MCPUtility.SMSS_MCP_EXECUTION));
		JSONObject properties = query.getJSONObject("inputSchema").getJSONObject("properties");
		assertEquals(ENGINE_ID, properties.getJSONObject("database").getString("default"));
		assertEquals(Set.of(ENGINE_ID),
				new HashSet<>(properties.getJSONObject("database").getJSONArray("enum").toList()));
		assertEquals("integer", properties.getJSONObject("limit").getString("type"));
		assertEquals(1, properties.getJSONObject("limit").getInt("minimum"));
		assertEquals("boolean", properties.getJSONObject("commit").getString("type"));
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void queryToolsCanExecuteWithOptionalArgumentsOmitted(boolean rdf) throws Exception {
		IDatabaseEngine engine = rdf ? database(IRDFDatabase.class, DATABASE_TYPE.RDF4J)
				: database(IRDBMSEngine.class, DATABASE_TYPE.RDBMS);
		String name = rdf ? "SparqlQuery" : "SqlQuery";
		JSONObject definition = tool(MakeDefaultRoomToolsForEngineReactor.generateTools(engine, false), name);
		Insight insight = mock(Insight.class);
		PixelRunner runner = mock(PixelRunner.class);
		when(insight.runPixel(anyString())).thenReturn(runner);
		when(runner.getResults()).thenReturn(List.of(new NounMetadata("ok", PixelDataType.CONST_STRING)));
		String query = rdf ? "SELECT ?s WHERE { ?s ?p ?o } LIMIT 10" : "SELECT COUNT(*) FROM measurements";
		assertEquals("\"ok\"", MCPUtility.runPixelTool(null, insight, name,
				definition.getJSONObject("inputSchema").getJSONObject("properties"), Map.of("query", query)));
		ArgumentCaptor<String> pixel = ArgumentCaptor.forClass(String.class);
		verify(insight).runPixel(pixel.capture());
		assertTrue(pixel.getValue().contains("database=\"" + ENGINE_ID + "\""));
		assertTrue(pixel.getValue().contains(query));
		PixelUtility.validatePixel(pixel.getValue());
	}

	@ParameterizedTest
	@EnumSource(value = DATABASE_TYPE.class, names = { "JENA", "JENA_TDB", "SESAME", "RDF4J" })
	void rdfEnginesGetSparqlOnlyAndDescribeFullUriInspection(DATABASE_TYPE type) {
		IRDFDatabase engine = database(IRDFDatabase.class, type);
		JSONArray tools = MakeDefaultRoomToolsForEngineReactor.generateTools(engine, false);
		JSONObject query = tool(tools, "SparqlQuery");
		assertFalse(names(tools).contains("SqlQuery"));
		assertTrue(query.getString("description").contains("implementation type " + type));
		assertTrue(query.getString("description").contains("raw=true"));
		assertEquals("auto", query.getJSONObject("_meta").getString(MCPUtility.SMSS_MCP_EXECUTION));
		JSONObject properties = query.getJSONObject("inputSchema").getJSONObject("properties");
		assertEquals(ENGINE_ID, properties.getJSONObject("database").getString("default"));
		assertEquals("boolean", properties.getJSONObject("raw").getString("type"));
	}

	@ParameterizedTest
	@EnumSource(value = DATABASE_TYPE.class, names = { "TINKER", "JANUS_GRAPH", "DATASTAX_GRAPH" })
	void gremlinEnginesUseStructuredPixelWithoutAdvertisingUnsupportedRawQueryTools(DATABASE_TYPE type) {
		JSONArray tools = MakeDefaultRoomToolsForEngineReactor.generateTools(database(IDatabaseEngine.class, type),
				false);
		assertFalse(names(tools).contains("SqlQuery"));
		assertFalse(names(tools).contains("SparqlQuery"));
		assertFalse(names(tools).contains("GremlinQuery"));
		String description = tool(tools, "GetDatabaseTableStructure").getString("description");
		assertTrue(description.contains("backend: Gremlin"));
		assertTrue(description.contains("structured Pixel route"));
		assertTrue(description.contains("no GremlinQuery reactor"));
	}

	@Test
	void neo4jIsIdentifiedAsCypherRatherThanGremlinOrSql() {
		JSONArray tools = MakeDefaultRoomToolsForEngineReactor
				.generateTools(database(IDatabaseEngine.class, DATABASE_TYPE.NEO4J), false);
		assertFalse(names(tools).contains("SqlQuery"));
		assertFalse(names(tools).contains("SparqlQuery"));
		assertTrue(tool(tools, "GetDatabaseTableStructure").getString("description").contains("dialect: Cypher"));
	}

	@Test
	void unknownImplementationDoesNotFallBackToSql() {
		JSONArray tools = MakeDefaultRoomToolsForEngineReactor.generateTools(database(IDatabaseEngine.class, null),
				false);
		assertFalse(names(tools).contains("SqlQuery"));
		assertFalse(names(tools).contains("SparqlQuery"));
		assertTrue(tool(tools, "GetDatabaseTableStructure").getString("description").contains("do not assume SQL"));
	}

	@Test
	void refreshingAChangedEngineRemovesItsOldQueryRouteButPreservesUnrelatedTools() {
		IRDBMSEngine sql = database(IRDBMSEngine.class, DATABASE_TYPE.RDBMS);
		JSONArray oldTools = MakeDefaultRoomToolsForEngineReactor.generateTools(sql, true);
		oldTools.put(new JSONObject().put("name", "CustomTool"));
		JSONArray freshTools = MakeDefaultRoomToolsForEngineReactor
				.generateTools(database(IRDFDatabase.class, DATABASE_TYPE.RDF4J), false);
		JSONArray merged = MCPUtility.mergeGeneratedTools(new JSONObject().put("tools", oldTools), freshTools,
				"MakeDefaultRoomToolsForEngine", true);
		assertTrue(names(merged).containsAll(Set.of("SparqlQuery", "CustomTool")));
		assertFalse(names(merged).contains("SqlQuery"));
		assertFalse(names(merged).contains("SaveEngineAssets"));
	}

	@Test
	void storageEngineKeepsItsExistingReadAndEditToolProfile() {
		IEngine storage = mock(IEngine.class);
		when(storage.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.STORAGE);
		when(storage.getEngineId()).thenReturn("storage");
		Set<String> readNames = names(MakeDefaultRoomToolsForEngineReactor.generateTools(storage, false));
		Set<String> editNames = names(MakeDefaultRoomToolsForEngineReactor.generateTools(storage, true));
		assertTrue(readNames.containsAll(Set.of("ListStoragePath", "ListStoragePathDetails", "PullFromStorage")));
		assertFalse(readNames.contains("PushToStorage"));
		assertTrue(editNames.contains("PushToStorage"));
		assertFalse(editNames.contains("GetDatabaseTableStructure"));
	}
}
