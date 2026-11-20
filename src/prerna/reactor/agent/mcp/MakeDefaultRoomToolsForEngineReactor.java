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
package prerna.reactor.agent.mcp;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRDFDatabase;
import prerna.reactor.AbstractReactor;
import prerna.reactor.IReactor;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.reactor.engine.fs.BrowseEngineAssetsReactor;
import prerna.reactor.engine.fs.CopyEngineAssetReactor;
import prerna.reactor.engine.fs.GetEngineAssetsReactor;
import prerna.reactor.engine.fs.NewEngineAssetsDirectoryReactor;
import prerna.reactor.engine.fs.NewEngineAssetsFileReactor;
import prerna.reactor.engine.fs.RenameEngineAssetReactor;
import prerna.reactor.engine.fs.SaveEngineAssetsReactor;
import prerna.reactor.engine.fs.SearchEngineAssetsReactor;
import prerna.reactor.function.ExecuteFunctionEngineReactor;
import prerna.reactor.masterdatabase.GetDatabaseTableStructureReactor;
import prerna.reactor.qs.SparqlQueryReactor;
import prerna.reactor.qs.SqlQueryReactor;
import prerna.reactor.storage.ListStoragePathDetailsReactor;
import prerna.reactor.storage.ListStoragePathReactor;
import prerna.reactor.storage.PullFromStorageReactor;
import prerna.reactor.storage.PushToStorageReactor;
import prerna.reactor.vector.CreateEmbeddingsFromDocumentsReactor;
import prerna.reactor.vector.ListDocumentsInVectorDatabaseReactor;
import prerna.reactor.vector.RemoveDocumentFromVectorDatabaseReactor;
import prerna.reactor.vector.VectorDatabaseQueryReactor;
import prerna.reactor.vector.VectorFileDownloadReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

/** Generates the default MCP tools for an engine into a room folder. */
public class MakeDefaultRoomToolsForEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MakeDefaultRoomToolsForEngineReactor.class);

	private static final String GENERATOR_ID = "MakeDefaultRoomToolsForEngine";

	private enum EngineRoomTool {
		BROWSE_ENGINE_ASSETS(BrowseEngineAssetsReactor.class, MCPExecution.AUTO, false),
		SEARCH_ENGINE_ASSETS(SearchEngineAssetsReactor.class, MCPExecution.AUTO, false),
		GET_ENGINE_ASSETS(GetEngineAssetsReactor.class, MCPExecution.AUTO, false),
		SAVE_ENGINE_ASSETS(SaveEngineAssetsReactor.class, MCPExecution.ASK, true),
		NEW_ENGINE_ASSETS_FILE(NewEngineAssetsFileReactor.class, MCPExecution.ASK, true),
		NEW_ENGINE_ASSETS_DIRECTORY(NewEngineAssetsDirectoryReactor.class, MCPExecution.ASK, true),
		RENAME_ENGINE_ASSET(RenameEngineAssetReactor.class, MCPExecution.ASK, true),
		COPY_ENGINE_ASSET(CopyEngineAssetReactor.class, MCPExecution.ASK, true),
		GET_DATABASE_TABLE_STRUCTURE(GetDatabaseTableStructureReactor.class, MCPExecution.AUTO, false),
		// SqlQuery also accepts writes, so retain an approval step even for editors.
		SQL_QUERY(SqlQueryReactor.class, MCPExecution.ASK, false),
		SPARQL_QUERY(SparqlQueryReactor.class, MCPExecution.AUTO, false),
		EXECUTE_FUNCTION_ENGINE(ExecuteFunctionEngineReactor.class, MCPExecution.ASK, false),
		LIST_STORAGE_PATH(ListStoragePathReactor.class, MCPExecution.AUTO, false),
		LIST_STORAGE_PATH_DETAILS(ListStoragePathDetailsReactor.class, MCPExecution.AUTO, false),
		PULL_FROM_STORAGE(PullFromStorageReactor.class, MCPExecution.ASK, false),
		PUSH_TO_STORAGE(PushToStorageReactor.class, MCPExecution.ASK, true),
		LIST_VECTOR_DOCUMENTS(ListDocumentsInVectorDatabaseReactor.class, MCPExecution.AUTO, false),
		CREATE_VECTOR_EMBEDDINGS(CreateEmbeddingsFromDocumentsReactor.class, MCPExecution.ASK, true),
		QUERY_VECTOR_DATABASE(VectorDatabaseQueryReactor.class, MCPExecution.AUTO, false),
		REMOVE_VECTOR_DOCUMENT(RemoveDocumentFromVectorDatabaseReactor.class, MCPExecution.ASK, true),
		DOWNLOAD_VECTOR_FILE(VectorFileDownloadReactor.class, MCPExecution.ASK, false);

		private final Class<? extends IReactor> reactorClass;
		private final MCPExecution execution;
		private final boolean editRequired;

		EngineRoomTool(Class<? extends IReactor> reactorClass, MCPExecution execution, boolean editRequired) {
			this.reactorClass = reactorClass;
			this.execution = execution;
			this.editRequired = editRequired;
		}
	}

	private static final List<EngineRoomTool> ENGINE_ASSET_TOOLS = List.of(EngineRoomTool.BROWSE_ENGINE_ASSETS,
			EngineRoomTool.SEARCH_ENGINE_ASSETS, EngineRoomTool.GET_ENGINE_ASSETS, EngineRoomTool.SAVE_ENGINE_ASSETS,
			EngineRoomTool.NEW_ENGINE_ASSETS_FILE, EngineRoomTool.NEW_ENGINE_ASSETS_DIRECTORY,
			EngineRoomTool.RENAME_ENGINE_ASSET, EngineRoomTool.COPY_ENGINE_ASSET);

	private static final Map<IEngine.CATALOG_TYPE, List<EngineRoomTool>> ENGINE_TYPE_TOOLS = Map
			.of(IEngine.CATALOG_TYPE.DATABASE, List.of(EngineRoomTool.GET_DATABASE_TABLE_STRUCTURE),
					IEngine.CATALOG_TYPE.FUNCTION, List.of(EngineRoomTool.EXECUTE_FUNCTION_ENGINE),
					IEngine.CATALOG_TYPE.STORAGE,
					List.of(EngineRoomTool.LIST_STORAGE_PATH, EngineRoomTool.LIST_STORAGE_PATH_DETAILS,
							EngineRoomTool.PULL_FROM_STORAGE, EngineRoomTool.PUSH_TO_STORAGE),
					IEngine.CATALOG_TYPE.VECTOR,
					List.of(EngineRoomTool.LIST_VECTOR_DOCUMENTS, EngineRoomTool.CREATE_VECTOR_EMBEDDINGS,
							EngineRoomTool.QUERY_VECTOR_DATABASE, EngineRoomTool.REMOVE_VECTOR_DOCUMENT,
							EngineRoomTool.DOWNLOAD_VECTOR_FILE));

	public MakeDefaultRoomToolsForEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have access to view.");
		}

		IEngine engine = Utility.getEngine(engineId);
		if (engine == null) {
			throw new IllegalArgumentException("Engine " + engineId + " does not exist");
		}

		String roomId = this.insight.getRoomId();
		if (roomId == null || roomId.isBlank()) {
			throw new IllegalArgumentException("The insight must be bound to a room");
		}

		String roomFolder = this.insight.getInsightFolder();
		if (roomFolder == null || roomFolder.isBlank()) {
			throw new IllegalStateException("No room asset folder is available for this insight");
		}

		boolean canEdit = SecurityEngineUtils.userCanEditEngine(user, engineId);
		JSONArray generatedTools = generateTools(engine, canEdit);

		JSONArray mergedTools = MCPUtility.mergeGeneratedTools(
				MCPUtility.readMcpJson(roomFolder + MCPUtility.PIXEL_MCP_RELATIVE_PATH), generatedTools, GENERATOR_ID,
				true);
		JSONObject mcpJson = wrapMcpJson(mergedTools);
		FileSystemUtil.saveAssetFiles(roomFolder, List.of(MCPUtility.PIXEL_MCP_RELATIVE_PATH),
				List.of(mcpJson.toString(4)));

		classLogger.info("Saved {} room MCP tool(s) for {} engine '{}'", generatedTools.length(),
				engine.getCatalogType(), engine.getEngineId());
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
	 * Builds the catalog from the loaded engine, not a guessed frontend subtype.
	 */
	static JSONArray generateTools(IEngine engine, boolean canEdit) {
		JSONArray tools = new JSONArray();
		for (EngineRoomTool tool : getDefaultTools(engine, canEdit)) {
			tools.put(buildTool(tool, engine));
		}
		MCPUtility.stampGenerator(tools, GENERATOR_ID);
		return tools;
	}

	private static List<EngineRoomTool> getDefaultTools(IEngine engine, boolean canEdit) {
		List<EngineRoomTool> engineTypeTools = ENGINE_TYPE_TOOLS.get(engine.getCatalogType());
		if (engineTypeTools == null) {
			return List.of();
		}

		List<EngineRoomTool> tools = new ArrayList<>();
		for (EngineRoomTool tool : ENGINE_ASSET_TOOLS) {
			if (canEdit || !tool.editRequired) {
				tools.add(tool);
			}
		}
		for (EngineRoomTool tool : engineTypeTools) {
			if (canEdit || !tool.editRequired) {
				tools.add(tool);
			}
		}
		if (engine.getCatalogType() == IEngine.CATALOG_TYPE.DATABASE) {
			if (engine instanceof IRDBMSEngine) {
				tools.add(EngineRoomTool.SQL_QUERY);
			} else if (engine instanceof IRDFDatabase) {
				tools.add(EngineRoomTool.SPARQL_QUERY);
			}
		}
		return tools;
	}

	/**
	 * Query routes supported by the engine and the existing query reactors
	 * 
	 * @param engine
	 * @return
	 */
	private static String databaseGuidance(IEngine engine) {
		String type = engine instanceof IDatabaseEngine database && database.getDatabaseType() != null
				? database.getDatabaseType().name()
				: "unknown";
		String context = "Active database engine " + engine.getEngineId() + "; implementation type " + type + ". ";
		if (engine instanceof IRDBMSEngine database) {
			String dialect = database.getDbType() == null ? "unknown" : database.getDbType().getLabel();
			return context + "Query language: SQL; dialect: " + dialect + ". "
					+ "Use the provided SqlQuery tool with database, query, and a small positive limit. "
					+ "Use physical schema names and syntax supported by this dialect. The tool accepts complete SQL "
					+ "queries, including joins, filters, and analytical expressions. SqlQuery also supports mutations; "
					+ "honor its approval mode and the user's requested scope.";
		}
		if (engine instanceof IRDFDatabase) {
			return context + "Query language: SPARQL. Use the provided SparqlQuery tool with database, query, "
					+ "a small positive limit, and raw=true when inspecting full RDF URIs. "
					+ "Only SELECT is supported by this tool. Build graph patterns and expressions using the actual "
					+ "RDF classes, predicates, and literal types discovered from the data.";
		}
		if (engine instanceof IDatabaseEngine database && database.getDatabaseType() != null) {
			switch (database.getDatabaseType()) {
			case TINKER, JANUS_GRAPH, DATASTAX_GRAPH:
				return context + "Graph query backend: Gremlin. Use reactor-help runPixel with the structured "
						+ "Pixel route Database(database=[engineId]) | Select(logicalConcept__logicalProperty) | Collect(50); "
						+ "replace the selector with names from this schema. The backend translates the query to Gremlin. "
						+ "There is no GremlinQuery reactor or supported raw Gremlin string execution for these engines. "
						+ "Do not use SqlQuery, SparqlQuery, or Query(\"g.V()...\"). Load the database exploration guide "
						+ "for supported graph operations and result handling.";
			case NEO4J:
				return context + "Graph query dialect: Cypher, not Gremlin or SQL. Use the structured Pixel "
						+ "Database | Select | Collect route with logical schema names through reactor-help runPixel. "
						+ "SqlQuery and SparqlQuery are not supported for this engine. Check a specific native operation "
						+ "only when needed; do not guess a CypherQuery or GremlinQuery reactor.";
			default:
				break;
			}
		}
		return context + "No SQL or SPARQL query tool was selected for this implementation. Inspect the schema "
				+ "and consult the database skill for its supported route; do not assume SQL. "
				+ "Report a missing query capability if it cannot be established.";
	}

	private static JSONObject buildTool(EngineRoomTool engineRoomTool, IEngine engine) {
		IReactor reactor;
		try {
			reactor = engineRoomTool.reactorClass.getConstructor().newInstance();
		} catch (Exception e) {
			throw new IllegalArgumentException("Could not instantiate reactor " + engineRoomTool.reactorClass.getName(),
					e);
		}

		JSONObject tool = reactor.asMcpTool();
		JSONObject properties = tool.getJSONObject("inputSchema").getJSONObject("properties");
		String typeKey = engine.getCatalogType().name().toLowerCase(Locale.ROOT);
		String targetKey = Arrays.asList(((AbstractReactor) reactor).keysToGet).contains(typeKey) ? typeKey
				: ReactorKeysEnum.ENGINE.getKey();
		JSONObject targetProperty = properties.getJSONObject(targetKey);
		targetProperty.put("enum", new JSONArray().put(engine.getEngineId()));
		targetProperty.put("default", engine.getEngineId());

		if (engineRoomTool == EngineRoomTool.GET_DATABASE_TABLE_STRUCTURE || engineRoomTool == EngineRoomTool.SQL_QUERY
				|| engineRoomTool == EngineRoomTool.SPARQL_QUERY) {
			tool.put("description", tool.getString("description") + "\n\n" + databaseGuidance(engine));
		}
		if (engineRoomTool == EngineRoomTool.SQL_QUERY || engineRoomTool == EngineRoomTool.SPARQL_QUERY) {
			properties.getJSONObject("limit").put("type", "integer").put("minimum", 1);
			String flag = engineRoomTool == EngineRoomTool.SQL_QUERY ? "commit" : "raw";
			// Leave optional defaults to the reactor. The current MCP executor reads
			// catalog defaults as strings, which cannot represent typed JSON defaults.
			properties.getJSONObject(flag).put("type", "boolean");
		}

		JSONObject meta = tool.optJSONObject("_meta");
		if (meta == null) {
			meta = new JSONObject();
		}
		meta.put(MCPUtility.SMSS_FUNCTION_NAME, tool.getString("name"));
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, engineRoomTool.execution.getValue());
		tool.put("_meta", meta);

		// a function engine already describes its own name, purpose, and parameters,
		// so present the executor as that function rather than as a tool taking an
		// opaque map. this runs after the meta is stamped so SMSS_FUNCTION_NAME
		// keeps pointing at the reactor being run
		if (engineRoomTool == EngineRoomTool.EXECUTE_FUNCTION_ENGINE && engine instanceof IFunctionEngine) {
			MCPFunctionEngineUtility.applyFunctionEngineDefinition(tool, (IFunctionEngine) engine);
		}

		return tool;
	}

	private static JSONObject wrapMcpJson(JSONArray tools) {
		JSONObject meta = new JSONObject();
		meta.put("last_modified_date", LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
		return new JSONObject().put("_meta", meta).put("tools", tools);
	}

	@Override
	public String getReactorDescription() {
		return "Generates mcp/pixel_mcp.json in a room using the default tools for an engine type";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The unique id for the engine whose tools should be added to the current room";
		}
		return super.getDescriptionForKey(key);
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.ASK.getValue());
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPUtility.MCPDisplayOption.SIDEBAR.getValue());
		return meta;
	}

}
