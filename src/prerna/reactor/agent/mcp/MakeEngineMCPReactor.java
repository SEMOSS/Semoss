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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.reactor.AbstractReactor;
import prerna.reactor.IReactor;
import prerna.reactor.ReactorFactory;
import prerna.reactor.agent.mcp.MCPUtility.MCPDisplayOption;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.reactor.function.ExecuteFunctionEngineReactor;
import prerna.reactor.masterdatabase.GetDatabaseTableStructureReactor;
import prerna.reactor.model.LLMReactor;
import prerna.reactor.qs.SqlQueryBase64Reactor;
import prerna.reactor.storage.DeleteFromStorageReactor;
import prerna.reactor.storage.ListStoragePathDetailsReactor;
import prerna.reactor.storage.ListStoragePathReactor;
import prerna.reactor.storage.PullFromStorageReactor;
import prerna.reactor.storage.PushToStorageReactor;
import prerna.reactor.vector.CreateEmbeddingsFromDocumentsReactor;
import prerna.reactor.vector.JenaEntityLinkReactor;
import prerna.reactor.vector.JenaGraphExpandReactor;
import prerna.reactor.vector.JenaHybridRetrieveReactor;
import prerna.reactor.vector.JenaIngestDocReactor;
import prerna.reactor.vector.JenaSparqlQueryReactor;
import prerna.reactor.vector.ListDocumentsInVectorDatabaseReactor;
import prerna.reactor.vector.QdrantAddPointsReactor;
import prerna.reactor.vector.QdrantDeleteByFilterReactor;
import prerna.reactor.vector.QdrantHybridSearchReactor;
import prerna.reactor.vector.QdrantListPointsReactor;
import prerna.reactor.vector.QdrantRecommendReactor;
import prerna.reactor.vector.RemoveDocumentFromVectorDatabaseReactor;
import prerna.reactor.vector.VectorDatabaseQueryReactor;
import prerna.reactor.vector.VectorFileDownloadReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class MakeEngineMCPReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MakeEngineMCPReactor.class);

	// @formatter:off
	private static final Map<IEngine.CATALOG_TYPE, List<Class<? extends IReactor>>> STANDARD_ENGINE_TOOLS = new HashMap<>() {
		{
        put(IEngine.CATALOG_TYPE.STORAGE, new ArrayList<>(Arrays.asList(
            ListStoragePathReactor.class,
            ListStoragePathDetailsReactor.class,
            PullFromStorageReactor.class,
            PushToStorageReactor.class,
            DeleteFromStorageReactor.class
        )));
        put(IEngine.CATALOG_TYPE.FUNCTION, new ArrayList<>(Arrays.asList(
        	ExecuteFunctionEngineReactor.class
        )));
        put(IEngine.CATALOG_TYPE.VECTOR, new ArrayList<>(Arrays.asList(
            	ListDocumentsInVectorDatabaseReactor.class,
            	CreateEmbeddingsFromDocumentsReactor.class,
            	VectorDatabaseQueryReactor.class,
            	RemoveDocumentFromVectorDatabaseReactor.class,
            	VectorFileDownloadReactor.class
            )));
        put(IEngine.CATALOG_TYPE.DATABASE, new ArrayList<>(Arrays.asList(
            	GetDatabaseTableStructureReactor.class,
            	SqlQueryBase64Reactor.class
            )));
        put(IEngine.CATALOG_TYPE.MODEL, new ArrayList<>(Arrays.asList(
            	LLMReactor.class
            )));
		}
	};

	private static final Map<VectorDatabaseTypeEnum, List<Class<? extends IReactor>>> VECTOR_SUBTYPE_TOOLS = new HashMap<>() {
		{
			put(VectorDatabaseTypeEnum.JENA_GRAPH_RAG, new ArrayList<>(Arrays.asList(
					JenaSparqlQueryReactor.class,
					JenaEntityLinkReactor.class,
					JenaGraphExpandReactor.class,
					JenaHybridRetrieveReactor.class,
					JenaIngestDocReactor.class,
					ListDocumentsInVectorDatabaseReactor.class,
					RemoveDocumentFromVectorDatabaseReactor.class
			)));
			put(VectorDatabaseTypeEnum.QDRANT, new ArrayList<>(Arrays.asList(
					ListDocumentsInVectorDatabaseReactor.class,
					CreateEmbeddingsFromDocumentsReactor.class,
					VectorDatabaseQueryReactor.class,
					RemoveDocumentFromVectorDatabaseReactor.class,
					VectorFileDownloadReactor.class,
					QdrantAddPointsReactor.class,
					QdrantHybridSearchReactor.class,
					QdrantRecommendReactor.class,
					QdrantDeleteByFilterReactor.class,
					QdrantListPointsReactor.class
			)));
		}
	};
    // @formatter:on

	public MakeEngineMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.REACTOR.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey(), ReactorKeysEnum.MCP_METADATA.getKey() };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have access to edit.");
		}

		IEngine engine = Utility.getEngine(engineId);
		IEngine.CATALOG_TYPE eType = engine.getCatalogType();
		String engineName = engine.getEngineName();

		String engineAssetsFolder = EngineUtility.getSpecificEngineAssetsFolder(eType, engineId, engineName);
		engineAssetsFolder = engineAssetsFolder.replace("\\", "/");

		JSONObject mcpJson = new JSONObject();
		JSONArray toolsArray = new JSONArray();

		List<String> reactorNames = getNounAsStringList(ReactorKeysEnum.REACTOR.getKey());
		List<Map<String, Object>> mcpMetadataList = getList(ReactorKeysEnum.MCP_METADATA.getKey());
		boolean mcpMetaExists = false;
		if (mcpMetadataList != null) {
			mcpMetaExists = true;
			if (mcpMetadataList.size() != reactorNames.size()) {
				throw new IllegalArgumentException("The number of " + ReactorKeysEnum.MCP_METADATA.getKey()
						+ " entries must match the number of REACTOR entries.");
			}
		}

		boolean useDefaultReactors = (reactorNames == null || reactorNames.isEmpty());
		List<Class<? extends IReactor>> defaultReactors = STANDARD_ENGINE_TOOLS.getOrDefault(eType, new ArrayList<>());
		if (eType == IEngine.CATALOG_TYPE.VECTOR && engine instanceof IVectorDatabaseEngine) {
			VectorDatabaseTypeEnum vectorSubtype = ((IVectorDatabaseEngine) engine).getVectorDatabaseType();
			if (vectorSubtype != null && VECTOR_SUBTYPE_TOOLS.containsKey(vectorSubtype)) {
				defaultReactors = VECTOR_SUBTYPE_TOOLS.get(vectorSubtype);
			}
		}

		for (int i = 0; i < (useDefaultReactors ? defaultReactors.size() : reactorNames.size()); i++) {
			IReactor thisReactor = null;
			JSONObject reactorTool = null;
			if (useDefaultReactors) {
				Class<? extends IReactor> reactorClass = defaultReactors.get(i);
				try {
					thisReactor = reactorClass.getConstructor().newInstance();
				} catch (Exception e) {
					classLogger.error("Could not instantiate new class {}", reactorClass.getName());
					throw new IllegalArgumentException("Could not instantiate new class " + reactorClass.getName());
				}
			} else {
				thisReactor = ReactorFactory.getReactor(this.insight, reactorNames.get(i), null,
						this.insight.getCurFrame());
				reactorTool = thisReactor.asMcpTool();
			}
			try {
				reactorTool = thisReactor.asMcpTool();
				JSONObject inputSchema = reactorTool.getJSONObject("inputSchema");
				JSONObject properties = inputSchema.getJSONObject("properties");
				String eTypeLower = eType.name().toLowerCase();
				String paramName = Arrays.asList(((AbstractReactor) thisReactor).keysToGet).contains(eTypeLower)
						? eTypeLower
						: "engine";
				JSONObject engineObj = properties.getJSONObject(paramName);
				if (engineObj != null) {
					engineObj.put("enum", new JSONArray().put(engineId));
					engineObj.put("default", engineId);
				}
			} catch (Exception e) {
				throw new IllegalArgumentException(
						"Unexpected error creating MCP tool from reactor class: " + thisReactor.getName());
			}
			JSONObject meta = reactorTool.optJSONObject("_meta");
			if (meta == null) {
				meta = new JSONObject();
			}
			String functionName = reactorTool.getString("name");
			meta.put(MCPUtility.SMSS_FUNCTION_NAME, functionName);

			// Populate additional metadata from the parameter
			Map<String, Object> additionalMeta = mcpMetaExists ? mcpMetadataList.get(i) : new HashMap<>();
			// Parse for specific known keys

			// execution mode
			String execModeInput = (String) additionalMeta.getOrDefault(MCPUtility.SMSS_MCP_EXECUTION, "auto");
			MCPExecution execModeEnum = MCPExecution.fromValue(execModeInput);
			if (execModeEnum == null && !execModeInput.isBlank()) {
				throw new IllegalArgumentException(MCPUtility.SMSS_MCP_EXECUTION + "can only be a value of: "
						+ Arrays.toString(MCPExecution.values()));
			}
			if (execModeEnum != null) {
				meta.put(MCPUtility.SMSS_MCP_EXECUTION, execModeEnum.getValue());
			} else {
				// default to AUTO
				meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
				if (execModeInput != null) {
					classLogger.warn("Invalid SMSS_MCP_EXECUTION value '{}' for reactor '{}'; falling back to 'auto'.",
							execModeInput, reactorNames.get(i));
				}
			}
			// UI
			JSONObject uiJson = new JSONObject();
			if (!additionalMeta.isEmpty()) {
				Map<String, Object> uiMap = null;
				try {
					uiMap = (Map<String, Object>) additionalMeta.get(MCPUtility.SMSS_MCP_UI);
				} catch (ClassCastException e) {
					classLogger.error(
							"Invalid type for SMSS_MCP_UI in reactor '{}'; expected a map of key-value pairs.",
							reactorNames.get(i));
					throw new IllegalArgumentException("Invalid type for SMSS_MCP_UI in reactor '" + uiMap
							+ "'; expected a map of key-value pairs.");
				}
				if (uiMap.containsKey(MCPUtility.UI_RESOURCE_URI)) {
					uiJson.put(MCPUtility.UI_RESOURCE_URI, uiMap.get(MCPUtility.UI_RESOURCE_URI));
				}
				if (uiMap.containsKey(MCPUtility.UI_LOADING_MESSAGE)) {
					uiJson.put(MCPUtility.UI_LOADING_MESSAGE, uiMap.get(MCPUtility.UI_LOADING_MESSAGE));
				}
				if (uiMap.containsKey(MCPUtility.UI_DISPLAY_LOCATION)) {
					String displayLocation = (String) uiMap.getOrDefault(MCPUtility.UI_DISPLAY_LOCATION, null);
					MCPDisplayOption displayEnum = MCPDisplayOption.fromValue(displayLocation);
					if (displayEnum == null && !displayLocation.isBlank()) {
						throw new IllegalArgumentException(MCPUtility.UI_DISPLAY_LOCATION + " can only be a value of: "
								+ Arrays.toString(MCPDisplayOption.values()));
					}
					String displayString = (displayEnum != null) ? displayEnum.getValue() : null;
					uiJson.put(MCPUtility.UI_DISPLAY_LOCATION, displayString);
				}
			}
			meta.put(MCPUtility.SMSS_MCP_UI, uiJson);

			reactorTool.put("_meta", meta);
			toolsArray.put(reactorTool);
		}

		if (toolsArray == null || toolsArray.isEmpty()) {
			throw new IllegalArgumentException("No tools were added to engine " + engine);
		}

		JSONObject _meta = new JSONObject();
		LocalDate todayUTC = LocalDate.now(ZoneOffset.UTC);
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		_meta.put("last_modified_date", todayUTC.format(formatter));
		mcpJson.put("_meta", _meta);
		mcpJson.put("tools", toolsArray);

		String outputFileLoc = engineAssetsFolder + "/mcp/pixel_mcp.json";
		File outputFile = new File(outputFileLoc);
		if (!outputFile.getParentFile().exists() || !outputFile.getParentFile().isDirectory()) {
			outputFile.getParentFile().mkdirs();
		}
		if (outputFile.exists()) {
			outputFile.delete();
		}
		try (FileWriter writer = new FileWriter(outputFile)) {
			String prettyJson = mcpJson.toString(4);
			writer.write(prettyJson);
		} catch (IOException e) {
			classLogger.error("Unable to write pixel_mcp.json file", e);
			throw new IllegalArgumentException(
					"Unable to write pixel_mcp.json file. Detailed error = " + e.getMessage());
		}

		// add tags
		MCPUtility.addMCPTag(engine);
		boolean mcpEnabled = engine.isMCPEnabled();
		if (!mcpEnabled) {
			String smssFilePath = engine.getSmssFilePath();
			Map<String, String> mcpEnabledMap = new HashMap<>();
			mcpEnabledMap.put(Constants.MCP_ENABLED, "true");
			try {
				Utility.changePropertiesFileValue(smssFilePath, mcpEnabledMap, false);
				engine.open(smssFilePath);
			} catch (Exception e) {
				throw new IllegalArgumentException("Error enabling mcp in smss");
			}
		}

		String versionGitFolder = EngineUtility.getSpecificEngineVersionFolder(eType, engineId, engineName);
		String comment = this.keyValue.get(ReactorKeysEnum.COMMENT_KEY.getKey());
		if (comment == null) {
			comment = "add: configured Engine MCP tool";
		}

		// add file to git
		List<String> gitRelativeFilePaths = new ArrayList<>();
		gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/mcp/pixel_mcp.json");

		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();

		GitRepoUtils.addSpecificFiles(versionGitFolder, gitRelativeFilePaths);
		// commit it
		GitRepoUtils.commitAddedFiles(versionGitFolder, comment, author, email);
		// handle synchronization to the cloud
		ClusterUtil.pushEngineFolder(engine, engineAssetsFolder);
		if (!mcpEnabled) {
			ClusterUtil.pushEngineSmss(engineId);
		}
		return new NounMetadata(mcpJson, PixelDataType.JSON_OBJECT);
	}

	@Override
	public String getReactorDescription() {
		return "Generates a mcp/pixel_mcp.json file from a set of reactors";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine to add MCP tools from";
		} else if (key.equals(ReactorKeysEnum.REACTOR.getKey())) {
			return "The list of reactors to turn into mcp tools in the pixel_mcp.json";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the engine";
		}
		return super.getDescriptionForKey(key);
	}

}