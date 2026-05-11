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
package prerna.reactor.database;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class TextToSQLReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(TextToSQLReactor.class);

	private static final String ERROR = "error";

	public TextToSQLReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.MODEL.getKey(),
				ReactorKeysEnum.COMMAND.getKey(), ERROR, ReactorKeysEnum.SQL.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();

		// check database permissions
		String databaseId = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey());
		if (!SecurityEngineUtils.userCanViewEngine(user, databaseId)) {
			throw new IllegalArgumentException(
					"Database " + databaseId + " does not exist or user does not have access to this database");
		}

		// check model permissions
		String engine = this.keyValue.get(ReactorKeysEnum.MODEL.getKey());
		if (!SecurityEngineUtils.userCanViewEngine(user, engine)) {
			throw new IllegalArgumentException(
					"Model " + engine + " does not exist or user does not have access to this model");
		}
		IDatabaseEngine dbEngine = Utility.getDatabase(databaseId);
		if (!(dbEngine instanceof IRDBMSEngine)) {
			throw new IllegalArgumentException("Database must be a RDBMS engine");
		}
		IRDBMSEngine database = (IRDBMSEngine) dbEngine;
		IModelEngine modelEngine = Utility.getModel(engine);

		// get relation info about the database
		List<String> conceptTableNames = database.getPixelConcepts();
		Map<String, Object[]> metamodel = database.getMetamodel();
		Object[] edges = metamodel.get("edges");

		Map<String, Object> conceptInfo = new HashMap<>();
		List<Map<String, String>> edgeInfo = new ArrayList<>();
		for (Object edge : edges) {
			Map<String, String> e = (Map<String, String>) edge;
			if (e.containsKey("rel")) {
				String[] splitRel = e.get("rel").split("\\.");
				if (splitRel.length == 4) {
					Map<String, String> relInfo = new HashMap<>();
					relInfo.put("source_table", splitRel[0]);
					relInfo.put("source_table_column", splitRel[1]);
					relInfo.put("target_table", splitRel[2]);
					relInfo.put("target_table_column", splitRel[3]);
					edgeInfo.add(relInfo);
				} else {
					classLogger.warn("Could not determine relation for " + e.get("rel"));
				}
			}
		}

		// get descriptions, logical names, and relation info for columns and put in map
		for (String conceptTable : conceptTableNames) {
			List<String> propertyColumnNames = database.getPixelSelectors(conceptTable);
			String tablePhysicalUri = database.getPhysicalUriFromPixelSelector(conceptTable);
			String description = database.getDescription(tablePhysicalUri);
			Map<String, Object> propertyMap = new HashMap<>();
			for (String propertyColumn : propertyColumnNames) {
				String physicalUri = database.getPhysicalUriFromPixelSelector(propertyColumn);
				String desc = database.getDescription(physicalUri);
				String dataType = database.getDataTypes(physicalUri);
				Set<String> logicalNames = database.getLogicalNames(physicalUri);
				Map<String, Object> dataMap = new HashMap<>();
				if (dataType != null && !(dataType = dataType.trim()).isEmpty()) {
					dataMap.put("dataType", dataType);
				}
				if (desc != null && !(desc = desc.trim()).isEmpty()) {
					dataMap.put("description", desc);
				}
				if (!logicalNames.isEmpty()) {
					dataMap.put("logical_names", logicalNames);
				}
				for (Map<String, String> relInfo : edgeInfo) {
					if (relInfo.get("target_table").equals(conceptTable)) {
						String p = database.getConceptualName(physicalUri);
						if (relInfo.get("target_table_column").equals(p)) {
							dataMap.put("foreign_key", relInfo);
						}
					}
				}
				String parsedProperty = propertyColumn.split("__")[1];
				propertyMap.put(parsedProperty, dataMap);
			}
			Map<String, Object> conceptJson = new HashMap<>();
			if (description != null && !description.trim().equals("")) {
				conceptJson.put("description", description);
			}
			conceptJson.put("columns", propertyMap);
			conceptInfo.put(conceptTable, conceptJson);
		}

		// generate context for llm
		String sqlContext = """
				You are an expert SQL assistant. You are provided with a database schema in JSON format, which includes tables, table-level descriptions, columns, and metadata such as descriptions, logical names, and foreign key relationships.
				Your task is to generate an SQL query that answers the user's question, using only the information explicitly available in the schema.

				Return your answer as a JSON string with the following fields:
				"question": The user's question, verbatim.
				"sql": The SQL query that answers the question, or an empty string if the schema does not provide enough information to generate the query.
				"explanation": A brief explanation of how you mapped the question to the schema and constructed the query, or why the query could not be generated.

				Instructions:
				1. Carefully read and interpret the user's question. Restate your understanding of what the user is asking for to yourself.
				2. Identify which tables and columns in the schema are relevant to answering the question, using only information explicitly present in the schema (including descriptions, logical names, and foreign key metadata).
				3. Generate the SQL query using only SELECT statements. Do not use INSERT, UPDATE, or DELETE.
				4. Only join tables if a foreign key relationship is defined in the schema. Do not assume relationships that are not explicitly specified.
				5. Do not use any columns, tables, or relationships that are not present or described in the schema.
				6. The SQL query should be compatible for a %s database.
				7. The SQL query should be valid and as efficient as possible.
				8. If you use a column or table based on its description or logical name, briefly explain your reasoning in the "explanation" field.
				9. Before finalizing your answer, think step-by-step about what result your SQL query will produce, and check if it matches the user's request.
				10. If the SQL query does not exactly answer the user's question, or if there is not enough information in the schema, leave the "sql" field blank and explain why in the "explanation" field.
				11. Make sure the JSON string can be parsed with GSON.

				Schema:
				%s

				Your output must be a plain JSON string with the keys: "question", "sql", and "explanation".
				Do not wrap your response in any formatting or code blocks.
				""";

		String context = String.format(sqlContext, database.getDbType(), GSON.toJson(conceptInfo));
		String prompt = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());
		String error = this.keyValue.get(ERROR);
		String oldSQL = this.keyValue.get(ReactorKeysEnum.SQL.getKey());
		if (error != null && !error.trim().isEmpty() && oldSQL != null) {
			String appendError = """
					The previous SQL you generated was the following:
					%s

					When running the SQL, there was the following error:
					%s

					Please regenerate the SQL while considering the error.
					""";
			String tailoredPrompt = String.format(appendError, oldSQL, error);
			prompt += "\n" + tailoredPrompt;
		}
		Map<String, Object> paramMap = getParamMap();
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}

		// add response format to ensure json schema
		paramMap.put("response_format", getJsonSchema());

		// ask model and parse response
		Map<String, Object> queryResponse = modelEngine.ask(prompt, context, this.insight, paramMap).toMap();
		String responseString = (String) queryResponse.get("response");
		Map<String, Object> responseMap = parseResponse(responseString.trim());
		if (responseMap == null || !responseMap.containsKey("question") || !responseMap.containsKey("sql")
				|| !responseMap.containsKey("explanation")) {
			throw new SemossPixelException("LLM could not generate proper response");
		}

		return new NounMetadata(responseMap, PixelDataType.MAP);
	}

	private Map<String, Object> getParamMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

	private Map<String, Object> parseResponse(String jsonString) {
		Type type = new TypeToken<Map<String, Object>>() {
		}.getType();
		Map<String, Object> map = null;

		try {
			map = GSON.fromJson(jsonString, type);
		} catch (JsonSyntaxException e) {
			classLogger.error("Failed to parse JSON response");
			throw new SemossPixelException("Failed to parse JSON response: " + jsonString, e);
		}
		return map;
	}

	private Map<String, Object> getJsonSchema() {
		Map<String, Object> questionProp = new HashMap<>();
		questionProp.put("type", "string");

		Map<String, Object> sqlProp = new HashMap<>();
		sqlProp.put("type", "string");

		Map<String, Object> explanationProp = new HashMap<>();
		explanationProp.put("type", "string");

		Map<String, Object> properties = new HashMap<>();
		properties.put("question", questionProp);
		properties.put("sql", sqlProp);
		properties.put("explanation", explanationProp);

		List<String> required = Arrays.asList("question", "sql", "explanation");

		Map<String, Object> schema = new HashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		schema.put("required", required);
		schema.put("additionalProperties", false);

		Map<String, Object> jsonSchema = new HashMap<>();
		jsonSchema.put("name", "sql_generator");
		jsonSchema.put("schema", schema);
		jsonSchema.put("strict", true);

		Map<String, Object> paramJson = new HashMap<>();
		paramJson.put("type", "json_schema");
		paramJson.put("json_schema", jsonSchema);
		return paramJson;
	}

	@Override
	public String getReactorDescription() {
		return "Convert natural language to SQL. Also allows users to pass in old SQL and SQL errors to generate new SQL";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "The database id for the RDBMS we want to generate a SQL query against";
		} else if (key.equals(ReactorKeysEnum.MODEL.getKey())) {
			return "The model id used to generate the SQL query";
		} else if (key.equals(ReactorKeysEnum.COMMAND.getKey())) {
			return "The user text to be turned into the SQL query based on the database schema";
		} else if (key.equals(ERROR)) {
			return "This is an optional parameter to pass in the error associated with a previous SQL query prediction";
		}
		return super.getDescriptionForKey(key);
	}

}
