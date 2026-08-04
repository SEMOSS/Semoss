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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Returns the physical table and column names for every database-engine node
 * in the automation that exposes its SQL expression as a playground-fillable
 * field. Used by the LLM to discover the schema before writing SQL queries for
 * {@code TriggerAutomation}.
 *
 * <p>Pixel: {@code GetAutomationSchema(project=["appId"])}
 */
public class GetAutomationSchemaReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetAutomationSchemaReactor.class);

	public GetAutomationSchemaReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (projectId == null || projectId.isBlank()) {
			throw new IllegalArgumentException("Must provide a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access");
		}

		Map<String, Object> doc = AutomationExecutionUtils.loadAutomationDoc(projectId);
		@SuppressWarnings("unchecked")
		Map<String, Object> graph = (Map<String, Object>) doc.get(AutomationConstants.DOC_GRAPH);
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> nodes = graph != null ? (List<Map<String, Object>>) graph.get(AutomationConstants.DOC_NODES) : null;

		List<Map<String, Object>> result = new ArrayList<>();
		if (nodes != null) {
			for (Map<String, Object> node : nodes) {
				if (!AutomationConstants.NODE_DATABASE_ENGINE.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
					continue;
				}
				@SuppressWarnings("unchecked")
				List<String> fillable = (List<String>) node.get("playgroundFillable");
				if (fillable == null || !fillable.contains(AutomationConstants.CONFIG_EXPRESSION)) {
					continue;
				}
				@SuppressWarnings("unchecked")
				Map<String, Object> config = (Map<String, Object>) node.get(AutomationConstants.NODE_FIELD_CONFIG);
				if (config == null) continue;

				String nodeLabel = (String) node.get(AutomationConstants.NODE_FIELD_LABEL);
				Object engineIdObj = config.get(AutomationConstants.CONFIG_ENGINE_ID);
				if (engineIdObj == null || engineIdObj.toString().isBlank()) continue;

				String engineId = engineIdObj.toString();
				Map<String, Object> nodeSchema = buildNodeSchema(nodeLabel, engineId);
				if (nodeSchema != null) {
					result.add(nodeSchema);
				}
			}
		}

		Map<String, Object> output = new HashMap<>();
		output.put("nodes", result);
		if (result.isEmpty()) {
			output.put("message", "No database nodes with playground-fillable SQL expressions found in this automation.");
		}
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private Map<String, Object> buildNodeSchema(String nodeLabel, String engineId) {
		IDatabaseEngine engine;
		try {
			engine = Utility.getDatabase(engineId);
		} catch (Exception e) {
			classLogger.warn("Could not load database engine {} for schema discovery", engineId, e);
			return null;
		}
		if (engine == null) {
			classLogger.warn("Database engine {} not found for node '{}'", engineId, nodeLabel);
			return null;
		}

		List<String> conceptUris = engine.getPhysicalConcepts();
		if (conceptUris == null || conceptUris.isEmpty()) {
			return null;
		}

		List<Map<String, Object>> tables = new ArrayList<>();
		for (String conceptUri : conceptUris) {
			String tableName = Utility.getInstanceName(conceptUri);
			if (tableName == null || tableName.isBlank()) continue;

			List<String> columns = new ArrayList<>();
			List<String> propUris = engine.getPropertyUris4PhysicalUri(conceptUri);
			if (propUris != null) {
				for (String propUri : propUris) {
					String colName = Utility.getInstanceName(propUri);
					if (colName != null && !colName.isBlank()) {
						columns.add(colName);
					}
				}
			}

			Map<String, Object> tableEntry = new HashMap<>();
			tableEntry.put("name", tableName);
			tableEntry.put("columns", columns);
			tables.add(tableEntry);
		}

		if (tables.isEmpty()) return null;

		Map<String, Object> nodeSchema = new HashMap<>();
		nodeSchema.put("nodeLabel", nodeLabel != null ? nodeLabel : AutomationConstants.UNNAMED_NODE_LABEL);
		nodeSchema.put("engineId", engineId);
		nodeSchema.put("tables", tables);
		return nodeSchema;
	}

	@Override
	public String getReactorDescription() {
		return "Returns the physical table and column names for database-engine nodes in the automation that accept SQL input. Call this before TriggerAutomation to discover the exact table and column names to use in your SQL query.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) return "The project (app) ID or alias to retrieve the database schema for.";
		return super.getDescriptionForKey(key);
	}
}
