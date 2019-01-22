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
package prerna.rdf.main; // TODO: move to prerna.poi.main

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.poi.main.AbstractEngineCreator;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.poi.main.helper.ImportOptions;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SQLQueryUtil;

public class ImportRDBMSProcessor extends AbstractEngineCreator {
	
	private static final Logger LOGGER = LogManager.getLogger(ImportRDBMSProcessor.class.getName());

	public IEngine addNewRDBMS(ImportOptions options) throws Exception {
		// information for connection details
		RdbmsTypeEnum sqlType = options.getRDBMSDriverType();
		String host = options.getHost();
		String port = options.getPort();
		String schema = options.getSchema();
		String username = options.getUsername();
		String password = options.getPassword();
		String engineName = options.getDbName();
		String appName = options.getEngineID();
		
		// the logical metamodel for the upload
		Map<String, Object> externalMetamodel = options.getExternalMetamodel();
		Map<String, List<String>> nodesAndProps = (Map<String, List<String>>) externalMetamodel.get("nodes");
		List<String[]> relationships = (List<String[]>) externalMetamodel.get("relationships");

		this.queryUtil = SQLQueryUtil.initialize(sqlType, host, port, schema, username, password);
		prepEngineCreator(null, options.getOwlFileLocation(), options.getSMSSLocation());
		// this will create the class variable this.engine
		generateEngineFromRDBMSConnection(schema, engineName, appName);
		
		// get the existing table names -> column name, column type
		Map<String, Map<String, String>> existingRDBMSStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(this.engine);
		
		// parse the nodes and get the prim keys
		// and write to OWL
		Map<String, String> nodesAndPrimKeys = parseNodesAndProps(nodesAndProps, existingRDBMSStructure);
		// parse the relationships
		// and write to OWL
		parseRelationships(relationships, existingRDBMSStructure, nodesAndPrimKeys);
		// commit / save the owl
		createBaseRelations();
		
		// generate base insights
		RDBMSEngineCreationHelper.insertAllTablesAsInsights(this.engine, this.owler);
		return this.engine;
	}

	/**
	 * Add the concepts and properties into the OWL
	 * @param nodesAndProps
	 * @param dataTypes
	 * @return
	 */
	private Map<String, String> parseNodesAndProps(Map<String, List<String>> nodesAndProps, Map<String, Map<String, String>> dataTypes) 
	{
		Map<String, String> nodesAndPrimKeys = new HashMap<String, String>(nodesAndProps.size());
		for (String node : nodesAndProps.keySet()) {
			String[] tableAndPrimaryKey = node.split("\\.");
			String nodeName = tableAndPrimaryKey[0];
			String primaryKey = tableAndPrimaryKey[1];
			nodesAndPrimKeys.put(nodeName, primaryKey);

			String cleanConceptTableName = RDBMSEngineCreationHelper.cleanTableName(nodeName);
			owler.addConcept(cleanConceptTableName, primaryKey, dataTypes.get(nodeName).get(primaryKey));
			for (String prop : nodesAndProps.get(node)) {
				if (!prop.equals(primaryKey)) {
					String cleanProp = RDBMSEngineCreationHelper.cleanTableName(prop);
					owler.addProp(cleanConceptTableName, primaryKey, cleanProp, dataTypes.get(nodeName).get(prop));
				}
			}
		}

		return nodesAndPrimKeys;
	}

	/**
	 * Add the relationships into the OWL
	 * @param relationships
	 * @param dataTypes
	 * @param nodesAndPrimKeys
	 */
	private void parseRelationships(List<String[]> relationships, Map<String, Map<String, String>> dataTypes, Map<String, String> nodesAndPrimKeys) 
	{
		for (String[] relationship : relationships) {
			String subject = RDBMSEngineCreationHelper.cleanTableName(relationship[0]);
			String object = RDBMSEngineCreationHelper.cleanTableName(relationship[2]);
			// TODO: check if this needs to be cleaned
			String[] joinColumns = relationship[1].split("\\."); 
			// predicate is: "fromTable.fromJoinCol.toTable.toJoinCol"
			String predicate = subject + "." + joinColumns[0] + "." + object + "." + joinColumns[1]; 
			owler.addRelation(subject, nodesAndPrimKeys.get(subject), object, nodesAndPrimKeys.get(object), predicate);
		}
	}

	/**
	 * Determine if a connection is valid
	 * @param type
	 * @param host
	 * @param port
	 * @param username
	 * @param password
	 * @param schema
	 * @return
	 */
	public String checkConnectionParams(String type, String host, String port, String username, String password, String schema, String additionalProperties) {
		boolean success;
		try {
			success = isValidConnection(RdbmsConnectionHelper.buildConnection(type, host, port, username, password, schema, additionalProperties));
		} catch (SQLException e) {
			return e.getMessage();
		}

		return success + "";
	}
	
	/**
	 * Determine if a connection is valid
	 * @param con
	 * @return
	 */
	private boolean isValidConnection(Connection con) {
		boolean isValid = false;
		try {
			if (con.isValid(5)) {
				isValid = true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return isValid;
	}

	/**
	 * Parse through the database metadata to get the schema of the rdbms database
	 * @param type
	 * @param host
	 * @param port
	 * @param username
	 * @param password
	 * @param schema
	 * @return
	 * @throws SQLException
	 */
	public Map<String, Object> getSchemaDetails(String type, String host, String port, String username, String password, String schema, String additonalProperties) throws SQLException {
		Connection con;
		try {
			con = RdbmsConnectionHelper.buildConnection(type, host, port, username, password, schema, additonalProperties);
		} catch (SQLException e) {
			throw new SQLException("Unable to establish connection");
		}
		
		// tablename
		Map<String, List<Map>> tableDetails = new HashMap<String, List<Map>>();
		// sub tatble [objtable, fromcol, tocol]
		Map<String, List<Map>> relations = new HashMap<String, List<Map>>();
		
		DatabaseMetaData meta;
		try {
			meta = con.getMetaData();
		} catch (SQLException e) {
			throw new SQLException("Unable to get database metadata");
		}
		ResultSet tables;
		try {
			tables = meta.getTables(null, null, null, new String[] { "TABLE", "VIEW" });
		} catch (SQLException e) {
			throw new SQLException("Unable to get tables from database metadata");
		}
		try {
			while (tables.next()) {
				String table = tables.getString("table_name");
				LOGGER.info("Processing table = " + table);

				List<String> primaryKeys = new ArrayList<String>();
				//name, type, isPK
				Map<String, Object> colDetails = new HashMap<String, Object>(); 

				List<Map> allCols = new ArrayList<Map>();
				Map<String, String> fkDetails = new HashMap<String, String>();
				List<Map> allRels = new ArrayList<Map>();

				ResultSet keys = null;
				try {
					keys = meta.getPrimaryKeys(null, null, table);
					while(keys.next()) {
						primaryKeys.add(keys.getString("column_name"));
					}
				} catch (SQLException e) {
					e.printStackTrace();
				} finally {
					closeRs(keys);
				}

				try {
					LOGGER.info("Processing table columns");
					keys = meta.getColumns(null, null, table, null);
					while (keys.next()) {
						colDetails = new HashMap<String, Object>();
						colDetails.put("name", keys.getString("column_name"));
						colDetails.put("type", keys.getString("type_name"));
						if (primaryKeys.contains(keys.getString("column_name"))) {
							colDetails.put("isPK", true);
						} else {
							colDetails.put("isPK", false);
						}
						allCols.add(colDetails);
					}
					tableDetails.put(table, allCols);
				} catch (SQLException e) {
					e.printStackTrace();
				} finally {
					closeRs(keys);
				}

				try {
					LOGGER.info("Processing table foreign keys");
					keys = meta.getExportedKeys(null, null, table);
					while (keys.next()) {
						fkDetails = new HashMap<String, String>();
						fkDetails.put("fromCol", keys.getString("PKCOLUMN_NAME"));
						fkDetails.put("toTable", keys.getString("FKTABLE_NAME"));
						fkDetails.put("toCol", keys.getString("FKCOLUMN_NAME"));
						allRels.add(fkDetails);
					}
					relations.put(table, allRels);
				} catch (SQLException e) {
					e.printStackTrace();
				} finally {
					closeRs(keys);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeRs(tables);
		}
		
		HashMap<String, Object> ret = new HashMap<String, Object>();
		ret.put("tables", tableDetails);
		ret.put("relationships", relations);
		return ret;
	}
	
	/**
	 * Close the result set
	 * @param rs
	 */
	private void closeRs(ResultSet rs) {
		if(rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}