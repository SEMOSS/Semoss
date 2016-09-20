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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.poi.main.AbstractEngineCreator;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.poi.main.helper.ImportOptions;
import prerna.util.DIHelper;
import prerna.util.sql.SQLQueryUtil;

public class ImportRDBMSProcessor extends AbstractEngineCreator {
	static final Logger logger = LogManager.getLogger(ImportRDBMSProcessor.class.getName());
	
	private String connectionURL;
	private String schema;
	private String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
	
	private final String MYSQL = "MySQL";
	private final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private final String ORACLE = "Oracle";
	private final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private final String SQLSERVER = "SQL_Server";
	private final String SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private final String ASTER = "Aster Database";
	private final String ASTER_DRIVER = "com.asterdata.ncluster.jdbc.core.NClusterJDBCDriver";
	
	private Connection buildConnection(String type, String host, String port, String username, String password, String schema) throws SQLException {
		Connection con = null;
		String url = "";

		try {
			if(type.equals(this.MYSQL)) {
				Class.forName(this.MYSQL_DRIVER);
				
				if(connectionURL != null && !connectionURL.isEmpty()) {
					schema = connectionURL.substring(0, connectionURL.indexOf("?")).split("/")[3];
					this.schema = schema;
					return DriverManager.getConnection(connectionURL);
				}

				// Connection URL format: jdbc:mysql://<hostname>[:port]/<DBname>?user=username&password=pw
				url = "jdbc:mysql://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
				if(port != null && !port.isEmpty()) {
					url = url.replace(":PORT", ":" + port);
				} else {
					url = url.replace(":PORT", "");
				}
				
				this.schema = schema;
				con = DriverManager.getConnection(url + "?user=" + username + "&password=" + new String(password));
			} else if(type.equals(this.ORACLE)) {
				Class.forName(this.ORACLE_DRIVER);
				
				if(connectionURL != null && !connectionURL.isEmpty()) {
					schema = connectionURL.substring(connectionURL.indexOf("/")).split("-")[0];
					this.schema = schema;
					return DriverManager.getConnection(connectionURL);
				}

				//Connection URL format: jdbc:oracle:thin:@<hostname>[:port]/<service or sid>[-schema name]
				url = "jdbc:oracle:thin:@HOST:PORT:SERVICE".replace("HOST", host).replace("SERVICE", schema);
				if(port != null && !port.isEmpty()) {
					url = url.replace(":PORT", ":" + port);
				} else {
					url = url.replace(":PORT", "");
				}

				this.schema = schema;
				con = DriverManager.getConnection(url, username, new String(password));
			} else if(type.equals(this.SQLSERVER)) {
				Class.forName(this.SQLSERVER_DRIVER);
				
				if(connectionURL != null && !connectionURL.isEmpty()) {
					schema = connectionURL.substring(connectionURL.indexOf("="));
					this.schema = schema;
					return DriverManager.getConnection(connectionURL);
				}
				
				//Connection URL format: jdbc:sqlserver://<hostname>[:port];databaseName=<DBname>
				url = "jdbc:sqlserver://HOST:PORT;databaseName=SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
				if(port != null && !port.isEmpty()) {
					url = url.replace(":PORT", ":" + port);
				} else {
					url = url.replace(":PORT", "");
				}
				
				this.schema = schema;
				con = DriverManager.getConnection(url, username, new String(password));
			}
		} catch(ClassNotFoundException e) {
			e.printStackTrace();
		}

		return con;
	}
	
	public HashMap<String, String> getForeignKeyRelationships(String type, String host, String port, String username, String password, String schema) {
		Statement stmt;
		ResultSet rs;
		String query = "";
		HashMap<String, String> allFKRelationships = new HashMap<String, String>();

		try {
			Connection con = buildConnection(type, host, port, username, password, schema);

			if(type.equals(this.MYSQL)) {
				query = "SELECT DISTINCT u.TABLE_NAME, u.COLUMN_NAME, u.REFERENCED_TABLE_NAME, u.REFERENCED_COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS c, INFORMATION_SCHEMA.KEY_COLUMN_USAGE u WHERE c.CONSTRAINT_NAME=u.CONSTRAINT_NAME AND c.CONSTRAINT_TYPE='FOREIGN KEY' AND u.TABLE_SCHEMA='" + this.schema + "';";

				stmt = con.createStatement();
				rs = stmt.executeQuery(query);

				while(rs.next()) {
					String tableName = "";
					String columnName = "";
					String refTable = "";
					String refColumn = "";

					tableName = rs.getString("TABLE_NAME");
					columnName = rs.getString("COLUMN_NAME");
					refTable = rs.getString("REFERENCED_TABLE_NAME");
					refColumn = rs.getString("REFERENCED_COLUMN_NAME");

					allFKRelationships.put(tableName + "." + columnName, refTable + "." + refColumn);
				}

				System.out.println("All Foreign Key Relationships: " + allFKRelationships);


			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return allFKRelationships;
	}
	
	public HashMap<String, ArrayList<String>> getAllFields(String type, String host, String port, String username, String password, String schema) {
		Statement stmt;
		ResultSet rs;
		String query = "";
		HashMap<String, ArrayList<String>> allTablesAndColumns = new HashMap<String, ArrayList<String>>(); 

		try {
			Connection con = buildConnection(type, host, port, username, password, schema);

			if(type.equals(this.MYSQL)) {
				query = "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + this.schema + "' ORDER BY TABLE_NAME;";
			} else if(type.equals(this.ORACLE)) {
				query = "SELECT TABLE_NAME, COLUMN_NAME FROM USER_TAB_COLS ORDER BY TABLE_NAME";
			} else if(type.equals(this.SQLSERVER)) {
				query = "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS";
			} else {
				return null;
			}

			stmt = con.createStatement();
			rs = stmt.executeQuery(query);

			while(rs.next()) {
				String tableName = "";
				String columnName = "";

				tableName = rs.getString("TABLE_NAME");
				columnName = rs.getString("COLUMN_NAME");

				if(allTablesAndColumns.get(tableName) == null) {
					allTablesAndColumns.put(tableName, new ArrayList<String>());
				}

				allTablesAndColumns.get(tableName).add(columnName);
			}

			System.out.println("All Tables and Columns: " + allTablesAndColumns);

		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return allTablesAndColumns;
	}
	
	public IEngine addNewRDBMS(ImportOptions options) throws IOException {
		SQLQueryUtil.DB_TYPE sqlType = options.getRDBMSDriverType();
		String host = options.getHost();
		String port = options.getPort();
		String schema = options.getSchema();
		String username = options.getUsername();
		String password = options.getPassword();
		String engineName = options.getDbName();
		HashMap<String, Object> externalMetamodel = options.getExternalMetamodel();
		queryUtil = SQLQueryUtil.initialize(sqlType, host, port, schema, username, password);
		prepEngineCreator(null, options.getOwlFileLocation(), options.getSMSSLocation());
		openRdbmsEngineWithoutConnection(engineName);
		HashMap<String, ArrayList<String>> nodesAndProps = (HashMap<String, ArrayList<String>>) externalMetamodel.get("nodes");
		ArrayList<String[]> relationships = (ArrayList<String[]>) externalMetamodel.get("relationships");
		Map<String, Map<String, String>> existingRDBMSStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(engine,queryUtil);
		Map<String, String> nodesAndPrimKeys = new HashMap<String, String>(); // Uncleaned concepts and their primkeys
		
		nodesAndPrimKeys = parseNodesAndProps(nodesAndProps, existingRDBMSStructure);
		parseRelationships(relationships, existingRDBMSStructure, nodesAndPrimKeys);
		createBaseRelations(); // TODO: this should be moved into ImportDataProcessor and removed from every subclass of AbstractEngineCreator
		
		RDBMSEngineCreationHelper.writeDefaultQuestionSheet(engineName, nodesAndPrimKeys.keySet());
		
		return this.engine;
	}
	
	private HashMap<String, String> parseNodesAndProps(HashMap<String, ArrayList<String>> nodesAndProps, Map<String, Map<String, String>> dataTypes) {
		HashMap<String, String> nodesAndPrimKeys = new HashMap<String, String>(nodesAndProps.size());
		for(String node : nodesAndProps.keySet()) {
			String[] tableAndPrimaryKey = node.split("\\.");
			String nodeName = tableAndPrimaryKey[0];
			String primaryKey = tableAndPrimaryKey[1];
			nodesAndPrimKeys.put(nodeName, primaryKey);
			
			String cleanConceptTableName = RDBMSEngineCreationHelper.cleanTableName(nodeName);
			owler.addConcept(cleanConceptTableName, primaryKey, dataTypes.get(nodeName).get(primaryKey));
			for(String prop: nodesAndProps.get(node)) {
				if(!prop.equals(primaryKey)) {
					String cleanProp = RDBMSEngineCreationHelper.cleanTableName(prop);
					owler.addProp(cleanConceptTableName, primaryKey, cleanProp, dataTypes.get(nodeName).get(prop));
				}
			}
		}
		
		return nodesAndPrimKeys;
	}
	
	private void parseRelationships(ArrayList<String[]> relationships, Map<String, Map<String, String>> dataTypes, Map<String, String> nodesAndPrimKeys) {
		for(String[] relationship: relationships) {
			String subject = RDBMSEngineCreationHelper.cleanTableName(relationship[0]);
			String object = RDBMSEngineCreationHelper.cleanTableName(relationship[2]);
			String predicate = subject + "." + relationship[1] + "." + object; //TODO: check if this needs to be cleaned
			owler.addRelation(subject, nodesAndPrimKeys.get(subject), object, nodesAndPrimKeys.get(object), predicate);
		}
	}
	
	private boolean isValidConnection(Connection con) {
		boolean isValid = false;
		
		try {
			if(con.isValid(5)) {
				isValid = true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
			
		return isValid;
	}
	
	public String checkConnectionParams(String type, String host, String port, String username, String password, String schema) {
		boolean success;
		try {
			success = isValidConnection(buildConnection(type, host, port, username, password, schema));
		} catch(SQLException e) {
			return e.getMessage();
		}
		
		return success+"";
	}
	
	public void setConnectionURL(String connectionURL) {
		this.connectionURL = connectionURL;
	}
}