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
package prerna.rdf.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;

import com.hp.hpl.jena.vocabulary.OWL;

import prerna.poi.main.BaseDatabaseCreator;
import prerna.poi.main.PropFileWriter;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.poi.main.helper.ImportOptions;
import prerna.util.AbstractFileWatcher;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.SMSSWebWatcher;
import prerna.util.sql.SQLQueryUtil;

public class ImportRDBMSProcessor {
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
	
	public boolean addNewRDBMS(String type, String host, String port, String username, String password, String schema, String engineName, HashMap<String, Object> metamodel) {
		boolean success = false;
		
		HashMap<String, ArrayList<String>> nodesAndProps = (HashMap<String, ArrayList<String>>) metamodel.get("nodes");
		ArrayList<String[]> relationships = (ArrayList<String[]>) metamodel.get("relationships");
		
		SQLQueryUtil queryUtil = SQLQueryUtil.initialize(SQLQueryUtil.DB_TYPE.valueOf(type), host, port, schema, username, password);
//		RDBMSReader reader = new RDBMSReader();
//		reader.setQueryUtil(queryUtil);
		File engineDir = new File(baseFolder + "/db/" + engineName);
		engineDir.mkdir();
		RDBMSEngineCreationHelper.writePropFile(engineName, queryUtil);
//		reader.writePropFile(engineName);
		
		BaseDatabaseCreator bdc = new BaseDatabaseCreator(baseFolder + "/db/" + engineName + "/" + engineName + "_OWL.OWL");
		String semossURI = "http://semoss.org/ontologies";
		
		String sub = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
		String pred = RDF.TYPE.stringValue();
		String obj = Constants.CLASS_URI;
		bdc.addToBaseEngine(new Object[] {sub, pred, obj, true});
		
		// necessary triple saying Relation is a type of Property
		sub =  semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
		pred = RDF.TYPE.stringValue();
		obj = Constants.DEFAULT_PROPERTY_URI;
		bdc.addToBaseEngine(new Object[] {sub, pred, obj, true});
		
		String basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + "Contains";
		bdc.addToBaseEngine(new Object[] {basePropURI, Constants.SUBPROPERTY_URI, basePropURI, true});
		
		HashMap<String, String> tables = new HashMap<String, String>();
		
		for(String s : nodesAndProps.keySet()) {
			String[] tableAndPrimaryKey = s.split("\\.");
			String nodeName = tableAndPrimaryKey[0];
			String PK = tableAndPrimaryKey[1];
			
			tables.put(nodeName, PK);
			
			sub = semossURI + "/" + Constants.DEFAULT_NODE_CLASS + "/" + PK + "/" + nodeName;
			pred = Constants.SUBCLASS_URI;
			obj = semossURI + "/Concept";
			bdc.addToBaseEngine(new Object[] {sub, pred, obj, true});
			
			pred = semossURI + "/" + Constants.DEFAULT_PROPERTY_CLASS + "/" + "PRIMARY_KEY";
			obj = PK;
			bdc.addToBaseEngine(new Object[] {sub, pred, obj, false});
			
			for(String prop : nodesAndProps.get(s)) {
				if(!prop.equals(PK)) {
					sub = basePropURI + "/" + prop;
					pred = RDF.TYPE +"";
					obj = semossURI + "/" + Constants.DEFAULT_PROPERTY_CLASS;
					bdc.addToBaseEngine(new Object[] {sub, pred, obj, true});
					
					sub = semossURI + "/" + Constants.DEFAULT_NODE_CLASS + "/" + PK + "/" + nodeName;
					pred = OWL.DatatypeProperty+"";
					obj = basePropURI + "/" + prop;
					bdc.addToBaseEngine(new Object[] {sub, pred, obj, true});
				}
			}
		}
		
		for(String[] rel : relationships) {
			sub = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + rel[0] + "." + rel[1].replace(".", "." + rel[2] + ".");
			pred = Constants.SUBPROPERTY_URI;
			obj = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
			bdc.addToBaseEngine(new Object[] {sub, pred, obj, true});
			
			sub = semossURI + "/" + Constants.DEFAULT_NODE_CLASS + "/" + tables.get(rel[0]) + "/" + rel[0];
			pred = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + rel[0] + "." + rel[1].replace(".", "." + rel[2] + ".");
			obj = semossURI + "/" + Constants.DEFAULT_NODE_CLASS + "/" + tables.get(rel[2]) + "/" + rel[2];
			bdc.addToBaseEngine(new Object[] {sub, pred, obj, true});
		}
		bdc.addToBaseEngine(new Object[] {semossURI + "/" + Constants.DEFAULT_PROPERTY_CLASS, RDF.TYPE+"", semossURI + "/" + Constants.DEFAULT_RELATION_CLASS, true});
		
		bdc.commit();
		try {
			bdc.exportBaseEng(false);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		RDBMSEngineCreationHelper.writeDefaultQuestionSheet(engineName, tables.keySet());
//		reader.setTables(tables.keySet());
//		reader.writeDefaultQuestionSheet(engineName);
		
		PropFileWriter propWriter = new PropFileWriter();
		propWriter.setBaseDir(baseFolder);
		propWriter.setSQLQueryUtil(queryUtil);
		propWriter.setShouldFillEmptyTypes("true");
		File oldFile = null;
		File newFile = null;
		try {
			String watcher = "SMSSWebWatcher";
			String folder = DIHelper.getInstance().getProperty(watcher + "_DIR");
			AbstractFileWatcher watcherInstance = new SMSSWebWatcher();
			watcherInstance.setMonitor(new Object[]{});
			watcherInstance.setFolderToWatch(folder);
			
			propWriter.runWriter(engineName, "", "", ImportOptions.DB_TYPE.RDBMS);
			oldFile = new File(propWriter.propFileName);
			
			watcherInstance.process(propWriter.propFileName.substring(propWriter.propFileName.lastIndexOf("/"))); 
			
			newFile = new File(propWriter.propFileName.replace("temp", "smss"));
			FileUtils.copyFile(oldFile, newFile);
			newFile.setReadable(true);
		} catch (IllegalArgumentException e1) {
			e1.printStackTrace();
		} catch (FileNotFoundException e2) {
			e2.printStackTrace();
		} catch (IOException e3) {
			e3.printStackTrace();
		} finally {
			if(oldFile != null && oldFile.exists()) {
				try {
					FileUtils.forceDelete(oldFile);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			success = true;
		}
		
		return success;
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