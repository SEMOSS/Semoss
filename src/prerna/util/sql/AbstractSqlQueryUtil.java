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
package prerna.util.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps.SystemOutCostLogger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.Utility;

public abstract class AbstractSqlQueryUtil {

	protected RdbmsTypeEnum dbType = null;
	protected String hostname;
	protected String port;
	protected String schema;
	protected String username;
	protected String password;
	protected String connectionUrl;

	protected List<String> reservedWords = null;
	
	AbstractSqlQueryUtil() {
		
	}
	
	AbstractSqlQueryUtil(String connectionURL, String username, String password) {
		this.connectionUrl = connectionURL;
		this.username = username;
		this.password = password;
	}
	
	AbstractSqlQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		this.dbType = dbType;
		this.hostname = hostname;
		this.port = port;
		this.schema = schema;
		this.username = username;
		this.password = password;
		try {
			this.connectionUrl = RdbmsConnectionHelper.getConnectionUrl(dbType.name(), hostname, port, schema, "");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Use this when we need to make any modifications to the 
	 * connection object for proper usage
	 * Example ::: Adding user defined functions for RDBMS types that allow it
	 * @param con
	 */
	public abstract void enhanceConnection(Connection con);

	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * All connection details the setters and getters
	 */
	
	public RdbmsTypeEnum getDbType() {
		return dbType;
	}

	void setDbType(RdbmsTypeEnum dbType) {
		this.dbType = dbType;
	}

	public String getDriver(){
		return this.dbType.getDriver();
	}
	
	public String getHostname() {
		return hostname;
	}

	public String getPort() {
		return port;
	}

	public String getSchema() {
		return schema;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public String getConnectionUrl() {
		return connectionUrl;
	}

	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Set the list of reserved words
	 * @param reservedWords
	 */
	public void setReservedWords(List<String> reservedWords) {
		this.reservedWords = reservedWords;
	}
	
	/**
	 * Check if the selector is in fact a reserved word
	 * @param selector
	 * @return
	 */
	public boolean isSelectorKeyword(String selector) {
		if(this.reservedWords != null) {
			if(this.reservedWords.contains(selector.toUpperCase())) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Get the escaped keyword
	 * Default is to wrap the selector in quotes
	 * @param selector
	 * @return
	 */
	public String getEscapeKeyword(String selector) {
		return "\"" + selector + "\"";
	}
	
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * This section is so I can properly convert the intended function names
	 */
	
	/**
	 * Get the sql function string
	 * @param inputFunction
	 * @return
	 */
	public abstract String getSqlFunctionSyntax(String inputFunction);
	
	// there are all the specific functions
	// the {@link #getSqlFunctionSyntax(String) getSqlFunctionSyntax} 
	// only needs to be implemented in the AnsiSqlQueryUtil 
	// where it loops through everything and the specifics can be 
	// implemented in the query util implementations
	
	public abstract String getMinFunctionSyntax();
	
	public abstract String getMaxFunctionSyntax();
	
	public abstract String getAvgFunctionSyntax();
	
	public abstract String getMedianFunctionSyntax();
	
	public abstract String getSumFunctionSyntax();
	
	public abstract String getStdevFunctionSyntax();
	
	public abstract String getCountFunctionSyntax();
	
	public abstract String getConcatFunctionSyntax();
	
	public abstract String getGroupConcatFunctionSyntax();
	
	public abstract String getLowerFunctionSyntax();
	
	public abstract String getCoalesceFunctionSyntax();
	
	public abstract String getRegexLikeFunctionSyntax();
	
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * This section is intended for modifications to select queries to pull data
	 */
	
	/**
	 * Add the limit and offset to a query
	 * @param query
	 * @param limit
	 * @param offset
	 * @return
	 */
	public abstract StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset);
	
	/**
	 * Remove duplicates that exist from an existing table by creating a new temp intermediary table
	 * @param tableName
	 * @param fullColumnNameList
	 * @return
	 */
	public abstract String removeDuplicatesFromTable(String tableName, String fullColumnNameList);
	
	/////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Does the RDBMS type support array data types
	 * @return
	 */
	public abstract boolean allowArrayDatatype();
	
	/**
	 * Does the engine allow you to add a column to an existing table
	 * @return
	 */
	public abstract boolean allowAddColumn();
	
	/**
	 * Does the engine allow you to rename a column in an existing table
	 * @return
	 */
	public abstract boolean allowRedefineColumn();
	
	/**
	 * Does the engine allow you to drop a column in an existing table
	 * @return
	 */
	public abstract boolean allowDropColumn();
	
	/**
	 * Does the engine allow "CREATE TABLE IF NOT EXISTS " syntax
	 * @return
	 */
	public abstract boolean allowsIfExistsTableSyntax();
	
	
	/**
	 * Does the engine allow "CREATE INDEX IF NOT EXISTS " syntax
	 * @return
	 */
	public abstract boolean allowIfExistsIndexSyntax();
	
	/**
	 * Does the engine allow "ALTER TABLE xxx ADD COLUMN IF NOT EXISTS" 
	 * and "ALTER TABLE xxx DROP COLUMN IF EXISTS" syntax
	 * @return
	 */
	public abstract boolean allowIfExistsModifyColumnSyntax();
	
	/////////////////////////////////////////////////////////////////////////

	/*
	 * Create table scripts
	 */

	/**
	 * Create a new table with passed in columns + types + default values
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @return
	 */
	public abstract String createTable(String tableName, String[] colNames, String[] types);
	
	/**
	 * Create a new table with passed in columns + types + default values
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @param defaultValues
	 * @return
	 */
	public abstract String createTableWithDefaults(String tableName, String[] colNames, String[] types, Object[] defaultValues);
	
	/**
	 * Create a new table if it does not exist with passed in columns + types + default values
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @return
	 */
	public abstract String createTableIfNotExists(String tableName, String[] colNames, String[] types);

	/**
	 * Create a new table if it does not exist with passed in columns + types + default values
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @param defaultValues
	 * @return
	 */
	public abstract String createTableIfNotExistsWithDefaults(String tableName, String[] colNames, String[] types, Object[] defaultValues);
	
	/*
	 * Drop table scripts
	 */
	
	/**
	 * Drop a table
	 * @param tableName
	 * @return
	 */
	public abstract String dropTable(String tableName);
	
	/**
	 * Drop a table if it exists
	 * @param tableName
	 * @return
	 */
	public abstract String dropTableIfExists(String tableName);
	
	/*
	 * Alter table scripts
	 */
	
	/**
	 * Rename a table
	 * @param tableName
	 * @param newName
	 * @return
	 */
	public abstract String alterTableName(String tableName, String newTableName);
	
	/**
	 * Add a new column to an existing table
	 * @param tableName
	 * @param newColumn
	 * @param newColType
	 * @return
	 */
	public abstract String alterTableAddColumn(String tableName, String newColumn, String newColType);
	
	/**
	 * Add a new column to an existing table with default value
	 * @param tableName
	 * @param newColumn
	 * @param newColType
	 * @param defaultValue
	 * @return
	 */
	public abstract String alterTableAddColumnWithDefault(String tableName, String newColumn, String newColType, Object defualtValue);
	
	/**
	 * Add a new column to an existing table if the column does not exist
	 * @param tableName
	 * @param newColumn
	 * @param newColType
	 * @return
	 */
	public abstract String alterTableAddColumnIfNotExists(String tableName, String newColumn, String newColType);
	
	/**
	 * Add a new column to an existing table if the column does not exist with default value
	 * @param tableName
	 * @param newColumn
	 * @param newColType
	 * @param defaultValue
	 * @return
	 */
	public abstract String alterTableAddColumnIfNotExistsWithDefault(String tableName, String newColumn, String newColType,  Object defualtValue);
	
	
	/**
	 * Drop a column from an existing table
	 * @param tableName
	 * @param columnName
	 * @return
	 */
	public abstract String alterTableDropColumn(String tableName, String columnName);
	
	/**
	 * Drop a column from an existing table if it exists
	 * @param tableName
	 * @param columnName
	 * @return
	 */
	public abstract String alterTableDropColumnIfExists(String tableName, String columnName);
	
	/**
	 * Modify a column definition
	 * @param tableName
	 * @param columnName
	 * @param dataType
	 * @return
	 */
	public abstract String modColumnType(String tableName, String columnName, String dataType);
	
	/**
	 * Modify a column definition with default value
	 * @param tableName
	 * @param columnName
	 * @param dataType
	 * @param defaultValue
	 * @return
	 */
	public abstract String modColumnTypeWithDefault(String tableName, String columnName, String dataType, Object defualtValue);
	
	/**
	 * Modify a column definition if it exists
	 * @param tableName
	 * @param columnName
	 * @param dataType
	 * @return
	 */
	public abstract String modColumnTypeIfExists(String tableName, String columnName, String dataType);
 
	/**
	 * Modify a column definition with a default value if it exists 
	 * @param tableName
	 * @param columnName
	 * @param dataType
	 * @param defaultValue
	 * @return
	 */
	public abstract String modColumnTypeIfExistsWithDefault(String tableName, String columnName, String dataType, Object defualtValue);
	
	/*
	 * Index
	 */
	
	/**
	 * Create an index on a table for a given column
	 * @param indexName
	 * @param tableName
	 * @param column
	 * @return
	 */
	public abstract String createIndex(String indexName, String tableName, String columnName);
	
	/**
	 * Create an index on a table with a set of columns
	 * @param indexName
	 * @param tableName
	 * @param columns
	 * @return
	 */
	public abstract String createIndex(String indexName, String tableName, Collection<String> columns);
	
	/**
	 * Create an index on a table for a given column
	 * @param indexName
	 * @param tableName
	 * @param column
	 * @return
	 */
	public abstract String createIndexIfNotExists(String indexName, String tableName, String columnName);
	
	/**
	 * Create an index on a table with a set of columns
	 * @param indexName
	 * @param tableName
	 * @param columns
	 * @return
	 */
	public abstract String createIndexIfNotExists(String indexName, String tableName, Collection<String> columns);
	
	/**
	 * Drop an existing index
	 * @param indexName
	 * @param tableName
	 * @return
	 */
	public abstract String dropIndex(String indexName, String tableName);
	
	/**
	 * Drop an index if it exists
	 * @param indexName
	 * @param tableName
	 * @return
	 */
	public abstract String dropIndexIfExists(String indexName, String tableName);
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Query database scripts
	 */

	/**
	 * Query to execute 
	 * If has next, the table exists
	 * The schema input is optional and only required by certain engines
	 * @param tableName
	 * @param schema
	 * @return
	 */
	public abstract String tableExistsQuery(String tableName, String schema);
	
	/**
	 * Query to execute to get the column details 
	 * Can also imply if the query returns that the column exists
	 * @param tableName
	 * @param columnName
	 * @param schema
	 * @return
	 */
	public abstract String columnDetailsQuery(String tableName, String columnName, String schema);
	
	/**
	 * Query to get a list of all the indexes in the schema
	 * Since indexes are not unique across tables, this must return (index based)
	 * 1) INDEX_NAME
	 * 2) TABLE_NAME
	 * The schema input is optional and only required by certain engines
	 * 
	 * @param schema
	 * @return
	 */
	public abstract String getIndexList(String schema);
	
	/**
	 * Query to get the index details
	 * Must return data in the following order (index based)
	 * 1) TABLE_NAME
	 * 2) COLUMN_NAME
	 * The schema input is optional and only required by certain engines
	 * 
	 * @param indexName
	 * @return
	 */
	public abstract String getIndexDetails(String indexName, String tableName, String schema);
	
	/**
	 * Query to get all the indexes on a given table
	 * Must return the data in the following order (index based)
	 * 1) INDEX NAME
	 * 2) COLUMN_NAME
	 * The schema input is optional and only required by certain engines
	 * 
	 * @param tableName
	 * @param schema
	 * @return
	 */
	public abstract String allIndexForTableQuery(String tableName, String schema);
	
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * These are older methods
	 * Need to come back and see where to 
	 * utilize these/clean up
	 */
	
	public String getDialectSelectRowCountFrom(String tableName, String whereClause){
		String query = "SELECT COUNT(*) as ROW_COUNT FROM " + tableName;
		if(whereClause.length() > 0){
			query += " WHERE " + whereClause;
		}
		return query;
	}
	
	public String getDialectMergeStatement(String tableKey, String insertIntoClause, List<String> columnList, HashMap<String, String> whereValues, String fkVal, String whereClause){
		ArrayList<String> subqueries = new ArrayList<String>();
		String query = "INSERT INTO " + tableKey + " ("+ insertIntoClause + ") SELECT DISTINCT ";
		for(String column : columnList) {
			String tempColumnName = column + "TEMP";
			String subquery = "(SELECT DISTINCT " + column + " FROM " + tableKey + " WHERE " + whereClause;
			String tempquery = subquery + " union select null where not exists" + subquery + ")) AS " + tempColumnName;
			subqueries.add(tempquery);
			query += tempColumnName + "." + column + " AS " + column + ",";
		}
		for (String whereKey : whereValues.keySet()) {
			query+= whereValues.get(whereKey) + " AS " + whereKey + ", ";
		}
		query += fkVal + " FROM " + tableKey;// + ", ";
		for (int i = 0; i < subqueries.size(); i++) {
			query += ", " + subqueries.get(i);
		}
		return query;
	}
	
	
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////

	public static void main(String[] args) throws Exception {
		TestUtilityMethods.loadAll("C:\\workspace2\\Semoss_Dev\\RDF_Map.prop");
		
		RDBMSNativeEngine security = (RDBMSNativeEngine) Utility.getEngine("security");
		AbstractSqlQueryUtil util = security.getQueryUtil();
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(security, "SELECT * FROM PRAGMA_TABLE_INFO('USER') WHERE NAME='email'");
		while(wrapper.hasNext()) {
			System.out.println(wrapper.next());
		}
	}
	
}