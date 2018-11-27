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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public abstract class SQLQueryUtil {

	private String defaultDbUserName = "";
	private String defaultDbPassword = "";

	private String dialectSelectRowCountFrom = " SELECT COUNT(*) as ROW_COUNT FROM ";

	//create
	public final String dialectAlterTable = " ALTER TABLE ";
	public final String dialectDropTable = " DROP TABLE ";
	public final String dialectSelect = "SELECT ";
	public final String dialectDistinct = " DISTINCT ";
	//index
	private String dialectAllIndexesInDB = "";
	private String dialectIndexInfo = "";
	private String dialectDropIndex = "DROP INDEX ";
	

	// Add SQLServer compatibility
	public static SQLQueryUtil initialize(RdbmsTypeEnum dbtype) {
		if(dbtype == RdbmsTypeEnum.H2_DB) {
			return new H2QueryUtil();
		} else if(dbtype == RdbmsTypeEnum.MARIADB){
			return new MariaDbQueryUtil();
		} else if(dbtype == RdbmsTypeEnum.SQLSERVER){
			return new SQLServerQueryUtil();
		} else if(dbtype == RdbmsTypeEnum.MYSQL) {
			return new MySQLQueryUtil();
		} else if(dbtype == RdbmsTypeEnum.ORACLE) {
			return new OracleQueryUtil();
		} else if(dbtype == RdbmsTypeEnum.IMPALA) {
			return new ImpalaQueryUtil();
		} else if(dbtype == RdbmsTypeEnum.TIBCO) {
			return new TibcoQueryUtil();
		}
		else {
			AnsiSqlQueryUtil queryUtil = new AnsiSqlQueryUtil();
			queryUtil.setDbType(dbtype);
			return queryUtil;
		}
	}
	
	public static SQLQueryUtil initialize(RdbmsTypeEnum dbtype, String hostname, String port, String schema, String username, String password) {
		if(dbtype == RdbmsTypeEnum.H2_DB) {
			return new H2QueryUtil();
		} else if(dbtype == RdbmsTypeEnum.SQLSERVER){
			return new SQLServerQueryUtil(hostname, port, schema, username, password);
		} else if(dbtype == RdbmsTypeEnum.MYSQL) {
			return new MySQLQueryUtil(hostname, port, schema, username, password);
		} else if(dbtype == RdbmsTypeEnum.ORACLE) {
			return new OracleQueryUtil(hostname, port, schema, username, password);
		} else if(dbtype == RdbmsTypeEnum.IMPALA) {
			return new ImpalaQueryUtil(hostname, port, schema, username, password);
		} else if(dbtype == RdbmsTypeEnum.TIBCO) {
			return new TibcoQueryUtil(hostname, port, schema, username, password);
		}
		else {
			AnsiSqlQueryUtil queryUtil = new AnsiSqlQueryUtil(dbtype, hostname, port, schema, username, password);
			return queryUtil;
		}
	}
	
	public static SQLQueryUtil initialize(RdbmsTypeEnum dbtype, String connectionURL, String username, String password) {
		if(dbtype == RdbmsTypeEnum.H2_DB) {
			return new H2QueryUtil();
		} else if(dbtype == RdbmsTypeEnum.SQLSERVER){
			return new SQLServerQueryUtil(connectionURL, username, password);
		} else if(dbtype == RdbmsTypeEnum.MYSQL) {
			return new MySQLQueryUtil(connectionURL, username, password);
		} else if(dbtype == RdbmsTypeEnum.ORACLE) {
			return new OracleQueryUtil(connectionURL, username, password);
		} else {
			AnsiSqlQueryUtil queryUtil = new AnsiSqlQueryUtil();
			queryUtil.setDbType(dbtype);
			return queryUtil;
		}
	}

	public abstract RdbmsTypeEnum getDatabaseType();

	public abstract String getConnectionURL(String baseFolder,String dbname);

	public abstract String getDatabaseDriverClassName();

	public abstract String getDialectIndexInfo(String indexName, String dbName);

	public String getDefaultDBUserName(){
		return this.defaultDbUserName;
	}

	public String getDefaultDBPassword(){
		return this.defaultDbPassword;
	}

	public String getDialectSelectRowCountFrom(String tableName, String whereClause){
		String query = this.dialectSelectRowCountFrom + tableName;
		if(whereClause.length()>0){
			query += " WHERE " + whereClause;
		}
		return query;
	}

	//index
	public String getDialectAllIndexesInDB(){
		return this.dialectAllIndexesInDB;
	}
	
	public String getDialectAllIndexesInDB(String schema){
		return this.dialectAllIndexesInDB + "'" + schema + "'"; //use bind bariables?
	}
	
	public String getDialectIndexInfo(){
		return this.dialectIndexInfo;
	}
	
	public String getDialectDropIndex(){
		return this.dialectDropIndex;
	}
	
	public String getDialectDropIndex(String indexName){
		return getDialectDropIndex(indexName,"");
	}
	
	public String getDialectDropIndex(String indexName, String tableName){
		return this.dialectDropIndex + indexName;
	}
	
	public String getDialectCreateIndex(String indexName, String tablename, String columnInIndex){
		List<String> columnsInIndexArr = new ArrayList<String>();
		columnsInIndexArr.add(columnInIndex);
		return getDialectCreateIndex(indexName, tablename, columnsInIndexArr);
	}
	
	public String getDialectCreateIndex(String indexName, String tablename, List<String> columnsInIndex){
		String createIndexText = "";
		for(String columnName: columnsInIndex){
			if(createIndexText.length() == 0){
				createIndexText = "CREATE INDEX " + indexName + " ON " + tablename  + "(";
			} else {
				createIndexText += ",";
			}
			createIndexText += columnName;
		}
		createIndexText += ")";
		return createIndexText;
	}

	public String getDialectRemoveDuplicates(String tableName, String fullColumnNameList){
		String createTable = "";
		String subQuery = "";
		//For SQL Server CTAS doesn't work, using select * into newTable from oldTable
		//Also LTRIM(RTRIM(tableName)) works instead of TRIM(tableName)
		if(this.getDatabaseType().equals(RdbmsTypeEnum.SQLSERVER)){
			createTable ="SELECT DISTINCT " + fullColumnNameList 
					+ " INTO " + tableName + "_TEMP " 
					+ " FROM " + tableName + " WHERE " + tableName 
					+ " IS NOT NULL AND LTRIM(RTRIM(" + tableName + ")) <> ''";
		}else{
			createTable = "CREATE TABLE " + tableName + "_TEMP AS ";
			subQuery = "(SELECT DISTINCT " + fullColumnNameList
					+ " FROM " + tableName+" WHERE " + tableName 
					+ " IS NOT NULL AND TRIM(" + tableName + ") <> '' )";
		}
		return createTable + subQuery;
	}

	public String dialectVerifyTableExists(String tableName){
		String query = getDialectSelectRowCountFrom("INFORMATION_SCHEMA.TABLES","TABLE_NAME = '" + tableName +"'");
		return query;
	}

	public String getDialectDropTable(String tableName){
		return this.dialectDropTable + tableName;
	}

	public String getDialectAlterTableName(String fromName, String toName){
		return this.dialectAlterTable + fromName + " RENAME TO " + toName; //alterTableName = "ALTER TABLE " + tableName + "_TEMP RENAME TO " + tableName;
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

	//SETTERS
	public void setDefaultDbUserName(String defaultDbUserName){
		this.defaultDbUserName = defaultDbUserName;
	}
	
	public void setDefaultDbPassword(String defaultDbPassword){
		this.defaultDbPassword = defaultDbPassword;
	}
	
	public void setDialectAllIndexesInDB(String dialectAllIndexesInDB){
		this.dialectAllIndexesInDB = dialectAllIndexesInDB;
	}
	
	public void setDialectIndexInfo(String dialectIndexInfo){
		this.dialectIndexInfo = dialectIndexInfo;
	}

	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset) {
		if(limit > 0) {
			query = query.append(" LIMIT "+limit);
		}
		if(offset > 0) {
			query = query.append(" OFFSET "+offset);
		}
		return query;
	}
	
}