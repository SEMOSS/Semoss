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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.PostgresSqlInterpreter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PostgresQueryUtil extends AnsiSqlQueryUtil {
	
	PostgresQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.POSTGRES);
	}
	
	PostgresQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.POSTGRES);
	}
	
	@Override
	public void initTypeConverstionMap() {
		super.initTypeConverstionMap();
		typeConversionMap.put("NUMBER", "FLOAT");
		typeConversionMap.put("FLOAT", "FLOAT");
		typeConversionMap.put("DOUBLE", "FLOAT");
	}
	
	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if(configMap == null || configMap.isEmpty()){
			throw new RuntimeException("Configuration map is null or empty");
		}
		
		this.connectionUrl = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_URL);
		this.hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		this.port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		this.database = (String) configMap.get(AbstractSqlQueryUtil.DATABASE);
		this.schema = (String) configMap.get(AbstractSqlQueryUtil.SCHEMA);
		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);
		this.username = (String) configMap.get(AbstractSqlQueryUtil.USERNAME);
		this.password = (String) configMap.get(AbstractSqlQueryUtil.PASSWORD);
		
		return buildConnectionString();
	}

	@Override
	public String setConnectionDetailsFromSMSS(CaseInsensitiveProperties prop) throws RuntimeException {
		if(prop == null || prop.isEmpty()){
			throw new RuntimeException("Properties object is null or empty");
		}
		
		this.connectionUrl = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_URL);
		this.hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		this.port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		this.database = (String) prop.get(AbstractSqlQueryUtil.DATABASE);
		this.schema = (String) prop.get(AbstractSqlQueryUtil.SCHEMA);
		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);
		this.username = (String) prop.get(AbstractSqlQueryUtil.USERNAME);
		this.password = (String) prop.get(AbstractSqlQueryUtil.PASSWORD);

		return buildConnectionString();	
	}

	@Override
	public String buildConnectionString() {
		if(this.connectionUrl != null && !this.connectionUrl.isEmpty()) {
			return this.connectionUrl;
		}
		
		if(this.hostname == null || this.hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		if(this.database == null || this.database.isEmpty()) {
			throw new RuntimeException("Must pass in database name");
		}
		
		if(this.schema == null || this.schema.isEmpty()) {
			throw new RuntimeException("Must pass in schema name");
		}
		
		this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+port+"/"+this.database+"?currentSchema="+this.schema;
		
		if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
			if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
				this.connectionUrl += ";" + this.additionalProps;
			} else {
				this.connectionUrl += this.additionalProps;
			}
		}
		
		return this.connectionUrl;
	}
	
	public IQueryInterpreter getInterpreter(IDatabaseEngine engine) {
		return new PostgresSqlInterpreter(engine);
	}

	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new PostgresSqlInterpreter(frame);
	}
	
	@Override
	public String escapeReferencedAlias(String alias) {
		return "\"" + alias + "\"";
	}
	
	public String escapeSubqueryColumnName(String columnReturnedFromSubquery) {
		return "\"" + columnReturnedFromSubquery + "\"";
	}
	
	@Override
	public String getRegexLikeFunctionSyntax() {
		return "REGEXP_MATCHES";
	}
	
	@Override
	public boolean allowBlobJavaObject() {
		return false;
	}
	
	@Override
	public void handleInsertionOfBlob(Connection conn, PreparedStatement statement, String object, int index) throws SQLException, UnsupportedEncodingException {
		if(object == null) {
			statement.setNull(index, java.sql.Types.BLOB);
		} else {
			statement.setBytes(index, object.getBytes("UTF-8"));
		}
	}
	
	@Override
	public String getBlobDataTypeName() {
		return "BYTEA";
	}
	
	@Override
	public String getImageDataTypeName() {
		return "BYTEA";
	}
	
	@Override
	public String getGroupConcatFunctionSyntax() {
		return "STRING_AGG";
	}
	
	@Override
	public String processGroupByFunction(String selectExpression, String separator, boolean distinct) {
		if(distinct) {
			return getSqlFunctionSyntax(QueryFunctionHelper.GROUP_CONCAT) + "(DISTINCT " + selectExpression + ", '" + separator + "')";
		} else {
			return getSqlFunctionSyntax(QueryFunctionHelper.GROUP_CONCAT) + "(" + selectExpression + ", '" + separator + "')";
		}
	}
	
	@Override
	public String handleBlobRetrieval(ResultSet result, String key) throws SQLException, IOException {
		return new String(result.getBytes(key));
	}
	
	@Override
	public String handleBlobRetrieval(ResultSet result, int index) throws SQLException, IOException {
		return new String(result.getBytes(index));
	}
	
	@Override
	public boolean allowClobJavaObject() {
		return false;
	}
	
	@Override
	public String getClobDataTypeName() {
		return "TEXT";
	}
	
	@Override
	public boolean allowIfExistsAddConstraint() {
		return false;
	}
	
	@Override
	public IQueryFilter getSearchRegexFilter(String columnQs, String searchTerm) {
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector(columnQs));
		NounMetadata lComparison = new NounMetadata(fun, PixelDataType.COLUMN);
		NounMetadata rComparison = new NounMetadata(searchTerm.toLowerCase(), PixelDataType.CONST_STRING);
		SimpleQueryFilter filter = new SimpleQueryFilter(lComparison, "~", rComparison);
		return filter;
	}
	
	@Override
	public String tableExistsQuery(String tableName, String database, String schema) {
		return "select table_name, table_type from information_schema.tables where table_schema='" + schema.toLowerCase() + "' and table_name='" + tableName.toLowerCase() + "'";
	}
	
	@Override
	public String getAllColumnDetails(String tableName, String database, String schema) {
		return "select column_name, udt_name, character_maximum_length, numeric_precision, numeric_scale from information_schema.columns where table_schema='" + schema.toLowerCase() + "' and table_name='" + tableName.toLowerCase() + "'";
	}
	
	@Override
	public String columnDetailsQuery(String tableName, String columnName, String database, String schema) {
		return "select column_name, udt_name, character_maximum_length, numeric_precision, numeric_scale from information_schema.columns where table_schema='" + schema.toLowerCase() + "' and table_name='" + tableName.toLowerCase() + "' and column_name='" + columnName + "'";
	}
	
	@Override
	public String tableConstraintExistsQuery(String constraintName, String tableName, String database, String schema) {
		return "select constraint_name from information_schema.table_constraints where constraint_name = '" + constraintName.toLowerCase() + "' and table_name = '" + tableName.toLowerCase() + "' and table_schema='" + schema.toLowerCase() + "'";
	}

	@Override
	public String referentialConstraintExistsQuery(String constraintName, String database, String schema) {
		return "select constraint_name from information_schema.referential_constraints where constraint_name = '" + constraintName.toLowerCase() + "' and constraint_schema='" + schema.toLowerCase() + "'";
	}
	
	@Override
	public String getIndexList(String database, String schema) {
		return "select indexname, tablename from pg_indexes where schema = '" + schema.toLowerCase() + "'";
	}
	
//	@Override
//	public String getIndexDetails(String indexName, String tableName, String database, String schema) {
//		return "select tablename from pg_indexes where indexname = '" + indexName.toLowerCase() + " and schema = '" + schema.toLowerCase() 
//			+ "' and tablename = '" + tableName.toLowerCase() + "'";
//	}
//	
//	@Override
//	public String allIndexForTableQuery(String tableName, String database, String schema) {
//		return "SELECT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME='" + tableName.toUpperCase() + "';";
//	}
	
	@Override
	public String alterTableDropColumns(String tableName, Collection<String> columnNames) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		
		StringBuilder alterString = new StringBuilder("ALTER TABLE " + tableName + " DROP COLUMN ");
		int i = 0;
		for(String newColumn : columnNames) {
			if (i > 0) {
				alterString.append(", DROP COLUMN ");
			}
			
			// should escape keywords
			if(isSelectorKeyword(newColumn)) {
				newColumn = getEscapeKeyword(newColumn);
			}
			
			alterString.append(newColumn);
			
			i++;
		}
		alterString.append(";");
		return alterString.toString();
	}
	
}