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

import java.util.Collection;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.PostgresSqlInterpreter;
import prerna.query.querystruct.AbstractQueryStruct;
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
		typeConversionMap.put("INT", "INT");
		typeConversionMap.put("LONG", "BIGINT");
		
		typeConversionMap.put("NUMBER", "FLOAT");
		typeConversionMap.put("FLOAT", "FLOAT");
		typeConversionMap.put("DOUBLE", "FLOAT");

		typeConversionMap.put("DATE", "DATE");
		typeConversionMap.put("TIMESTAMP", "TIMESTAMP");
		
		typeConversionMap.put("STRING", "VARCHAR(800)");
	}
	
	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if(configMap == null || configMap.isEmpty()){
			throw new RuntimeException("Configuration map is null or empty");
		}
		
		this.connectionUrl = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_URL);
		
		this.hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(hostname == null || hostname.isEmpty())
			) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		this.port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		this.database = (String) configMap.get(AbstractSqlQueryUtil.DATABASE);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.database == null || this.database.isEmpty())
				){
			throw new RuntimeException("Must pass in database name");
		}
		
		this.schema = (String) configMap.get(AbstractSqlQueryUtil.SCHEMA);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.schema == null || this.schema.isEmpty())
				){
			throw new RuntimeException("Must pass in schema name");
		}
		
		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+port+"/"+this.database+"?currentSchema="+this.schema;
			
			if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
				if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
					this.connectionUrl += ";" + this.additionalProps;
				} else {
					this.connectionUrl += this.additionalProps;
				}
			}
		}
		
		return this.connectionUrl;
	}

	@Override
	public String setConnectionDetailsFromSMSS(CaseInsensitiveProperties prop) throws RuntimeException {
		if(prop == null || prop.isEmpty()){
			throw new RuntimeException("Properties object is null or empty");
		}
		
		this.connectionUrl = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_URL);
		
		this.hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(hostname == null || hostname.isEmpty())
			) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		this.port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		this.database = (String) prop.get(AbstractSqlQueryUtil.DATABASE);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.database == null || this.database.isEmpty())
				){
			throw new RuntimeException("Must pass in database name");
		}
		
		this.schema = (String) prop.get(AbstractSqlQueryUtil.SCHEMA);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.schema == null || this.schema.isEmpty())
				){
			throw new RuntimeException("Must pass in schema name");
		}
		
		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+port+"/"+this.database+"?currentSchema="+this.schema;
			
			if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
				if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
					this.connectionUrl += ";" + this.additionalProps;
				} else {
					this.connectionUrl += this.additionalProps;
				}
			}
		}
		
		return this.connectionUrl;
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
	
	public IQueryInterpreter getInterpreter(IEngine engine) {
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
	public String getBlobDataTypeName() {
		return "BYTEA";
	}
	
	@Override
	public String getImageDataTypeName() {
		return "BYTEA";
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
	public void appendSearchRegexFilter(AbstractQueryStruct qs, String columnQs, String searchTerm) {
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector(columnQs));
		NounMetadata lComparison = new NounMetadata(fun, PixelDataType.COLUMN);
		NounMetadata rComparison = new NounMetadata(searchTerm.toLowerCase(), PixelDataType.CONST_STRING);
		SimpleQueryFilter filter = new SimpleQueryFilter(lComparison, "~", rComparison);
		qs.addExplicitFilter(filter);
	}
	
	@Override
	public String tableExistsQuery(String tableName, String database, String schema) {
		return "select table_name, table_type from information_schema.tables where table_schema='" + schema.toLowerCase() + "' and table_name='" + tableName.toLowerCase() + "'";
	}
	
	@Override
	public String getAllColumnDetails(String tableName, String database, String schema) {
		return "select column_name, data_type from information_schema.columns where table_schema='" + schema.toLowerCase() + "' and table_name='" + tableName.toLowerCase() + "'";
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