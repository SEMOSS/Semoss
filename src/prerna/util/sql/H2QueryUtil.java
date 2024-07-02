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

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.H2SqlInterpreter;
import prerna.util.Constants;
import prerna.util.Utility;

public class H2QueryUtil extends AnsiSqlQueryUtil {
	
	private static final Logger classLogger = LogManager.getLogger(H2QueryUtil.class);

	private boolean forceFile;
	
	H2QueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.H2_DB);
	}
	
	H2QueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.H2_DB);
	}
	
	@Override
	public IQueryInterpreter getInterpreter(IDatabaseEngine engine) {
		return new H2SqlInterpreter(engine);
	}

	@Override
	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new H2SqlInterpreter(frame);
	}
	
	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if(configMap == null || configMap.isEmpty()){
			throw new RuntimeException("Configuration map is null or empty");
		}
		
		this.forceFile = Boolean.parseBoolean( configMap.get(AbstractSqlQueryUtil.FORCE_FILE) + "");
		this.connectionUrl = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_URL);
		this.hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		this.port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
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
		
		this.forceFile = Boolean.parseBoolean( prop.get(AbstractSqlQueryUtil.FORCE_FILE) + "");
		this.connectionUrl = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_URL);
		this.hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		this.port = (String) prop.get(AbstractSqlQueryUtil.PORT);
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
		
		this.connectionUrl = this.dbType.getUrlPrefix();
		
		if(this.hostname == null || this.hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		File f = new File(Utility.normalizePath(hostname));
		if(this.forceFile || f.exists()) {
			hostname = hostname.replace(".mv.db", "");
			this.connectionUrl += ":nio:" + hostname;
		} else {
			this.connectionUrl += ":tcp://"+hostname+":"+port;
		}
		
		if(this.schema != null && !this.schema.isEmpty()) {
			this.connectionUrl += "/"+schema;
		}
		
		if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
			if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
				this.connectionUrl += ";" + this.additionalProps;
			} else {
				this.connectionUrl += this.additionalProps;
			}
		}
		
		return this.connectionUrl;
	}
	
	@Override
	public void enhanceConnection(Connection con) {
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			stmt.execute("DROP AGGREGATE IF EXISTS MEDIAN");
			stmt.close();
			stmt = con.createStatement();
			stmt.execute("CREATE AGGREGATE IF NOT EXISTS SMSS_MEDIAN FOR \"prerna.ds.rdbms.h2.H2MedianAggregation\";");
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	@Override
	public String getMedianFunctionSyntax() {
		return "SMSS_MEDIAN";
	}

	@Override
	public String dropIndex(String indexName, String tableName) {
		return "DROP INDEX " + indexName;
	}

	@Override
	public String dropIndexIfExists(String indexName, String tableName) {
		return "DROP INDEX IF EXISTS " + indexName;
	}

	@Override
	public String getDateFormatFunctionSyntax() {
		return "FORMATDATETIME";
	}
	
	@Override
	public String escapeReferencedAlias(String alias) {
		return "\"" + alias + "\"";
	}

	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Query database scripts
	 */
	
	@Override
	public String tableExistsQuery(String tableName, String database, String schema) {
		// do not need to use the schema
		return "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName.toUpperCase() + "'";
	}
	
	@Override
	public String tableConstraintExistsQuery(String constraintName, String tableName, String database, String schema) {
		// do not need to use the schema
		return "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME = '" + constraintName.toUpperCase() + "' AND TABLE_NAME = '" + tableName.toUpperCase() + "'";
	}

	@Override
	public String referentialConstraintExistsQuery(String constraintName, String database, String schema) {
		// do not need to use the schema
		return "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_NAME = '" + constraintName.toUpperCase() + "'";
	}
	
	@Override
	public String getAllColumnDetails(String tableName, String database, String schema) {
		// do not need to use the schema
		return "SELECT COLUMN_NAME, TYPE_NAME, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName.toUpperCase() + "';";
	}
	
	@Override
	public String columnDetailsQuery(String tableName, String columnName, String database, String schema) {
		// do not need to use the schema
		return "SELECT COLUMN_NAME, TYPE_NAME, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName.toUpperCase() + "' AND COLUMN_NAME='" + columnName.toUpperCase() + "';";
	}
	
	@Override
	public String getIndexList(String database, String schema) {
		// do not need to use the schema
		return "SELECT DISTINCT INDEX_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.INDEXES;";
	}
	
	@Override
	public String getIndexDetails(String indexName, String tableName, String database, String schema) {
		// do not use the schema
		return "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE INDEX_NAME='" + indexName.toUpperCase() + "' AND TABLE_NAME='" + tableName.toUpperCase() + "';";
	}
	
	@Override
	public String allIndexForTableQuery(String tableName, String database, String schema) {
		// do not need to use the schema
		return "SELECT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME='" + tableName.toUpperCase() + "';";
	}
	
	@Override
	public String alterTableDropColumns(String tableName, Collection<String> columnNames) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		
		StringBuilder alterString = new StringBuilder("ALTER TABLE " + tableName + " DROP COLUMN (");
		int i = 0;
		for(String newColumn : columnNames) {
			if (i > 0) {
				alterString.append(", ");
			}
			
			// should escape keywords
			if(isSelectorKeyword(newColumn)) {
				newColumn = getEscapeKeyword(newColumn);
			}
			
			alterString.append(newColumn);
			
			i++;
		}
		alterString.append(");");
		return alterString.toString();
	}

	public String hashColumn(String tableName, String[] columns){
		StringBuilder builder = new StringBuilder();
		builder.append("UPDATE " + tableName + " SET ");
		builder.append(String.join(",",Stream.of(columns).map(c -> c + " = HASH('SHA256', STRINGTOUTF8(" + c + "), 1000)").collect(Collectors.toList())));
		return builder.toString();
	}
}