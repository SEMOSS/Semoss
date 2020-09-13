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

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.PostgresSqlInterpreter;

public class PostgresQueryUtil extends AnsiSqlQueryUtil {
	
	PostgresQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.POSTGRES);
	}
	
	PostgresQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.POSTGRES);
	}
	
	PostgresQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
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
	
	@Override
	public String tableExistsQuery(String tableName, String schema) {
		return "select table_name, table_type from information_schema.tables where table_schema='" + schema + "' and table_name='" + tableName + "'";
	}
	
	@Override
	public String buildConnectionString(Map<String, Object> configMap) throws SQLException, RuntimeException {
		if(configMap.isEmpty()){
			throw new RuntimeException("Configuration Map is Empty.");
		}
		String urlPrefix = ((String) RdbmsTypeEnum.POSTGRES.getUrlPrefix()).toUpperCase();
		String hostname = ((String) configMap.get(AbstractSqlQueryUtil.HOSTNAME)).toUpperCase();
		String port = ((String) configMap.get(AbstractSqlQueryUtil.PORT)).toUpperCase();
		String schema = ((String) configMap.get(AbstractSqlQueryUtil.SCHEMA)).toUpperCase();
		String username = ((String) configMap.get(AbstractSqlQueryUtil.USERNAME)).toUpperCase();
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		String connectionString = urlPrefix+"://"+hostname+port+"/"+schema; 
		
		return connectionString;
	}

	@Override
	public String buildConnectionString(Properties prop) {
		if(prop == null){
			throw new RuntimeException("Properties ojbect is null");
		}
		String urlPrefix = ((String) RdbmsTypeEnum.POSTGRES.getUrlPrefix()).toUpperCase();
		String hostname = ((String) prop.getProperty(AbstractSqlQueryUtil.HOSTNAME)).toUpperCase();
		String port = ((String) prop.getProperty(AbstractSqlQueryUtil.PORT)).toUpperCase();
		String schema = ((String) prop.getProperty(AbstractSqlQueryUtil.SCHEMA)).toUpperCase();
		String username = ((String) prop.getProperty(AbstractSqlQueryUtil.USERNAME)).toUpperCase();
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		String connectionString = urlPrefix+"://"+hostname+port+"/"+schema; 
		
		return connectionString;
	}
}