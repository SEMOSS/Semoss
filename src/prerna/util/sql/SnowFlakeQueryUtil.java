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

import java.util.Map;
import java.util.Properties;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.SnowFlakeSqlInterpreter;
import prerna.query.querystruct.selectors.QueryFunctionSelector;

public class SnowFlakeQueryUtil extends AnsiSqlQueryUtil {
	
	SnowFlakeQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.SNOWFLAKE);
	}
	
	SnowFlakeQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.SNOWFLAKE);
	}
	
	public IQueryInterpreter getInterpreter(IEngine engine) {
		return new SnowFlakeSqlInterpreter(engine);
	}

	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new SnowFlakeSqlInterpreter(frame);
	}

	@Override
	public String buildConnectionString(Map<String, Object> configMap) throws RuntimeException {
		if(configMap.isEmpty()){
			throw new RuntimeException("Configuration map is empty");
		}
		
		String connectionString = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_STRING);
		if(connectionString != null && !connectionString.isEmpty()) {
			return connectionString;
		}
		
		String urlPrefix = this.dbType.getUrlPrefix();
		String hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		if(hostname == null || hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		String port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		String warehouse = (String) configMap.get(AbstractSqlQueryUtil.WAREHOUSE);
		if(warehouse == null || warehouse.isEmpty()) {
			throw new RuntimeException("Must pass in the warehouse to compute the queries");
		}
		
		String role = (String) configMap.get(AbstractSqlQueryUtil.ROLE);
		if(role == null || role.isEmpty()) {
			role = "PUBLIC";
		}
		
		String database = (String) configMap.get(AbstractSqlQueryUtil.DATABASE);
		if(database == null || database.isEmpty()) {
			throw new RuntimeException("Must pass in the database");
		}
		
		String schema = (String) configMap.get(AbstractSqlQueryUtil.SCHEMA);
		if(schema == null || schema.isEmpty()) {
			throw new RuntimeException("Must pass in schema name");
		}
		
		connectionString = urlPrefix+"://"+hostname+port+"/?warehouse="+warehouse+"&role="+role+"&db="+database+"&schema="+schema;
		
		String additonalProperties = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);
		if(additonalProperties != null && !additonalProperties.isEmpty()) {
			if(!additonalProperties.startsWith("&")) {
				connectionString += "&" + additonalProperties;
			} else {
				connectionString += additonalProperties;
			}
		}
		
		return connectionString;
	}

	@Override
	public String buildConnectionString(Properties prop) throws RuntimeException {
		if(prop == null){
			throw new RuntimeException("Properties ojbect is null");
		}
		
		String connectionString = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_STRING);
		if(connectionString != null && !connectionString.isEmpty()) {
			return connectionString;
		}
		
		String urlPrefix = this.dbType.getUrlPrefix();
		String hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		if(hostname == null || hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		String port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		String warehouse = (String) prop.get(AbstractSqlQueryUtil.WAREHOUSE);
		if(warehouse == null || warehouse.isEmpty()) {
			throw new RuntimeException("Must pass in the warehouse to compute the queries");
		}
		
		String role = (String) prop.get(AbstractSqlQueryUtil.ROLE);
		if(role == null || role.isEmpty()) {
			role = "PUBLIC";
		}
		
		String database = (String) prop.get(AbstractSqlQueryUtil.DATABASE);
		if(database == null || database.isEmpty()) {
			throw new RuntimeException("Must pass in the database");
		}
		
		String schema = (String) prop.get(AbstractSqlQueryUtil.SCHEMA);
		if(schema == null || schema.isEmpty()) {
			throw new RuntimeException("Must pass in schema name");
		}
		
		connectionString = urlPrefix+"://"+hostname+port+"/?warehouse="+warehouse+"&role="+role+"&db="+database+"&schema="+schema;
		
		String additonalProperties = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);
		if(additonalProperties != null && !additonalProperties.isEmpty()) {
			if(!additonalProperties.startsWith("&")) {
				connectionString += "&" + additonalProperties;
			} else {
				connectionString += additonalProperties;
			}
		}
		
		return connectionString;
	}
	
	@Override
	public String escapeReferencedAlias(String alias) {
		return "\"" + alias + "\"";
	}
	
	@Override
	public String getGroupConcatFunctionSyntax() {
		return "LISTAGG";
	}
	
	@Override
	public void appendDefaultFunctionOptions(QueryFunctionSelector fun) {
		String function = getSqlFunctionSyntax(fun.getFunction());
		if(function.equals(getGroupConcatFunctionSyntax())) {
			if(fun.getAdditionalFunctionParams().isEmpty()) {
				fun.addAdditionalParam(new Object[] {"noname", "', '"});
			}
		}
	}
	
}