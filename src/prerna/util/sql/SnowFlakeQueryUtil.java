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

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabase;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.SnowFlakeSqlInterpreter;
import prerna.query.querystruct.selectors.QueryFunctionSelector;

public class SnowFlakeQueryUtil extends AnsiSqlQueryUtil {
	
	private String warehouse;
	private String role;
	
	SnowFlakeQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.SNOWFLAKE);
	}
	
	SnowFlakeQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.SNOWFLAKE);
	}
	
	public IQueryInterpreter getInterpreter(IDatabase engine) {
		return new SnowFlakeSqlInterpreter(engine);
	}

	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new SnowFlakeSqlInterpreter(frame);
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
		
		this.warehouse = (String) configMap.get(AbstractSqlQueryUtil.WAREHOUSE);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.warehouse == null || this.warehouse.isEmpty())
			) {
			throw new RuntimeException("Must pass in the warehouse to compute the queries");
		}
		
		this.role = (String) configMap.get(AbstractSqlQueryUtil.ROLE);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.role == null || this.role.isEmpty())
			) {
			this.role = "PUBLIC";
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
			this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+port
					+"/?warehouse="+this.warehouse+"&role="+this.role+"&db="+this.database+"&schema="+this.schema;
			
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
		
		this.warehouse = (String) prop.get(AbstractSqlQueryUtil.WAREHOUSE);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.warehouse == null || this.warehouse.isEmpty())
			) {
			throw new RuntimeException("Must pass in the warehouse to compute the queries");
		}
		
		this.role = (String) prop.get(AbstractSqlQueryUtil.ROLE);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.role == null || this.role.isEmpty())
			) {
			this.role = "PUBLIC";
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
			this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+port
					+"/?warehouse="+this.warehouse+"&role="+this.role+"&db="+this.database+"&schema="+this.schema;
			
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
		
		if(this.warehouse == null || this.warehouse.isEmpty()) {
			throw new RuntimeException("Must pass in the warehouse to compute the queries");
		}
		
		if(this.role == null || this.role.isEmpty()) {
			this.role = "PUBLIC";
		}
		
		if(this.database == null || this.database.isEmpty()) {
			throw new RuntimeException("Must pass in database name");
		}
		
		if(this.schema == null || this.schema.isEmpty()) {
			throw new RuntimeException("Must pass in schema name");
		}
		
		this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+port
				+"/?warehouse="+this.warehouse+"&role="+this.role+"&db="+this.database+"&schema="+this.schema;
		
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