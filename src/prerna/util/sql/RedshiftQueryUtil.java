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
import java.sql.Statement;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.util.ConnectionUtils;

public class RedshiftQueryUtil extends AnsiSqlQueryUtil {

	private static final Logger classLogger = LogManager.getLogger(RedshiftQueryUtil.class);

	RedshiftQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.REDSHIFT);
	}

	RedshiftQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.REDSHIFT);
	}

	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if (configMap == null || configMap.isEmpty()) {
			throw new RuntimeException("Configuration map is null or empty");
		}

		this.connectionUrl = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_URL);

		this.hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty()) && (hostname == null || hostname.isEmpty())) {
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
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty())
				&& (this.database == null || this.database.isEmpty())) {
			throw new RuntimeException("Must pass in database name");
		}

		this.schema = (String) configMap.get(AbstractSqlQueryUtil.SCHEMA);
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty())
				&& (this.schema == null || this.schema.isEmpty())) {
			throw new RuntimeException("Must pass in schema name");
		}

		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if (this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix() + "://" + this.hostname + port + "/" + this.database;

			this.connectionUrl = appendAdditionalProps(this.connectionUrl);
		}

		return this.connectionUrl;
	}

	@Override
	public String setConnectionDetailsFromSMSS(CaseInsensitiveProperties prop) throws RuntimeException {
		if (prop == null || prop.isEmpty()) {
			throw new RuntimeException("Properties object is null or empty");
		}

		this.connectionUrl = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_URL);

		this.hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty()) && (hostname == null || hostname.isEmpty())) {
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
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty())
				&& (this.database == null || this.database.isEmpty())) {
			throw new RuntimeException("Must pass in database name");
		}

		this.schema = (String) prop.get(AbstractSqlQueryUtil.SCHEMA);
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty())
				&& (this.schema == null || this.schema.isEmpty())) {
			throw new RuntimeException("Must pass in schema name");
		}

		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if (this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix() + "://" + this.hostname + port + "/" + this.database;

			this.connectionUrl = appendAdditionalProps(this.connectionUrl);
		}

		return this.connectionUrl;
	}

	@Override
	public String buildConnectionString() {
		if (this.connectionUrl != null && !this.connectionUrl.isEmpty()) {
			return this.connectionUrl;
		}

		if (this.hostname == null || this.hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}

		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}

		if (this.database == null || this.database.isEmpty()) {
			throw new RuntimeException("Must pass in database name");
		}

		if (this.schema == null || this.schema.isEmpty()) {
			throw new RuntimeException("Must pass in schema name");
		}

		this.connectionUrl = this.dbType.getUrlPrefix() + "://" + this.hostname + port + "/" + this.database;

		this.connectionUrl = appendAdditionalProps(this.connectionUrl);

		return this.connectionUrl;
	}

	@Override
	public void enhanceConnection(Connection con) {
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			if (this.schema != null && !this.schema.isEmpty() && this.schema.matches("\\w+")) {
				stmt.execute("SET search_path TO \"" + this.schema + "\";");
			} else {
				classLogger.warn("Unable to enhance redshift connection with schema '{}'", this.schema);
			}
		} catch (SQLException e) {
			classLogger.error("Error running SET search_path for redshift connection", e);
		} finally {
			ConnectionUtils.closeAllConnections(null, stmt);
		}
	}

	@Override
	public String getDatabaseMetadataCatalogFilter() {
		return this.database;
	}

	@Override
	public String getDatabaseMetadataSchemaFilter() {
		return this.schema;
	}

	@Override
	/**
	 * Redshift takes ?key=value&key2=value2
	 */
	protected String getAdditionalPropsSeparator() {
		return "?";
	}

}
