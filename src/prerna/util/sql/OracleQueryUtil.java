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

import prerna.engine.impl.CaseInsensitiveProperties;

public class OracleQueryUtil extends AnsiSqlQueryUtil {

	private String service;

	OracleQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.ORACLE);
	}

	OracleQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.ORACLE);
	}

	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if (configMap == null || configMap.isEmpty()) {
			throw new RuntimeException("Configuration map is null or empty");
		}

		this.connectionUrl = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_URL);

		this.hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty())
				&& (this.hostname == null || this.hostname.isEmpty())) {
			throw new RuntimeException("Must pass in a hostname");
		}

		this.port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}

		this.service = (String) configMap.get(AbstractSqlQueryUtil.SERVICE);
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty())
				&& (this.service == null || this.service.isEmpty())) {
			throw new RuntimeException("Must pass in a sid / service name");
		}

		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if (this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix() + ":@" + this.hostname + port + "/" + this.service;

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
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty())
				&& (this.hostname == null || this.hostname.isEmpty())) {
			throw new RuntimeException("Must pass in a hostname");
		}

		this.port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}

		this.service = (String) prop.get(AbstractSqlQueryUtil.SERVICE);
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty())
				&& (this.service == null || this.service.isEmpty())) {
			throw new RuntimeException("Must pass in a sid / service name");
		}

		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if (this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix() + ":@" + this.hostname + port + "/" + this.service;

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

		if (this.service == null || this.service.isEmpty()) {
			throw new RuntimeException("Must pass in a sid / service name");
		}

		String port = getPort();
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}

		this.connectionUrl = this.dbType.getUrlPrefix() + ":@" + this.hostname + port + "/" + this.service;

		this.connectionUrl = appendAdditionalProps(this.connectionUrl);

		return this.connectionUrl;
	}

	@Override
	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset) {
		if (offset > 0) {
			query = query.append(" OFFSET " + offset + " ROWS ");
		}
		if (limit > 0) {
			query = query.append(" FETCH NEXT " + limit + " ROWS ONLY ");
		}
		return query;
	}

	@Override
	public StringBuffer addLimitOffsetToQuery(StringBuffer query, long limit, long offset) {
		if (offset > 0) {
			query = query.append(" OFFSET " + offset + " ROWS ");
		}
		if (limit > 0) {
			query = query.append(" FETCH NEXT " + limit + " ROWS ONLY ");
		}
		return query;
	}

	/////////////////////////////////////////////////////////////////////////////////////

	@Override
	public boolean allowIfExistsModifyColumnSyntax() {
		return false;
	}

	/////////////////////////////////////////////////////////////////////////////////////

	@Override
	public String modColumnType(String tableName, String columnName, String dataType) {
		// should escape keywords
		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if (isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		return "ALTER TABLE " + tableName + " MODIFY " + columnName + " " + dataType + ";";
	}

	@Override
	public String dropIndex(String indexName, String tableName) {
		return "DROP INDEX " + indexName;
	}

	@Override
	public String alterTableDropColumns(String tableName, Collection<String> columnNames) {
		// should escape keywords
		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}

		StringBuilder alterString = new StringBuilder("ALTER TABLE " + tableName + " DROP (");
		int i = 0;
		for (String newColumn : columnNames) {
			if (i > 0) {
				alterString.append(", ");
			}

			// should escape keywords
			if (isSelectorKeyword(newColumn)) {
				newColumn = getEscapeKeyword(newColumn);
			}

			alterString.append(newColumn);

			i++;
		}
		alterString.append(")");
		return alterString.toString();
	}

	@Override
	public String tableExistsQuery(String tableName, String database, String schema) {
		return "SELECT TABLE_NAME FROM ALL_TABLES WHERE TABLE_NAME = '" + tableName.toUpperCase() + "'";
	}

	@Override
	public String getAllColumnDetails(String tableName, String database, String schema) {
		return "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '"
				+ tableName.toUpperCase() + "'";
	}

	@Override
	/**
	 * Oracle EZConnect Plus takes ?key=value&key2=value2 on the host:port/service
	 * url; the thin driver never took ; separated pairs
	 */
	protected String getAdditionalPropsSeparator() {
		return "?";
	}

}
