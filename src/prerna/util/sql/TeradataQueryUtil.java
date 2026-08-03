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
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.TeradataSqlInterpreter;

public class TeradataQueryUtil extends AnsiSqlQueryUtil {

	TeradataQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.TERADATA);
	}

	TeradataQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.TERADATA);
	}

	@Override
	public IQueryInterpreter getInterpreter(IDatabaseEngine engine) {
		return new TeradataSqlInterpreter(engine);
	}

	@Override
	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new TeradataSqlInterpreter(frame);
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

		this.database = (String) configMap.get(AbstractSqlQueryUtil.DATABASE);
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty())
				&& (this.database == null || this.database.isEmpty())) {
			throw new RuntimeException("Must pass in database name");
		}

		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if (this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix() + "://" + this.hostname + "/DATABASE=" + this.database;

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

		this.database = (String) prop.get(AbstractSqlQueryUtil.DATABASE);
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty())
				&& (this.database == null || this.database.isEmpty())) {
			throw new RuntimeException("Must pass in database name");
		}

		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if (this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix() + "://" + this.hostname + "/DATABASE=" + this.database;

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

		if (this.database == null || this.database.isEmpty()) {
			throw new RuntimeException("Must pass in database name");
		}

		this.connectionUrl = this.dbType.getUrlPrefix() + "://" + this.hostname + "/DATABASE=" + this.database;

		this.connectionUrl = appendAdditionalProps(this.connectionUrl);

		return this.connectionUrl;
	}

	@Override
	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset) {

		if (limit > 0) {
			String strquery = query.toString();
			strquery = strquery.replaceFirst("SELECT", "SELECT TOP " + limit + " ");
			query = new StringBuilder();
			query.append(strquery);
		}

		// TODO there is no offset for now
//		if(offset > 0) {
//			query = query.append(" OFFSET "+offset);
//		}
		return query;
	}

	@Override
	public StringBuffer addLimitOffsetToQuery(StringBuffer query, long limit, long offset) {

		if (limit > 0) {
			String strquery = query.toString();
			strquery = strquery.replaceFirst("SELECT", "SELECT TOP " + limit + " ");
			query = new StringBuffer();
			query.append(strquery);
		}

		// TODO there is no offset for now
//		if(offset > 0) {
//			query = query.append(" OFFSET "+offset);
//		}
		return query;
	}

	// this creates the temp table to select top from the entire list of distinct
	// selectors.
	// this is only used with distinct
	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset, String tempTable) {

		if (limit > 0) {
			query = query.insert(0, "SELECT TOP " + limit + " * from (");
			query = query.append(") as " + tempTable);
		}

		// TODO there is no offset for now
//		if(offset > 0) {
//			query = query.append(" OFFSET "+offset);
//		}
		return query;
	}

	@Override
	/**
	 * Teradata takes a comma separated list, as in
	 * jdbc:teradata://host/DATABASE=db,DBS_PORT=1025
	 */
	protected String getAdditionalPropsSeparator() {
		return ",";
	}

}