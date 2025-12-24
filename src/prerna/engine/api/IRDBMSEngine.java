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
package prerna.engine.api;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import com.zaxxer.hikari.HikariDataSource;

import prerna.logging.IgnoreEngineLogging;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

/**
 * An Abstract Engine that sets up the base constructs needed to create an
 * engine.
 */
public interface IRDBMSEngine extends IDatabaseEngine {

	String STATEMENT_OBJECT = "STATEMENT_OBJECT";
	String RESULTSET_OBJECT = "RESULTSET_OBJECT";
	String CONNECTION_OBJECT = "CONNECTION_OBJECT";
	String ENGINE_CONNECTION_OBJECT = "ENGINE_CONNECTION_OBJECT";
	String DATASOURCE_POOLING_OBJECT = "DATASOURCE_POOLING_OBJECT";

	/**
	 * Get the connection
	 * 
	 * @return
	 * @throws SQLException
	 */
	java.sql.Connection getConnection() throws SQLException;

	/**
	 * Deprecated - switch to {@link #getConnection()}
	 * 
	 * @return
	 * @throws SQLException
	 */
	@IgnoreEngineLogging
	@Deprecated
	java.sql.Connection makeConnection() throws SQLException;


	/**
	 * This is intended to be executed via doAction
	 * 
	 * @param args Object[] where the first index is the table name and every other
	 *             entry are the column names
	 * @return PreparedStatement to perform a bulk insert
	 * @throws SQLException
	 */
	java.sql.PreparedStatement bulkInsertPreparedStatement(Object[] args) throws SQLException;

	/**
	 * This is to get a prepared statement based on the input query
	 * 
	 * @param query
	 * @return
	 * @throws SQLException
	 */
	java.sql.PreparedStatement getPreparedStatement(String sql) throws SQLException;

	/**
	 * Return the engine metadata
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	DatabaseMetaData getConnectionMetadata();

	/**
	 * Get the RDBMS Type Enum
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	RdbmsTypeEnum getDbType();

	/**
	 * Get the query util
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	AbstractSqlQueryUtil getQueryUtil();

	/**
	 * 
	 * @param queryUtil
	 */
	@IgnoreEngineLogging
	void setQueryUtil(AbstractSqlQueryUtil queryUtil);

	/**
	 * Get the schema if its defined
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	public String getSchema();

	/**
	 * Get the database if its defined
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	public String getDatabase();

	/**
	 * Get the connection url
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	String getConnectionUrl();

	/**
	 * Get the data source
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	HikariDataSource getDataSource();

	/**
	 * 
	 */
	@IgnoreEngineLogging
	void closeDataSource();

	/**
	 * Get if the database is using connection pooling or a single connection
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	boolean isConnectionPooling();

	/**
	 * 
	 * @param conn
	 */
	@IgnoreEngineLogging
	void setConnection(Connection conn);

	/**
	 * 
	 * @param autoCommit
	 */
	@IgnoreEngineLogging
	void setAutoCommit(boolean autoCommit);

	/**
	 * 
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	@IgnoreEngineLogging
	Clob createClob(Connection connection) throws SQLException;

}
