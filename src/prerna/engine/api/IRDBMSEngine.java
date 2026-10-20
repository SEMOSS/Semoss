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
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

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
	 * Runs SQL statements in order inside one transaction on a connection of their
	 * own, and commits once after the last one. A failing statement rolls back the
	 * statements before it, and the statements after it do not run. The engine's
	 * shared connection is never used, so other callers can neither commit nor roll
	 * back part of the script.
	 *
	 * <p>
	 * Statements that control the transaction themselves are for the caller to
	 * reject first. A statement the database will not run inside a transaction
	 * fails, which rolls the script back. A database that commits data definition
	 * as soon as it runs, as its driver reports and as H2, MySQL and Oracle do, can
	 * commit other statements on their own too and would keep the statements before
	 * such a commit, so on such a database a script of more than one statement with
	 * a statement that does not only read or change rows, such as CREATE TABLE,
	 * LOCK TABLES or CALL, is refused before anything runs; a script of one
	 * statement runs. A statement's final semicolon is left off when it is sent,
	 * since some drivers reject it.
	 * </p>
	 *
	 * <p>
	 * Up to {@code maxRows} rows are kept from each statement that returns rows,
	 * with each value converted as an engine query converts it, including the rows
	 * of a statement that was then rolled back, since they show what the script
	 * saw.
	 * </p>
	 *
	 * @param statements     the statements, in order, each as one SQL statement
	 * @param timeoutSeconds the time limit for all the statements together, or 0
	 *                       for none; it is not applied to the commit
	 * @param maxRows        the most rows to keep from each statement that returns
	 *                       rows; 0 keeps none, and a negative number keeps them
	 *                       all
	 * @return what happened to each statement, after the commit succeeded
	 * @throws SqlTransactionException         when the transaction did not commit:
	 *                                         a statement failed, the time limit
	 *                                         was reached, or the commit failed. It
	 *                                         carries what happened to each
	 *                                         statement.
	 * @throws SQLFeatureNotSupportedException when the database commits data
	 *                                         definition as soon as it runs and a
	 *                                         script of more than one statement has
	 *                                         a statement that does not only read
	 *                                         or change rows, so no statement ran
	 * @throws SQLException                    when no connection could be opened,
	 *                                         so no statement ran
	 */
	default SqlTransactionResult executeInTransaction(List<String> statements, int timeoutSeconds, int maxRows)
			throws SQLException {
		throw new UnsupportedOperationException(
				"Database " + getEngineId() + " does not run SQL statements in one transaction");
	}

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
