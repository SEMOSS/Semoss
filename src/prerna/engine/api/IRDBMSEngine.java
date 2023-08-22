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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import com.zaxxer.hikari.HikariDataSource;

import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

/**
 * An Abstract Engine that sets up the base constructs needed to create an
 * engine.
 */
public interface IRDBMSEngine extends IDatabaseEngine {

	/**
	 * Get the connection
	 * @return
	 * @throws SQLException
	 */
	java.sql.Connection getConnection() throws SQLException;
	
	/**
	 * Make the connection and return it
	 * @return
	 * @throws SQLException
	 */
	java.sql.Connection makeConnection() throws SQLException;
	
	/**
	 * This is intended to be executed via doAction
	 * @param args			Object[] where the first index is the table name
	 * 						and every other entry are the column names
	 * @return				PreparedStatement to perform a bulk insert
	 * @throws SQLException 
	 */
	java.sql.PreparedStatement bulkInsertPreparedStatement(Object[] args) throws SQLException;
	
	/**
	 * This is to get a prepared statement based on the input query
	 * @param query
	 * @return
	 * @throws SQLException 
	 */
	java.sql.PreparedStatement getPreparedStatement(String sql) throws SQLException;
	
	/**
	 * Return the engine metadata
	 * @return
	 */
	DatabaseMetaData getConnectionMetadata();
	
	/**
	 * Get the RDBMS Type Enum
	 * @return
	 */
	RdbmsTypeEnum getDbType();
	
	/**
	 * Get the query util
	 * @return
	 */
	AbstractSqlQueryUtil getQueryUtil();
	
	/**
	 * Get the schema if its defined
	 * @return
	 */
	public String getSchema();
	
	/**
	 * Get the database if its defined
	 * @return
	 */
	public String getDatabase();
	
	/**
	 * Get the connection url
	 * @return
	 */
	String getConnectionUrl();

	/**
	 * Get the data source
	 * @return
	 */
	HikariDataSource getDataSource();
	
	/**
	 * Get if the database is using connection pooling or a single connection
	 * @return
	 */
	boolean isConnectionPooling();
}
