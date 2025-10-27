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
 * Interface for Relational Database Management System (RDBMS) engines in the SEMOSS platform.
 * 
 * <p>This interface extends {@link IDatabaseEngine} to provide specialized functionality
 * for relational databases that use SQL as their query language. It provides access to
 * JDBC connections, prepared statements, metadata, and other RDBMS-specific features.</p>
 * 
 * <p>Key capabilities include:</p>
 * <ul>
 *   <li><strong>Connection Management:</strong> JDBC connection handling with pooling support</li>
 *   <li><strong>Prepared Statements:</strong> Efficient query execution with parameter binding</li>
 *   <li><strong>Bulk Operations:</strong> Optimized bulk insert operations</li>
 *   <li><strong>Database Metadata:</strong> Schema information and database characteristics</li>
 *   <li><strong>Query Utilities:</strong> Database-specific SQL query builders and utilities</li>
 * </ul>
 * 
 * <p>Supported RDBMS types include major databases such as PostgreSQL, MySQL, SQLite,
 * Oracle, SQL Server, and others as defined in {@link RdbmsTypeEnum}.</p>
 * 
 * @see {@link IDatabaseEngine} for base database engine functionality
 * @see {@link RdbmsTypeEnum} for supported database types
 * @see {@link AbstractSqlQueryUtil} for SQL query utilities
 * @see {@link HikariDataSource} for connection pooling
 * @author SEMOSS
 */
public interface IRDBMSEngine extends IDatabaseEngine {

	/**
	 * Gets an active JDBC connection to the database.
	 * 
	 * <p>This method returns a connection from the connection pool if pooling
	 * is enabled, or the single connection if using a direct connection approach.
	 * The connection should be properly managed and closed when no longer needed.</p>
	 * 
	 * @return Active JDBC connection to the database
	 * @throws SQLException If connection cannot be established or retrieved
	 */
	java.sql.Connection getConnection() throws SQLException;
	
	/**
	 * Creates and returns a new JDBC connection to the database.
	 * 
	 * <p>This method establishes a fresh connection to the database using the
	 * configured connection parameters. Unlike {@link #getConnection()}, this
	 * method always creates a new connection rather than reusing from a pool.</p>
	 * 
	 * @return New JDBC connection to the database
	 * @throws SQLException If connection cannot be established
	 */
	java.sql.Connection makeConnection() throws SQLException;
	
	/**
	 * Creates a prepared statement optimized for bulk insert operations.
	 * 
	 * <p>This method generates a prepared statement specifically designed for
	 * efficient bulk data insertion. The arguments array should contain the
	 * table name as the first element, followed by column names for the insert.</p>
	 * 
	 * @param args Object array where the first element is the table name and
	 *             subsequent elements are column names for the insert operation
	 * @return PreparedStatement configured for bulk insert operations
	 * @throws SQLException If statement creation fails
	 */
	java.sql.PreparedStatement bulkInsertPreparedStatement(Object[] args) throws SQLException;
	
	/**
	 * Creates a prepared statement for the specified SQL query.
	 * 
	 * <p>This method creates a prepared statement that can be executed multiple
	 * times with different parameter values, providing better performance and
	 * security through parameter binding.</p>
	 * 
	 * @param sql The SQL query string to prepare
	 * @return PreparedStatement ready for parameter binding and execution
	 * @throws SQLException If statement preparation fails
	 */
	java.sql.PreparedStatement getPreparedStatement(String sql) throws SQLException;
	
	/**
	 * Gets database metadata information for the current connection.
	 * 
	 * <p>This method provides access to comprehensive database metadata including
	 * table information, column details, supported features, and database
	 * characteristics that can be used for introspection and dynamic operations.</p>
	 * 
	 * @return DatabaseMetaData object containing database metadata
	 */
	DatabaseMetaData getConnectionMetadata();
	
	/**
	 * Gets the specific RDBMS type for this database engine.
	 * 
	 * <p>This method identifies the specific relational database management
	 * system being used, which enables database-specific optimizations and
	 * feature support.</p>
	 * 
	 * @return {@link RdbmsTypeEnum} indicating the database type
	 * @see {@link RdbmsTypeEnum} for supported database types
	 */
	RdbmsTypeEnum getDbType();
	
	/**
	 * Gets the SQL query utility for database-specific query operations.
	 * 
	 * <p>This method returns a utility class that provides database-specific
	 * SQL query building, syntax handling, and optimization capabilities
	 * tailored to the specific RDBMS type.</p>
	 * 
	 * @return {@link AbstractSqlQueryUtil} for SQL query operations
	 * @see {@link AbstractSqlQueryUtil} for SQL utility functions
	 */
	AbstractSqlQueryUtil getQueryUtil();
	
	/**
	 * Gets the schema name if one is configured for this database connection.
	 * 
	 * <p>This method returns the database schema that is used as the default
	 * namespace for tables and other database objects. Not all databases
	 * use schemas, so this may return null.</p>
	 * 
	 * @return The schema name, or null if no schema is configured
	 */
	public String getSchema();
	
	/**
	 * Gets the database name if one is configured for this connection.
	 * 
	 * <p>This method returns the specific database name within the RDBMS
	 * instance that this engine is connected to. This is particularly
	 * relevant for multi-database systems.</p>
	 * 
	 * @return The database name, or null if not applicable
	 */
	public String getDatabase();
	
	/**
	 * Gets the JDBC connection URL used to connect to this database.
	 * 
	 * <p>This method returns the complete JDBC URL that specifies the
	 * database connection parameters including host, port, database name,
	 * and other connection properties.</p>
	 * 
	 * @return The JDBC connection URL string
	 */
	String getConnectionUrl();

	/**
	 * Gets the HikariCP data source used for connection pooling.
	 * 
	 * <p>This method returns the HikariCP data source that manages the
	 * connection pool for this database engine. Returns null if connection
	 * pooling is not enabled.</p>
	 * 
	 * @return {@link HikariDataSource} instance, or null if pooling is disabled
	 * @see {@link HikariDataSource} for connection pooling functionality
	 */
	HikariDataSource getDataSource();
	
	/**
	 * Indicates whether this engine uses connection pooling or single connections.
	 * 
	 * <p>This method returns true if the engine is configured to use a connection
	 * pool for managing multiple concurrent connections, or false if it uses a
	 * single direct connection approach.</p>
	 * 
	 * @return true if connection pooling is enabled, false for single connections
	 */
	boolean isConnectionPooling();
}
