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

import java.io.Closeable;
import java.io.IOException;

/**
 * Base interface for query execution wrappers in the SEMOSS platform.
 * 
 * <p>This interface provides a standardized way to wrap and execute queries
 * against various database engines. It extends {@link Closeable} to ensure
 * proper resource management and cleanup after query execution. The wrapper
 * pattern allows for consistent query handling across different database
 * types while abstracting engine-specific implementation details.</p>
 * 
 * <p>Key features of engine wrappers:</p>
 * <ul>
 *   <li><strong>Query Management:</strong> Store and manage query strings</li>
 *   <li><strong>Engine Association:</strong> Link to specific database engines</li>
 *   <li><strong>Execution Control:</strong> Trigger query execution on demand</li>
 *   <li><strong>Resource Management:</strong> Automatic cleanup via Closeable interface</li>
 * </ul>
 * 
 * <p>Typical usage pattern:</p>
 * <pre>
 * try (IEngineWrapper wrapper = engine.getWrapper()) {
 *     wrapper.setQuery("SELECT * FROM table");
 *     wrapper.execute();
 *     // Process results
 * } // Automatic cleanup
 * </pre>
 * 
 * @see {@link IDatabaseEngine} for database engine operations
 * @see {@link ISelectWrapper} for SELECT query results
 * @see {@link IConstructWrapper} for CONSTRUCT query results
 * @author SEMOSS
 */
public interface IEngineWrapper extends Closeable {

	/**
	 * Executes the configured query against the associated database engine.
	 * 
	 * <p>This method triggers the actual execution of the query that has been
	 * set via {@link #setQuery(String)}. The specific behavior depends on the
	 * wrapper implementation and the type of query being executed.</p>
	 * 
	 * @throws Exception If query execution fails due to syntax errors, connection
	 *                   issues, or other database-related problems
	 */
	void execute() throws Exception;

	/**
	 * Sets the query string to be executed by this wrapper.
	 * 
	 * <p>This method configures the wrapper with the query to execute. The
	 * query format depends on the underlying database engine (SQL for RDBMS,
	 * SPARQL for RDF, Cypher for Graph databases, etc.).</p>
	 * 
	 * @param query The query string in the appropriate format for the target engine
	 */
	void setQuery(String query);
	
	/**
	 * Gets the currently configured query string.
	 * 
	 * @return The query string that will be or has been executed
	 */
	String getQuery();
	
	/**
	 * Associates this wrapper with a specific database engine.
	 * 
	 * <p>This method links the wrapper to the database engine that will
	 * execute the query. The engine provides the connection, query execution
	 * capabilities, and result processing functionality.</p>
	 * 
	 * @param engine The {@link IDatabaseEngine} to use for query execution
	 * @see {@link IDatabaseEngine} for database engine interface
	 */
	void setEngine(IDatabaseEngine engine);

	/**
	 * Gets the database engine associated with this wrapper.
	 * 
	 * @return The {@link IDatabaseEngine} used for query execution
	 * @see {@link IDatabaseEngine} for database engine interface
	 */
	public IDatabaseEngine getEngine();

//	/**
//	 * 
//	 * @param val
//	 */
//	void setTimeZone(TimeZone val);
//
//	/**
//	 * 
//	 * @return
//	 */
//	TimeZone getTimeZone();
	
}
