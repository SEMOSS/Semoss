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

import java.util.Vector;

import prerna.engine.impl.owl.OWLEngineFactory;
import prerna.engine.impl.rdbms.AuditDatabase;

/**
 * This interface standardizes the functionality of all engines.  All engines must implement this interface 
 * so that they can be used without first recognizing what specific engine class it is.  A lot of different classes call on 
 * IEngine to refer to a specific engine, including, most notably, ProcessQueryListener.
 */
public interface IDatabaseEngine extends IExplorable, IEngine {
	
	enum DATABASE_TYPE {
		@Deprecated
		APP, // this is now just an IProject
		JENA, 
		SESAME, 
		RDBMS, 
		@Deprecated
		IMPALA, 
		R, 
		TINKER, 
		NEO4J, 
//		NEO4J_EMBEDDED, 
		DATASTAX_GRAPH, 
		JANUS_GRAPH, 
		JMES_API, 
		JSON, 
		JSON2, 
		WEB, 
		REMOTE_SEMOSS, 
		// THIS IS THE OLD ONE THAT ISN'T USED ANYMORE!!!
		SEMOSS_SESAME_REMOTE};
	
	enum ACTION_TYPE {
		ADD_STATEMENT, // this is for rdf
		REMOVE_STATEMENT, // this is for rdf
		BULK_INSERT,  // this is for rdbms
		VERTEX_UPSERT, // this is for tinker
		EDGE_UPSERT // this is for tinker
	}; 
	
	/**
	 * Runs the passed string query against the engine.  The query passed must be in the structure that the specific engine implementation
	 * requires
	 * @param query the string version of the SELECT query to be run against the engine
	 * @return results specific to the engine type. Usually contained within ISelectWrapper
	*/
	Object execQuery(String query) throws Exception;
	
	/**
	 * Runs the passed string query against the engine as an insert query. Query must be in the structure that the specific engine implementation
	 * requires
	 * @param query the insert query to be run against the engine
	 * @throws  
	 */
	void insertData(String query) throws Exception;
	
	/**
	 * Runs a delete query on the database
	 * @param query delete query
	 */
	void removeData(String query) throws Exception;
	
	/**
	 * Commit the database. Commits the active transaction.  This operation ends the active transaction. Saves the db to a file
	 */
	void commit();
	
	/**
	 * Gets the type of the database.  The database type is often used to determine what API to use while running queries against the 
	 * database.
	 * @return the type of the database 
	*/
	DATABASE_TYPE getDatabaseType();
	
	/**
	 * Uses the passed in type to return a vector of all of the instances of that type
	 * 
	 * @param type the type that which all returned instances must be
	 * @return the Vector of Strings representing all of the instance names of that type
	*/
	@Deprecated
	Vector<Object> getEntityOfType(String type);
	
	/**
	 * Returns whether or not an engine is currently connected to the data store.  The connection becomes true when {@link #open(String)} 
	 * is called and the connection becomes false when {@link #close()} is called.
	 * @return true if the engine is connected to its data store and false if it is not 
	*/
	boolean isConnected();
	
	/**
	 * Performs a specific action with the given args
	 * @param actionType The type of action to perform
	 * @param args Arguments needed for that action
	 * @return Object based on the type of action
	 */
	Object doAction(IDatabaseEngine.ACTION_TYPE actionType, Object[] args);
	
	/**
	 * Gets the UDF - user defined functions in this data catalog
	 * @return
	 */
	String [] getUDF();
	
	/**
	 * Get the meta helper which does all the IExplorable operations
	 * @return
	 */
	OWLEngineFactory getOWLEngineFactory();

	/**
	 * Generate an audit database
	 */
	AuditDatabase generateAudit();
}


