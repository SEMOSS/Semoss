/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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

/**
 * This interface standardizes the functionality of all engines.  All engines must implement this interface 
 * so that they can be used without first recognizing what specific engine class it is.  A lot of different classes call on 
 * IEngine to refer to a specific engine, including, most notably, ProcessQueryListener.
 * @author karverma
 * @version $Revision: 1.0 $
 */
public interface IEngine extends IExplorable{
	
	/**
	 * This specifies the type of the engine and determines what API should be used when processing the engine.
	 * @author karverma
	 * @version $Revision: 1.0 $
	 */
	public enum ENGINE_TYPE {JENA, SESAME, SEMOSS_SESAME_REMOTE, RDBMS};
	
	/**
	 * This standardizes the optional method calls that engines can choose to implement.
	 * These actions are called through doMethod
	 * @author bisutton
	 *
	 */
	public enum ACTION_TYPE {ADD_STATEMENT, REMOVE_STATEMENT};
	
	/**
	 * Opens a database as defined by its properties file.  What is included in the properties file is dependent on the type of 
	 * engine that is being initiated.  This is the function that first initializes an engine with the property file at the very 
	 * least defining the data store.
	 * @param propFile contains all information regarding the data store and how the engine should be instantiated.  Dependent on 
	 * what type of engine is being instantiated.
	 */
	public void openDB(String propFile);

	/**
	 * Closes the data base associated with the engine.  This will prevent further changes from being made in the data store and 
	 * safely ends the active transactions and closes the engine.
	 */
	public void closeDB();

	/**
	 * Deletes the engine and any stored configuration
	 */
	public void deleteDB();
	
	/**
	 * Runs the passed string query against the engine.  The query passed must be in the structure that the specific engine implementation
	 * requires
	 * @param query the string version of the SELECT query to be run against the engine
	 * @return results specific to the engine type. Usually contained within ISelectWrapper
	*/
	public Object execQuery(String query);
	
	/**
	 * Runs the passed string query against the engine as an insert query. Query must be in the structure that the specific engine implementation
	 * requires
	 * @param query the insert query to be run against the engine
	 */
	public void insertData(String query);
	
	/**
	 * Gets the type of the engine.  The engine type is often used to determine what API to use while running queries against the 
	 * engine.
	 * @return the type of the engine 
	*/
	public ENGINE_TYPE getEngineType();
	
	/**
	 * Uses the passed in type to return a vector of all of the instances of that type
	 * 
	 * @param type the type that which all returned instances must be
	 * @return the Vector of Strings representing all of the instance names of that type
	*/
	public Vector<String> getEntityOfType(String type);
	
	/**
	 * Returns whether or not an engine is currently connected to the data store.  The connection becomes true when {@link #openDB(String)} 
	 * is called and the connection becomes false when {@link #closeDB()} is called.
	 * @return true if the engine is connected to its data store and false if it is not 
	*/
	public boolean isConnected();

	/**
	 * Sets the name of the engine. This may be a lot of times the same as the Repository Name
	 * @param engineName - Name of the engine that this is being set to 
	 */
	public void setEngineName(String engineName);
	
	/**
	 * Gets the engine name for this engine	
	 * @return Name of the engine
	 */
	public String getEngineName();

	/**
	 * Runs a delete query on the database
	 * @param query delete query
	 */
	public void removeData(String query);
	
	/**
	 * Commit the database. Commits the active transaction.  This operation ends the active transaction. Saves the db to a file
	 */
	public void commit();
	
	/**
	 * Performs a specific action with the given args
	 * @param actionType The type of action to perform
	 * @param args Arguments needed for that action
	 * @return Object based on the type of action
	 */
	public Object doAction(IEngine.ACTION_TYPE actionType, Object[] args);
}


