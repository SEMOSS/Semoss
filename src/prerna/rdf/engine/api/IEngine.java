/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.rdf.engine.api;

import java.util.Vector;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

/**
 * This interface standardizes the functionality of the engines to be used.  All current engines must implement this interface 
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
	 * Runs the passed string query against the engine and returns graph query results.  The query passed must be in the structure 
	 * of a CONSTRUCT SPARQL query.  The exact format of the results will be 
	 * dependent on the type of the engine, but regardless the results are able to be graphed.
	 * @param query the string version of the query to be run against the engine
	
	 * @return the graph query results */
	public Object execGraphQuery(String query);
	
	/**
	 * Runs the passed string query against the engine as a SELECT query.  The query passed must be in the structure of a SELECT 
	 * SPARQL query and the result format will depend on the engine type.
	 * @param query the string version of the SELECT query to be run against the engine
	
	 * @return triple query results that can be displayed as a grid */
	public Object execSelectQuery(String query);
	
	/**
	 * Runs the passed string query against the engine as an INSERT query.  The query passed must be in the structure of an INSERT 
	 * SPARQL query or an INSERT DATA SPARQL query 
	 * and there are no returned results.  The query will result in the specified triples getting added to the 
	 * data store.
	 * @param query the INSERT or INSERT DATA SPARQL query to be run against the engine
	 */
	public void execInsertQuery(String query) throws SailException, UpdateExecutionException, RepositoryException, MalformedQueryException;
	
	/**
	 * Gets the type of the engine.  The engine type is often used to determine what API to use while running queries against the 
	 * engine.
	
	 * @return the type of the engine */
	public ENGINE_TYPE getEngineType();
	
	/**
	 * Processes a SELECT query just like {@link #execSelectQuery(String)} but then parses the results to get only their 
	 * instance names.  These instance names are then returned as the Vector of Strings.
	 * @param sparqlQuery the SELECT SPARQL query to be run against the engine
	
	 * @return the Vector of Strings representing the instance names of all of the query results */
	public Vector<String> getEntityOfType(String sparqlQuery);
	
	/**
	 * Returns whether or not an engine is currently connected to the data store.  The connection becomes true when {@link #openDB(String)} 
	 * is called and the connection becomes false when {@link #closeDB()} is called.
	
	 * @return true if the engine is connected to its data store and false if it is not */
	public boolean isConnected();

	/**
	 * Processes the passed ASK SPARQL query against the engine.  The query must be in the structure of an ASK query and the 
	 * result will be a boolean indicating whether or not the data store connected to the engine has triples matching the 
	 * pattern of the ASK query.
	 * @param query the ASK SPARQL query to be run against the engine
	
	 * @return true if the data store connected to the engine contains triples that match the pattern of the query and false 
	 * if it does not. */
	public Boolean execAskQuery(String query);

	/**
	 * Sets the name of the engine. This may be a lot of times the same as the Repository Name
	 * @param engineName - Name of the engine that this is being set to 
	 */
	public void setEngineName(String engineName);
	
	/**
	 * Gets the engine name for this engine	
	 * @return Name of the engine it is being set to
	 */
	public String getEngineName();

	/**
	 * Creates a statement with the given subject, predicate and object
	 * @param subject - Subject for the triple
	 * @param predicate - Predicate for the triple
	 * @param object - Object for the triple
	 * @param concept - Specifies if it is a concept
	 */
	public void addStatement(String subject, String predicate, Object object, boolean concept);

	/**
	 * Removes a statement with the given subject, predicate and object
	 * @param subject - Subject for the triple
	 * @param predicate - Predicate for the triple
	 * @param object - Object for the triple
	 * @param concept - Specifies if it is a concept
	 */
	public void removeStatement(String subject, String predicate, Object object, boolean concept);
	
	/**
	 * Commit the database. Commits the active transaction.  This operation ends the active transaction.
	 */
	public void commit();
	
	/**
	 * Writes the database back 
	 * with updated properties if necessary
	 */
	public void saveConfiguration();
	
	/**
	 * Adds a new property to the engine
	 * @param name String - The name of the property.
	 * @param value String - The value of the property.
	 */
	public void addConfiguration(String name, String value);
	
	
	// sets the dreamer properties file
	public void setDreamer(String dreamer);
	
	// sets the ontology file
	public void setOntology(String ontologyFile);
	
	// sets the owl
	public void setOWL(String owl);

	// sets the rdbms map, if it exists
	public void setMap(String map);
	
	// get property
	public String getProperty(String key);
	
	// gets the param values for a parameter
	public Vector<String> getParamValues(String label, String type, String insightId, String query);

	// gets the param values for a parameter
	// overloaded query - I just need to make the query optional
	public Vector<String> getParamValues(String label, String type, String insightId);

	// gets the OWL engine
	// this needs to change later
	public RepositoryConnection getOWL();	

	// gets the insight definitions
	public String getOWLDefinition();
	
}


