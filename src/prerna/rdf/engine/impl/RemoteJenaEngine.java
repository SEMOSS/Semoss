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
package prerna.rdf.engine.impl;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;

import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;

/**
 * References the RDF source from a remote engine and uses the Jena API to query a database stored in an RDF file (.jnl file).
 */
public class RemoteJenaEngine extends AbstractEngine implements IEngine {
	
	Model jenaModel = null;
	static final Logger logger = LogManager.getLogger(RemoteJenaEngine.class.getName());
	String propFile = null;
	String serviceURI = null;
	boolean connected = false;

	/**
	 * Closes the data base associated with the engine.  This will prevent further changes from being made in the data store and 
	 * safely ends the active transactions and closes the engine.
	 */
	@Override
	public void closeDB() {
		jenaModel.close();
		logger.info("Closing the database to the file " + propFile);		
	}

	/**
	 * Runs the passed string query against the engine and returns graph query results.  The query passed must be in the structure 
	 * of a CONSTRUCT SPARQL query.  The exact format of the results will be 
	 * dependent on the type of the engine, but regardless the results are able to be graphed.
	 * @param query the string version of the query to be run against the engine
	
	 * @return the graph query results */
	@Override
	public Object execGraphQuery(String query) {
	
		com.hp.hpl.jena.query.Query queryVar = QueryFactory.create(query) ;
		
		QueryEngineHTTP qexec = QueryExecutionFactory.createServiceRequest(this.serviceURI, queryVar);
		String params = prop.getProperty(Constants.URL_PARAM);
		StringTokenizer paramTokens = new StringTokenizer(params, ";");
		while(paramTokens.hasMoreTokens())
		{
			String token = paramTokens.nextToken();
			qexec.addParam(token, prop.getProperty(token));			
		}
		Model resultModel = qexec.execConstruct() ;
		logger.info("Executing the RDF File Graph Query " + query);
		return resultModel;
		//qexec.close() ;
	}

	/**
	 * Runs the passed string query against the engine as a SELECT query.  The query passed must be in the structure of a SELECT 
	 * SPARQL query and the result format will depend on the engine type.
	 * @param query the string version of the SELECT query to be run against the engine
	
	 * @return triple query results that can be displayed as a grid */
	@Override
	public Object execSelectQuery(String query) {
		
		com.hp.hpl.jena.query.Query q2 = QueryFactory.create(query); 
		com.hp.hpl.jena.query.Query queryVar = QueryFactory.create(query) ;
		
		QueryEngineHTTP qexec = QueryExecutionFactory.createServiceRequest(this.serviceURI, queryVar);
		String params = prop.getProperty(Constants.URL_PARAM);
		StringTokenizer paramTokens = new StringTokenizer(params, ";");
		while(paramTokens.hasMoreTokens())
		{
			String token = paramTokens.nextToken();
			qexec.addParam(token, prop.getProperty(token));			
		}
		com.hp.hpl.jena.query.ResultSet rs = qexec.execSelect();
		return rs;
	}

	/**
	 * Runs the passed string query against the engine as an INSERT query.  The query passed must be in the structure of an INSERT 
	 * SPARQL query or an INSERT DATA SPARQL query 
	 * and there are no returned results.  The query will result in the specified triples getting added to the 
	 * data store.
	 * @param query the INSERT or INSERT DATA SPARQL query to be run against the engine
	 */
	@Override
	public void execInsertQuery(String query) {
		// TODO Auto-generated method stub		
	}

	/**
	 * Gets the type of the engine.  The engine type is often used to determine what API to use while running queries against the 
	 * engine.
	
	 * @return the type of the engine */
	@Override
	public ENGINE_TYPE getEngineType() {
		return IEngine.ENGINE_TYPE.JENA;
	}

	/**
	 * Processes a SELECT query just like {@link #execSelectQuery(String)} but then parses the results to get only their 
	 * instance names.  These instance names are then returned as the Vector of Strings.
	 * @param sparqlQuery the SELECT SPARQL query to be run against the engine
	
	 * @return the Vector of Strings representing the instance names of all of the query results */
	@Override
	public Vector<String> getEntityOfType(String sparqlQuery) {
		// run the query 
		// convert to string
		Vector <String> retString = new Vector<String>();
		ResultSet rs = (ResultSet)execSelectQuery(sparqlQuery);
		
		// gets only the first variable
		Iterator varIterator = rs.getResultVars().iterator();
		String varName = (String)varIterator.next();
		while(rs.hasNext())
		{
			QuerySolution row = rs.next();
			retString.addElement(row.get(varName)+"");
		}
		return retString;
	}

	/**
	 * Returns whether or not an engine is currently connected to the data store.  The connection becomes true when {@link #openDB(String)} 
	 * is called and the connection becomes false when {@link #closeDB()} is called.	
	 * @return true if the engine is connected to its data store and false if it is not */
	@Override
	public boolean isConnected() {
		return connected;
	}

	/**
	 * Opens a database as defined by its properties file.  What is included in the properties file is dependent on the type of 
	 * engine that is being initiated.  This is the function that first initializes an engine with the property file at the very 
	 * least defining the data store.
	 * @param propFile contains all information regarding the data store and how the engine should be instantiated.  Dependent on 
	 * what type of engine is being instantiated.
	 */
	@Override
	public void openDB(String propFile) {
		FileInputStream fileIn = null;
		try {
			prop = new Properties();
			fileIn = new FileInputStream(propFile);
			prop.load(fileIn);
			this.serviceURI = prop.getProperty(Constants.SPARQL_QUERY_ENDPOINT);
			this.connected = true;
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try{
				if(fileIn!=null)
					fileIn.close();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Processes the passed ASK SPARQL query against the engine.  The query must be in the structure of an ASK query and the 
	 * result will be a boolean indicating whether or not the data store connected to the engine has triples matching the 
	 * pattern of the ASK query.
	 * @param query the ASK SPARQL query to be run against the engine
	
	 * @return true if the data store connected to the engine contains triples that match the pattern of the query and false 
	 * if it does not. */
	@Override
	public Boolean execAskQuery(String query) {
		// TODO: Don't return null
		return null;
	}
	
	

}
