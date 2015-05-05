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
package prerna.engine.impl.rdf;

import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLConnection;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.sail.SailException;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.sail.BigdataSail;

/**
 * References the RDF source from a remote engine and uses the Sesame API to query a database stored in an RDF file (.jnl file).
 */
public class RemoteSparqlEngine extends AbstractEngine implements IEngine {

	private static final Logger logger = LogManager.getLogger(RemoteSparqlEngine.class.getName());
	BigdataSail bdSail = null;
	Properties rdfMap = null;
	RepositoryConnection rc = null;
	ValueFactory vf = null;
	boolean connected = false;

	
	/**
	 * Opens a database as defined by its properties file.  What is included in the properties file is dependent on the type of 
	 * engine that is being initiated.  This is the function that first initializes an engine with the property file at the very 
	 * least defining the data store.
	 * @param propFile contains all information regarding the data store and how the engine should be instantiated.  Dependent on 
	 * what type of engine is being instantiated.
	 */
	@Override
	public void openDB(String propFile)
	{
		try
		{
			
			super.openDB(propFile);
			
			String sparqlQEndpoint = prop.getProperty(Constants.SPARQL_QUERY_ENDPOINT);
			String sparqlUEndpoint = prop.getProperty(Constants.SPARQL_UPDATE_ENDPOINT);

			//com.bigdata.rdf.sail.webapp.client.RemoteRepository repo = new com.bigdata.rdf.sail.webapp.client.RemoteRepository(sparqlQEndpoint, null, null);
			//repo.
			SPARQLRepository repo = new SPARQLRepository(sparqlQEndpoint);
			Hashtable <String, String> myMap = new Hashtable<String,String>();
			myMap.put("apikey","d0184dd3-fb6b-4228-9302-1c6e62b01465");
			//repo.setAdditionalHttpHeaders(myMap);
			rc = new SPARQLConnection(repo);//, sparqlQEndpoint, sparqlUEndpoint);
			//rc = new SPARQLConnection(repo, sparqlQEndpoint, sparqlUEndpoint);
	
			// new ForwardChainingRDFSInferencer(bdSail);
			//InferenceEngine ie = bdSail.getInferenceEngine();
	
			// logger.info("ie forward chaining " + ie);
			// need to convert to constants
			String dbcmFile = prop.getProperty(Constants.DBCM_Prop);
			String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			
			dbcmFile = workingDir + "/" + dbcmFile;
			rdfMap = DIHelper.getInstance().getCoreProp();
			
			this.connected = true;
			// return g;
		}catch(RuntimeException ignored)
		{
			ignored.printStackTrace();
		}
	}
	
	/**
	 * Closes the data base associated with the engine.  This will prevent further changes from being made in the data store and 
	 * safely ends the active transactions and closes the engine.
	 */
	@Override
	public void closeDB() {
		// ng.stopTransaction(Conclusion.SUCCESS);
		try {
			bdSail.shutDown();
			connected = false;
		} catch (SailException e) {
			e.printStackTrace();
		}
		// ng.shutdown();
	}

	/**
	 * Runs the passed string query against the engine as a SELECT query.  The query passed must be in the structure of a SELECT 
	 * SPARQL query and the result format will depend on the engine type.
	 * @param query the string version of the SELECT query to be run against the engine
	
	 * @return triple query results that can be displayed as a grid */
	@Override
	public Object execQuery(String query) {

		try {
			Query fullQuery = rc.prepareQuery(QueryLanguage.SPARQL, query);
			logger.debug("\nSPARQL: " + query);
			fullQuery.setIncludeInferred(true /* includeInferred */);
			if(fullQuery instanceof TupleQuery){
				TupleQueryResult sparqlResults = ((TupleQuery) fullQuery).evaluate();
				return sparqlResults;
			}
			else if (fullQuery instanceof GraphQuery){
				GraphQueryResult res = ((GraphQuery) fullQuery).evaluate();
				return res;
			}
			else if (fullQuery instanceof BooleanQuery){
				Boolean bool = ((BooleanQuery) fullQuery).evaluate();
				return bool;
			}
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Runs the passed string query against the engine as an INSERT query.  The query passed must be in the structure of an INSERT 
	 * SPARQL query or an INSERT DATA SPARQL query 
	 * and there are no returned results.  The query will result in the specified triples getting added to the 
	 * data store.
	 * @param query the INSERT or INSERT DATA SPARQL query to be run against the engine
	 */
	@Override
	public void insertData(String query){

		Update up;
		try {
			up = rc.prepareUpdate(QueryLanguage.SPARQL, query);
			logger.debug("\nSPARQL: " + query);
			//tq.setIncludeInferred(true /* includeInferred */);
			//tq.evaluate();
			rc.setAutoCommit(false);
			rc.begin();
			up.execute();
			//rc.commit();
	        InferenceEngine ie = ((BigdataSail)bdSail).getInferenceEngine();
	        ie.computeClosure(null);
			rc.commit();
		} catch (MalformedQueryException e) {
			e.printStackTrace();
		} catch (RepositoryException e){
			e.printStackTrace();
		} catch (UpdateExecutionException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Gets the type of the engine.  The engine type is often used to determine what API to use while running queries agains the 
	 * engine.
	
	 * @return the type of the engine */
	public ENGINE_TYPE getEngineType()
	{
		return IEngine.ENGINE_TYPE.SESAME;
	}

	/**
	 * Processes a SELECT query just like {@link #execSelectQuery(String)} but gets the results in the exact format that the database stores them.
	 * This is important for things like param values so that we can take the returned value and fill the main query without needing modification
	 * @param sparqlQuery the SELECT SPARQL query to be run against the engine
	 * @return the Vector of Strings representing the full uris of all of the query results */
	public Vector<String> getCleanSelect(String sparqlQuery)
	{
		try {
			TupleQuery tq = rc.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);
			logger.debug("\nSPARQL: " + sparqlQuery);
			tq.setIncludeInferred(true /* includeInferred */);
			TupleQueryResult sparqlResults = tq.evaluate();
			Vector<String> strVector = new Vector();
			while(sparqlResults.hasNext())
				strVector.add(sparqlResults.next().getValue(Constants.ENTITY)+ "");

			return strVector;
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Uses a type URI to get the URIs of all instances of that type. These instance URIs are returned as the Vector of Strings.
	 * @param type The full URI of the node type that we want to get the instances of
	 * @return the Vector of Strings representing the full uris of all of the instances of the passed in type */
	public Vector<String> getEntityOfType(String type)
	{
		// Get query from smss
		// If the query is not there, get from RDFMap
		// Fill query with type
		// run through getCleanSelect()
		String query = this.getProperty(Constants.TYPE_QUERY);
		if(query==null){
			query = DIHelper.getInstance().getProperty(Constants.TYPE_QUERY);
		}
		Hashtable paramHash = new Hashtable();
		paramHash.put("entity", type);
		query = Utility.fillParam(query, paramHash);
		
		return getCleanSelect(query);
	}

	/**
	 * Returns whether or not an engine is currently connected to the data store.  The connection becomes true when {@link #openDB(String)} 
	 * is called and the connection becomes false when {@link #closeDB()} is called.
	
	 * @return true if the engine is connected to its data store and false if it is not */
	@Override
	public boolean isConnected()
	{
		return connected;
	}

	@Override
	public void removeData(String query) {
		insertData(query);
		
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void deleteDB(){
		// this does nothing
		logger.info("cannot delete remote engine");
	}
}
