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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Graph;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
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
import org.openrdf.sail.SailException;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;

/**
 * Connects the database to the java engine, except database (.jnl file) is sitting on a server (or the web).
 */
public class RemoteBigdataEngine extends AbstractEngine implements IEngine {

	private static final Logger logger = LogManager.getLogger(RemoteBigdataEngine.class.getName());
	BigdataSail bdSail = null;
	Properties rdfMap = null;
	RepositoryConnection rc = null;
	ValueFactory vf = null;
	boolean connected = false;
	String doubleURI = "http://www.w3.org/2001/XMLSchema#double";
	String dateURI = "http://www.w3.org/2001/XMLSchema#dateTime";
	String intURI = "http://www.w3.org/2001/XMLSchema#int";

	
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
			

			BigdataSailRemoteRepository repo = new BigdataSailRemoteRepository(sparqlUEndpoint);
			repo.initialize();
			
			//SPARQLRepository repo = new SPARQLRepository(sparqlQEndpoint);
			Hashtable <String, String> myMap = new Hashtable<String,String>();
			myMap.put("apikey","d0184dd3-fb6b-4228-9302-1c6e62b01465");
			
			//System.err.println("Host is " + sparqlQEndpoint);
			//HTTPRepository hRepo = new HTTPRepository(sparqlQEndpoint);
			//hRepo.setPreferredRDFFormat(RDFFormat.forMIMEType("application/x-www-form-urlencoded"));
//			hRepo.
			//hRepo.initialize();
			
			
			rc = repo.getConnection();
			vf = new ValueFactoryImpl();
			
			
//			InferenceEngine ie = new InferenceEngine(rc.getRepository());
			
			//rc = new SPARQLConnection(repo, sparqlQEndpoint, sparqlUEndpoint);
			
			// test
//            testIt();
			
	
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
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
//	/**
//	 * Method testIt.
//	 */
//	private void testIt() throws Exception
//	{
//        URI pk = new URIImpl("http://www.semoss.org/person/Ali");
//        URI loves = new URIImpl("http://www.semoss.org/action/loves");
//        URI rdf = new URIImpl("http://www.semoss.org/RDF3");       
//        //rc2.add(graph);
//        //rc2.commit();
//        rc.add(pk, loves, rdf);
//        rc.commit();
//        System.err.println("INSERT SUCCESSFULLY COMPLETED.....................");
//		
//	}
	
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
	public void insertData(String query) {
		Update up;
		try {
			up = rc.prepareUpdate(QueryLanguage.SPARQL, query);
			//sc.addStatement(vf.createURI("<http://semoss.org/ontologies/Concept/Service/tom2>"),vf.createURI("<http://semoss.org/ontologies/Relation/Exposes>"),vf.createURI("<http://semoss.org/ontologies/Concept/BusinessLogicUnit/tom1>"));
			logger.debug("\nSPARQL: " + query);
			//tq.setIncludeInferred(true /* includeInferred */);
			//tq.evaluate();
			//rc.setAutoCommit(false);
//			sc.begin();
			// TODO DO WE NEED SC.BEGIN AND SC.COMMIT??
			up.execute();
			//rc.commit();
//			sc.commit();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UpdateExecutionException e) {
			// TODO Auto-generated catch block
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
//			tq.setIncludeInferred(true /* includeInferred */);
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
	 * Method addStatement. Processes a given subject, predicate, object triple and adds the statement to the given graph.
	 * @param subject String - RDF Subject for the triple
	 * @param predicate String - RDF Predicate for the triple
	 * @param object Object - RDF Object for the triple
	 * @param concept boolean - True if the statement is a concept
	 * @param graph Graph - The graph where the triple will be added.
	 */
//	public void addStatement(String subject, String predicate, Object object, boolean concept, Graph graph)
	public void addStatement(Object[] args)
	{
		String subject = args[0]+"";
		String predicate = args[1]+"";
		Object object = args[2];
		Boolean concept = (Boolean) args[3];
		URI newSub = null;
				URI newPred = null;
				Value newObj = null;
				String subString = null;
				String predString = null;
				String objString = null;
				String sub = subject.trim();
				String pred = predicate.trim();
						
				subString = Utility.cleanString(sub, false);
				newSub = vf.createURI(subString);
				
				predString = Utility.cleanString(pred, false);
				newPred = vf.createURI(predString);
				
				if(!concept)
				{
					if(object.getClass() == new Double(1).getClass())
					{
						logger.debug("Found Double " + object);
//						graph.add(newSub, newPred, vf.createLiteral(((Double)object).doubleValue()));
					}
					else if(object.getClass() == new Date(1).getClass())
					{
						logger.debug("Found Date " + object);
						DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
						String date = df.format(object);
						URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
//						graph.add(newSub, newPred, vf.createLiteral(date, datatype));
					}
					else
					{
						logger.debug("Found String " + object);
						String value = object + "";
						// try to see if it already has properties then add to it
						String cleanValue = value.replaceAll("/", "-").replaceAll("\"", "'");			
//						graph.add(newSub, newPred, vf.createLiteral(cleanValue));
					} 
				}
//				else
//					graph.add(newSub, newPred, vf.createURI(object+""));
	}
	
	/**
	 * Method addGraphToRepository.  Adds the specified graph to the current repository.
	 * @param graph Graph - The graph to be added to the repository.
	 */
	public void addGraphToRepository(Graph graph){
		try {
			rc.add(graph);
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Commit the database. Commits the active transaction.  This operation ends the active transaction.
	 */
	@Override
	public void commit(){
		try {
			rc.commit();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
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
	public void deleteDB(){
		// this does nothing
		logger.info("cannot delete remote engine");
	}
}
