/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.rdf.engine.impl;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import org.openrdf.model.Graph;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;

/**
 * Connects the database to the java engine, except database (.jnl file) is sitting on a server (or the web).
 */
public class RemoteBigdataEngine extends AbstractEngine implements IEngine {

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
		}catch(Exception ignored)
		{
			ignored.printStackTrace();
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
	 * Runs the passed string query against the engine and returns graph query results.  The query passed must be in the structure 
	 * of a CONSTRUCT SPARQL query.  The exact format of the results will be 
	 * dependent on the type of the engine, but regardless the results are able to be graphed.
	 * @param query the string version of the query to be run against the engine
	
	 * @return the graph query results */
	@Override
	public GraphQueryResult execGraphQuery(String query) {
		 GraphQueryResult res = null;
		try {
			GraphQuery sagq = rc.prepareGraphQuery(QueryLanguage.SPARQL,
						query);
				res = sagq.evaluate();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		return res;	
	}

	/**
	 * Runs the passed string query against the engine as a SELECT query.  The query passed must be in the structure of a SELECT 
	 * SPARQL query and the result format will depend on the engine type.
	 * @param query the string version of the SELECT query to be run against the engine
	
	 * @return triple query results that can be displayed as a grid */
	@Override
	public TupleQueryResult execSelectQuery(String query) {
		
		TupleQueryResult sparqlResults = null;
		
		try {
			TupleQuery tq = rc.prepareTupleQuery(QueryLanguage.SPARQL, query);
			logger.debug("\nSPARQL: " + query);
			//tq.setIncludeInferred(true /* includeInferred */);
			sparqlResults = tq.evaluate();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		return sparqlResults;
	}

	/**
	 * Runs the passed string query against the engine as an INSERT query.  The query passed must be in the structure of an INSERT 
	 * SPARQL query or an INSERT DATA SPARQL query 
	 * and there are no returned results.  The query will result in the specified triples getting added to the 
	 * data store.
	 * @param query the INSERT or INSERT DATA SPARQL query to be run against the engine
	 */
	@Override
	public void execInsertQuery(String query) throws SailException, UpdateExecutionException, RepositoryException, MalformedQueryException {

		Update up = rc.prepareUpdate(QueryLanguage.SPARQL, query);
		//sc.addStatement(vf.createURI("<http://semoss.org/ontologies/Concept/Service/tom2>"),vf.createURI("<http://semoss.org/ontologies/Relation/Exposes>"),vf.createURI("<http://semoss.org/ontologies/Concept/BusinessLogicUnit/tom1>"));
		logger.debug("\nSPARQL: " + query);
		//tq.setIncludeInferred(true /* includeInferred */);
		//tq.evaluate();
//		rc.setAutoCommit(false);
		up.execute();
		//rc.commit();
//        InferenceEngine ie = ((BigdataSail)bdSail).getInferenceEngine();
//        ie.computeClosure(null);
		rc.commit();
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
		
		BooleanQuery bq;
		boolean response = false;
		try {
			bq = rc.prepareBooleanQuery(QueryLanguage.SPARQL, query);
			logger.debug("\nSPARQL: " + query);
			response = bq.evaluate();
		} catch (MalformedQueryException e) {
			e.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		
		return response;	

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
	 * Processes a SELECT query just like {@link #execSelectQuery(String)} but then parses the results to get only their 
	 * instance names.  These instance names are then returned as the Vector of Strings.
	 * @param sparqlQuery the SELECT SPARQL query to be run against the engine
	
	 * @return the Vector of Strings representing the instance names of all of the query results */
	public Vector<String> getEntityOfType(String sparqlQuery)
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
	 * Method addStatement. Processes a given subject, predicate, object triple and adds the statement to the given graph.
	 * @param subject String - RDF Subject for the triple
	 * @param predicate String - RDF Predicate for the triple
	 * @param object Object - RDF Object for the triple
	 * @param concept boolean - True if the statement is a concept
	 * @param graph Graph - The graph where the triple will be added.
	 */
	public void addStatement(String subject, String predicate, Object object, boolean concept, Graph graph)
	{
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
						graph.add(newSub, newPred, vf.createLiteral(((Double)object).doubleValue()));
					}
					else if(object.getClass() == new Date(1).getClass())
					{
						logger.debug("Found Date " + object);
						DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
						String date = df.format(object);
						URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
						graph.add(newSub, newPred, vf.createLiteral(date, datatype));
					}
					else
					{
						logger.debug("Found String " + object);
						String value = object + "";
						// try to see if it already has properties then add to it
						String cleanValue = value.replaceAll("/", "-").replaceAll("\"", "'");			
						graph.add(newSub, newPred, vf.createLiteral(cleanValue));
					} 
				}
				else
					graph.add(newSub, newPred, vf.createURI(object+""));
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
}
