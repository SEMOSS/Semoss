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
package prerna.engine.impl.rdf;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
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
import org.openrdf.repository.UnknownTransactionStateException;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.model.BigdataLiteralImpl;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.AbstractEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Holds the database in memory, and uses the Sesame API to facilitate querying of RDF data sources.
 */
public class InMemorySesameEngine extends AbstractEngine {

	private static final Logger logger = LogManager.getLogger(InMemorySesameEngine.class.getName());
	RepositoryConnection rc = null;
	SailConnection sc = null;
	ValueFactory vf = null;
	boolean connected = false;

	/**
	 * Method setRepositoryConnection. Sets the repository connection.
	 * @param rc RepositoryConnection. The repository connection that this is being set to.
	 */
	public void setRepositoryConnection(RepositoryConnection rc) 
	{
		this.rc = rc;
		this.sc = ((SailRepositoryConnection) rc).getSailConnection();
		this.vf = rc.getValueFactory();
		this.connected = true;
	}
	
	/**
	 * Method getRepositoryConnection.  Gets the repository connection.	
	 * @return RepositoryConnection - the connection to the repository.*/
	public RepositoryConnection getRepositoryConnection()
	{
		return this.rc;
	}
	
	/**
	 * Opens a database as defined by its properties file.  What is included in the properties file is dependent on the type of 
	 * engine that is being initiated.  This is the function that first initializes an engine with the property file at the very 
	 * least defining the data store.
	 * @param propFile contains all information regarding the data store and how the engine should be instantiated.  Dependent on 
	 * what type of engine is being instantiated.
	 */
	public void openDB(String propFile)
	{
		// no meaning to this now
	}
	
	/**
	 * Closes the data base associated with the engine.  This will prevent further changes from being made in the data store and 
	 * safely ends the active transactions and closes the engine.
	 */
	public void closeDB() {
		if(connected) {
			try {
				rc.clear();
				rc.rollback();
				rc.close();
				connected = false;
			} catch (RepositoryException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Runs the passed string query against the engine as a SELECT query.  The query passed must be in the structure of a SELECT 
	 * SPARQL query and the result format will depend on the engine type.
	 * @param query the string version of the SELECT query to be run against the engine
	
	 * @return triple query results that can be displayed as a grid */
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
	public void insertData(String query) {
		Update up;
		try {
			up = rc.prepareUpdate(QueryLanguage.SPARQL, query);
			//sc.addStatement(vf.createURI("<http://semoss.org/ontologies/Concept/Service/tom2>"),vf.createURI("<http://semoss.org/ontologies/Relation/Exposes>"),vf.createURI("<http://semoss.org/ontologies/Concept/BusinessLogicUnit/tom1>"));
			logger.debug("\nSPARQL: " + query);
			//tq.setIncludeInferred(true /* includeInferred */);
			//tq.evaluate();
			rc.setAutoCommit(false);
			up.execute();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (UpdateExecutionException e) {
			e.printStackTrace();
		}catch (MalformedQueryException e)
		{
			e.printStackTrace();
		}

	}
	
	/**
	 * Gets the type of the engine.  The engine type is often used to determine what API to use while running queries against the 
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
	public Vector<Object> getCleanSelect(String sparqlQuery)
	{
		try {
			TupleQuery tq = rc.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);
			logger.debug("\nSPARQL: " + sparqlQuery);
			tq.setIncludeInferred(true /* includeInferred */);
			TupleQueryResult sparqlResults = tq.evaluate();
			Vector<Object> retVec = new Vector<Object>();
			while(sparqlResults.hasNext()) {
				Value val = sparqlResults.next().getValue(Constants.ENTITY);
				Object next = null;
				if (val instanceof BigdataLiteralImpl && ((BigdataLiteralImpl) val).getDatatype() != null) {
					try {
						next = ((BigdataLiteralImpl)val).doubleValue();
					} catch(NumberFormatException ex) {
						next = ((BigdataLiteralImpl) val).getLabel();
					}
				} else if(val instanceof Literal){
					next = ((Literal)val).getLabel();
				} else {
					next = "" + val;
				}
				
				retVec.add(next);
			}

			return retVec;
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
	public Vector<Object> getEntityOfType(String type)
	{
		// Get query from smss
		// If the query is not there, get from RDFMap
		// Fill query with type
		// run through getCleanSelect()
		String query = this.getProperty(Constants.TYPE_QUERY);
		if(query==null){
			query = DIHelper.getInstance().getProperty(Constants.TYPE_QUERY);
		}
		Map<String, List<Object>> paramHash = new Hashtable<String, List<Object>>();
		List<Object> retList = new ArrayList<Object>();
		retList.add(type);
		paramHash.put("entity", retList);
		query = Utility.fillParam(query, paramHash);
		
		return getCleanSelect(query);
	}

	/**
	 * Returns whether or not an engine is currently connected to the data store.  The connection becomes true when {@link #openDB(String)} 
	 * is called and the connection becomes false when {@link #closeDB()} is called.
	
	 * @return true if the engine is connected to its data store and false if it is not */
	public boolean isConnected()
	{
		return connected;
	}
	
	public void addStatement(Object[] args)
	{
		String subject = args[0]+"";
		String predicate = args[1]+"";
		Object object = args[2];
		Boolean concept = (Boolean) args[3];
		try {
			if(!rc.isActive()) {
				rc.begin();
			}
			// subject and predicate must be URIs
			URI newSub = vf.createURI(subject);
			URI newPred = vf.createURI(predicate);
			
			if(!concept) {
				if(object.getClass() == new Double(1).getClass()) {
					logger.debug("Found Double " + object);
					sc.addStatement(newSub, newPred, vf.createLiteral(((Double)object).doubleValue()));
				} else if(object.getClass() == new Date(1).getClass()) {
					logger.debug("Found Date " + object);
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
					String date = df.format(object);
					URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
					sc.addStatement(newSub, newPred, vf.createLiteral(date, datatype));
				} else if(object.getClass() == new Boolean(true).getClass()) {
					logger.debug("Found Boolean " + object);
					sc.addStatement(newSub, newPred, vf.createLiteral((Boolean)object));
				} else {
					logger.debug("Found String " + object);
					String value = object + "";
					// try to see if it already has properties then add to it
					//String cleanValue = value.replaceAll("/", "-").replaceAll("\"", "'");			
					sc.addStatement(newSub, newPred, vf.createLiteral(value));
				} 
			} else {
				if(object instanceof Literal) {
					sc.addStatement(newSub, newPred, (Literal)object);
				} else if(object instanceof URI) {
					sc.addStatement(newSub, newPred, (URI)object);
				} else if(object instanceof Value) {
					sc.addStatement(newSub, newPred, (Value)object);
				} else {
					sc.addStatement(newSub, newPred, vf.createURI(object+""));
				}
			}
		} catch (SailException e) {
			e.printStackTrace();
		} catch (UnknownTransactionStateException e) {
			e.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
	}

	public void removeStatement(Object[] args)
	{
		String subject = args[0]+"";
		String predicate = args[1]+"";
		Object object = args[2];
		Boolean concept = (Boolean) args[3];
		try {
			if(!rc.isActive()) {
				rc.begin();
			}
			// subject and predicate must be URIs
			URI newSub = vf.createURI(subject);
			URI newPred = vf.createURI(predicate);
			
			if(!concept) {
				if(object.getClass() == new Double(1).getClass()) {
					logger.debug("Found Double " + object);
					sc.removeStatements(newSub, newPred, vf.createLiteral(((Double)object).doubleValue()));
				} else if(object.getClass() == new Date(1).getClass()) {
					logger.debug("Found Date " + object);
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
					String date = df.format(object);
					URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
					sc.removeStatements(newSub, newPred, vf.createLiteral(date, datatype));
				} else {
					logger.debug("Found String " + object);
					String value = object + "";
					// try to see if it already has properties then add to it
					//String cleanValue = value.replaceAll("/", "-").replaceAll("\"", "'");			
					sc.removeStatements(newSub, newPred, vf.createLiteral(value));
				} 
			} else {
				if(object instanceof Literal) {
					sc.removeStatements(newSub, newPred, (Literal)object);
				} else if(object instanceof URI) {
					sc.removeStatements(newSub, newPred, (URI)object);
				} else if(object instanceof Value) {
					sc.removeStatements(newSub, newPred, (Value)object);
				} else {
					sc.removeStatements(newSub, newPred, vf.createURI(object+""));
				}
			}
		} catch (SailException e) {
			e.printStackTrace();
		} catch (UnknownTransactionStateException e) {
			e.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Processes the passed ASK SPARQL query against the engine.  The query must be in the structure of an ASK query and the 
	 * result will be a boolean indicating whether or not the data store connected to the engine has triples matching the 
	 * pattern of the ASK query.
	 * @param query the ASK SPARQL query to be run against the engine
	
	 * @return true if the data store connected to the engine contains triples that match the pattern of the query and false 
	 * if it does not. */
//	@Override
//	public Boolean execAskQuery(String query) {
//		return null;
//	}

	@Override
	public void removeData(String query) {
		insertData(query);
		
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteDB() {
		// This does nothing
		
	}
	
	public void writeData(RDFXMLWriter writer) throws RepositoryException, RDFHandlerException {
		try {
			rc.export(writer);
		} catch (RepositoryException e) {
			e.printStackTrace();
			throw new RepositoryException("Could not export base relationships from OWL database");
		} catch (RDFHandlerException e) {
			e.printStackTrace();
			throw new RDFHandlerException("Could not export base relationships from OWL database");
		}
	}
	
}
