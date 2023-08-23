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

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.model.BigdataLiteralImpl;
import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Big data engine serves to connect the .jnl files, which contain the RDF database, to the java engine.
 */
public class BigDataEngine extends AbstractDatabaseEngine {

	private static final Logger logger = LogManager.getLogger(BigDataEngine.class.getName());
	private BigdataSail bdSail = null;
	private SailRepositoryConnection rc = null;
	private SailConnection sc = null;
	private ValueFactory vf = null;
	boolean connected = false;
	private InferenceEngine ie = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		String fileName = SmssUtilities.getSysTapJnl(smssProp).getAbsolutePath();
		smssProp.put("com.bigdata.journal.AbstractJournal.file", fileName);
		bdSail = new BigdataSail(smssProp);
		BigdataSailRepository repo = new BigdataSailRepository(bdSail);
		repo.initialize();
		// need to grab the connection to get the inference engine
		BigdataSailConnection rwConnection = bdSail.getConnection();
		ie = rwConnection.getTripleStore().getInferenceEngine();
		// close the connection since we use the rc/sc directly from sesame api
		rwConnection.close();
		
		rc = repo.getUnisolatedConnection();
		sc = rc.getSailConnection();
		vf = bdSail.getValueFactory();
		this.connected = true;
	}

	/**
	 * Closes the data base associated with the engine.  This will prevent further changes from being made in the data store and 
	 * safely ends the active transactions and closes the engine.
	 */
	@Override
	public void close() {
		super.close();
		try {
			bdSail.shutDown();
			connected = false;
		} catch (SailException e) {
			e.printStackTrace();
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
			logger.debug("\nSPARQL: " + Utility.cleanLogString(query));
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
	public void insertData(String query) throws Exception {
		Update up;
		up = rc.prepareUpdate(QueryLanguage.SPARQL, query);
		logger.debug("\nSPARQL: " + query);
		rc.setAutoCommit(false);
		rc.begin();
		up.execute();
		ie.computeClosure(null);
		sc.commit();
		rc.commit();
	}

	@Override
	public DATABASE_TYPE getDatabaseType() {
		return IDatabaseEngine.DATABASE_TYPE.SESAME;
	}

	/**
	 * Processes a SELECT query to get the results in the exact format that the database stores them.
	 * This is important for things like param values so that we can take the returned value and fill the main query without needing modification
	 * @param sparqlQuery the SELECT SPARQL query to be run against the engine
	 * @return the Vector of Strings representing the full uris of all of the query results */
	public Vector<Object> getCleanSelect(String sparqlQuery) {
		try {
			if(sparqlQuery != null) {
				TupleQuery tq = rc.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);
				logger.debug("\nSPARQL: " + sparqlQuery);
				tq.setIncludeInferred(true /* includeInferred */);
				TupleQueryResult sparqlResults = tq.evaluate();
				Vector<Object> retVec = new Vector<Object>();

				while(sparqlResults.hasNext()) {
					try {
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
					} catch (RuntimeException e) {
						e.printStackTrace();
					}	
				}

				logger.info("Found " + retVec.size() + " elements in result set");
				return retVec;
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
	 * Returns whether or not an engine is currently connected to the data store.  The connection becomes true when {@link #open(String)} 
	 * is called and the connection becomes false when {@link #close()} is called.	
	 * @return true if the engine is connected to its data store and false if it is not */
	@Override
	public boolean isConnected()
	{
		return connected;
	}

	/**
	 * Method addStatement. Processes a given subject, predicate, object triple and adds the statement to the SailConnection.
	 * @param subject String - RDF Subject for the triple
	 * @param predicate String - RDF Predicate for the triple
	 * @param object Object - RDF Object for the triple
	 * @param concept boolean - True if the statement is a concept
	 */
	//	public void addStatement(String subject, String predicate, Object object, boolean concept)
	public void addStatement(Object[] args)
	{
		String subject = args[0]+"";
		String predicate = args[1]+"";
		Object object = args[2];
		Boolean concept = (Boolean) args[3];
		//logger.debug("Updating Triple " + subject + "<>" + predicate + "<>" + object);
		try {
			URI newSub = null;
			URI newPred = null;
			String subString = null;
			String predString = null;
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
					sc.addStatement(newSub, newPred, vf.createLiteral(((Double)object).doubleValue()));
				}
				else if(object.getClass() == new Date(1).getClass())
				{
					logger.debug("Found Date " + object);
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
					String date = df.format(object);
					URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
					sc.addStatement(newSub, newPred, vf.createLiteral(date, datatype));
				}
				else
				{
					logger.debug("Found String " + object);
					String value = object + "";
					// try to see if it already has smssProperties then add to it
					//					String cleanValue = value.replaceAll("/", "-").replaceAll("\"", "'");			
					sc.addStatement(newSub, newPred, vf.createLiteral(value));
				} 
			}
			else {
				URI newObj = vf.createURI(Utility.cleanString((object + "").trim(), false));
				sc.addStatement(newSub, newPred, newObj);
			}

		} catch (SailException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Method removeStatement. Processes a given subject, predicate, object triple and adds the statement to the SailConnection.
	 * @param subject String - RDF Subject for the triple
	 * @param predicate String - RDF Predicate for the triple
	 * @param object Object - RDF Object for the triple
	 * @param concept boolean - True if the statement is a concept
	 */
	//	public void removeStatement(String subject, String predicate, Object object, boolean concept)
	public void removeStatement(Object[] args)
	{
		String subject = args[0]+"";
		String predicate = args[1]+"";
		Object object = args[2];
		Boolean concept = (Boolean) args[3];
		//logger.debug("Updating Triple " + subject + "<>" + predicate + "<>" + object);
		try {
			URI newSub = null;
			URI newPred = null;
			String subString = null;
			String predString = null;
			String sub = subject.trim();
			String pred = predicate.trim();

			subString = Utility.cleanString(sub, false);
			newSub = vf.createURI(subString);

			predString = Utility.cleanString(pred, false);
			newPred = vf.createURI(predString);

			if(!concept) {
				if(object == null) {
					sc.removeStatements(newSub, newPred, null);
				}
				else if(object.getClass() == new Double(1).getClass())
				{
					logger.debug("Found Double " + object);
					sc.removeStatements(newSub, newPred, vf.createLiteral(((Double)object).doubleValue()));
				}
				else if(object.getClass() == new Date(1).getClass())
				{
					logger.debug("Found Date " + object);
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
					String date = df.format(object);
					URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
					sc.removeStatements(newSub, newPred, vf.createLiteral(date, datatype));
				}
				else
				{
					logger.debug("Found String " + object);
					String value = object + "";
					// try to see if it already has smssProperties then add to it
					//					String cleanValue = value.replaceAll("/", "-").replaceAll("\"", "'");			
					sc.removeStatements(newSub, newPred, vf.createLiteral(value));
				}
			} else {
				sc.removeStatements(newSub, newPred, vf.createURI(object+""));
			}
		} catch (SailException e) {
			e.printStackTrace();
		}
	}


	/**
	 * Method infer.	
	 */
	public void infer()
	{
		try {
			ie.computeClosure(null);
			sc.commit();
		} catch (SailException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Commit the database. Commits the active transaction.  This operation ends the active transaction.
	 */
	@Override
	public void commit()
	{
		try {
			sc.commit();
			((BigdataSailRepositoryConnection)rc).flush();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (SailException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void removeData(String query) throws Exception {
		insertData(query);
	}

	/**
	 * This method permanently deletes the database and all of its associated files
	 */
	@Override
	public void delete() {
		super.delete();
		// delete JNL if above doesn't
		String jnlLoc = SmssUtilities.getSysTapJnl(smssProp).getAbsolutePath();
		if(jnlLoc != null){
			System.out.println("Deleting jnl file " + jnlLoc);
			File jnlFile = new File(jnlLoc);
			jnlFile.delete();
		}		
	}

	/**
	 * Method to get the SC
	 * @return
	 */
	SailConnection getSc() {
		return this.sc;
	}
}
