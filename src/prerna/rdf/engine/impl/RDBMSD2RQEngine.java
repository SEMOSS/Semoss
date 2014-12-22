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

import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateRequest;

import de.fuberlin.wiwiss.d2rq.jena.ModelD2RQ;

/**
 * Connects to an RDBMS and facilitates query execution.
 */
public class RDBMSD2RQEngine extends AbstractEngine implements IEngine {
	
	Model d2rqModel = null;
	static final Logger logger = LogManager.getLogger(RDBMSD2RQEngine.class.getName());
	String propFile = null;
	boolean connected = false;

	/**
	 * Closes the data base associated with the engine.  This will prevent further changes from being made in the data store and 
	 * safely ends the active transactions and closes the engine.
	 */
	@Override
	public void closeDB() {
		d2rqModel.close();
		logger.info("Closed the RDBMS Database " + propFile);		
	}

	/**
	 * Runs the passed string query against the engine and returns graph query results.  The query passed must be in the structure 
	 * of a CONSTRUCT SPARQL query.  The exact format of the results will be 
	 * dependent on the type of the engine, but regardless the results are able to be graphed.
	 * @param query the string version of the query to be run against the engine
	
	 * @return the graph query results */
	@Override
	public Object execGraphQuery(String query) {
		logger.info("EXEC GRAPH QUERY: " + query);
		Query q = QueryFactory.create(query) ;
		QueryExecution qexec = QueryExecutionFactory.create(q, d2rqModel) ;
		Model resultModel = qexec.execConstruct() ;
		return resultModel;
	}

	/**
	 * Runs the passed string query against the engine as a SELECT query.  The query passed must be in the structure of a SELECT 
	 * SPARQL query and the result format will depend on the engine type.
	 * @param query the string version of the SELECT query to be run against the engine
	
	 * @return triple query results that can be displayed as a grid */
	@Override
	public ResultSet execSelectQuery(String query) {
		logger.info("EXEC SELECT QUERY: " + query);
		Query q = QueryFactory.create(query); 
		QueryExecution qexec = QueryExecutionFactory.create(q, d2rqModel);
		ResultSet rs = qexec.execSelect();
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
		UpdateRequest update = UpdateFactory.create(query);
		UpdateAction.execute(update, d2rqModel);
	}

	/**
	 * Gets the type of the engine. The engine type is often used to determine what API to use while running queries against the 
	 * engine. D2RQ uses the JENA API so we return an engine type of JENA.
	
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
		logger.info("ENTITY OF TYPE QUERY: " + sparqlQuery);
		
		Vector <String> retString = new Vector<String>();
		ResultSet rs = (ResultSet)execSelectQuery(sparqlQuery);
		
		String varName = Constants.ENTITY;
		while(rs.hasNext())
		{
			QuerySolution row = rs.next();
			retString.add(row.get(varName)+"");
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
		if(this.map != null) {
			d2rqModel = new ModelD2RQ(DIHelper.getInstance().getProperty("BaseFolder") + "/" + this.map);
			if(d2rqModel != null) {
				this.connected = true;
			}
			super.openDB(propFile);
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
		boolean response = false;
		
		Query q = QueryFactory.create(query);
		QueryExecution qexec = QueryExecutionFactory.create(q, d2rqModel);
		response = qexec.execAsk();
		
		return response;
	}
}
