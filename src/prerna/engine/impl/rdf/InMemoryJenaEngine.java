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

import java.util.Properties;
import java.util.Vector;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.update.UpdateAction;

import prerna.engine.impl.AbstractDatabaseEngine;

/**
 * Holds the database in memory, and uses the Jena API to facilitate querying of RDF data sources.
 */
public class InMemoryJenaEngine extends AbstractDatabaseEngine {

	Model jenaModel = null;

	@Override
	public void open(String propFile) {
		// does nothing .. have to set model directly
	}
	
	@Override
	public void open(Properties smssProp) {
		// does nothing .. have to set model directly
	}

	/**
	 * Closes the data base associated with the engine.  This will prevent further changes from being made in the data store and 
	 * safely ends the active transactions and closes the engine.
	 */
	@Override
	public void close() {
		// do nothing
	}

	/**
	 * Runs the passed string query against the engine as a SELECT query.  The query passed must be in the structure of a SELECT 
	 * SPARQL query and the result format will depend on the engine type.
	 * @param query the string version of the SELECT query to be run against the engine
	
	 * @return triple query results that can be displayed as a grid */
	@Override
	public Object execQuery(String query) {
		ResultSet rs = null;
		try{
			//QueryExecutionFactory.
			Query q2 = QueryFactory.create(query); 
			QueryExecution qex = QueryExecutionFactory.create(q2, jenaModel);
			rs = qex.execSelect();
		}catch (RuntimeException e){
			e.printStackTrace();
		}
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
	public void insertData(String query) {
		UpdateAction.parseExecute(query, jenaModel);

	}

	/**
	 * Method setModel. Sets the jena Model to the parameter jena model.
	 * @param jenaModel Model - Name of the model that this is being set to.
	 */
	public void setModel(Model jenaModel) {
		this.jenaModel = jenaModel;

	}
	
	@Override
	public DATABASE_TYPE getDatabaseType() {
		return DATABASE_TYPE.JENA;
	}

	/**
	 * Processes a SELECT query just like {@link #execSelectQuery(String)} but then parses the results to get only their 
	 * instance names.  These instance names are then returned as the Vector of Strings.
	 * @param sparqlQuery the SELECT SPARQL query to be run against the engine
	
	 * @return the Vector of Strings representing the instance names of all of the query results */
	@Override
	public Vector<Object> getEntityOfType(String sparqlQuery) {
		// TODO: Don't return null
		return null;
	}

	/**
	 * Returns whether or not an engine is currently connected to the data store.  The connection becomes true when {@link #open(String)} 
	 * is called and the connection becomes false when {@link #close()} is called.
	
	 * @return true if the engine is connected to its data store and false if it is not */
	@Override
	public boolean isConnected() {
		return false;
	}

	@Override
	public void removeData(String query) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete() {
		// This does nothing
		
	}
	
	@Override
	public boolean holdsFileLocks() {
		return false;
	}
}
