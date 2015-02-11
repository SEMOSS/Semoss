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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import prerna.rdf.engine.api.IEngine;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * Insert and delete queries.  This will set the graph query result for the sesame.
 */
@Deprecated
public class SesameJenaUpdateWrapper {
	GraphQueryResult gqr = null;
	Model model = null;	
	StmtIterator si = null;	
	IEngine engine = null;
	Enum engineType = IEngine.ENGINE_TYPE.SESAME;
	String query = null;

	com.hp.hpl.jena.rdf.model.Statement curSt = null;
	
	static final Logger logger = LogManager.getLogger(SesameJenaUpdateWrapper.class.getName());
	
	/**
	 * Method setGqr.  Sets the graph query result to the active graph query result.
	 * @param gqr GraphQueryResult
	 */
	public void setGqr(GraphQueryResult gqr)
	{
		this.gqr = gqr;
	}
	
	/**
	 * Method setEngine. Sets the engine.
	 * @param engine IEngine - The engine that this is being set to.
	 */
	public void setEngine(IEngine engine)
	{
		this.engine = engine;
		engineType = engine.getEngineType();
	}
	
	/**
	 * Method setQuery. - Sets the SPARQL query statement.
	 * @param query String - The string version of the SPARQL query.
	 */
	public void setQuery(String query)
	{
		this.query = query;
	}
	
	/**
	 * Method execute.  Executes the SPARQL query based on the type of engine selected.
	
	 * @return boolean - True if inserting the query is a success.*/
	public boolean execute()
	{
		boolean success = true;
		try {

			engine.execInsertQuery(query);

			
		} catch (SailException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UpdateExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return success;
	}
	

}
