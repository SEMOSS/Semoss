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

import prerna.rdf.engine.api.IEngine;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * This helps insert and delete boolean queries to the database.
 */
public class SesameJenaBooleanWrapper {
	GraphQueryResult gqr = null;
	Model model = null;	
	StmtIterator si = null;	
	IEngine engine = null;	
	Enum engineType = IEngine.ENGINE_TYPE.SESAME;
	String query = null;
	com.hp.hpl.jena.rdf.model.Statement curSt = null;
	
	static final Logger logger = LogManager.getLogger(SesameJenaBooleanWrapper.class.getName());
	
	/**
	 * Method setGqr. - Sets the Graph query result.
	 * @param gqr GraphQueryResult - The graph query result that this is being set to.
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
	 * Method execute.  Executes the query.
	
	 * @return boolean true if the query is returned. */
	public boolean execute()
	{
		boolean ret= false;
		try {
			ret = engine.execAskQuery(query);			
		} catch (RuntimeException e) {
			// TODO: Specify exception
			e.printStackTrace();
		}
		return ret;
	}
	

}
