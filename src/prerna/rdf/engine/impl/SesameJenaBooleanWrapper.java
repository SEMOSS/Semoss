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
