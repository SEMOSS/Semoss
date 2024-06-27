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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.query.GraphQueryResult;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
import prerna.util.Constants;


/**
 * This helps insert and delete boolean queries to the database.
 */
public class SesameJenaBooleanWrapper {
	
	GraphQueryResult gqr = null;
	
	org.apache.jena.rdf.model.Model model = null;	
	org.apache.jena.rdf.model.StmtIterator si = null;	
	org.apache.jena.rdf.model.Statement curSt = null;

	IDatabaseEngine engine = null;	
	DATABASE_TYPE databaseType = IDatabaseEngine.DATABASE_TYPE.SESAME;
	String query = null;
	
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
	 * @param engine IDatabase - The engine that this is being set to.
	 */
	public void setEngine(IDatabaseEngine engine)
	{
		this.engine = engine;
		databaseType = engine.getDatabaseType();
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
	
	 * @return boolean true if the query is returned. 
	 * @throws Exception */
	public boolean execute() throws Exception 
	{
		boolean ret= false;
		try {
			ret = (boolean) engine.execQuery(query);			
		} catch (RuntimeException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return ret;
	}
	

}
