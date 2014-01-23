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

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.query.GraphQueryResult;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Utility;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * The wrapper helps takes care of selection of the type of engine you are using (Jena/Sesame).  This wrapper processes CONSTRUCT statements. 
 */
public class SesameJenaConstructWrapper {
	public GraphQueryResult gqr = null;	
	Model model = null;
	StmtIterator si = null;	
	public IEngine engine = null;
	Enum engineType = IEngine.ENGINE_TYPE.SESAME;
	String query = null;
	com.hp.hpl.jena.rdf.model.Statement curSt = null;
	public boolean queryBoolean = true;
	Logger logger = Logger.getLogger(getClass());
	
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
	 * Method execute.  Executes the SPARQL query based on the type of engine selected.
	 */
	public void execute()
	{
		try {
			if(engineType == IEngine.ENGINE_TYPE.SESAME)
			{
				gqr = (GraphQueryResult)engine.execGraphQuery(this.query);
			}
			else if (engineType == IEngine.ENGINE_TYPE.JENA)
			{
				model = (Model)engine.execGraphQuery(query);
				setModel(model);
			}
		} catch (Exception e) {
			// TODO: Specify exception
			e.printStackTrace();
		}
	}
	
	/**
	 * Method setModel. Sets the type of model being used.
	 * @param model Model - The model type.
	 */
	public void setModel(Model model)
	{
		this.model = model;
		si = model.listStatements();
	}
	
	/**
	 * Method hasNext.  Checks to see if the tuple query result has additional results.
	
	 * @return boolean - True if the Tuple Query result has additional results.
	 * */
	public boolean hasNext() 
	{
		boolean retBool = false;
		try
		{
			logger.debug("Checking for next " );
			if(engineType == IEngine.ENGINE_TYPE.SESAME)
			{
				retBool = gqr.hasNext();
				if(!retBool)
					gqr.close();
			}
			else
			{
				retBool = si.hasNext();
				if(!retBool)
					si.close();
			}
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		logger.debug(" Next " + retBool);
		return retBool;
	}
	
	/**
	 * Method next.  Processes the select statement for either Sesame or Jena.
	
	 * @return SesameJenaConstructStatement - returns the construct statement.
	 */
	public SesameJenaConstructStatement next()
	{
		SesameJenaConstructStatement retSt = new SesameJenaConstructStatement();
		try
		{
			if(engineType == IEngine.ENGINE_TYPE.SESAME)
			{
				logger.debug("Adding a sesame statement ");
				Statement stmt = gqr.next();
				retSt.setSubject(stmt.getSubject()+"");
				retSt.setObject(stmt.getObject());
				retSt.setPredicate(stmt.getPredicate() + "");
				
			}
			else
			{
				com.hp.hpl.jena.rdf.model.Statement stmt = si.next();
				logger.debug("Adding a JENA statement ");
				curSt = stmt;
				Resource sub = stmt.getSubject();
				Property pred = stmt.getPredicate();
				RDFNode node = stmt.getObject();
				if(node.isAnon())
					retSt.setPredicate(Utility.getNextID());
				else 	
					retSt.setPredicate(stmt.getPredicate() + "");

				if(sub.isAnon())
					retSt.setSubject(Utility.getNextID());
				else
					retSt.setSubject(stmt.getSubject()+"");
				
				if(node.isAnon())
					retSt.setObject(Utility.getNextID());
				else
					retSt.setObject(stmt.getObject());
			}
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return retSt;
	}

	/**
	 * Method getJenaStatement.  Gets the query solution for a JENA model.
	
	 * @return com.hp.hpl.jena.rdf.model.Statement */
	public com.hp.hpl.jena.rdf.model.Statement getJenaStatement()
	{
		return curSt;
	}
	
	/**
	 * Method setEngineType. Sets the engine type.
	 * @param engineType Enum - The type engine that this is being set to.
	 */
	public void setEngineType(Enum engineType)
	{
		this.engineType = engineType;
	}
}
