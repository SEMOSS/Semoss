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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Hashtable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
import prerna.rdf.engine.wrappers.AbstractWrapper;
import prerna.util.Utility;
import prerna.util.Constants;

/**
 * The wrapper helps takes care of selection of the type of engine you are using (Jena/Sesame).  This wrapper processes CONSTRUCT statements. 
 */
@Deprecated
public class SesameJenaConstructWrapper extends AbstractWrapper {
	
	public transient GraphQueryResult gqr = null;	
	
	transient org.apache.jena.rdf.model.Model model = null;
	transient org.apache.jena.rdf.model.StmtIterator si = null;	
	transient org.apache.jena.rdf.model.Statement curSt = null;

	public transient IDatabaseEngine engine = null;
	transient DATABASE_TYPE databaseType = IDatabaseEngine.DATABASE_TYPE.SESAME;
	transient String query = null;
	transient SesameJenaConstructStatement retSt = null;
	public transient boolean queryBoolean = true;
	static final Logger logger = LogManager.getLogger(SesameJenaConstructWrapper.class.getName());
	transient SesameJenaConstructWrapper remoteWrapperProxy = null;
	transient ObjectInputStream ris = null;// remote input stream
	
	
	protected SesameJenaConstructWrapper()
	{
		
	}
	
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
	 * Method execute.  Executes the SPARQL query based on the type of engine selected.
	 * @throws Exception 
	 */
	public void execute() throws Exception
	{
		try {
			if(databaseType == IDatabaseEngine.DATABASE_TYPE.SESAME)
			{
				gqr = (GraphQueryResult)engine.execQuery(this.query);
			}
			else if (databaseType == IDatabaseEngine.DATABASE_TYPE.JENA)
			{
				model = (org.apache.jena.rdf.model.Model)engine.execQuery(query);
				setModel(model);
			}
			else if(databaseType == IDatabaseEngine.DATABASE_TYPE.SEMOSS_SESAME_REMOTE)
			{
				// get the actual SesameJenaConstructWrapper from the engine
				// this is json output
				//System.out.println("Trying to get the wrapper remotely now");
				// get the input stream directly here
				remoteWrapperProxy = (SesameJenaConstructWrapper)engine.execQuery(query);
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Method setModel. Sets the type of model being used.
	 * @param model Model - The model type.
	 */
	public void setModel(org.apache.jena.rdf.model.Model model)
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
			if(databaseType == IDatabaseEngine.DATABASE_TYPE.SESAME)
			{
				retBool = gqr.hasNext();
				if(!retBool)
					gqr.close();
			}
			else if(databaseType == IDatabaseEngine.DATABASE_TYPE.JENA)
			{
				retBool = si.hasNext();
				if(!retBool)
					si.close();
			}
			// need to include an engine type remote so that it can pull it through REST API
			else if(databaseType == IDatabaseEngine.DATABASE_TYPE.SEMOSS_SESAME_REMOTE)
			{
				if(retSt != null) // they have not picked it up yet
					return true;
				retSt = new SesameJenaConstructStatement();
				// I need to pull from remote
				// this is just so stupid to call its own
				if(ris == null)
				{
					Hashtable params = new Hashtable<String,String>();
					params.put("id", remoteWrapperProxy.getRemoteId());
					ris = new ObjectInputStream(Utility.getStream(remoteWrapperProxy.getRemoteAPI() + "/next", params));
				}					
				try {
					Object myObject = ris.readObject();
					
					if(!myObject.toString().equalsIgnoreCase("null"))
					{
						org.apache.jena.rdf.model.Statement stmt = (org.apache.jena.rdf.model.Statement) myObject;
						retSt.setSubject(stmt.getSubject()+"");
						retSt.setObject(stmt.getObject());
						retSt.setPredicate(stmt.getPredicate() + "");
						//System.out.println("Abile to get the object appropriately here " + retSt.getSubject());
						retBool = true;
					}
					else
					{
						try{
							if(ris!=null) {
								ris.close();
							}
						} catch(IOException e) {
							e.printStackTrace();
						}
					}

				} catch (RuntimeException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					retSt = null;
					retBool = false;
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					retSt = null;
					retBool = false;
				}

				
				/*Hashtable params = new Hashtable<String,String>();
				params.put("id", remoteWrapperProxy.getRemoteID());
				System.out.println("ID for remote is " + remoteWrapperProxy.getRemoteID());
				String output = Utility.retrieveResult(remoteWrapperProxy.getRemoteAPI() + "/hasNext", params);
				Gson gson = new Gson();
				retBool = gson.fromJson(output, Boolean.class); // cleans up automatically at the remote end
				*/
				
			}
			
			
		}catch(RuntimeException ex)
		{
			logger.error(Constants.STACKTRACE, ex);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(Constants.STACKTRACE, e);
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			logger.error(Constants.STACKTRACE, e);
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
		SesameJenaConstructStatement thisSt = null;
		try
		{
			if(databaseType == IDatabaseEngine.DATABASE_TYPE.SESAME)
			{
				thisSt = new SesameJenaConstructStatement();
				logger.debug("Adding a sesame statement ");
				org.openrdf.model.Statement stmt = gqr.next();
				thisSt.setSubject(stmt.getSubject()+"");
				thisSt.setObject(stmt.getObject());
				thisSt.setPredicate(stmt.getPredicate() + "");
				
			}
			else if(databaseType == IDatabaseEngine.DATABASE_TYPE.JENA)
			{
				thisSt = new SesameJenaConstructStatement();
				org.apache.jena.rdf.model.Statement stmt = si.next();
				logger.debug("Adding a JENA statement ");
				curSt = stmt;
				org.apache.jena.rdf.model.Resource sub = stmt.getSubject();
				org.apache.jena.rdf.model.Property pred = stmt.getPredicate();
				org.apache.jena.rdf.model.RDFNode node = stmt.getObject();
				if(node.isAnon())
					thisSt.setPredicate(Utility.getNextID());
				else 	
					thisSt.setPredicate(stmt.getPredicate() + "");

				if(sub.isAnon())
					thisSt.setSubject(Utility.getNextID());
				else
					thisSt.setSubject(stmt.getSubject()+"");
				
				if(node.isAnon())
					thisSt.setObject(Utility.getNextID());
				else
					thisSt.setObject(stmt.getObject());
			}
			else if(databaseType == IDatabaseEngine.DATABASE_TYPE.SEMOSS_SESAME_REMOTE)
			{
				thisSt = retSt;
				retSt = null;
			}

		}catch(RuntimeException ex)
		{
			logger.error(Constants.STACKTRACE, ex);
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			logger.error(Constants.STACKTRACE, e);
		}
		return thisSt;
	}	

	/**
	 * Method getJenaStatement.  Gets the query solution for a JENA model.
	
	 * @return com.hp.hpl.jena.rdf.model.Statement */
	public org.apache.jena.rdf.model.Statement getJenaStatement()
	{
		return curSt;
	}
	
	/**
	 * Method setEngineType. Sets the engine type.
	 * @param databaseType Enum - The type engine that this is being set to.
	 */
	public void setEngineType(DATABASE_TYPE databaseType)
	{
		this.databaseType = databaseType;
	}

//	public static void main(String [] args) throws Exception
//	{
//		RemoteSemossSesameEngine engine = new RemoteSemossSesameEngine();
//		engine.setAPI("http://localhost:9080/Monolith/api/engine");
//		engine.setDatabase("Movie_DB");
//		engine.setEngineId("Movie_DB");
//		
//		engine.open(null);
//		
////		System.out.println("Perspectives is .... " + engine.getPerspectives());
//		
//		System.out.println("Trying.. ");
//		SesameJenaConstructWrapper sjcw = new SesameJenaConstructWrapper(); //(SesameJenaSelectWrapper) engine.execSelectQuery("SELECT ?S ?P ?O WHERE {{?S ?P ?O}.} LIMIT 1");
//		sjcw.setEngine(engine);
//		sjcw.setEngineType(engine.getEngineType());
//		sjcw.setQuery("CONSTRUCT {?subject ?predicate ?object} WHERE {{?subject ?predicate ?object.}}");
//		
//		sjcw.execute();
//		
//		System.out.println(" has next " + sjcw.hasNext());
//		SesameJenaConstructStatement st = sjcw.next();
//		
//		System.out.println(st.getSubject());
//		
//		//System.out.println(" var " + sjcw.getVariables());
//		
//	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
