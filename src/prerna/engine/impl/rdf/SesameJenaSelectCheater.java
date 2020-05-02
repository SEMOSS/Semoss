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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import prerna.engine.api.IEngine;
import prerna.util.Utility;

/**
 * This takes a select query and turns it over into a Construct Statement
 * it assumes the first one is subject, second one is predicate and third one is object
 */
@Deprecated
public class SesameJenaSelectCheater extends SesameJenaConstructWrapper{
	
	public transient TupleQueryResult tqr = null;
	transient ResultSet rs = null;	
	transient Enum engineType = IEngine.ENGINE_TYPE.SESAME;	
	transient QuerySolution curSt = null;	
	//IEngine engine = null;
	transient BindingSet bs;
	transient String query = null;	
	static final Logger logger = LogManager.getLogger(SesameJenaSelectCheater.class);
	transient int count=0;
	String [] var = null;
	transient int tqrCount=0;
	transient int triples;
	String queryVar[];
	transient SesameJenaSelectCheater proxy = null;
	private static final String STACKTRACE = "StackTrace: ";

	
	/**
	 * Method setEngine. Sets the engine.
	 * @param engine IEngine - The engine that this is being set to.
	 */
	@Override
	public void setEngine(IEngine engine)
	{
		logger.debug("Set the engine " );
		this.engine = engine;
		engineType = engine.getEngineType();
	}
	
	/**
	 * Method setQuery. - Sets the SPARQL query statement.
	 * @param query String - The string version of the SPARQL query.
	 */
	@Override
	public void setQuery(String query)
	{
		logger.debug("Setting the query " + query);
		this.query = query;
	}

	/**
	 * Method execute. - Executes the SPARQL query based on the type of engine selected, and processes the variables.
	 * @throws Exception 
	 */
	@Override
	public void execute() throws Exception
	{
		if(engineType == IEngine.ENGINE_TYPE.SESAME)
		{
			tqr = (TupleQueryResult) engine.execQuery(query);
			getVariables();
			
			processSelectVar();
			count=0;
		}
		else if(engineType == IEngine.ENGINE_TYPE.JENA)
		{
			rs = (ResultSet)engine.execQuery(query);
			getVariables();
			
			processSelectVar();
			count=0;
			
		}
		else if(engineType == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
		{
			// get the actual SesameJenaConstructWrapper from the engine
			// this is json output
			logger.info("Trying to get the wrapper remotely now");
			processSelectVar();
			count = 0;
			proxy = (SesameJenaSelectCheater)((RemoteSemossSesameEngine)(engine)).execCheaterQuery(query);
		}
	}
	
	/**
	 * Method getVariables.  Based on the type of engine, this returns the variables from the query result.
	
	 * @return String[] - An array containing the names of the variables from the result.
	 * */
	public String[] getVariables()
	{
		try {
			if(var == null)
			{
				if(engineType == IEngine.ENGINE_TYPE.SESAME)
				{
					var = new String[tqr.getBindingNames().size()];
					List <String> names = tqr.getBindingNames();
					for(int colIndex = 0;colIndex < names.size();var[colIndex] = names.get(colIndex), colIndex++);
				}
				else if(engineType == IEngine.ENGINE_TYPE.JENA)
				{
					var = new String[rs.getResultVars().size()];
					List <String> names = rs.getResultVars();
					for(int colIndex = 0;
							colIndex < names.size();
							var[colIndex] = names.get(colIndex), colIndex++);
				}
				else if(engineType == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
				{
					var = proxy.getVariables();
				}

			}
		} catch (Exception e) {
			logger.error(STACKTRACE, e);
		}
		return var;
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
				retBool = tqr.hasNext();
				if(!retBool)
					tqr.close();
			}
			else if(engineType == IEngine.ENGINE_TYPE.JENA)
			{
				retBool = rs.hasNext();
			}
			else if(engineType == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
			{
				if(retSt != null) // they have not taken the previous one yet
					return true;
				retSt = new SesameJenaConstructStatement();
				
				// I need to pull from remote
				// this is just so stupid to call its own
				if(ris == null)
				{
					Hashtable params = new Hashtable<String,String>();
					params.put("id", proxy.getRemoteId());
					ris = new ObjectInputStream(Utility.getStream(proxy.getRemoteAPI() + "/next", params));
				}	

				if(count==0)
				{
					Object myObject = ris.readObject();
					if(!myObject.toString().equalsIgnoreCase("null"))
					{
						bs = (BindingSet)myObject;
						retBool = true;
					}
					//tqrCount++;
					//logger.info(tqrCount);
				}
				logger.debug("Adding a sesame statement ");
				
				// there should only be three values

				Object sub=null;
				Object pred = null;
				Object obj = null;
				while (sub==null || pred==null || obj==null)
				{
					if (count==triples)
					{
						count=0;
						Object myObject = ris.readObject();
						if(!myObject.toString().equalsIgnoreCase("null"))
						{
							bs = (BindingSet)myObject;
							tqrCount++;
						}
						else
						{
							try{
								if(ris!=null) {
									ris.close();
								}
							} catch(IOException e) {
								logger.error(STACKTRACE, e);
							}
						}
					}
					sub = bs.getValue(queryVar[count*3].substring(1));
					pred = bs.getValue(queryVar[count*3+1].substring(1));
					obj = bs.getValue(queryVar[count*3+2].substring(1));
					count++;
				}
				retSt.setSubject(sub+"");
				retSt.setPredicate(pred+"");
				retSt.setObject(obj);
				if (count==triples)
				{
					count=0;
				}
				retBool = true;
			}
		}catch(RuntimeException ex)
		{
			ex.printStackTrace();
			retBool = false;
		} catch (ClassNotFoundException e) {
			logger.error(STACKTRACE, e);
			retBool = false;
		} catch (IOException ioe) {
			logger.error(STACKTRACE, ioe);
			retBool = false;
		} catch (QueryEvaluationException ex) {
			logger.error(STACKTRACE, ex);
			retBool = false;
		}
		logger.debug(" Next " + retBool);
		return retBool;
	}
	
	/**
	 * Method next.  Processes the selelct transition to construct statement for either Sesame or Jena.
	
	 * @return SesameJenaConstructStatement - returns the construct statement.
	 * */
	@Override
	public SesameJenaConstructStatement next()
	{
		SesameJenaConstructStatement thisSt = null;
		
		try
		{	
				
			if(engineType == IEngine.ENGINE_TYPE.SESAME)
			{
				thisSt = new SesameJenaConstructStatement();
				if(count==0)
				{
					bs = tqr.next();
					//tqrCount++;
					//logger.info(tqrCount);
				}
				logger.debug("Adding a sesame statement ");
				
				// there should only be three values

				Object sub=null;
				Object pred = null;
				Object obj = null;
				while (sub==null || pred==null || obj==null)
				{
					if (count==triples)
					{
						count=0;
						bs = tqr.next();
						tqrCount++;
					}
					sub = bs.getValue(queryVar[count*3].substring(1));
					pred = bs.getValue(queryVar[count*3+1].substring(1));
					obj = bs.getValue(queryVar[count*3+2].substring(1));
					count++;
				}
				thisSt.setSubject(sub+"");
				thisSt.setPredicate(pred+"");
				thisSt.setObject(obj);
				if (count==triples)
				{
					count=0;
				}
			}
			else if(engineType == IEngine.ENGINE_TYPE.JENA)
			{
				thisSt = new SesameJenaConstructStatement();
			    logger.debug("Adding a JENA statement ");
			    QuerySolution row = rs.nextSolution();
			    thisSt.setSubject(row.get(var[0])+"");
			    thisSt.setPredicate(row.get(var[1])+"");
			    thisSt.setObject(row.get(var[2]));
			}			
			else if(engineType == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
			{
				thisSt = retSt;
				retSt = null;
			}

		} catch (Exception ex) {
			logger.error(STACKTRACE, ex);
		}
		return thisSt;
	}

	/**
	 * Method processSelectVar - Processes the select query based on its contents and converts to construct.
	 */
	public void processSelectVar()
	{
		if(query.contains("DISTINCT"))
		{
			Pattern pattern = Pattern.compile("SELECT DISTINCT(.*?)WHERE");
		    Matcher matcher = pattern.matcher(query);
		    String varString = null;
		    while (matcher.find()) 
		    {
		    	varString = matcher.group(1);
		    }
		    
		    if (varString != null) {
			    varString = varString.trim();
			    queryVar = varString.split(" ");
			    int num = queryVar.length+1;
			    triples = num/3;
		    }
		}
		else
		{
			Pattern pattern = Pattern.compile("SELECT (.*?)WHERE");
		    Matcher matcher = pattern.matcher(query);
		    String varString = null;
		    while (matcher.find()) {
		        varString = matcher.group(1);
		    }

		    if (varString != null) {
		    	varString = varString.trim();
		    	queryVar = varString.split(" ");
			    int num = queryVar.length+1;
			    triples = num/3;
		    }
		}
	}
	
	/**
	 * Method setEngineType. Sets the engine type.
	 * @param engineType Enum - The type engine that this is being set to.
	 */
	@Override
	public void setEngineType(Enum engineType)
	{
		this.engineType = engineType;
	}
}
