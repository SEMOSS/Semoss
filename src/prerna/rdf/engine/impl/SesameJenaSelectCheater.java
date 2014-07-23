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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Hashtable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.DatatypeConverter;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResult;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Utility;

import com.google.gson.Gson;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

/**
 * This takes a select query and turns it over into a Construct Statement
 * it assumes the first one is subject, second one is predicate and third one is object
 */
public class SesameJenaSelectCheater extends SesameJenaConstructWrapper{
	
	transient TupleQueryResult tqr = null;
	transient ResultSet rs = null;	
	transient Enum engineType = IEngine.ENGINE_TYPE.SESAME;	
	transient QuerySolution curSt = null;	
	//IEngine engine = null;
	transient BindingSet bs;
	transient String query = null;	
	transient Logger logger = Logger.getLogger(getClass());
	transient int count=0;
	String [] var = null;
	transient int tqrCount=0;
	transient int triples;
	String queryVar[];
	transient SesameJenaSelectCheater proxy = null;
	
	/**
	 * Method setEngine. Sets the engine.
	 * @param engine IEngine - The engine that this is being set to.
	 */
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
	public void setQuery(String query)
	{
		logger.debug("Setting the query " + query);
		this.query = query;
	}

	/**
	 * Method execute. - Executes the SPARQL query based on the type of engine selected, and processes the variables.
	 */
	public void execute()
	{
		if(engineType == IEngine.ENGINE_TYPE.SESAME)
		{
			tqr = (TupleQueryResult) engine.execSelectQuery(query);
			getVariables();
			
			processSelectVar();
			count=0;
		}
		else if(engineType == IEngine.ENGINE_TYPE.JENA)
		{
			rs = (ResultSet)engine.execSelectQuery(query);
			getVariables();
			
			processSelectVar();
			count=0;
			
		}
		else if(engineType == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
		{
			// get the actual SesameJenaConstructWrapper from the engine
			// this is json output
			System.out.println("Trying to get the wrapper remotely now");
			proxy = (SesameJenaSelectCheater)((RemoteSemossSesameEngine)(engine)).execCheaterQuery(query);
		}
	}
	
	/**
	 * Method getVariables.  Based on the type of engine, this returns the variables from the query result.
	
	 * @return String[] - An array containing the names of the variables from the result.
	 * */
	public String[] getVariables()
	{
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
				// I need to pull from remote
				// this is just so stupid to call its own
				Hashtable params = new Hashtable<String,String>();
				params.put("id", proxy.getRemoteID());
				System.out.println("ID for remote is " + proxy.getRemoteID());
				String output = Utility.retrieveResult(proxy.getRemoteAPI() + "/hasNext", params);
				Gson gson = new Gson();
				retBool = gson.fromJson(output, Boolean.class); // cleans up automatically at the remote end
			}
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		logger.debug(" Next " + retBool);
		return retBool;
	}
	
	/**
	 * Method next.  Processes the selelct transition to construct statement for either Sesame or Jena.
	
	 * @return SesameJenaConstructStatement - returns the construct statement.
	 * */
	public SesameJenaConstructStatement next()
	{
		SesameJenaConstructStatement retSt = new SesameJenaConstructStatement();
		try
		{	
				
			if(engineType == IEngine.ENGINE_TYPE.SESAME)
			{
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
						//logger.info(tqrCount);
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
			}
			else if(engineType == IEngine.ENGINE_TYPE.JENA)
			{
			    logger.debug("Adding a JENA statement ");
			    QuerySolution row = rs.nextSolution();
			    retSt.setSubject(row.get(var[0])+"");
			    retSt.setPredicate(row.get(var[1])+"");
			    retSt.setObject(row.get(var[2]));
			}			
			else if(engineType == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
			{
				// I need to pull from remote
				// this is just so stupid to call its own
				// unserialize directly from the java object
				
				// I need to pull from remote
				// this is just so stupid to call its own
				Hashtable params = new Hashtable<String,String>();
				params.put("id", proxy.getRemoteID());
				String output = Utility.retrieveResult(proxy.getRemoteAPI() + "/next", params);
				Gson gson = new Gson();
				retSt = gson.fromJson(output, SesameJenaConstructStatement.class); // cleans up automatically at the remote end
				
				ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(DatatypeConverter.parseBase64Binary(retSt.serialRep)));
				((SesameJenaConstructStatement)(retSt)).setSubject((String)(ois.readObject()));
				((SesameJenaConstructStatement)(retSt)).setPredicate((String)(ois.readObject()));
				((SesameJenaConstructStatement)(retSt)).setObject((Object)(ois.readObject()));

			}

		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return retSt;
	}

	/**
	 * Method processSelectVar - Processes the select query based on its contents and converts to construct.
	 */
	public void processSelectVar()
	{
		query.toUpperCase();
		if(query.contains("DISTINCT"))
		{
			Pattern pattern = Pattern.compile("SELECT DISTINCT(.*?)WHERE");
		    Matcher matcher = pattern.matcher(query);
		    String varString = null;
		    while (matcher.find()) 
		    {
		    	varString = matcher.group(1);
		    }
		    varString = varString.trim();
		    queryVar = varString.split(" ");
		    int num = queryVar.length+1;
		    triples = (int) Math.floor(num/3);
		}
		else
		{
			Pattern pattern = Pattern.compile("SELECT (.*?)WHERE");
		    Matcher matcher = pattern.matcher(query);
		    String varString = null;
		    while (matcher.find()) {
		        varString = matcher.group(1);
		    }
		    varString = varString.trim();
		    queryVar = varString.split(" ");
		    int num = queryVar.length+1;
		    triples = (int) Math.floor(num/3);
		}
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
