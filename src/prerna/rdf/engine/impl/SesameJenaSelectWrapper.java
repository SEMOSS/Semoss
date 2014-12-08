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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Utility;

import com.google.gson.Gson;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

/**
 * The wrapper helps takes care of selection of the type of engine you are using (Jena/Sesame).  This wrapper processes SELECT statements. 
 */
public class SesameJenaSelectWrapper extends AbstractWrapper{
	public transient TupleQueryResult tqr = null;
	transient ResultSet rs = null;
	transient Enum engineType = IEngine.ENGINE_TYPE.SESAME;
	transient QuerySolution curSt = null;	
	transient public IEngine engine = null;
	transient String query = null;
	static final Logger logger = LogManager.getLogger(SesameJenaSelectWrapper.class.getName());
	transient SesameJenaSelectWrapper remoteWrapperProxy = null;
	transient SesameJenaSelectStatement retSt = null;
	transient ObjectInputStream ris = null;
	String [] var = null;
	
	/**
	 * Method setEngine.  Sets the engine type.
	 * @param engine IEngine - The engine type being set.
	 */
	public void setEngine(IEngine engine)
	{
		logger.debug("Set the engine " );
		this.engine = engine;
		if(engine == null) engineType = IEngine.ENGINE_TYPE.JENA;
		else engineType = engine.getEngineType();
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
	 * Method executeQuery.  Executes the SPARQL query based on the type of engine selected.
	 */
	public void executeQuery()
	{
		if(engineType == IEngine.ENGINE_TYPE.SESAME)
			tqr = (TupleQueryResult) engine.execSelectQuery(query);
		else if(engineType == IEngine.ENGINE_TYPE.JENA)
			rs = (ResultSet) engine.execSelectQuery(query);
		else if(engineType == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
		{
			// get the actual SesameJenaConstructWrapper from the engine
			// this is json output
			System.out.println("Trying to get the wrapper remotely now");
			remoteWrapperProxy = (SesameJenaSelectWrapper)engine.execSelectQuery(query);
			var = remoteWrapperProxy.var;
			System.out.println("Output variables is " + remoteWrapperProxy.getVariables());
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
					for(int colIndex = 0;colIndex < names.size();var[colIndex] = names.get(colIndex), colIndex++);
				}
				else if(engineType == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
				{
					var = remoteWrapperProxy.getVariables();
				}

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
				if(retSt != null) // this means they have not picked it up yet
					return true;
				retSt = new SesameJenaSelectStatement();
				
				// I need to pull from remote
				// this is just so stupid to call its own
				if(ris == null)
				{
					Hashtable params = new Hashtable<String,String>();
					params.put("id", remoteWrapperProxy.getRemoteID());
					ris = new ObjectInputStream(Utility.getStream(remoteWrapperProxy.getRemoteAPI() + "/next", params));
				}	
				Object myObject = ris.readObject();
				if(!myObject.toString().equalsIgnoreCase("null"))
				{
					BindingSet bs = (BindingSet)myObject;
					//System.out.println("Proceeded to first");
					retSt = getSJSSfromBinding(bs);
					retBool = true;
				}
			}

		}catch(RuntimeException ex)
		{
			ex.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		logger.debug(" Next " + retBool);
		return retBool;
	}
	
	/**
	 * Method next.  Processes the select statement for either Sesame or Jena.
	
	 * @return SesameJenaSelectStatement - returns the select statement.
	 * */
	public SesameJenaSelectStatement next()
	{
		//retSt.remote = remote;
		SesameJenaSelectStatement thisSt = null;
		try
		{
			if(engineType == IEngine.ENGINE_TYPE.SESAME)
			{
				thisSt = new SesameJenaSelectStatement();
				logger.debug("Adding a sesame statement ");
				BindingSet bs = tqr.next();
				thisSt = getSJSSfromBinding(bs);
			}
			else if(engineType == IEngine.ENGINE_TYPE.JENA)
			{
				thisSt = new SesameJenaSelectStatement();
			    QuerySolution row = rs.nextSolution();
			    curSt = row; 
				String [] values = new String[var.length];
				for(int colIndex = 0;colIndex < var.length;colIndex++)
				{
					String value = row.get(var[colIndex])+"";
					RDFNode node = row.get(var[colIndex]);
					if(node.isAnon())
					{
						logger.debug("Ok.. an anon node");
						String id = Utility.getNextID();
						thisSt.setVar(var[colIndex], id);
					}
					else
					{
						
						logger.debug("Raw data JENA For Column " +  var[colIndex]+" >>  " + value);
						String instanceName = Utility.getInstanceName(value);
						thisSt.setVar(var[colIndex], instanceName);
					}
					thisSt.setRawVar(var[colIndex], value);
					logger.debug("Binding Name " + var[colIndex]);
					logger.debug("Binding Value " + value);
				}
			    logger.debug("Adding a JENA statement ");
			}
			else if(engineType == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
			{
				thisSt = retSt;
				retSt = null;
			}

		}catch(RuntimeException ex)
		{
			ex.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return thisSt;
	}
	
	private SesameJenaSelectStatement getSJSSfromBinding(BindingSet bs)
	{
		SesameJenaSelectStatement sjss = new SesameJenaSelectStatement();
		for(int colIndex = 0;colIndex < var.length;colIndex++)
		{
			Object val = bs.getValue(var[colIndex]);
			Double weightVal = null;
			String dateStr=null;
			String stringVal = null;
			try
			{
				if(val != null && val instanceof Literal)
				{
					if(QueryEvaluationUtil.isStringLiteral((Value) val)){
						stringVal = ((Literal)val).getLabel();
					}
					else if((val.toString()).contains("http://www.w3.org/2001/XMLSchema#dateTime")){
						dateStr = (val.toString()).substring((val.toString()).indexOf("\"")+1, (val.toString()).lastIndexOf("\""));
					}
					else{
						logger.debug("This is a literal impl >>>>>> "  + ((Literal)val).doubleValue());
						weightVal = new Double(((Literal)val).doubleValue());
					}
				}else if(val != null && val instanceof com.hp.hpl.jena.rdf.model.Literal)
				{
					logger.debug("Class is " + val.getClass());
					weightVal = new Double(((Literal)val).doubleValue());
				}
			}catch(RuntimeException ex)
			{
				logger.debug(ex);
			}
			String value = bs.getValue(var[colIndex])+"";
			String instanceName = Utility.getInstanceName(value);
			if(weightVal == null && dateStr==null && stringVal==null && val != null)
				sjss.setVar(var[colIndex], instanceName);
			else if (weightVal != null)
				sjss.setVar(var[colIndex], weightVal);
			else if (dateStr != null)
				sjss.setVar(var[colIndex], dateStr);
			else if (stringVal != null)
				sjss.setVar(var[colIndex], stringVal);
			else if(val == null) {
				sjss.setVar(var[colIndex], "");
				continue;
			}
			sjss.setRawVar(var[colIndex], val);
			logger.debug("Binding Name " + var[colIndex]);
			logger.debug("Binding Value " + value);
		}		
		return sjss;
	}

	/**
	 * Method BVnext. Returns full URIs instead of just the instances of the select statement.
	 * This contains a check so that property values are only sent once.
	
	 * @return SesameJenaSelectStatement - The full URL version of the select statement. */
	public SesameJenaSelectStatement BVnext()
	{
		SesameJenaSelectStatement retSt = new SesameJenaSelectStatement();
		ArrayList<String> checker = new ArrayList<String>();
		try
		{
			if(engineType == IEngine.ENGINE_TYPE.SESAME)
			{
				logger.debug("Adding a sesame statement ");
				BindingSet bs = tqr.next();
				String [] values = new String[var.length];
				for(int colIndex = 0;colIndex < var.length;colIndex++)
				{
					//if(checker.contains(bs.getValue(var[0])+""+ bs.getValue(var[1])+bs.getValue(var[3])))return null;
					Object val = bs.getValue(var[colIndex]);
					Double weightVal = null;
					try
					{
						if(val != null && val instanceof Literal)
						{
							logger.debug("This is a literal impl >>>>>> "  + ((Literal)val).doubleValue());
							weightVal = new Double(((Literal)val).doubleValue());
						}else if(val != null && val instanceof com.hp.hpl.jena.rdf.model.Literal)
						{
							logger.info("Class is " + val.getClass());
							weightVal = new Double(((Literal)val).doubleValue());
						}
					}catch(RuntimeException ex)
					{
						logger.debug(ex);
					}
					String value = bs.getValue(var[colIndex])+"";
					if(weightVal == null && val != null)
						retSt.setVar(var[colIndex], value);
					else if (weightVal != null)
						retSt.setVar(var[colIndex], weightVal);
					logger.debug("Binding Name " + var[colIndex]);
					logger.debug("Binding Value " + value);
				}
				//need to figure out what the checker should hold
				//checker.add(bs.getValue(var[0])+""+ bs.getValue(var[1])+bs.getValue(var[3]));
			}
			else if (engineType == IEngine.ENGINE_TYPE.JENA)
			{
			    QuerySolution row = rs.nextSolution();
			    curSt = row;
				String [] values = new String[var.length];
				for(int colIndex = 0;colIndex < var.length;colIndex++)
				{
					String value = row.get(var[colIndex])+"";
					String instanceName = Utility.getInstanceName(value);
					retSt.setVar(var[colIndex], instanceName);
					logger.debug("Binding Name " + var[colIndex]);
					logger.debug("Binding Value " + value);
				}
			    logger.debug("Adding a JENA statement ");
			}
			else if(engineType == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
			{
				// I need to pull from remote
				// this is just so stupid to call its own
				Hashtable params = new Hashtable<String,String>();
				params.put("id", remoteWrapperProxy.getRemoteID());
				System.out.println("ID for remote is " + remoteWrapperProxy.getRemoteID());
				String output = Utility.retrieveResult(remoteWrapperProxy.getRemoteAPI() + "/bvnext", params);
				Gson gson = new Gson();
				retSt = gson.fromJson(output, SesameJenaSelectStatement.class); // cleans up automatically at the remote end				
			}
		}catch(RuntimeException ex)
		{
			ex.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retSt;
	}
	
	/**
	 * Method getJenaStatement.  Gets the query solution for a JENA model.
	
	 * @return QuerySolution */
	public QuerySolution getJenaStatement()
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
	
	/**
	 * Method setResultSet.  Sets the result set.
	 * @param rs ResultSet - The result set.
	 */
	public void setResultSet(ResultSet rs)
	{
		this.rs = rs;
	}
	
	
	public static void main(String [] args)
	{
		RemoteSemossSesameEngine engine = new RemoteSemossSesameEngine();
		engine.setAPI("http://localhost:9080/Monolith/api/engine");
		engine.setDatabase("Movie_DB");
		engine.setEngineName("Movie_DB");
		
		engine.openDB(null);
		
		System.out.println("Perspectives is .... " + engine.getPerspectives());
		
		System.out.println("Trying.. ");
		SesameJenaSelectWrapper sjcw = new SesameJenaSelectWrapper(); //(SesameJenaSelectWrapper) engine.execSelectQuery("SELECT ?S ?P ?O WHERE {{?S ?P ?O}.} LIMIT 1");
		sjcw.setEngine(engine);
		sjcw.setEngineType(engine.getEngineType());
		sjcw.setQuery("SELECT ?subject WHERE {{?subject ?predicate ?object.}}");
		
		sjcw.executeQuery();
		
		System.out.println(" has next " + sjcw.hasNext());
		SesameJenaSelectStatement st = sjcw.next();
		System.out.println("Variables is "+ sjcw.getVariables());
		
		System.out.println(st.propHash);
		
		//System.out.println(" var " + sjcw.getVariables());
		
	}

}
