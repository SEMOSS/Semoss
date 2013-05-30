package prerna.rdf.engine.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResult;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Utility;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

public class SesameJenaSelectWrapper {
	
	// sets the graph query result for the sesame
	TupleQueryResult tqr = null;
	
	// sets the jena model
	ResultSet rs = null;
	
	// sets the type
	// defaulted to sesame
	Enum engineType = IEngine.ENGINE_TYPE.SESAME;
	
	// Object curStatement
	QuerySolution curSt = null;
	
	IEngine engine = null;
	
	String query = null;
	
	Logger logger = Logger.getLogger(getClass());
	
	String [] var = null;
	
	public void setEngine(IEngine engine)
	{
		logger.debug("Set the engine " );
		this.engine = engine;
		if(engine == null) engineType = IEngine.ENGINE_TYPE.JENA;
		else engineType = engine.getEngineType();
	}
	
	public void setQuery(String query)
	{
		logger.debug("Setting the query " + query);
		this.query = query;
	}
	
	public void executeQuery()
	{
		if(engineType == IEngine.ENGINE_TYPE.SESAME)
			tqr = (TupleQueryResult) engine.execSelectQuery(query);
		else if(engineType == IEngine.ENGINE_TYPE.JENA)
			rs = (ResultSet)engine.execSelectQuery(query);
	}
	
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
				for(int colIndex = 0;colIndex < names.size();var[colIndex] = names.get(colIndex), colIndex++);
			}
		}
		return var;
	}
	
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
			else
			{
				retBool = rs.hasNext();
			}
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		logger.debug(" Next " + retBool);
		return retBool;
	}
	
	public SesameJenaSelectStatement next()
	{
		SesameJenaSelectStatement retSt = new SesameJenaSelectStatement();
		try
		{
			if(engineType == IEngine.ENGINE_TYPE.SESAME)
			{
				logger.debug("Adding a sesame statement ");
				BindingSet bs = tqr.next();
				for(int colIndex = 0;colIndex < var.length;colIndex++)
				{
					Object val = bs.getValue(var[colIndex]);
					Double weightVal = null;
					String dateStr=null;
					try
					{
						if(val != null && val instanceof Literal)
						{
							if((val.toString()).contains("http://www.w3.org/2001/XMLSchema#dateTime")){
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
					}catch(Exception ex)
					{
						logger.debug(ex);
					}
					String value = bs.getValue(var[colIndex])+"";
					String instanceName = Utility.getInstanceName(value);
					if(weightVal == null && dateStr==null && val != null)
						retSt.setVar(var[colIndex], instanceName);
					else if (weightVal != null)
						retSt.setVar(var[colIndex], weightVal);
					else if (dateStr != null)
						retSt.setVar(var[colIndex], dateStr);
					retSt.setRawVar(var[colIndex], val);
					logger.debug("Binding Name " + var[colIndex]);
					logger.debug("Binding Value " + value);
				}
			}
			else
			{
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
						retSt.setVar(var[colIndex], id);
					}
					else
					{
						
						logger.debug("Raw data JENA For Column " +  var[colIndex]+" >>  " + value);
						String instanceName = Utility.getInstanceName(value);
						retSt.setVar(var[colIndex], instanceName);
					}
					retSt.setRawVar(var[colIndex], value);
					logger.debug("Binding Name " + var[colIndex]);
					logger.debug("Binding Value " + value);
				}
			    logger.debug("Adding a JENA statement ");
			}
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return retSt;
	}

	//returns full URIs instead of just the instances
	//has a check so that property values are only sent once
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
					}catch(Exception ex)
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
			else
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
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return retSt;
	}
	
	public QuerySolution getJenaStatement()
	{
		// I need this only if the sucker is a JENA else I need to create it any which way
		return curSt;
	}
	
	public void setEngineType(Enum engineType)
	{
		this.engineType = engineType;
	}
	
	public void setResultSet(ResultSet rs)
	{
		this.rs = rs;
	}
}
