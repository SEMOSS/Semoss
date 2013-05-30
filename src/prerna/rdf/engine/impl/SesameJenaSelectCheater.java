package prerna.rdf.engine.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResult;

import prerna.rdf.engine.api.IEngine;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;


// this is a cheater class
// this takes a select query and turns it over into a Construct Statement
// it assumes the first one is subject, second one is predicate and third one is object

public class SesameJenaSelectCheater extends SesameJenaConstructWrapper{
	
	// sets the graph query result for the sesame
	// this is a 
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
		engineType = engine.getEngineType();
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

	public void execute()
	{
		if(engineType == IEngine.ENGINE_TYPE.SESAME)
			tqr = (TupleQueryResult) engine.execSelectQuery(query);
		else if(engineType == IEngine.ENGINE_TYPE.JENA)
			rs = (ResultSet)engine.execSelectQuery(query);
		getVariables();
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
	
	public SesameJenaConstructStatement next()
	{
		SesameJenaConstructStatement retSt = new SesameJenaConstructStatement();
		try
		{
			if(engineType == IEngine.ENGINE_TYPE.SESAME)
			{
				logger.debug("Adding a sesame statement ");
				BindingSet bs = tqr.next();
				
				// there should only be three values
				retSt.setSubject(bs.getValue(var[0]) + "");
				retSt.setPredicate(bs.getValue(var[1]) + "");
				retSt.setObject(bs.getValue(var[2]));				
			}
			else
			{
			    logger.debug("Adding a JENA statement ");
			    QuerySolution row = rs.nextSolution();
			    retSt.setSubject(row.get(var[0])+"");
			    retSt.setPredicate(row.get(var[1])+"");
			    retSt.setObject(row.get(var[2]));
			}
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return retSt;
	}
		
	public void setEngineType(Enum engineType)
	{
		this.engineType = engineType;
	}
}
