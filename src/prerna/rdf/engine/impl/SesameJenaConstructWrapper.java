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

public class SesameJenaConstructWrapper {
	
	// sets the graph query result for the sesame
	GraphQueryResult gqr = null;
	
	// sets the jena model
	Model model = null;
	
	// sets the statement iterator
	StmtIterator si = null;
	
	IEngine engine = null;
	// sets the type
	// defaulted to sesame
	Enum engineType = IEngine.ENGINE_TYPE.SESAME;
	String query = null;
	// Object curStatement
	com.hp.hpl.jena.rdf.model.Statement curSt = null;
	
	Logger logger = Logger.getLogger(getClass());
	
	public void setGqr(GraphQueryResult gqr)
	{
		this.gqr = gqr;
	}
	
	public void setEngine(IEngine engine)
	{
		this.engine = engine;
		engineType = engine.getEngineType();
	}
	
	public void setQuery(String query)
	{
		this.query = query;
	}
	
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void setModel(Model model)
	{
		this.model = model;
		si = model.listStatements();
	}
	
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
	
	public com.hp.hpl.jena.rdf.model.Statement getJenaStatement()
	{
		// I need this only if the sucker is a JENA else I need to create it any which way
		return curSt;
	}
	
	public void setEngineType(Enum engineType)
	{
		this.engineType = engineType;
	}
}
