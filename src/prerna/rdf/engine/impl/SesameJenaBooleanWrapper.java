package prerna.rdf.engine.impl;

import org.apache.log4j.Logger;
import org.openrdf.query.GraphQueryResult;

import prerna.rdf.engine.api.IEngine;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.StmtIterator;

public class SesameJenaBooleanWrapper {
	
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
	
	public boolean execute()
	{
		boolean ret= false;
		try {

			ret = engine.execAskQuery(query);

			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ret;
	}
	

}
