package prerna.rdf.engine.wrappers;

import java.util.List;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.util.Utility;

public class JenaSelectWrapper extends AbstractWrapper implements ISelectWrapper {
	
	transient ResultSet rs = null;
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return rs.hasNext();
	}

	@Override
	public ISelectStatement next() 
	{
		ISelectStatement thisSt = new SelectStatement();
	    QuerySolution row = rs.nextSolution();
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
		return thisSt;
	}

	@Override
	public String[] getVariables() {
		var = new String[rs.getResultVars().size()];
		List <String> names = rs.getResultVars();
		for(int colIndex = 0;colIndex < names.size();var[colIndex] = names.get(colIndex), colIndex++);
		return var;
	}

	@Override
	public void execute() {
		rs = (ResultSet) engine.execSelectQuery(query);		
	}

}
