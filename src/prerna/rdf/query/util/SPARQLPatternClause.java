package prerna.rdf.query.util;

import java.util.ArrayList;

public class SPARQLPatternClause{

	ArrayList<SPARQLTriple> triples = new ArrayList<SPARQLTriple>();
	ArrayList<SPARQLBind> binds = new ArrayList<SPARQLBind>();
	ArrayList<SPARQLFilter> filters = new ArrayList<SPARQLFilter>();
	String clauseString;
	
	public void addTriple(SPARQLTriple triple)
	{
		triples.add(triple);
	}
	
	public void addBind(SPARQLBind bind)
	{
		binds.add(bind);
	}
	
	public void addFilter(SPARQLFilter filter)
	{
		filters.add(filter);
	}
	
	public String getClauseString()
	{
		clauseString ="";
		addAllBindsToClause();
		addAllTriplesToClause();
		addAllFiltersToClause();
		return clauseString;
	}
	
	private void addAllTriplesToClause()
	{
		for (int triIdx=0; triIdx<triples.size();triIdx++)
		{
			//space out the variables
			clauseString = clauseString + triples.get(triIdx).getTripleString()+" ";
		}
	}
	
	private void addAllBindsToClause()
	{
		for (int bindIdx=0; bindIdx<binds.size();bindIdx++)
		{
			//space out the variables
			clauseString = clauseString + binds.get(bindIdx).getBindString()+" ";
		}
	}
	
	public boolean hasTriple(SPARQLTriple triple)
	{
		boolean retBoolean = false;
		getClauseString();
		if(clauseString.contains(triple.getTripleString()))
			retBoolean=true;
		
		return retBoolean;
	}
	
	private void addAllFiltersToClause()
	{
		
	}
}
