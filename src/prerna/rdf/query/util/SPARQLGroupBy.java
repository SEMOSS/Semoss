package prerna.rdf.query.util;

import java.util.ArrayList;



public class SPARQLGroupBy {
	ArrayList<TriplePart> vars;
	public SPARQLGroupBy (ArrayList<TriplePart> vars)
	{
		for(int gIdx = 0; gIdx < vars.size(); gIdx++)
		{
			TriplePart part = vars.get(gIdx);
			if (!part.getType().equals(TriplePart.VARIABLE))
			{
				throw new IllegalArgumentException("One or more tripleParts is not type variable");
			}
		}
		setVariables(vars);
	}
	
	public ArrayList<TriplePart> getVariables()
	{
		return vars;
	}
	
	public void setVariables(ArrayList<TriplePart> vars)
	{
		this.vars = vars;
	}
	
	public String getString()
	{
		String retString = "GROUP BY";
		for(int gIdx = 0; gIdx < vars.size(); gIdx++)
		{
			retString = retString + " " + SPARQLQueryHelper.createComponentString(vars.get(gIdx));
		}
		return retString; 
	}
	

}
