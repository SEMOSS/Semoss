package prerna.rdf.engine.impl;

import java.util.Hashtable;

public class SesameJenaSelectStatement {
	
	Hashtable propHash = new Hashtable();
	Hashtable rawPropHash = new Hashtable();

	public void setVar(Object var, Object val)
	{
		propHash.put(var, val);
	}
	
	public Object getVar(Object var)
	{
		return propHash.get(var);
	}
	
	public void setRawVar(Object var, Object val)
	{
		rawPropHash.put(var, val);
	}
	
	public Object getRawVar(Object var)
	{
		return rawPropHash.get(var);
	}
	
	
}
