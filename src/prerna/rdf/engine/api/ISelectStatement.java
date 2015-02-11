package prerna.rdf.engine.api;

import java.util.Hashtable;

public interface ISelectStatement {

	public Object getVar(Object var);

	public void setVar(Object key, Object value);

	public void setRawVar(Object key, Object value);
	
	public Object getRawVar(Object var);
	
	public void setPropHash(Hashtable propHash);
	
	public void setRPropHash(Hashtable rawPropHash);
	
	public Hashtable getPropHash();

	public Hashtable getRPropHash();

}
