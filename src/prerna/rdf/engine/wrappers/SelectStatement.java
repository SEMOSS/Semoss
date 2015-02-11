package prerna.rdf.engine.wrappers;

import java.util.Hashtable;

import prerna.rdf.engine.api.ISelectStatement;

public class SelectStatement implements ISelectStatement {
	
	transient public Hashtable propHash = new Hashtable();
	transient public Hashtable rawPropHash = new Hashtable();
	String serialRep = null;

	public Object getVar(Object var) {
		Object retVal = propHash.get(var);
		return retVal;
	}

	public Object getRawVar(Object var) {
		// TODO Auto-generated method stub
		return rawPropHash.get(var);
	}

	public void setPropHash(Hashtable propHash) {
		this.propHash = propHash;
	}

	public void setRPropHash(Hashtable rawPropHash) {
		// TODO Auto-generated method stub
		this.rawPropHash = rawPropHash;
	}

	public Hashtable getPropHash() {
		// TODO Auto-generated method stub
		return propHash;
	}

	public Hashtable getRPropHash() {
		// TODO Auto-generated method stub
		return rawPropHash;
	}

	
	@Override
	public void setVar(Object key, Object value) {
		propHash.put(key, value);
		
	}

	@Override
	public void setRawVar(Object key, Object value) {
		rawPropHash.put(key, value);		
	}

}
