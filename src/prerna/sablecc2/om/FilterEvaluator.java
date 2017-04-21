package prerna.sablecc2.om;

import java.util.HashMap;
import java.util.Map;

public abstract class FilterEvaluator {

	/*
	 * This class is just a wrapper so we have a default
	 * method to execute and determine if it evaluates
	 * to true or false
	 */
	public Map<String, Object> vars = new HashMap<>();
	
	/**
	 * This method is to be evaluated to execute the filter and determine
	 * if the result is true or false
	 * @return
	 */
	public abstract boolean evaluate();
	
	public void setVar(String key, Object value) {
		this.vars.put(key, value);
	}
	
	public Object getVar(String key) {
		return this.vars.get(key);
	}	
}
