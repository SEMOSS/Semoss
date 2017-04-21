package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.Map;

/**
 * This is just a internal class
 * so that when we compile to execute the assimilator
 * we can have a method to call based on the super that is 
 * assigned to the new class
 *
 */
public abstract class AssimilatorEvaluator {

	public Map<String, Object> vars = new HashMap<>();
	public boolean containsStringValue = false;
	
	public AssimilatorEvaluator() {
		
	}
	
	/**
	 * Method that return the evaluation of 
	 * the signature
	 * @return
	 */
	public Object execute() {
		return null;
	}
	
	public void setVar(String key, Object value) {
		this.vars.put(key, value);
	}
	
	public Object getVar(String key) {
		return this.vars.get(key);
	}
}