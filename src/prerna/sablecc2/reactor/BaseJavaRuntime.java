package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.Map;

public class BaseJavaRuntime {

	Map<String, Object> variables = new HashMap<>();
	
	/**
	 * Method that return the evaluation of 
	 * the signature
	 * @return
	 */
	public void execute() {

	}
	
	public Map<String, Object> getVariables() {
		return this.variables;
	}
	
	public void addVariable(String var) {
		variables.put(var, 1);
	}
	
	public void addVariable(String var, double value) {
		variables.put(var, value);
	}
	
	public void addVariable(String var, int value) {
		variables.put(var, value);
	}
	
	public void addVariable(String var, String value) {
		variables.put(var, value);
	}
	
	public void addVariable(String var, boolean value) {
		variables.put(var, value);
	}
	
	public Object getVariable(String var) {
		return variables.get(var);
	}
}
