package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.Map;

public class BaseJavaRuntime {

	Map<String, Object> variables = new HashMap<>();

	/**
	 * Method that return the evaluation of the signature
	 * 
	 * @return
	 */
	public void execute() {

	}

	public Map<String, Object> getVariables() {
		return this.variables;
	}

	public void a(String var) {
		variables.put(var, 1);
	}

	public void a(String var, double value) {
		variables.put(var, value);
	}

	public void a(String var, int value) {
		variables.put(var, value);
	}

	public void a(String var, String value) {
		variables.put(var, value);
	}

	public void a(String var, boolean value) {
		variables.put(var, value);
	}

	public boolean compareString(String lString, String comparator, String rString) {
		if (comparator.equals("==")) {
			return lString == rString;
		} else {
			return lString != rString;
		}

	}
	
	public boolean compareString(double lValue, String comparator, String rString) {
		return false;
	}
	
	public boolean compareString(int lValue, String comparator, String rString) {
		return false;
	}
	
	public void update(){
		
	}
}
