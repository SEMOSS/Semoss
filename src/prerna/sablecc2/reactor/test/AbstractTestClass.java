package prerna.sablecc2.reactor.test;

import java.util.*;

import prerna.sablecc2.om.VarStore;

/**
 * This is just a internal class
 * so that when we compile to execute the assimilator
 * we can have a method to call based on the super that is 
 * assigned to the new class
 *
 */
public abstract class AbstractTestClass {

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
	
	public void addVariable(String var, int value) {
		variables.put(var, value);
	}
	
//	public void addVariable(String var, double value) {
//		variables.put(var, value);
//	}
}
