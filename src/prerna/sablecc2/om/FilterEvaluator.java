package prerna.sablecc2.om;

public abstract class FilterEvaluator {

	/*
	 * This class is just a wrapper so we have a default
	 * method to execute and determine if it evaluates
	 * to true or false
	 */
	
	
	/**
	 * This method is to be evaluated to execute the filter and determine
	 * if the result is true or false
	 * @return
	 */
	public abstract boolean evaluate();
	
}
