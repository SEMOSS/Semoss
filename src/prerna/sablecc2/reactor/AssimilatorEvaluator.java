package prerna.sablecc2.reactor;

/**
 * This is just a internal class
 * so that when we compile to execute the assimilator
 * we can have a method to call based on the super that is 
 * assigned to the new class
 *
 */
public abstract class AssimilatorEvaluator {

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
}