package prerna.sablecc2.om;

import prerna.sablecc2.reactor.IReactor;

public class Expression {
	
	private String expression = null;
	private String[] inputs = null;
	
	private Object left; //could be a string, decimal, an expression, or a lambda
	private Object right; //could be a string, decimal, an expression, or a lamda
	private String operation; //+, -, *, /, etc.
	
	public Expression(String expression, String [] inputs)
	{
		this.expression = expression;
		this.inputs = inputs;
	}

	/**
	 * @return
	 * 
	 * Builds the expression by recursively evaluating its left and right sub expressions and returns the string representation which will feed directly to java
	 */
	public String getExpression() {
		
//		return this.expression;
		
		//build the left side
		String leftSide = getEvaluatedString(left);
		
		//build the right side
		String rightSide = getEvaluatedString(right);
		
		return leftSide+" "+operation+" "+rightSide; 
	}
	
	
	/**
	 * 
	 * @param expr
	 * @return
	 * 
	 * This function evaluates one side of the expression and returns the
	 * string representation which can be put into java
	 */
	private String getEvaluatedString(Object expr) {
		
		//The expr object can either be an expression, a lambda, or a constant
		
		//if we have a lambda (Or IReactor) execute and get the string representation of the execution
		//Ex: Sum(MovieBudget) -> 1324792349
		if(expr instanceof IReactor) {
			Object result = ((IReactor)expr).execute();
			NounMetadata resultNoun = (NounMetadata)result;
			return resultNoun.getValue().toString();
		} 
		
		//if we have an expression then get the evaluated Expression String from the instance
		//Ex: 3 + Sum(MovieBudget) -> 3 + 1324792349
		else if(expr instanceof Expression) {
			return "(" + ((Expression)expr).getExpression() + ")"; //add the parenthesis to preserve order of operations
		} 
		
		//otherwise just return it as a string
		//Ex: 2 or a
		else {
			return expr.toString().trim();
		}
	}
	
	/**
	 * 
	 * @param left
	 * 
	 * Set the left side of the expression
	 */
	public void setLeft(Object left) {
		this.left = left;
	}
	
	/**
	 * 
	 * @param right
	 * 
	 * Set the right side of the expression
	 */
	public void setRight(Object right) {
		this.right = right;
	}
	
	/**
	 * 
	 * @param operation
	 * 
	 * What operator are we using for this expression
	 * 		*, /, +, -
	 */
	public void setOperation(String operation) {
		this.operation = operation;
	}
	
	public String[] getInputs() {
		return inputs;
	}
	/**
	 * Used for debugging to easily see what expression is
	 * being used
	 */
	public String toString() {
		return this.expression;
	}
}
