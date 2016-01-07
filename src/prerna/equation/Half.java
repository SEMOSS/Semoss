package prerna.equation;

import java.util.*;

import org.nfunk.jep.*;
import org.nfunk.jep.function.*;

/**
 * An example custom function class for JEP.
 */
class Half extends PostfixMathCommand {

	/**
	 * Constructor
	 */
	public Half() {
		numberOfParameters = 1;
	}
	
	/**
	 * Runs the square root operation on the inStack. The parameter is popped
	 * off the <code>inStack</code>, and the square root of it's value is 
	 * pushed back to the top of <code>inStack</code>.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void run(Stack inStack) throws ParseException {

		// check the stack
		checkStack(inStack);

		// get the parameter from the stack
		Object param = inStack.pop();

		// check whether the argument is of the right type
		if (param instanceof Double) {
			// calculate the result
			double r = ((Double)param).doubleValue() / 2;
			// push the result on the inStack
			inStack.push(new Double(r));
		} else {
			throw new ParseException("Invalid parameter type");
		}
	}
}