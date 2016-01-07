package prerna.equation;

import java.util.*;

import org.nfunk.jep.*;
import org.nfunk.jep.function.*;

/**
 * This class serves mainly as an example of a function that accepts any
 * number of parameters. Note that the numberOfParameters is initialized to 
 * -1.
 */
public class If extends PostfixMathCommand {
	/**
	 ** Constructor.
	 ** Has three inputs: a boolean statement, and double values if statement is True or False.
	 **/
	public If() {
		numberOfParameters = 3;
	}
	
	/**
	 * Calculates the result of summing up all parameters, which are assumed
	 * to be of the Double type.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void run(Stack stack) throws ParseException {
		
  		// Check if stack is null
  		if (null == stack) {
			throw new ParseException("Stack argument null");
		}
        
        Object ifFalse = stack.pop();
        Object ifTrue = stack.pop();
        Object statement = stack.pop();
        
        boolean stateEval;
        
        if (statement.equals(1.0)) { stateEval = true; }
        else                       { stateEval = false; }
        
        if (stateEval) {
        	stack.push(new Double(((Number) ifTrue).doubleValue()));
        }
        else {
        	stack.push(new Double(((Number) ifFalse).doubleValue()));
        }
	}
}