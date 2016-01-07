package prerna.equation;

import java.util.*;

import org.nfunk.jep.*;
import org.nfunk.jep.function.*;

/**
 * This class serves mainly as an example of a function that accepts any
 * number of parameters. Note that the numberOfParameters is initialized to 
 * -1.
 */
public class Count extends PostfixMathCommand
{
	/**
	 * Constructor.
	 */
	public Count() {
		numberOfParameters = -1;
	}
	
	/**
	 * Counts the numebr of objects passed to it.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void run(Stack stack) throws ParseException {
		
  		// Check if stack is null
  		if (null == stack) {
			throw new ParseException("Stack argument null");
		}
        
        int i = 0;
        
        // repeat summation for each one of the current parameters
        while (i < curNumberOfParameters) {
        	// get the parameter from the stack
            stack.pop();
        }
        
        // push the result on the inStack
        stack.push(new Double(curNumberOfParameters));
	}
}
