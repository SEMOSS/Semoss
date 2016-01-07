package prerna.equation;

import java.util.*;

import org.nfunk.jep.*;
import org.nfunk.jep.function.*;

/**
 * This class serves mainly as an example of a function that accepts any
 * number of parameters. Note that the numberOfParameters is initialized to 
 * -1.
 */
public class Mean extends PostfixMathCommand
{
	/**
	 * Constructor.
	 */
	public Mean() {
		numberOfParameters = -1;
	}
	
	/**
	 * Calculates the mean of all parameters, which are assumed
	 * to be of the Double type.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void run(Stack stack) throws ParseException {
		
  		// Check if stack is null
  		if (null == stack) {
			throw new ParseException("Stack argument null");
		}
        
        Object param = null;
        int i = 0;
        double sum = 0;
        
        // repeat summation for each one of the current parameters
        while (i < curNumberOfParameters) {
        	// get the parameter from the stack
            param = stack.pop();
            if (param instanceof Number) {
                // calculate the result
                sum += (((Number) param).doubleValue());
            } else {
                throw new ParseException("Invalid parameter type");
            }
            i++;
        }
        
        double mean = sum / curNumberOfParameters;
        
        // push the result on the inStack
        stack.push(new Double(mean));
	}
}
