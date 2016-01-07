package prerna.equation;

import java.util.*;

import org.nfunk.jep.*;
import org.nfunk.jep.function.*;

/**
 * This class serves mainly as an example of a function that accepts any
 * number of parameters. Note that the numberOfParameters is initialized to 
 * -1.
 */
public class Mode extends PostfixMathCommand
{
	/**
	 * Constructor.
	 */
	public Mode() {
		numberOfParameters = -1;
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
        
        Object param = null;
        Hashtable<Double, Integer> counter = new Hashtable<Double, Integer>();
        int i = 0;
        
        // repeat summation for each one of the current parameters
        while (i < curNumberOfParameters) {
        	// get the parameter from the stack
            param = stack.pop();
            if (param instanceof Number) {
            	Double value = (double) param;
            	// add to counter
                Set<Double> keys = counter.keySet();
                if (keys.contains(value)) {
                	int count = counter.get(value);
                	counter.put(value, count+1);
                } 
                else { counter.put(value, 1); }
            } else {
                throw new ParseException("Invalid parameter type");
            }
                
            i++;
        }
        
        // figure out which key is most common
        int modeCount = 0;
        Double modeKey = Double.NaN;
        Set<Double> keys = counter.keySet();
        for (Double key: keys) {
        	int count = counter.get(key);
        	if (count > modeCount) {
        		modeCount = count;
        		modeKey = key;
        	}
        }
        
        // push the result on the inStack
        stack.push(new Double(modeKey));
	}
}
