package prerna.equation;

import java.util.*;

import org.nfunk.jep.*;
import org.nfunk.jep.function.*;

/**
 * This class serves mainly as an example of a function that accepts any
 * number of parameters. Note that the numberOfParameters is initialized to 
 * -1.
 */
public class Median extends PostfixMathCommand
{
	/**
	 * Constructor.
	 */
	public Median() {
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
        ArrayList<Double> list = new ArrayList<Double>();
        int i = 0;
        
        // repeat summation for each one of the current parameters
        while (i < curNumberOfParameters) {
        	// get the parameter from the stack
            param = stack.pop();
            if (param instanceof Number) {
                // calculate the result
                list.add(((Number) param).doubleValue());
            } else {
                throw new ParseException("Invalid parameter type");
            }
                
            i++;
        }
        
        // sort list
        Collections.sort(list);
        
        // get median
        double median = 0;
        int medianIndex = (int) Math.floor(list.size() / 2);
        if (list.size() % 2 == 1) { 
        	median = list.get(medianIndex); 
        } else {
        	median = ((list.get(medianIndex-1)+list.get(medianIndex)) / 2);
        }
        
        
        // push the result on the inStack
        stack.push(new Double(median));
	}
}
