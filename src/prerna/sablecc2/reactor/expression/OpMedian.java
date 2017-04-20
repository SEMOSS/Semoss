package prerna.sablecc2.reactor.expression;

import java.util.Arrays;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpMedian extends OpBasicMath {

	@Override
	public NounMetadata execute() {
		// get the values
		// this evaluated any lambda that 
		// was stored in currow
		NounMetadata[] values = getValues();

		// we need to calculate all of the values
		// and then order them
		// and then find the middle one
		double[] evals = new double[values.length];

		for(int i = 0; i < values.length; i++) {
			NounMetadata val = values[i];
			PkslDataTypes valType = val.getNounName();
			if(valType == PkslDataTypes.CONST_DECIMAL) {
				evals[i] = ((Number) val.getValue()).doubleValue(); 
			} else if(valType == PkslDataTypes.COLUMN) {
				// at this point, we have already checked if this is a 
				// variable, so it better exist on the frame
				// also, you can only have one of these
				
				//TODO: need to figure out how to do this
				// since H2 doesn't have median as a function!!!
				evals[i] = evaluateString("median", val);
			} else {
				throw new IllegalArgumentException("Invalid input for Median. Require all values to be numeric or column names");
			}
		}
		
		double medianValue = performComp(evals);
		NounMetadata medianNoun = new NounMetadata(medianValue, PkslDataTypes.CONST_DECIMAL);
		return medianNoun;
	}
	
	/**
	 * Do a basic math comparison
	 * @param curMax
	 * @param newMax
	 * @return
	 */
	public double performComp(double[] evals) {
		// sort the values
		Arrays.sort(evals);
		int numValues = evals.length;
		if(numValues == 1) {
			// if length is one
			// this would happen if they want the median value in
			// a column of data
			return evals[0];
		} else if(numValues == 2) {
			// if length is two
			// return the average
			return (evals[0] + evals[1]) / 2.0;
		} else if(numValues % 2 == 0) {
			// if n is even, find middle one
			return evals[numValues/2];
		} else {
			// if n is odd, find the two nearest values
			// and return their average
			return (evals[ (numValues + 1)/2 ] + evals[ (numValues - 1)/2 ]) / 2.0;
		}
	}
	
}
