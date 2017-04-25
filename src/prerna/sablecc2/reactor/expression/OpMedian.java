package prerna.sablecc2.reactor.expression;

import java.util.Arrays;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpMedian extends OpBasicMath {

	public OpMedian() {
		this.operation = "median";
	}

	@Override
	protected NounMetadata evaluate(Object[] values) {
		double[] evals = convertToDoubleArray(values);
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
	
	private double[] convertToDoubleArray(Object[] values) {
		double[] dblArray = new double[values.length-1];
		for(int i = 0; i < values.length-1; i++) {
			dblArray[i] = ((Number)values[i]).doubleValue();
		}
		
		return dblArray;
	}
	
}
