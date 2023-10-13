package prerna.reactor.expression;

import java.util.Arrays;

public class OpMedian extends OpBasicMath {

	public OpMedian() {
		this.operation = "median";
	}

	@Override
	protected double evaluate(Object[] values) {
		return eval(values);
	}
	
	public static double eval(Object... values) {
		double[] evals = convertToDoubleArray(values);
		double medianValue = performComp(evals);
		return medianValue;
	}
	
	public static double eval(double[] values) {
		double medianValue = performComp(values);
		return medianValue;
	}
	
	/**
	 * Do a basic math comparison
	 * @param curMax
	 * @param newMax
	 * @return
	 */
	public static double performComp(double[] evals) {
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
