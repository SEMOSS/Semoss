package prerna.sablecc2.reactor.expression;

import java.util.Arrays;

public class OpSmall extends OpBasicMath {

	public OpSmall() {
		this.operation = "small";
	}
	
	@Override
	protected double evaluate(Object[] values) {
		// grab the index
		int valIndex = ((Number) values[values.length-1]).intValue();
		// convert everything to a double array except the last index
		double[] doubleValues = convertToDoubleArray(values, 0, values.length-1);
		// sort in ascending order
		Arrays.sort(doubleValues);
		// return the index - 1
		// remember, excel is 1 based
		return doubleValues[valIndex-1];
	}
}
