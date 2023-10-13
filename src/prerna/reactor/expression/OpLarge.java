package prerna.reactor.expression;

import java.util.Arrays;

public class OpLarge extends OpBasicMath {

	public OpLarge() {
		this.operation = "large";
	}
	
	@Override
	protected double evaluate(Object[] values) {
		return eval(values);
	}
	
	public static double eval(Object...values) {
		// grab the index
		int valIndex = ((Number) values[values.length-1]).intValue();
		// convert everything to a double array except the last index
		double[] doubleValues = convertToDoubleArray(values, 0, values.length-1);
		// sort in ascending order
		Arrays.sort(doubleValues);
		// start at the length and move left based on the value index
		return doubleValues[doubleValues.length-valIndex];
	}
	
	public static double eval(double[] values) {
		// grab the index
		int valIndex = ((Number) values[values.length-1]).intValue();
		// convert everything to a double array except the last index
		double[] doubleValues = Arrays.copyOf(values, values.length - 1);
		// sort in ascending order
		Arrays.sort(doubleValues);
		// start at the length and move left based on the value index
		return doubleValues[doubleValues.length-valIndex];
	}
}
