package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Iterator;

public class StandardDeviationAlgorithm extends BaseReducer{

	public void set(Iterator inputIterator, String[] ids, String script) {
		super.set(inputIterator, ids,script, null);
	}

	public void set(Iterator inputIterator, String[] ids, String script, String prop) {
		super.set(inputIterator, ids,script, prop);
	}

	@Override
	public Object reduce() {
		int count = 0;
		double[] sumAndSumSquared = new double[2];; // index 0 for sum, index 1 for sum squared
		double output = 0.0;
		while(inputIterator.hasNext() && !errored) {
			ArrayList dec = (ArrayList)getNextValue();
			if(dec.get(0) instanceof Number) {
				sumAndSumSquared[0] += ((Number)dec.get(0)).doubleValue();
				sumAndSumSquared[1] += (Math.pow(((Number)dec.get(0)).doubleValue(), 2));
				count++;
			}
		}
		double sum = sumAndSumSquared[0];
		double sumSquared = sumAndSumSquared[1];
		double average = sum/count;
		output = Math.sqrt(((count * average * average) - (2 * average * sum) + sumSquared)/count);
		System.out.println(output);
		return output;
	}
}
