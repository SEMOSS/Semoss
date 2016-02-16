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
		int[] counts = null;
		double[][] sumAndSumSquared = null; // index 0 for sum, index 1 for sum squared
		double[] output = null;
		while(inputIterator.hasNext() && !errored) {
			ArrayList dec = (ArrayList)getNextValue();
			if(sumAndSumSquared == null) {
				sumAndSumSquared = new double[dec.size()][2];
			}
			if(counts == null) {
				counts = new int[dec.size()];
			}
			if(output == null) {
				output = new double[dec.size()];
			}
			for(int outIndex = 0;outIndex < dec.size();outIndex++)
			{	
				if(dec.get(outIndex) instanceof Number) {
					sumAndSumSquared[outIndex][0] += ((Number)dec.get(outIndex)).doubleValue();
					sumAndSumSquared[outIndex][1] += (Math.pow(((Number)dec.get(outIndex)).doubleValue(), 2));
					counts[outIndex]++;
				}
			}
		}
		for (int i = 0; i < output.length; i++) {
			int count = counts[i];
			double sum = sumAndSumSquared[i][0];
			double sumSquared = sumAndSumSquared[i][1];
			double average = sum/count;
			output[i] = Math.sqrt(((count * average * average) - (2 * average * sum) + sumSquared)/count);
		}
		System.out.println(output[0]);
		return output;
	}
}
