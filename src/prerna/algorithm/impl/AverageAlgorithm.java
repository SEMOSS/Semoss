package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Iterator;

public class AverageAlgorithm extends BaseReducer {

	public void set(Iterator inputIterator, String[] ids, String script) {
		super.set(inputIterator, ids, script, null);
	}

	public void set(Iterator inputIterator, String[] ids, String script, String prop) {
		super.set(inputIterator, ids, script, prop);
	}

	@Override
	public Object reduce() {
		double [] output = null;
		int[] counts = null;
		while(inputIterator.hasNext() && !errored)
		{
			ArrayList dec = (ArrayList)getNextValue();
			if(output == null) {
				output = new double[dec.size()];
			}
			if(counts == null) {
				counts = new int[dec.size()];
			}
			for(int outIndex = 0;outIndex < dec.size();outIndex++)
			{	
				if(dec.get(outIndex) instanceof Number) {
					output[outIndex] += ((Number)dec.get(outIndex)).doubleValue();
					counts[outIndex]++;
				}
			}
		}
		for (int i = 0; i < output.length; i++) {
			output[i] = output[i]/counts[i];
		}
		System.out.println(output[0]);
		return output;
	}
	
}
