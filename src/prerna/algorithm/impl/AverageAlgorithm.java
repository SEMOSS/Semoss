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
		double output = 0.0;
		int count = 0;
		while(inputIterator.hasNext() && !errored)
		{
			ArrayList dec = (ArrayList)getNextValue();
//			if(output == null) {
//				output = new double[dec.size()];
//			}
//			if(counts == null) {
//				counts = new int[dec.size()];
//			}
//			for(int outIndex = 0;outIndex < dec.size();outIndex++)
//			{	
				if(dec.get(0) instanceof Number) {
					output += ((Number)dec.get(0)).doubleValue();
					count++;
				}
//			}
		}
//		for (int i = 0; i < output.length; i++) {
			output = output/count;
//		}
		System.out.println(output);
		return output;
	}
	
}
