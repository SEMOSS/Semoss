package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Collections;

public class MedianReactor extends BaseReducerReactor {

	@Override
	public Object reduce() {
		double output = 0.0;
		ArrayList<Double> values = new ArrayList<Double>(); // temporarily holds data
		int count = 0;
		while(inputIterator.hasNext() && !errored)
		{
			ArrayList dec = (ArrayList)getNextValue();
			if(dec.get(0) instanceof Number) {
				values.add(((Number)dec.get(0)).doubleValue());
				count++; // only count numbers
			}
		}
		// sort each arraylist, return median
		Collections.sort(values);
		double median;
		if (count % 2 == 0) { 
			median = values.get(count/2);
		}
		else {
			median = (values.get(count/2) + values.get((int)Math.ceil(count/2))) / 2.0;
		}
		output = median;
		return output;
	}
	
}
