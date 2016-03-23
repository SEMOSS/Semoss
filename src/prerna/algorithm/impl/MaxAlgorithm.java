package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Iterator;

public class MaxAlgorithm extends BaseReducer {
	
	@Override
	public void set(Iterator inputIterator, String[] ids, String script) {
		super.set(inputIterator, ids,script, null);
	}
	
	@Override
	public Object reduce() {
		double output = 0.0;
		if(inputIterator.hasNext() && !errored) {
			ArrayList row = (ArrayList)getNextValue();
			if(row.get(0) instanceof Number) {
				output = ((Number)row.get(0)).doubleValue();
			}
			while(inputIterator.hasNext()) {
				ArrayList dec = (ArrayList)getNextValue();
				if(dec.get(0) instanceof Number) {
					double value = ((Number)dec.get(0)).doubleValue();
					if(value > output) {
						output = value;
					}
				}
			}
		}
		System.out.println(output);
		return output;
	}
	
}
