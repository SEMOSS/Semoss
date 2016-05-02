package prerna.algorithm.impl;

import java.util.ArrayList;

public class MinReactor extends BaseReducerReactor {
	
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
					if(value < output) {
						output = value;
					}
				}
			}
		}
		System.out.println(output);
		return output;
	}
	
}
