package prerna.algorithm.impl;

import java.util.ArrayList;

public class AverageReactor extends BaseReducerReactor {
	
	@Override
	public Object reduce() {
		double output = 0.0;
		int count = 0;
		while(inputIterator.hasNext() && !errored)
		{
			ArrayList dec = (ArrayList)getNextValue();
				if(dec.get(0) instanceof Number) {
					output += ((Number)dec.get(0)).doubleValue();
					count++;
				}
		}
			output = output/count;
		System.out.println(output);
		return output;
	}
	
}
