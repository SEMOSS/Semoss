package prerna.algorithm.impl;

import java.math.BigDecimal;
import java.util.Iterator;

public class SumAlgorithm extends BaseReducer {

	int max = 10;

	public void set(Iterator inputIterator, String[] ids, String script) {
		super.set(inputIterator, ids,script, null);
	}

	public void set(Iterator inputIterator, String[] ids, String script, String prop) {
		super.set(inputIterator, ids,script, prop);
	}

	@Override
	public Object reduce() {
		// TODO Auto-generated method stub
		double output = 0.0f;
		int count = 0;
		while(inputIterator.hasNext() && !errored && count < max)
		{
			BigDecimal dec = (BigDecimal)getNextValue();
			//double thisOut = (double)getNextValue();
			output += dec.doubleValue();
			count++;
		}
		return output;
	}
	


}
