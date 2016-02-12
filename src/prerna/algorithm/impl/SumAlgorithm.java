package prerna.algorithm.impl;

import java.math.BigDecimal;
import java.util.ArrayList;
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
		double [] output = null;
		int count = 0;
		while(inputIterator.hasNext() && !errored && count < max)
		{
			Object nextValue = getNextValue();
			System.out.println("Next value .. " + nextValue.getClass());
			ArrayList dec = (ArrayList)getNextValue();
			//double thisOut = (double)getNextValue();
			if(output == null)
				output = new double[dec.size()];
			for(int outIndex = 0;outIndex < dec.size();outIndex++)
			{	
				if(dec.get(outIndex) instanceof Integer)
					output[outIndex] += ((Integer)dec.get(outIndex)).doubleValue();
				else if(dec.get(outIndex) instanceof BigDecimal)
					output[outIndex] += ((BigDecimal)dec.get(outIndex)).doubleValue();

			}
			count++;
		}
		return output;
	}
	


}
