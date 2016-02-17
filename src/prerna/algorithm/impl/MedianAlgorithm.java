package prerna.algorithm.impl;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class MedianAlgorithm extends BaseReducer {

	int max = 10;

	public void set(Iterator inputIterator, String[] ids, String script) {
		super.set(inputIterator, ids,script, null);
	}

	public void set(Iterator inputIterator, String[] ids, String script, String prop) {
		super.set(inputIterator, ids,script, prop);
	}

	/* 
	 * 
	 * create a temporary copy of all data 
	 * 
	 * */
	@Override
	public Object reduce() {
		// TODO Auto-generated method stub
		double [] output = null;
		ArrayList<ArrayList<Double>> values = new ArrayList<ArrayList<Double>>();
		int count = 0;
		while(inputIterator.hasNext() && !errored)// && count < max)
		{
			//Object nextValue = getNextValue();
			//System.out.println("Next value .. " + nextValue.getClass());
			ArrayList dec = (ArrayList)getNextValue();
			//double thisOut = (double)getNextValue();
			if(output == null)
				output = new double[dec.size()];
			if (count == 0) {
				for (int i = 0; i < dec.size(); i++)
					values.add(new ArrayList<Double>());
			}
			for(int outIndex = 0;outIndex < dec.size();outIndex++)
			{	
				if(dec.get(outIndex) instanceof Integer)
					values.get(outIndex).add(((Integer)dec.get(outIndex)).doubleValue());
				else if(dec.get(outIndex) instanceof BigDecimal)
					values.get(outIndex).add(((BigDecimal)dec.get(outIndex)).doubleValue());
				else if(dec.get(outIndex) instanceof Double)
					values.get(outIndex).add((Double)dec.get(outIndex));
			}
			count++;
		}
		// sort each arraylist, return median
		for (int i = 0; i < values.size(); i++) {
			Collections.sort(values.get(i));
			double median;
			if (count % 2 == 0) { 
				median = values.get(i).get(count/2);
			}
			else {
				median = (values.get(i).get(count/2)+values.get(i).get((int)Math.ceil(count/2)))/2.0;
			}
			output[i] = median;
		}
		return output;
	}
}
