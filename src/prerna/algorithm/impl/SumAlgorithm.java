package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Iterator;

public class SumAlgorithm extends BaseReducer {

	public void set(Iterator inputIterator, String[] ids, String script) {
		super.set(inputIterator, ids,script, null);
	}

	public void set(Iterator inputIterator, String[] ids, String script, String prop) {
		super.set(inputIterator, ids,script, prop);
	}

	@Override
	public Object reduce() {
		double output = 0.0;
		while(inputIterator.hasNext() && !errored) {
			ArrayList dec = (ArrayList)getNextValue();
			if(dec.get(0) instanceof Number) {
				output += ((Number)dec.get(0)).doubleValue();
			}
		}
		System.out.println(output);
		return output;
	}
	
	@Override
	public void setData(Iterator inputIterator, String[] ids, String script) {
		super.set(inputIterator, ids, script, null);
	}

	@Override
	public Object execute() {
		return reduce();
	}
}
