package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class CountAlgorithm extends BaseReducer {

	public void set(Iterator inputIterator, String[] ids, String script) {
		super.set(inputIterator, ids,script, null);
	}

	public void set(Iterator inputIterator, String[] ids, String script, String prop) {
		super.set(inputIterator, ids,script, prop);
	}

	@Override
	public Object reduce() {
		double count = 0;
		Set<String> values = new TreeSet<String>();
		while(inputIterator.hasNext() && !errored)
		{
			ArrayList dec = (ArrayList)getNextValue();
			if (!values.contains(dec.get(0).toString())) {
				count++;
				values.add(dec.get(0).toString());
			}
		}
		System.out.println(count);
		return count;
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
