package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

public class CountReactor extends BaseReducerReactor {

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
	
}
