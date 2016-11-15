package prerna.algorithm.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.util.ArrayUtilityMethods;

public class CountReactor extends BaseReducerReactor {

	public CountReactor() {
		setMathRoutine("Count");
	}
	
	@Override
	public Object reduce() {
		double count = 0;
		while(inputIterator.hasNext() && !errored) {
			count++;
		}
		return count;
	}
	
	@Override
	public HashMap<HashMap<Object,Object>,Object> reduceGroupBy(Vector<String> groupBys, Vector<String> processedColumns, String[] columnsArray, Iterator it) {
		HashMap<HashMap<Object,Object>, Object> groupByHash = new HashMap<HashMap<Object,Object>,Object>();

		while(it.hasNext()){
			Object[] row = (Object[]) it.next();
			HashMap<Object, Object> key = new HashMap<Object,Object>();
			for(String groupBy : groupBys) {
				int groupByIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(columnsArray, groupBy);
				Object instance = row[groupByIndex];
				key.put(groupBy, instance);
			}
			Double count = (Double) groupByHash.get(key);
			if(count == null) {
				count = 1.0;
			} else {
				count++;
			}
			groupByHash.put(key, count);
		}
		
		return groupByHash;
	}
	
	@Override
	public Map<String, Object> getColumnDataMap() {
		return getBaseColumnDataMap();
	}
}
