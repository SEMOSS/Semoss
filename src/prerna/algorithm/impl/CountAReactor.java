package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

import prerna.util.ArrayUtilityMethods;

public class CountAReactor extends BaseReducerReactor {

	public CountAReactor() {
		setMathRoutine("CountA");
	}
	
	@Override
	public Object reduce() {
		double count = 0;
		
		while(inputIterator.hasNext()) {
			ArrayList dec = (ArrayList)getNextValue();
			if(dec.get(0).toString().trim().isEmpty()) {
				count++;
			}
		}
		System.out.println(count);
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
			int processedIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(columnsArray, processedColumns.get(0));
			Object value = row[processedIndex];
			Set<Object> values = (TreeSet<Object>)groupByHash.get(key);
			if(values == null) {
				values = new TreeSet<Object>();
				groupByHash.put(key, values);
			}
			if (values.toString().trim().isEmpty()) {
				values.add(value);
			}
		}
		for(HashMap<Object,Object> key: groupByHash.keySet()) {
			groupByHash.put(key, ((TreeSet<Object>)groupByHash.get(key)).size());
		}
		
		return groupByHash;
	}
	
	@Override
	public Map<String, Object> getColumnDataMap() {
		return getBaseColumnDataMap();
	}
	
}
