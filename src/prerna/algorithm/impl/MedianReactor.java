package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.util.ArrayUtilityMethods;

public class MedianReactor extends BaseReducerReactor {

	public MedianReactor() {
		setMathRoutine("Median");
	}
	
	@Override
	public Object reduce() {
		double output = 0.0;
		ArrayList<Double> values = new ArrayList<Double>(); // temporarily holds data
		while(inputIterator.hasNext() && !errored)
		{
			ArrayList dec = (ArrayList)getNextValue();
			if(dec.get(0) instanceof Number) {
				values.add(((Number)dec.get(0)).doubleValue());
			}
		}
		// sort each arraylist, return median
		Collections.sort(values);
		int count = values.size();
		double median;
		if (count % 2 == 0) { 
			median = values.get(count/2);
		}
		else {
			median = (values.get(count/2) + values.get((int)Math.ceil(count/2))) / 2.0;
		}
		output = median;
		return output;
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
			if (row[processedIndex] instanceof Number) {
				double value = ((Number)row[processedIndex]).doubleValue();
				HashMap<String,Object> paramMap = (HashMap<String,Object>)groupByHash.get(key);
				if(paramMap == null) {
					paramMap = new HashMap<String,Object>();
					groupByHash.put(key, paramMap);
					paramMap.put("VALUES", new ArrayList<Double>());
				}
				ArrayList<Double> values = (ArrayList<Double>)paramMap.get("VALUES");
				values.add(value);
			}
		}
		for(HashMap<Object,Object> key: groupByHash.keySet()) {
			HashMap<String,Object> paramMap = (HashMap<String,Object>)groupByHash.get(key);
			ArrayList<Double> values = (ArrayList<Double>)paramMap.get("VALUES");
			int count = values.size();
			Collections.sort(values);
			double median;
			if (count % 2 == 0) { 
				median = values.get(count/2);
			}
			else {
				median = (values.get(count/2) + values.get((int)Math.ceil(count/2))) / 2.0;
			}
			groupByHash.put(key, median);
		}
		
		return groupByHash;
	}
	
	@Override
	public Map<String, Object> getColumnDataMap() {
		return getBaseColumnDataMap();
	}
	
}
