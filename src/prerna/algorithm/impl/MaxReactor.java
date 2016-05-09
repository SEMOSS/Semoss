package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import prerna.util.ArrayUtilityMethods;

public class MaxReactor extends BaseReducerReactor {
	
	@Override
	public Object reduce() {
		double output = 0.0;
		if(inputIterator.hasNext() && !errored) {
			ArrayList row = (ArrayList)getNextValue();
			if(row.get(0) instanceof Number) {
				output = ((Number)row.get(0)).doubleValue();
			}
			while(inputIterator.hasNext()) {
				ArrayList dec = (ArrayList)getNextValue();
				if(dec.get(0) instanceof Number) {
					double value = ((Number)dec.get(0)).doubleValue();
					if(value > output) {
						output = value;
					}
				}
			}
		}
		System.out.println(output);
		return output;
	}
	
	@Override
	public HashMap<HashMap<String,String>,Object> reduceGroupBy(Vector<String> groupBys, Vector<String> processedColumns, String[] columnsArray, Iterator it) {
		HashMap<HashMap<String,String>, Object> groupByHash = new HashMap<HashMap<String,String>,Object>();
		
		while(it.hasNext()){
			Object[] row = (Object[]) it.next();
			HashMap<String, String> key = new HashMap<String,String>();
			for(String groupBy : groupBys) {
				int groupByIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(columnsArray, groupBy);
				String instance = (String)row[groupByIndex];
				key.put(groupBy, instance);
			}
			int processedIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(columnsArray, processedColumns.get(0));
			if (row[processedIndex] instanceof Number) {
				double value = ((Number)row[processedIndex]).doubleValue();
				if(!groupByHash.containsKey(key)) {
					groupByHash.put(key, value);
				} else if(((Double)groupByHash.get(key)) < value){
					groupByHash.put(key, value);
				}
			}
		}
		
		return groupByHash;
	}
	
}
