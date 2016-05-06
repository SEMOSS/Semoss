package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import prerna.util.ArrayUtilityMethods;

public class SumReactor extends BaseReducerReactor {

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
			Object value = row[processedIndex];
			if(!groupByHash.containsKey(key)) {
				groupByHash.put(key, 0.0);
			}
			groupByHash.put(key, (Double) groupByHash.get(key) + (Double)value);
		}
		
		return groupByHash;
	}
	
}
