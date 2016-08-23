package prerna.algorithm.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import prerna.util.ArrayUtilityMethods;

public class PercentReactor extends BaseReducerReactor {

	@Override
	public Object reduce() {
		return 1;
	}

	@Override
	public HashMap<HashMap<Object,Object>,Object> reduceGroupBy(Vector<String> groupBys, Vector<String> processedColumns, String[] columnsArray, Iterator it) {
		HashMap<HashMap<Object,Object>, Object> groupByHash = new HashMap<HashMap<Object,Object>,Object>();
		double totalValue = 0;
		
		
		while(it.hasNext()){
			Object[] row = (Object[]) it.next();
			HashMap<Object,Object> key = new HashMap<Object,Object>();
			for(String groupBy : groupBys) {
				int groupByIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(columnsArray, groupBy);
				Object instance = row[groupByIndex];
				key.put(groupBy, instance);
			}
			int processedIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(columnsArray, processedColumns.get(0));
			if (row[processedIndex] instanceof Number) {
				double value = ((Number)row[processedIndex]).doubleValue();
				totalValue += value;
				// put in column category
				if(!groupByHash.containsKey(key)) {
					groupByHash.put(key, 0.0);
				}
				// add new value to existing column value
				groupByHash.put(key, (Double) groupByHash.get(key) + value);
			}
		}
		
		//calculate percent
		for(HashMap<Object,Object> key: groupByHash.keySet()) {
			groupByHash.put(key, (Double) groupByHash.get(key) / totalValue);
//			System.out.println("Key: "+ key + "Value: "+ groupByHash.get(key));
		}
		
	
		return groupByHash;	}

}
