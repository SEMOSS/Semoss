package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import prerna.util.ArrayUtilityMethods;

public class StandardDeviationReactor extends BaseReducerReactor{

	@Override
	public Object reduce() {
		int count = 0;
		double[] sumAndSumOfSquares = new double[2];; // index 0 for sum, index 1 for sum squared
		double output = 0.0;
		while(inputIterator.hasNext() && !errored) {
			ArrayList dec = (ArrayList)getNextValue();
			if(dec.get(0) instanceof Number) {
				sumAndSumOfSquares[0] += ((Number)dec.get(0)).doubleValue();
				sumAndSumOfSquares[1] += (Math.pow(((Number)dec.get(0)).doubleValue(), 2));
				count++;
			}
		}
		double sum = sumAndSumOfSquares[0];
		double sumOfSquares = sumAndSumOfSquares[1];
		double average = sum/count;
		output = Math.sqrt(((count * average * average) - (2 * average * sum) + sumOfSquares)/count);
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
				HashMap<String,Object> paramMap = (HashMap<String,Object>)groupByHash.get(key);
				if(paramMap == null) {
					paramMap = new HashMap<String,Object>();
					groupByHash.put(key, paramMap);
					paramMap.put("SUM", 0.0);
					paramMap.put("SUMOFSQUARES", 0.0);
					paramMap.put("COUNT", 0);
				}
				paramMap.put("SUM", (Double)paramMap.get("SUM") + value);
				paramMap.put("SUMOFSQUARES", (Double)paramMap.get("SUMOFSQUARES") + Math.pow(value, 2));
				paramMap.put("COUNT", (Integer)paramMap.get("COUNT")+1);
			}
		}
		for(HashMap<String,String> key: groupByHash.keySet()) {
			HashMap<String,Object> paramMap = (HashMap<String,Object>)groupByHash.get(key);
			int count = (Integer)paramMap.get("COUNT");
			double sum = (Double)paramMap.get("SUM");
			double sumOfSquares = (Double)paramMap.get("SUMOFSQUARES");
			double average = sum/count;
			groupByHash.put(key, Math.sqrt(((count * average * average) - (2 * average * sum) + sumOfSquares)/count));
		}
		
		return groupByHash;
	}
	
}
