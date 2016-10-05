package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;
import prerna.sablecc.PKQLRunner.STATUS;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import prerna.util.ArrayUtilityMethods;


public class BasicStatsReactor extends BaseReducerReactor {
	
	@Override
	public Object reduce() {
		SummaryStatistics stats = new SummaryStatistics();
		double output = 0.0;
		int count = 0;
		int positiveCount =0, negativeCount =0, zeroCount =0;
		
		while(inputIterator.hasNext() && !errored)
		{
			ArrayList dec = (ArrayList)getNextValue();
				if(dec.get(0) instanceof Number) {
					stats.addValue(((Number)dec.get(0)).doubleValue());
					//output += ((Number)dec.get(0)).doubleValue();
					//count++;
					if (((Number)dec.get(0)).doubleValue() > 0)
						positiveCount++;
					else if(((Number)dec.get(0)).doubleValue() < 0)
						negativeCount++;
					else
						zeroCount++;
				}
		}
		
		double mean = stats.getMean();
		double std = stats.getStandardDeviation();
		double min = stats.getMin();
		double max = stats.getMax();
		double variance = stats.getVariance();
		double geometricMean = stats.getGeometricMean();
		double sum = stats.getSum();
		double sumOfSquare = stats.getSumsq();
		
		HashMap<String,Object> returnData = new HashMap<>();
		returnData.put("mean", mean);
		returnData.put("standardDeviation", std);
		returnData.put("min", min);
		returnData.put("max", max);
		returnData.put("variance", variance);
		returnData.put("geometricMean", geometricMean);
		returnData.put("sum", sum);
		returnData.put("positveCount", positiveCount);
		returnData.put("negativeCount", negativeCount);
		returnData.put("zeroCount", zeroCount);
		returnData.put("sumOfSquare", sumOfSquare);
		
		
		myStore.put("ADDITIONAL_INFO", returnData);
		myStore.put("STATUS", STATUS.SUCCESS);
		System.out.println(output);
		return returnData;
	}
	
	@Override
	public HashMap<HashMap<Object, Object>,Object> reduceGroupBy(Vector<String> groupBys, Vector<String> processedColumns, String[] columnsArray, Iterator it) {
		HashMap<HashMap<Object,Object>, Object> groupByHash = new HashMap<HashMap<Object,Object>,Object>();
		
		
		return null;
	}
	
}

