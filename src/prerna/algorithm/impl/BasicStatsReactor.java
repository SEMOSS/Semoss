package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.sablecc.PKQLEnum;
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
		ArrayList<Double> values = new ArrayList<Double>();
		
		while(inputIterator.hasNext() && !errored)
		{
			ArrayList dec = (ArrayList)getNextValue();
				if(dec.get(0) instanceof Number) {
					stats.addValue(((Number)dec.get(0)).doubleValue());
					values.add(((Number)dec.get(0)).doubleValue());
					if (((Number)dec.get(0)).doubleValue() > 0)
						positiveCount++;
					else if(((Number)dec.get(0)).doubleValue() < 0)
						negativeCount++;
					else
						zeroCount++;
				}
		}
		
		//Calculate Median
		Collections.sort(values);
		int totalCount = values.size();
		double median;
		if (totalCount % 2 == 0) { 
			median = values.get(totalCount/2);
		}
		else {
			median = (values.get(totalCount/2) + values.get((int)Math.ceil(totalCount/2))) / 2.0;
		}
		
		//Calcuate Mode
		HashMap<Double,Integer> hm=new HashMap<Double,Integer>();
		int max=1;
	    double mode = 0;
	    for(int i=0;i<values.size();i++)
	        {
	            if(hm.get(values.get(i))!=null)
	            {int counthm=hm.get(values.get(i));
	            counthm=counthm+1;
	            hm.put(values.get(i),counthm);
	            if(counthm>max)
	                {max=counthm;
	                 mode=values.get(i);}
	            }
	            else
	            {hm.put(values.get(i),1);}
	        }
		
		//Rest of Basic Stats
		double mean = stats.getMean();
		double std = stats.getStandardDeviation();
		double min = stats.getMin();
		double maximum = stats.getMax();
		double variance = stats.getVariance();
		double geometricMean = stats.getGeometricMean();
		double sum = stats.getSum();
		double sumOfSquare = stats.getSumsq();
		
		HashMap<String,Object> returnData = new HashMap<>();
		returnData.put("mean", mean);
		returnData.put("standardDeviation", std);
		returnData.put("min", min);
		returnData.put("max", maximum);
		returnData.put("variance", variance);
		returnData.put("geometricMean", geometricMean);
		returnData.put("sum", sum);
		returnData.put("positveCount", positiveCount);
		returnData.put("negativeCount", negativeCount);
		returnData.put("zeroCount", zeroCount);
		returnData.put("sumOfSquare", sumOfSquare);
		returnData.put("median", median);
		returnData.put("mode", mode);
		
		HashMap<String,Object> basicStats = new HashMap<>();
		basicStats.put("basicStats", returnData);
		myStore.put("ADDITIONAL_INFO", basicStats);
		myStore.put("STATUS", STATUS.SUCCESS);
		System.out.println(output);
		return returnData;
	}
	
	@Override
	public HashMap<HashMap<Object, Object>,Object> reduceGroupBy(Vector<String> groupBys, Vector<String> processedColumns, String[] columnsArray, Iterator it) {
		HashMap<HashMap<Object,Object>, Object> groupByHash = new HashMap<HashMap<Object,Object>,Object>();
		
		
		return null;
	}
	
	@Override
	public Map<String, Object> getColumnDataMap() {
		// this cannot be added into a frame
		// just return null
		return null;
	}
	
}

