package prerna.algorithm.impl;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.meta.MathPkqlMetadata;


public class BasicStatsReactor extends BaseReducerReactor {
	
	@Override
	public Object reduce() {
		SummaryStatistics stats = new SummaryStatistics();
		DescriptiveStatistics descStats = new DescriptiveStatistics();
		double output = 0.0;
		int count = 0;
		int positiveCount =0, negativeCount =0, zeroCount =0;
		ArrayList<Double> values = new ArrayList<Double>();
		
		double posTotal = 0, negTotal = 0, absTotal = 0;
		
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MAP_OBJ);
		
		double greaterThreshold = options != null && options.containsKey(">") ? Double.valueOf(options.get(">").toString()) : 0.0;
		double lesserThreshold = options != null && options.containsKey("<") ? Double.valueOf(options.get("<").toString()) : 0.0;
		double equalValue = options != null && options.containsKey("=") ? Double.valueOf(options.get("=").toString()) : 0.0;
		
		while(inputIterator.hasNext() && !errored)
		{
			ArrayList dec = (ArrayList)getNextValue();
				if(dec.get(0) instanceof Number) {
					stats.addValue(((Number)dec.get(0)).doubleValue());
					descStats.addValue(((Number)dec.get(0)).doubleValue());
					values.add(((Number)dec.get(0)).doubleValue());
					if (((Number)dec.get(0)).doubleValue() > greaterThreshold){
						posTotal += ((Number)dec.get(0)).doubleValue();
						positiveCount++;
					}
					else if(((Number)dec.get(0)).doubleValue() < lesserThreshold){
						negTotal += Math.abs(((Number)dec.get(0)).doubleValue());
						negativeCount++;
					}
					else if(((Number)dec.get(0)).doubleValue() == equalValue){
						zeroCount++;
					}
					absTotal += Math.abs(((Number)dec.get(0)).doubleValue());
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
	    int maxMode =  Collections.max(hm.values());
	    for(double key : hm.keySet()){
	    	if(maxMode ==  hm.get(key)){
	    		mode = key;
	    		break;
	    	}
	    }
	    //Kurt
	    double kurt;
        kurt=descStats.getKurtosis();
	    

		HashMap<String,Object> returnData = new HashMap<>();
	    
	    Object mean = Double.isNaN(stats.getMean()) ? "NaN" : new BigDecimal(stats.getMean()).setScale(2, RoundingMode.DOWN);
		Object std = Double.isNaN(stats.getStandardDeviation()) ? "NaN" : new BigDecimal(stats.getStandardDeviation()).setScale(2, RoundingMode.DOWN);
		Object min = Double.isNaN(stats.getMin()) ? "NaN" :  new BigDecimal(stats.getMin()).setScale(2, RoundingMode.DOWN);
		Object maximum = Double.isNaN(stats.getMax()) ? "NaN" : new BigDecimal(stats.getMax()).setScale(2, RoundingMode.DOWN);
		Object variance = Double.isNaN(stats.getVariance()) ? "NaN" :  new BigDecimal(stats.getVariance()).setScale(2, RoundingMode.DOWN);
		Object geometricMean = Double.isNaN(stats.getGeometricMean()) ? "NaN" : new BigDecimal(stats.getGeometricMean()).setScale(2, RoundingMode.DOWN);
		Object sum = Double.isNaN(stats.getSum()) ? "NaN" : new BigDecimal(stats.getSum()).setScale(2, RoundingMode.DOWN);
		Object sumOfSquare = Double.isNaN(stats.getSumsq()) ? "NaN" : new BigDecimal(stats.getSumsq()).setScale(2, RoundingMode.DOWN);
		
		//Rest of Basic Stats
//	    DecimalFormat formatter = new DecimalFormat("#.00");
//	    formatter.setRoundingMode(RoundingMode.DOWN);
//		String mean = Double.isNaN(stats.getMean()) ? "NaN" : formatter.format(stats.getMean());
//		String std = Double.isNaN(stats.getStandardDeviation()) ? "NaN" : formatter.format(stats.getStandardDeviation());
//		String min = Double.isNaN(stats.getMin()) ? "NaN" :  formatter.format(stats.getMin());
//		String maximum = Double.isNaN(stats.getMax()) ? "NaN" : formatter.format(stats.getMax());
//		String variance = Double.isNaN(stats.getVariance()) ? "NaN" :  formatter.format(stats.getVariance());
//		String geometricMean = Double.isNaN(stats.getGeometricMean()) ? "NaN" : formatter.format(stats.getGeometricMean());
//		String sum = Double.isNaN(stats.getSum()) ? "NaN" : formatter.format(stats.getSum());
//		String sumOfSquare = Double.isNaN(stats.getSumsq()) ? "NaN" : formatter.format(stats.getSumsq());
		
		
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
		returnData.put("posTotal", posTotal);
		returnData.put("negTotal", negTotal);
		returnData.put("absTotal", absTotal);
		returnData.put("Kurtosis", kurt);
		
		HashMap<String,Object> basicStats = new HashMap<>();
		basicStats.put("basicStats", returnData);
		myStore.put("ADDITIONAL_INFO", basicStats);
		myStore.put("STATUS", STATUS.SUCCESS);
		System.out.println(output);
		return returnData;
	}
	
	public IPkqlMetadata getPkqlMetadata() {
		MathPkqlMetadata metadata = new MathPkqlMetadata();
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.MATH_FUN));
		metadata.setColumnsOperatedOn((Vector<String>) myStore.get(PKQLEnum.COL_DEF));
		metadata.setProcedureName("Basic Statistics");
		metadata.setAdditionalInfo(myStore.get("ADDITIONAL_INFO"));
		return metadata;
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

