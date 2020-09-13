package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import prerna.sablecc.PKQLRunner.STATUS;

public class CorrelationCoefficientReactor extends BaseReducerReactor {
	
	public CorrelationCoefficientReactor() {
		setMathRoutine("CorrelationCoefficient");
	}
	
	@Override
	public Object reduce() {

	    ArrayList<Double> x1  = new ArrayList<Double>();
	    ArrayList<Double> y1  = new ArrayList<Double>();

	 	while(inputIterator.hasNext() && !errored)
		{
			ArrayList dec = (ArrayList)getNextValue();
			String a = dec.get(0).toString().trim();
			Double b = Double.parseDouble(a);
			x1.add(Double.parseDouble( dec.get(0).toString()));
			y1.add(Double.parseDouble( dec.get(1).toString()));
		}
	 	int numRows = x1.size();
		double[] x = new double[numRows] ;
		double[] y = new double[numRows] ;
		
		for(int i=0; i< numRows;i++)
		{
		    x[i] = Double.parseDouble( x1.get(i).toString()) ;
		    y[i] = Double.parseDouble( y1.get(i).toString()) ;
		}
		double corr = new PearsonsCorrelation().correlation(y,x);	
		System.out.println(corr);
		HashMap<String,Object> CorrelationCoefficient = new HashMap<>();
		CorrelationCoefficient.put("CorrelationCoefficient", corr);
		myStore.put("ADDITIONAL_INFO", CorrelationCoefficient);
		myStore.put("STATUS", STATUS.SUCCESS);
		return corr;
	}
	@Override
	public HashMap<HashMap<Object, Object>, Object> reduceGroupBy(Vector<String> groupBys,
			Vector<String> processedColumns, String[] columnsArray, Iterator it) {
		// TODO Auto-generated method stub
		return null;
	}
	
//	@Override
	//public HashMap<HashMap<Object, Object>,Object> reduceGroupBy(Vector<String> groupBys, Vector<String> processedColumns, String[] columnsArray, Iterator it) {
		//HashMap<HashMap<Object,Object>, Object> groupByHash = new HashMap<HashMap<Object,Object>,Object>();
		
//		while(it.hasNext()){
//			Object[] row = (Object[]) it.next();
//			HashMap<Object, Object> key = new HashMap<Object, Object>();
//			for(String groupBy : groupBys) {
//				int groupByIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(columnsArray, groupBy);
//				Object instance = row[groupByIndex];
//				key.put(groupBy, instance);
//			}
//			int processedIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(columnsArray, processedColumns.get(0));
//			if (row[processedIndex] instanceof Number) {
//				double value = ((Number)row[processedIndex]).doubleValue();
//				HashMap<String,Object> paramMap = (HashMap<String,Object>)groupByHash.get(key);
//				if(paramMap == null) {
//					paramMap = new HashMap<String,Object>();
//					groupByHash.put(key, paramMap);
//					paramMap.put("SUM", 0.0);
//					paramMap.put("COUNT", 0);
//				}
//				paramMap.put("SUM", (Double)paramMap.get("SUM") + value);
//				paramMap.put("COUNT", (Integer)paramMap.get("COUNT")+1);
//			}
//		}
//		for(HashMap<Object,Object> key: groupByHash.keySet()) {
//			HashMap<Object,Object> paramMap = (HashMap<Object,Object>)groupByHash.get(key);
//			groupByHash.put(key, (Double)paramMap.get("SUM")/(Integer)paramMap.get("COUNT"));
//		}
//		
	//	return groupByHash;
	//}
	
	@Override
	public Map<String, Object> getColumnDataMap() {
		return getBaseColumnDataMap();
	}
	
}

