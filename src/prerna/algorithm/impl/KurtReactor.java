package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import prerna.sablecc.PKQLEnum;

public class KurtReactor extends BaseReducerReactor {
	
	@Override
	public Object reduce() {
		DescriptiveStatistics stats = new DescriptiveStatistics();
		// double[] Arr=new double[arraySize]; 
		// ArrayList<Double> test =  new ArrayList<>();
		while(inputIterator.hasNext() && !errored) {
            ArrayList dec = (ArrayList)getNextValue();
            if(dec.get(0) instanceof Number) {
                  stats.addValue(((Number)dec.get(0)).doubleValue());
}}
   		 double kurt;
         kurt=stats.getKurtosis();

		 System.out.println(kurt);
		 return kurt;}
	@Override
	public HashMap<HashMap<Object, Object>, Object> reduceGroupBy(Vector<String> groupBys,
			Vector<String> processedColumns, String[] columnsArray, Iterator it) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Map<String, Object> getColumnDataMap() {
		return getBaseColumnDataMap("Kurt");
	}
}

