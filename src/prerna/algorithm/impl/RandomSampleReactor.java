package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

public class RandomSampleReactor extends MathReactor{
	@Override
	public Iterator process() {
		modExpression();
		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);

		Vector<String> filterColumns = null;

		if(myStore.containsKey(PKQLEnum.COL_CSV)) {
			filterColumns = ((Vector<String>)myStore.get(PKQLEnum.COL_CSV));
			for(String filterCol : filterColumns){
				columns.add(filterCol);
			}
		}
		
		String[] columnsArray = convertVectorToArray(columns);
		Iterator itr = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), false);

		ITableDataFrame df = (ITableDataFrame)myStore.get("G");
		int numRows = df.getNumRows();
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		int numSamples = Integer.parseInt(options.get("numSamples".toUpperCase()) + "");
		List<SampleRegion> regions = new ArrayList<>();
		if(filterColumns == null)
		{
			SampleRegion region = new SampleRegion(0, numSamples, 0, null);
			regions.add(region);
		}
		else if(filterColumns.size() == 1){
			if(filterColumns.contains("Bounds"))
			{
				float aboveBoundPercent = Float.parseFloat(options.get("aboveBoundPercent".toUpperCase()) + "")/100;
				float withinBoundPercent = Float.parseFloat(options.get("withinBoundPercent".toUpperCase()) + "")/100;
				float belowBoundPercent = Float.parseFloat(options.get("belowBoundPercent".toUpperCase()) + "")/100;

				int aboveTgtCount = Math.round(numSamples * aboveBoundPercent);
				int withinTgtCount = Math.round(numSamples * withinBoundPercent);
				int belowTgtCount = Math.round(numSamples * belowBoundPercent);
				
				SampleRegion aboveBounds = new SampleRegion(0, aboveTgtCount, 1, "0.0");
				SampleRegion withinBounds = new SampleRegion(1, withinTgtCount, 1, "1.0");
				SampleRegion belowBounds = new SampleRegion(2, belowTgtCount, 1, "2.0");
				
				regions.add(aboveBounds);
				regions.add(withinBounds);
				regions.add(belowBounds);
			}
			if(filterColumns.contains("Cluster"))
			{
				int clusterNum = options.keySet().size() - 1;
				
				for(int i=0; i<clusterNum; i++){
					int clusterCount = Math.round(numSamples * Float.parseFloat(options.get("Cluster".toUpperCase() + i) + "")/100);
					SampleRegion clusterRegion = new SampleRegion(i, clusterCount, 1, i + ".0");
					regions.add(clusterRegion);
				}
			}
		}
		else if(filterColumns.contains("Bounds") && filterColumns.contains("Cluster")){
			float aboveBoundPercent = Float.parseFloat(options.get("aboveBoundPercent".toUpperCase()) + "")/100;
			float belowBoundPercent = Float.parseFloat(options.get("belowBoundPercent".toUpperCase()) + "")/100;

			int aboveTgtCount = Math.round(numSamples * aboveBoundPercent);
			int belowTgtCount = Math.round(numSamples * belowBoundPercent);
			
			SampleRegion aboveBounds = new SampleRegion(0, aboveTgtCount, columns.indexOf("Bounds"), "0.0");
			SampleRegion belowBounds = new SampleRegion(1, belowTgtCount, columns.indexOf("Bounds"), "2.0");
			regions.add(aboveBounds);
			regions.add(belowBounds);

			int clusterNum = options.keySet().size() - 3;
			int clusterColIndex = columns.indexOf("Cluster");
			for(int i=0; i<clusterNum; i++){
				int clusterCount = Math.round(numSamples * Float.parseFloat(options.get("Cluster".toUpperCase() + i) + "")/100);
				SampleRegion clusterRegion = new SampleRegion(i+2, clusterCount, clusterColIndex, i + ".0");
				regions.add(clusterRegion);
			}
		}
		String script = columnsArray[0];
		Iterator resultItr = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), false);
		Map<Object,Integer> samples = new HashMap<>();
		while(itr.hasNext()){
			Object[] row = (Object[]) itr.next();
			int sampled = 0;
			for(SampleRegion region : regions){
				if(region.CheckAndAdd(row)){
					sampled = 1;
					break;
				}
			}
			samples.put(row[0], sampled);
		}
		RandomSampleIterator expItr = new RandomSampleIterator(resultItr, columnsArray,script, samples);
		
		String nodeStr = myStore.get(whoAmI).toString();
		myStore.put(nodeStr, expItr);
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return expItr;
	}
}

class SampleRegion {
	int regionNum = -1;
	int colToCheck = -1;
	String valueToMatch = null;
	int samplesToTake = -1;
	
	public SampleRegion(int regionNum, int samplesToTake, int colToCheck,String valueToMatch ){
		this.regionNum = regionNum;
		this.samplesToTake = samplesToTake;
		this.colToCheck = colToCheck;
		this.valueToMatch = valueToMatch;
	}
	
	public boolean CheckAndAdd(Object[] datapoint){
		if(samplesToTake < 1)
			return false;
		if(valueToMatch != null && valueToMatch.equals(String.valueOf(datapoint[colToCheck]))){
			samplesToTake--;
			return true;
		}
		else if (valueToMatch == null && datapoint[colToCheck] != null){
			samplesToTake--;
			return true;
		}
		return false;
			
	}
}

class RandomSampleIterator extends ExpressionIterator{
	
	protected Map<Object,Integer> samples;
	
	protected RandomSampleIterator() {
		
	}
	
	public RandomSampleIterator(Iterator results, String [] columnsUsed, String script, Map<Object,Integer> samples)
	{
		this.samples = samples;
		setData(results, columnsUsed, script);
	}
		
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return (results != null && results.hasNext());
	}
	
	@Override
	public Object next() {
		Object retObject = null;
		
		if(results != null && !errored)
		{
			setOtherBindings();
			retObject = samples.get(otherBindings.get(columnsUsed[0]));
		}
		return retObject;
	}
}