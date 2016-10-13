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
		
		String[] columnsArray = convertVectorToArray(columns);
		ITableDataFrame dataFrame = (ITableDataFrame)myStore.get("G");
		
		Map<String,Object> sampleDetails = new HashMap<>();
		int totalSamples = 0;
		
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		
		List<Map<Object,Integer>> regions = new ArrayList<>();
		if(options.containsKey("aboveBoundSamples".toUpperCase())){
			int numSamples = (int)options.get("aboveBoundSamples".toUpperCase());
			sampleDetails.put("aboveBoundSamples",numSamples);
			totalSamples += numSamples;
			regions.add(fetchSamplesForRegion(dataFrame, columns, numSamples, "Bounds", 0));
		}
		if(options.containsKey("withinBoundSamples".toUpperCase())){
			int numSamples = (int)options.get("withinBoundSamples".toUpperCase());
			sampleDetails.put("withinBoundSamples",numSamples);
			totalSamples += numSamples;
			regions.add(fetchSamplesForRegion(dataFrame, columns, numSamples, "Bounds", 1));
		}
		if(options.containsKey("belowBoundSamples".toUpperCase())){
			int numSamples = (int)options.get("belowBoundSamples".toUpperCase());
			sampleDetails.put("belowBoundSamples",numSamples);
			totalSamples += numSamples;
			regions.add(fetchSamplesForRegion(dataFrame, columns, numSamples, "Bounds", 2));
		}
		int clusterNum = 0;
		List<Integer> clusterSamples = new ArrayList<>();
		while(true){
			if(!options.containsKey("CLUSTER"+ clusterNum))
				break;
			int numSamples = (int)options.get("CLUSTER"+clusterNum);
			clusterSamples.add(numSamples);
			totalSamples += numSamples;
			regions.add(fetchSamplesForRegion(dataFrame, columns, numSamples, "Cluster", clusterNum));
			clusterNum++;
		}
		sampleDetails.put("clusterSamples", clusterSamples);
		
		if(options.containsKey("all".toUpperCase())){
			int numSamples = (int)options.get("all".toUpperCase());
			totalSamples += numSamples;
			regions.add(fetchSamplesForRegion(dataFrame, columns, numSamples, null, null));
		}
		sampleDetails.put("numSamples", totalSamples);
		
		Iterator resultItr = getTinkerData(columns, dataFrame, false);
		String script = columnsArray[0];
		RandomSampleIterator expItr = new RandomSampleIterator(resultItr, columnsArray,script, regions);
		
		String nodeStr = myStore.get(whoAmI).toString();
		myStore.put(nodeStr, expItr);
		HashMap<String,Object> additionalInfo = new HashMap<>();
		additionalInfo.put("SampleDetails", sampleDetails);
		myStore.put("ADDITIONAL_INFO", additionalInfo);
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return expItr;
	}
	
	Map<Object,Integer> fetchSamplesForRegion(ITableDataFrame dataFrame,Vector<String> columns, int numSamples, String sampleColumn, Object colValue){
		List<Object> filterValues = new ArrayList<>();
		if(sampleColumn != null){
			filterValues.add(colValue);
			dataFrame.filter(sampleColumn, filterValues);
		}

		Iterator itr = getTinkerData(columns, dataFrame, false);
		int numRows = 0;
		while(itr.hasNext()){
			numRows++;
			itr.next();
		}
		List<Integer> samples = new ArrayList<>(numRows);
		for(int i=0;i<numRows;i++){
			int sampled = (numSamples > 0)? 1 : 0;
			numSamples--;
			samples.add(sampled);
		}
		Collections.shuffle(samples);
		
		Map<Object,Integer> sampleMap = new HashMap<>();
		itr = getTinkerData(columns, dataFrame, false);
		int i = 0;
		while(itr.hasNext()){
			Object[] row = (Object[])itr.next();
			sampleMap.put(row[0], samples.get(i));
			i++;
		}
		if(sampleColumn != null)
			dataFrame.unfilter(sampleColumn);
		return sampleMap;
	}
}

class RandomSampleIterator extends ExpressionIterator{
	
	protected List<Map<Object,Integer>> regions;
	
	protected RandomSampleIterator() {
		
	}
	
	public RandomSampleIterator(Iterator results, String [] columnsUsed, String script, List<Map<Object,Integer>> regions)
	{
		this.regions = regions;
		setData(results, columnsUsed, script);
	}
		
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return (results != null && results.hasNext());
	}
	
	@Override
	public Object next() {
		Object retObject = new Integer(0);
		
		if(results != null && !errored)
		{
			setOtherBindings();
			Object key = otherBindings.get(columnsUsed[0]);
			for(Map<Object,Integer> region : regions){
				if(region.containsKey(key))
				{
					retObject = region.get(key);
					break;
				}
			}
		}
		return retObject;
	}
}