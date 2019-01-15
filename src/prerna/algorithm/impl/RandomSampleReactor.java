package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RandomSampleReactor extends MathReactor {
	
	public RandomSampleReactor() {
		setMathRoutine("RandSample");
	}
	
	@Override
	public Iterator process() {
		modExpression();
		
		ITableDataFrame dataFrame = (ITableDataFrame)myStore.get("G");
		
		String[] columnsArray = dataFrame.getColumnHeaders(); //convertVectorToArray(columns);
		
		Vector<String> columns = new Vector<>(Arrays.asList(columnsArray));//(Vector <String>) myStore.get(PKQLEnum.COL_DEF);
			
		Map<String,Object> sampleDetails = new HashMap<>();
		
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MAP_OBJ);
		
		String samplingRegionColumn = null;
		if (columns.contains("Region"))
			samplingRegionColumn = "Region";
		
		List<Map<Object,Integer>> regions = new ArrayList<>();
		for(String regionName : options.keySet()){
			int numSamples = (int)options.get(regionName);
			sampleDetails.put(regionName,numSamples);
			regions.add(fetchSamplesForRegion(dataFrame, columns, numSamples, samplingRegionColumn, regionName));
		}
		
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
			NounMetadata valNoun = new NounMetadata(filterValues, PixelDataType.CONST_STRING);
			NounMetadata colNoun = new NounMetadata(dataFrame.getName() + "__" + sampleColumn, PixelDataType.COLUMN);
			SimpleQueryFilter qf = new SimpleQueryFilter(colNoun, "==", valNoun);
			GenRowFilters grf = new GenRowFilters();
			grf.addFilters(qf);
			dataFrame.setFilter(grf);
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