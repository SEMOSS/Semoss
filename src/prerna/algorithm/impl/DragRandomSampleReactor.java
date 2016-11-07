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
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

public class DragRandomSampleReactor extends MathReactor{
	@Override
	public Iterator process() {
		modExpression();
		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		
		String[] columnsArray = convertVectorToArray(columns);
		ITableDataFrame dataFrame = (ITableDataFrame)myStore.get("G");
		String[] columnHeaders = dataFrame.getColumnHeaders();
		List<String> cols = Arrays.asList(columnHeaders);
		Vector<String> allCols = new Vector<>(Arrays.asList(columnHeaders));
		
		Map<String,Object> sampleDetails = new HashMap<>();
		int totalSamples = 0;
		
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		
		List<Region> regions = new ArrayList<>();
		
		if(options.containsKey("aboveBoundSamples".toUpperCase())){
			int numSamples = (int)options.get("aboveBoundSamples".toUpperCase());
			Region region = new Region(cols.indexOf("Bounds"), numSamples, "0");
			regions.add(region);
		}
		if(options.containsKey("withinBoundSamples".toUpperCase())){
			int numSamples = (int)options.get("withinBoundSamples".toUpperCase());
			Region region = new Region(cols.indexOf("Bounds"), numSamples, "1");
			regions.add(region);
		}
		if(options.containsKey("belowBoundSamples".toUpperCase())){
			int numSamples = (int)options.get("belowBoundSamples".toUpperCase());
			Region region = new Region(cols.indexOf("Bounds"), numSamples, "2");
			regions.add(region);
		}
		int i =1;
		while(options.containsKey(("Cluster"+i).toUpperCase()))
		{
			int numSamples = (int)options.get(("Cluster"+i).toUpperCase());
			Region region = new Region(cols.indexOf("Cluster"), numSamples, String.valueOf(i));
			regions.add(region);
			i++;
		}
		
		i=1;
		while(options.containsKey(("DragCluster"+i).toUpperCase()))
		{
			int numSamples = (int)options.get(("DragCluster"+i).toUpperCase());
			Region region = new Region(cols.indexOf("DragClusters"+i), numSamples, "1");
			regions.add(region);
			i++;
		}
		
		List<Object[]> keys = new ArrayList<>(dataFrame.getNumRows());
		Iterator dataItr = getTinkerData(allCols, dataFrame, false);
		
		while(dataItr.hasNext()){
			Object[] row = (Object[]) dataItr.next();
			keys.add(row);
		}

		Collections.shuffle(keys);
		HashMap<Object,Integer> result  =  new HashMap<>();
		
		for(Object[] key: keys){
			int sampled = 0;
			for(Region r:regions){
				if(r.needSamples(key))
				{
					sampled = 1;
					break;
				}
			}
			result.put(key, sampled);
		}
		
		
		
		Iterator resultItr = getTinkerData(allCols, dataFrame, false);
		String script = columnsArray[0];
		String[] allColsArray = convertVectorToArray(allCols);
		DragRandomSampleIterator expItr = new DragRandomSampleIterator(resultItr, allColsArray,script, result);
		
		String nodeStr = myStore.get(whoAmI).toString();
		myStore.put(nodeStr, expItr);
		HashMap<String,Object> additionalInfo = new HashMap<>();
		additionalInfo.put("SampleDetails", sampleDetails);
		myStore.put("ADDITIONAL_INFO", additionalInfo);
		myStore.put("STATUS", STATUS.SUCCESS);
//		
//		List<Object[]> keys1 = new ArrayList<>(dataFrame.getNumRows());
//		while(expItr.hasNext()){
//			Object[] row = (Object[]) expItr.next();
//			keys1.add(row);
//		}
		
		return expItr;
	}
	
}

class DragRandomSampleIterator extends ExpressionIterator{
	
	protected Map<Object,Integer> result;
	
	protected DragRandomSampleIterator() {
		
	}
	
	public DragRandomSampleIterator(Iterator results, String [] columnsUsed, String script, Map<Object,Integer> result)
	{
		this.result = result;
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
			List<Object> keyList = new ArrayList<>(columnsUsed.length);
			for(int i=0;i<columnsUsed.length;i++)
				keyList.add(otherBindings.get(columnsUsed[i]));
			Object[] key = keyList.toArray();
			retObject = result.get(key);
			boolean keyCheck = result.containsKey(key);
			System.out.println();
			//retObject = result.get(otherBindings.get(columnsUsed[0]));
		}
		return retObject;
	}
}
class Region{
	int colNum;
	int numOfSamples;
	String desiredValue;
	
	public Region (int colNum,int numOfSamples,String desiredValue){
		this.colNum = colNum;
		this.numOfSamples = numOfSamples;
		this.desiredValue = desiredValue;
	}
	public boolean needSamples(Object[] row){
		if(numOfSamples <= 0)
			return false;
		if(String.valueOf(row[colNum]).equals(desiredValue)){
			numOfSamples--;
			return true;
		}
		return false;
	}
}