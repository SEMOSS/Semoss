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

		String filterColumn = null;

		if(myStore.containsKey(PKQLEnum.COL_CSV)) {
			filterColumn = ((Vector<String>)myStore.get(PKQLEnum.COL_CSV)).firstElement();
			columns.add(filterColumn);
		}
		
		String[] columnsArray = convertVectorToArray(columns);
		Iterator itr = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), false);
		

		ITableDataFrame df = (ITableDataFrame)myStore.get("G");
		int numRows = df.getNumRows();
		int numSamples = 0;
		float aboveBoundPercent = -1, withinBoundPercent = -1, belowBoundPercent = -1;
		if(myStore.containsKey(PKQLEnum.MATH_PARAM)) {
			Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
			if(options.containsKey("numSamples".toUpperCase())) {
				numSamples = Integer.parseInt(options.get("numSamples".toUpperCase()) + "");
			}
			if(options.containsKey("aboveBoundPercent".toUpperCase())) {
				aboveBoundPercent = Float.parseFloat(options.get("aboveBoundPercent".toUpperCase()) + "")/100;
			}
			if(options.containsKey("withinBoundPercent".toUpperCase())) {
				withinBoundPercent = Float.parseFloat(options.get("withinBoundPercent".toUpperCase()) + "")/100;
			}
			if(options.containsKey("belowBoundPercent".toUpperCase())) {
				belowBoundPercent = Float.parseFloat(options.get("belowBoundPercent".toUpperCase()) + "")/100;
			}
		}
		numSamples = numSamples == 0 ? numRows/5 : numSamples;
		aboveBoundPercent = aboveBoundPercent < 0? 0.3f : aboveBoundPercent;
		withinBoundPercent = withinBoundPercent < 0? 0.3f : withinBoundPercent;
		belowBoundPercent = belowBoundPercent < 0? 0.3f : belowBoundPercent;
		
		String script = columnsArray[0];

		RandomSampleIterator expItr = null;
		if(filterColumn == null){		
			List<Integer> randomSamples = new ArrayList<>(numRows);
			for(int i=0; i < numRows; i++){
				int j = (i < numSamples) ? 1 : 0;
				randomSamples.add(j);
			}
			Collections.shuffle(randomSamples);
			Iterator<Integer> randomIterator = randomSamples.iterator();
			Map<Object,Integer> samples = new HashMap<>();
			while(itr.hasNext()){
				Object[] row = (Object[])itr.next();
				samples.put(row[0],randomIterator.next());
			}
			Iterator resultItr = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), false);
			expItr = new RandomSampleIterator(resultItr, columnsArray,script, samples);
		}else{
			List<Integer> aboveBoundSamples = new ArrayList<>();
			List<Integer> withinBoundSamples = new ArrayList<>();
			List<Integer> belowBoundSamples = new ArrayList<>();
			int aboveTgtCount = Math.round(numSamples * aboveBoundPercent);
			int withinTgtCount = Math.round(numSamples * withinBoundPercent);
			int belowTgtCount = Math.round(numSamples * belowBoundPercent);
			while(itr.hasNext()){
				Object[] row = (Object[])itr.next();
				if((double)row[1] == 0){
					int i = (aboveBoundSamples.size() < aboveTgtCount) ? 1 : 0;
					aboveBoundSamples.add(i);
				}
				if((double)row[1] == 1){
					int i = (withinBoundSamples.size() < withinTgtCount) ? 1 : 0;
					withinBoundSamples.add(i);
				}
				if((double)row[1] == 2){
					int i = (belowBoundSamples.size() < belowTgtCount) ? 1 : 0;
					belowBoundSamples.add(i);
				}
			}
			Collections.shuffle(aboveBoundSamples);
			Collections.shuffle(withinBoundSamples);
			Collections.shuffle(belowBoundSamples);
			Map<Object,Integer> samples = new HashMap<>();
			Iterator<Integer> aboveSamplesIterator = aboveBoundSamples.iterator();
			Iterator<Integer> withinSamplesIterator = withinBoundSamples.iterator();
			Iterator<Integer> belowSamplesIterator = belowBoundSamples.iterator();
			Iterator itr2 = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), false);
			while(itr2.hasNext()){
				Object[] row = (Object[])itr2.next();
				if((double) row[1] == 0)
					samples.put(row[0], aboveSamplesIterator.next());
				if((double) row[1] == 1)
					samples.put(row[0], withinSamplesIterator.next());
				if((double) row[1] == 2)
					samples.put(row[0], belowSamplesIterator.next());
			}
			Iterator resultItr = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), false);
			expItr = new RandomSampleIterator(resultItr, columnsArray,script, samples);
		}
		
		
		String nodeStr = myStore.get(whoAmI).toString();
		myStore.put(nodeStr, expItr);
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return expItr;
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