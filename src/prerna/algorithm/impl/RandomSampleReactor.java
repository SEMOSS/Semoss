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
		Iterator itr = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), false);
		

		ITableDataFrame df = (ITableDataFrame)myStore.get("G");
		Object[] rows = df.getUniqueRawValues(columnsArray[0]);
		int numSamples = 0;
		if(myStore.containsKey(PKQLEnum.MATH_PARAM)) {
			Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
			if(options.containsKey("numSamples".toUpperCase())) {
				numSamples = Integer.parseInt(options.get("numSamples".toUpperCase()) + "");
			} else {
				numSamples = rows.length/5;
			}
		} else {
			//TODO: need to throw an error saying parameters are required
			numSamples = rows.length/5;
		}
		
		List<Integer> samplesList = new ArrayList<>(rows.length);
		for(int i=0;i<rows.length;i++){
			int j = (i<numSamples)? 1 : 0;
			samplesList.add(j);
		}
		Collections.shuffle(samplesList);
		Map<Object,Integer> samples = new HashMap<>();
		for(int i=0;i<rows.length;i++){
			samples.put(rows[i],samplesList.get(i));
		}
		
		String script = columnsArray[0];
		
		RandomSampleIterator expItr = new RandomSampleIterator(itr, columnsArray,script, samples);
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