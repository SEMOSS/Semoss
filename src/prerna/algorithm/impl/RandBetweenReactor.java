package prerna.algorithm.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

public class RandBetweenReactor extends MathReactor{
	@Override
	public Iterator process(){
		modExpression();
		Vector<String> columns=  (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		String[] columnArray = convertVectorToArray(columns);
		Iterator itr = getTinkerData(columns, (ITableDataFrame)myStore.get("G") , false);
		
		ITableDataFrame idf = (ITableDataFrame)myStore.get("G");
		String[] cols = idf.getColumnHeaders();
		String[] colArray = new String[1];
		colArray[0] = cols[0];
		
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		int rangeFrom = Integer.parseInt(options.get("CONDITION1") + "");
		int rangeTo = Integer.parseInt(options.get("CONDITION2") + "") - rangeFrom;
		String script = "Math.abs(new Random().nextInt() %" + rangeTo + ") + " + rangeFrom;
					
		ExpressionIterator expItr =  new ExpressionIterator(itr, columnArray, script);
		String pkql = myStore.get(whoAmI).toString();
		myStore.put(pkql, expItr);
		myStore.put("STATUS",STATUS.SUCCESS);
			
		
		return expItr;
	
	}
}
