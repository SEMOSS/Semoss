package prerna.algorithm.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

public class RoundReactor extends MathReactor{
	@Override
	public Iterator process(){
		modExpression();
		Vector<String> columns=  (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		String[] columnArray = convertVectorToArray(columns);
		Iterator itr = getTinkerData(columns, (ITableDataFrame)myStore.get("G") , false);
				
		
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		double significantDigit = Integer.parseInt(options.get("CONDITION1") + "");
		double digit = Math.pow(10, significantDigit);
		String script = "Math.round("+columnArray[0] +"* "+ digit+")/" + digit;
					
		ExpressionIterator expItr =  new ExpressionIterator(itr, columnArray, script);
		String pkql = myStore.get(whoAmI).toString();
		myStore.put(pkql, expItr);
		myStore.put("STATUS",STATUS.SUCCESS);
			
		
		return expItr;
	
	}
}
