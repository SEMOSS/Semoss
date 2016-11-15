package prerna.algorithm.impl;

import java.util.Iterator;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;


public class AbsoluteReactor extends MathReactor {

	public AbsoluteReactor() {
		setMathRoutine("Absolute");
	}
	
	@Override
	public Iterator process(){
		modExpression();
		Vector<String> columns=  (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		String[] columnArray = convertVectorToArray(columns);
		Iterator itr = getTinkerData(columns, (ITableDataFrame)myStore.get("G") , false);
		String script;
		
		script = "Math.abs(" + columnArray[0] + ")";
				
		ExpressionIterator expItr =  new ExpressionIterator(itr, columnArray, script);
		String pkql = myStore.get(whoAmI).toString();
		myStore.put(pkql, expItr);
		myStore.put("STATUS",STATUS.SUCCESS);
			
		
		return expItr;
	
	}
	
}
