package prerna.algorithm.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

public class LeftReactor  extends MathReactor { // TODO create BaseMapperReactor once more mapping algorithms have been added
	
	public LeftReactor() {
		setMathRoutine("Left");
	}
	
	@Override
	public Iterator process() {
		modExpression();
		Vector <String> columns = (Vector <String>)myStore.get(PKQLEnum.COL_DEF);
		String[] columnsArray = convertVectorToArray(columns);
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MAP_OBJ);
		String script = myStore.get("MOD_" + whoAmI).toString();
		String indx = (options.get("CONDITION1") + "").toString();
		Iterator iterator = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), false);
		 String str=script.replace(" ]", "")+".take("+indx+")]";
		String nodeStr = myStore.get(whoAmI).toString();
		
		ExpressionIterator expIt = new ExpressionIterator(iterator, columnsArray,str );
		myStore.put(nodeStr, expIt);
		myStore.put("STATUS",STATUS.SUCCESS);
		
		return expIt;
	}
	
}

//col.add(c:new,m:Left([c:Customer],{"CONDITION1":3,"CONDITION2":3}));