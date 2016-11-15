package prerna.algorithm.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

public class MIDReactor   extends MathReactor { // TODO create BaseMapperReactor once more mapping algorithms have been added
	
	public MIDReactor() {
		setMathRoutine("MID");
	}
	
	@Override
	public Iterator process() {
		modExpression();
		Vector <String> columns = (Vector <String>)myStore.get(PKQLEnum.COL_DEF);
		String[] columnsArray = convertVectorToArray(columns);
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MAP_OBJ);
		int start_position = Integer.parseInt(options.get("CONDITION1") + "");
		int number_of_characters = Integer.parseInt(options.get("CONDITION2") + "");
		String script = myStore.get("MOD_" + whoAmI).toString().replace(" ]", "");
		Iterator iterator = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), false);
	    String str=script+".drop("+start_position+").take("+number_of_characters+")]";
		String nodeStr = myStore.get(whoAmI).toString();
		
		ExpressionIterator expIt = new ExpressionIterator(iterator, columnsArray,str );
		myStore.put(nodeStr, expIt);
		myStore.put("STATUS",STATUS.SUCCESS);
		
		return expIt;
	}
	
}