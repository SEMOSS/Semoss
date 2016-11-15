package prerna.algorithm.impl;

import java.util.Iterator;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

public class ConditionReactor extends MathReactor { // TODO create BaseMapperReactor once more mapping algorithms have been added

	public ConditionReactor() {
		setMathRoutine("Condition");
	}

	@Override
	public Iterator process() {
		modExpression();
		Vector <String> columns = (Vector <String>)myStore.get(PKQLEnum.COL_DEF);
		String[] columnsArray = convertVectorToArray(columns);

		Iterator iterator = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), false);

		String nodeStr = myStore.get(whoAmI).toString();
		String expression = myStore.get("MATH_FUN").toString();

		String script = expression.replace("c:", " ");

		ExpressionIterator expIt = new ExpressionIterator(iterator, columnsArray,script );
		myStore.put(nodeStr, expIt);
		myStore.put("STATUS",STATUS.SUCCESS);

		return expIt;
	}

}
