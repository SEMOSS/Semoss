package prerna.algorithm.impl;

import java.util.Iterator;

import prerna.ds.ExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLRunner.STATUS;

public class ConcatReactor extends MathReactor { // TODO create BaseMapperReactor once more mapping algorithms have been added
	
	Iterator it = null;
	String[] headers = null;
	String script = null;

	
	public void setData(Iterator results, String[] columnsUsed, String script) {
		it = results;
		headers = columnsUsed;
		this.script = script;
	}
	
	@Override
	public Iterator process() {
		modExpression();
		String nodeStr = myStore.get(whoAmI).toString();
		
		ExpressionIterator expIt = new ExpressionIterator(it, headers, script.replace(",", "+"));
		myStore.put(nodeStr, expIt);
		myStore.put("STATUS",STATUS.SUCCESS);
		
		return expIt;
	}
	
}
