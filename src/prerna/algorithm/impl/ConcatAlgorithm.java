package prerna.algorithm.impl;

import java.util.Iterator;

import prerna.ds.ExpressionIterator;

public class ConcatAlgorithm extends ExpressionIterator{
	
	public ConcatAlgorithm() {
		
	}
	
	public ConcatAlgorithm(Iterator results, String[] columnsUsed, String script) {
		super(results, columnsUsed, script.replace(",", "+"));
	}

	@Override
	public void setData(Iterator results, String[] columnsUsed, String script) {
		super.setData(results, columnsUsed, script.replace(",", "+"));
	}
	
//	@Override
//	public Object next() {
//		Object retObject = null;
//		
//		if(results != null && !errored)
//		{
//			setOtherBindings();	
//			long nanoTime = System.nanoTime();
//			try {
//				retObject = cs.eval();
//				if(retObject instanceof ArrayList) {
//					String result = "";
//					for (Object value : (ArrayList)retObject) {
//						result += value.toString();
//					}
//					retObject = result;
//				}
//				long now = System.nanoTime();
//				System.out.println("Time Difference..  " + ((now - nanoTime)/1000000));
//			} catch (ScriptException e) {
//				retObject = e;
//				baseException = e;
//				errored = true;
//			}			
//		}
//		return retObject;
//	}
	
}
