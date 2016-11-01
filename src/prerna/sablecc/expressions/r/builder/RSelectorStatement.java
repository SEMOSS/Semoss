package prerna.sablecc.expressions.r.builder;

import prerna.sablecc.expressions.AbstractExpressionSelectorStatement;
import prerna.sablecc.expressions.IExpressionSelector;

public class RSelectorStatement extends AbstractExpressionSelectorStatement{

	/*
	 * Only need to implement the toString method
	 */
	
	@Override
	public String toString() {
		int colId = 0;
		StringBuilder builder = new StringBuilder("{");
		StringBuilder outputNames = new StringBuilder("list(");
		for(IExpressionSelector selector : selectors) {
			IRExpressionSelector rSelector = (IRExpressionSelector) selector;
			String name = "V" + colId;
			if(builder.length() == 1) {
				builder.append(name).append("=").append(rSelector);
				outputNames.append(rSelector.getName());
			} else {
				builder.append(" ; ").append(name).append("=").append(rSelector);
			}
			// update the col id so everything is unique
			colId++;
		}
		
		return builder.toString();
	}

}
