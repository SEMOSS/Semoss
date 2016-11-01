package prerna.sablecc.expressions.sql.builder;

import prerna.sablecc.expressions.AbstractExpressionSelectorStatement;
import prerna.sablecc.expressions.IExpressionSelector;

public class SqlSelectorStatement extends AbstractExpressionSelectorStatement{

	/*
	 * Only need to implement the toString method
	 */
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for(IExpressionSelector selector : selectors) {
			if(builder.length() == 0) {
				builder.append(selector);
			} else {
				builder.append(" , ").append(selector);
			}
		}
		
		return builder.toString();
	}
}
