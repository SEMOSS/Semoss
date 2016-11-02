package prerna.sablecc.expressions.r.builder;

import prerna.sablecc.expressions.AbstractExpressionGroupBy;
import prerna.sablecc.expressions.IExpressionSelector;

public class RGroupBy extends AbstractExpressionGroupBy{
	
	/*
	 * Only need to implement the toString method
	 */

	/**
	 * Adding check that group by needs to be a column and not another expression
	 */
	public void addGroupBy(IExpressionSelector groupBySelector) {
		if( !(groupBySelector instanceof RColumnSelector) ){
			throw new IllegalArgumentException("Can only group by a column, not an expression");
		}
		super.addGroupBy(groupBySelector);
	}

	@Override
	public String toString() {
		if(groupByList.isEmpty()) {
			return "";
		}

		StringBuilder builder = new StringBuilder(" by = c( ");
		int counter = 0;
		for(IExpressionSelector groupBySelector : groupByList) {
			if(counter == 0) {
				builder.append("\"").append(groupBySelector).append("\"");
			} else {
				builder.append(" , \"").append(groupBySelector).append("\"");
			}
			counter++;
		}
		
		builder.append(")");
		return builder.toString();
	}
}
