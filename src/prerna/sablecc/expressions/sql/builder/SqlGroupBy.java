package prerna.sablecc.expressions.sql.builder;

import prerna.sablecc.expressions.AbstractExpressionGroupBy;
import prerna.sablecc.expressions.IExpressionSelector;

public class SqlGroupBy extends AbstractExpressionGroupBy {

	/*
	 * Only need to implement the toString method
	 */

	/**
	 * Adding check that group by needs to be a column and not another expression
	 */
	public void addGroupBy(IExpressionSelector groupBySelector) {
		if( !(groupBySelector instanceof SqlColumnSelector) ){
			throw new IllegalArgumentException("Can only group by a column, not an expression");
		}
		super.addGroupBy(groupBySelector);
	}

	@Override
	public String toString() {
		if(groupByList.isEmpty()) {
			return "";
		}

		StringBuilder builder = new StringBuilder(" GROUP BY ");
		int counter = 0;
		for(IExpressionSelector groupBySelector : groupByList) {
			if(counter == 0) {
				builder.append(groupBySelector);
			} else {
				builder.append(" , ").append(groupBySelector);
			}
			counter++;
		}

		return builder.toString();
	}

}
