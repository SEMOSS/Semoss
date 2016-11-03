package prerna.sablecc.expressions.sql.builder;

import prerna.sablecc.expressions.IExpressionSelector;

public class SqlDistinctMathSelector extends SqlMathSelector {

	/*
	 * Create a math routine around an existing selector
	 */
	
	public SqlDistinctMathSelector(IExpressionSelector selector, String math, String pkqlMath) {
		super(selector, math, pkqlMath);
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(math).append("(DISTINCT ").append(selector.toString()).append(")");
		return builder.toString();
	}
	
}
