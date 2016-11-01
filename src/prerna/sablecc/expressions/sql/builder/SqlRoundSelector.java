package prerna.sablecc.expressions.sql.builder;

import java.util.List;

import prerna.sablecc.expressions.IExpressionSelector;

public class SqlRoundSelector implements IExpressionSelector {

	private int precision;
	private IExpressionSelector selector;
	
	/*
	 * Rounds a specific math output
	 */
	
	public SqlRoundSelector(IExpressionSelector selector, int precision) {
		this.selector = selector;
		this.precision = precision;
	}
	
	public int getPrecision() {
		return this.precision;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ROUND( ").append(selector.toString()).append(" , ").append(this.precision).append(" )");
		return builder.toString();
	}
	
	@Override
	public List<String> getTableColumns() {
		return this.selector.getTableColumns();
	}
	
}
