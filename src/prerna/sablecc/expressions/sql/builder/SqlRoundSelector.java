package prerna.sablecc.expressions.sql.builder;

import java.util.List;

public class SqlRoundSelector implements ISqlSelector {

	private int precision;
	private ISqlSelector selector;
	
	/*
	 * Rounds a specific math output
	 */
	
	public SqlRoundSelector(ISqlSelector selector, int precision) {
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
