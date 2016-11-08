package prerna.sablecc.expressions.sql.builder;

import prerna.sablecc.expressions.IExpressionSelector;

public class SqlDistinctGroupConcat extends SqlGroupConcat {

	public SqlDistinctGroupConcat(IExpressionSelector selector, String separator) {
		super(selector, separator);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		if(separator != null && !separator.isEmpty()) {
			builder.append("GROUP_CONCAT(DISTINCT ").append(selector.toString()).append(" SEPARATOR '").append(separator).append("')");
		} else {
			builder.append("GROUP_CONCAT(DISTINCT ").append(selector.toString()).append(")");
		}
		return builder.toString();
	}

}
