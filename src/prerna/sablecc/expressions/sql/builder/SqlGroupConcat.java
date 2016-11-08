package prerna.sablecc.expressions.sql.builder;

import java.util.List;

import prerna.sablecc.expressions.IExpressionSelector;

public class SqlGroupConcat implements IExpressionSelector {

	protected IExpressionSelector selector;
	protected String separator;
	
	/*
	 * Create a math routine around an existing selector
	 */
	
	public SqlGroupConcat(IExpressionSelector selector, String separator) {
		this.selector = selector;
		this.separator = separator;
	}
	
	public String getSeparator() {
		return this.separator;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		if(separator != null && !separator.isEmpty()) {
			builder.append("GROUP_CONCAT(").append(selector.toString()).append(" SEPARATOR '").append(separator).append("')");
		} else {
			builder.append("GROUP_CONCAT(").append(selector.toString()).append(")");
		}
		return builder.toString();
	}
	
	@Override
	public List<String> getTableColumns() {
		return this.selector.getTableColumns();
	}

	@Override
	public String getName() {
		StringBuilder builder = new StringBuilder();
		builder.append("GROUP_CONCAT_").append(selector.getName());
		return builder.toString();
	}

}
