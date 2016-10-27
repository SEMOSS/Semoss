package prerna.sablecc.expressions.sql.builder;

import java.util.List;

public class SqlCastSelector implements ISqlSelector {

	private String type;
	private ISqlSelector selector;
	
	/*
	 * Cast a return to a specific sql type
	 */
	
	public SqlCastSelector(String type, ISqlSelector selector) {
		this.type = type;
		this.selector = selector;
	}
	
	public String getCastType() {
		return this.type;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("CAST( ").append(selector.toString()).append(" AS ").append(this.type).append(" )");
		return builder.toString();
	}
	
	@Override
	public List<String> getTableColumns() {
		return this.selector.getTableColumns();
	}

}
