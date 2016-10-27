package prerna.sablecc.expressions.sql.builder;

import java.util.List;
import java.util.Vector;

public class SqlConcatSelector implements ISqlSelector {

	private ISqlSelector[] selectors;
	
	/*
	 * Create a math routine around an existing selector
	 */
	
	public SqlConcatSelector(ISqlSelector... selectors) {
		this.selectors = selectors;
	}
	
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		// we assume at least one value in selector
		builder.append("CONCAT( ").append(selectors[0].toString());
		for(int i = 1; i < selectors.length; i++) {
			builder.append(" , ").append(selectors[i].toString());
		}
		builder.append(" )");
		return builder.toString();
	}
	
	@Override
	public List<String> getTableColumns() {
		List<String> tables = new Vector<String>();
		for(int i = 0; i < selectors.length; i++) {
			tables.addAll(selectors[i].getTableColumns());
		}
		
		return tables;
	}
}
