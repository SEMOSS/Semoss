package prerna.sablecc.expressions.sql.builder;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.expressions.AbstractExpressionBuilder;

public class SqlBuilder extends AbstractExpressionBuilder {

	// the data frame to execute the expression on
	protected H2Frame frame;
	
	public SqlBuilder(H2Frame frame) {
		this.frame = frame;
		this.selectors = new SqlSelectorStatement();
		this.groups = new SqlGroupBy();
	}
	
	@Override
	public H2Frame getFrame() {
		return this.frame;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("SELECT DISTINCT ").append(this.selectors.toString()).append(" FROM ");
		
		// determine if querying view or table
		if(frame.isJoined()) {
			builder.append(frame.getViewTableName());
		} else {
			builder.append(frame.getTableName());
		}
		
		// add filters
		String filters = frame.getSqlFilter();
		if(filters != null && !filters.isEmpty()) {
			builder.append(filters);
		}
		
		builder.append(" ").append(groups.toString());
		
		return builder.toString();
	}
}
