package prerna.sablecc.expressions.sql.builder;

import java.util.List;

import prerna.ds.h2.H2Frame;
import prerna.sablecc.expressions.AbstractExpressionBuilder;
import prerna.sablecc.expressions.IExpressionSelector;

public class SqlExpressionBuilder extends AbstractExpressionBuilder {

	// the data frame to execute the expression on
	protected H2Frame frame;
	
	public SqlExpressionBuilder(H2Frame frame) {
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
//		if(frame.isJoined()) {
//			builder.append(frame.getViewTableName());
//		} else {
			builder.append(frame.getName());
//		}
		
		// add filters
		String filters = frame.getFilterString();
		if(filters != null && !filters.isEmpty()) {
			builder.append(filters);
		}
		
		builder.append(" ").append(groups.toString());
		
		if(this.sortBy != null) {
			builder.append(this.sortBy.toString());
		}
		
		if(this.limit > 0) {
			builder.append(" LIMIT ").append(this.limit);
		}
		
		if(this.offset > 0) {
			builder.append(" OFFSET ").append(this.offset);
		}
		
		return builder.toString();
	}
	
	@Override
	public boolean isScalar() {
		List<IExpressionSelector> selectorList = selectors.getSelectors();
		if(selectorList.size() == 1) {
			if(selectorList.get(0) instanceof SqlConstantSelector) {
				return true;
			}
		}
		
		return false;
	}
	
	@Override
	public Object getScalarValue() {
		if(isScalar()) {
			List<IExpressionSelector> selectorList = selectors.getSelectors();
			return ((SqlConstantSelector) selectorList.get(0)).getValue();
		}
		return null;
	}
}
