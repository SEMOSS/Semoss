package prerna.sablecc.expressions.r.builder;

import java.util.List;

import prerna.ds.R.RDataTable;
import prerna.sablecc.expressions.AbstractExpressionBuilder;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.sql.builder.SqlConstantSelector;

public class RExpressionBuilder extends AbstractExpressionBuilder{

	// the data frame to execute the expression on
	protected RDataTable frame;
	
	public RExpressionBuilder(RDataTable frame) {
		this.frame = frame;
		this.selectors = new RSelectorStatement();
		this.groups = new RGroupBy();
	}
	
	@Override
	public RDataTable getFrame() {
		return this.frame;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(this.frame.getTableVarName()).append("[ ").append(this.frame.getFilterString())
			.append(" , ").append(this.selectors.toString());
		String groupStr = this.groups.toString();
		if(groupStr.length() > 0) {
			builder.append(" , ").append(groupStr);
		}
		builder.append(" ]");
		return builder.toString();
	}
	
	@Override
	public boolean isScalar() {
		List<IExpressionSelector> selectorList = selectors.getSelectors();
		if(selectorList.size() == 1) {
			if(selectorList.get(0) instanceof RConstantSelector) {
				return true;
			}
		}
		
		return false;
	}
	
	@Override
	public Object getScalarValue() {
		if(isScalar()) {
			List<IExpressionSelector> selectorList = selectors.getSelectors();
			return ((RConstantSelector) selectorList.get(0)).getValue();
		}
		return null;
	}
}
