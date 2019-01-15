package prerna.sablecc.expressions.r.builder;

import java.util.List;

import prerna.ds.r.RDataTable;
import prerna.sablecc.expressions.AbstractExpressionBuilder;
import prerna.sablecc.expressions.IExpressionSelector;

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
		builder.append(this.frame.getName()).append("[ ").append(this.frame.getFilterString())
			.append(" , ").append(this.selectors.toString());

		// for R, the group by's end up being returned, so we can get duplicate headers
		// dont want that.. so going to clear that up here
		// so keep a list of the headers we want from the main selectors
		
		boolean fixHeaders = false;;
		String groupStr = this.groups.toString();
		if(groupStr.length() > 0) {
			fixHeaders = true;
			builder.append(" , ").append(groupStr);
		}
		builder.append(" ]");
		
		// gotta make sure we keep the ordering though
		StringBuilder headerOrdering = new StringBuilder();
		if(fixHeaders) {
			List<IExpressionSelector> mainSelectors = this.selectors.getSelectors();
			for(IExpressionSelector selector : mainSelectors) {
				if(headerOrdering.length() == 0) {
					headerOrdering.append("c(\"").append(selector.getName()).append("\"");
				} else {
					headerOrdering.append(", \"").append(selector.getName()).append("\"");
				}
			}
			headerOrdering.append(")");
			builder.append("[,").append(headerOrdering).append("]");
		}
		
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
