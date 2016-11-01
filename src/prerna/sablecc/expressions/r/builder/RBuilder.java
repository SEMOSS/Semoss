package prerna.sablecc.expressions.r.builder;

import prerna.ds.R.RDataTable;
import prerna.sablecc.expressions.AbstractExpressionBuilder;
import prerna.sablecc.expressions.IExpressionSelector;

public class RBuilder extends AbstractExpressionBuilder{

	// the data frame to execute the expression on
	protected RDataTable frame;
	
	// sql objects
	protected RSelectorStatement selectors = new RSelectorStatement();
	protected RGroupBy groups = new RGroupBy();
	
	public RBuilder(RDataTable frame) {
		this.frame = frame;
	}
	
	@Override
	public RDataTable getFrame() {
		return this.frame;
	}
	
	@Override
	public void addSelector(IExpressionSelector selector) {
		selectors.addSelector(selector);
	}
	
	@Override
	public void addSelector(int index, IExpressionSelector selector) {
		selectors.addSelector(index, selector);
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return null;
	}
}
