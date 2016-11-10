package prerna.sablecc.expressions.r.builder;

import java.util.List;

import prerna.sablecc.expressions.IExpressionSelector;

public class RMathSelector implements IExpressionSelector {

	private IExpressionSelector selector;
	private String math;
	private String pkqlMath;
	private boolean castAsNumber;
	
	/*
	 * Create a math routine around an existing selector
	 */
	
	public RMathSelector(IExpressionSelector selector, String math, String pkqlMath, boolean castAsNumber) {
		this.selector = selector;
		this.math = math;
		this.pkqlMath = pkqlMath;
		this.castAsNumber = castAsNumber;
	}
	
	public String getPkqlMath() {
		return this.pkqlMath;
	}
	
	public String getMath() {
		return this.math;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		if(castAsNumber) {
			builder.append(math).append("(as.numeric(na.omit(").append(selector.toString()).append(")))");
		} else {
			builder.append(math).append("(na.omit(").append(selector.toString()).append("))");
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
		builder.append(pkqlMath).append("_").append(selector.getName());
		return builder.toString();
	}
	
}