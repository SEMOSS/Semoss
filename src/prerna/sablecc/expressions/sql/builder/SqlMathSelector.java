package prerna.sablecc.expressions.sql.builder;

import java.util.List;

import prerna.sablecc.expressions.IExpressionSelector;

public class SqlMathSelector implements IExpressionSelector {

	protected IExpressionSelector selector;
	protected String math;
	protected String pkqlMath;
	
	/*
	 * Create a math routine around an existing selector
	 */
	
	public SqlMathSelector(IExpressionSelector selector, String math, String pkqlMath) {
		this.selector = selector;
		this.math = math;
		this.pkqlMath = pkqlMath;
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
		builder.append(math).append("(").append(selector.toString()).append(")");
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
