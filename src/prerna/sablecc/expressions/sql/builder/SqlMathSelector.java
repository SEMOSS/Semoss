package prerna.sablecc.expressions.sql.builder;

import java.util.List;

public class SqlMathSelector implements ISqlSelector {

	private ISqlSelector selector;
	private String math;
	private String pkqlMath;
	
	/*
	 * Create a math routine around an existing selector
	 */
	
	public SqlMathSelector(ISqlSelector selector, String math, String pkqlMath) {
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

}
