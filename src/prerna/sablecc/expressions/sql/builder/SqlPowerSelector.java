package prerna.sablecc.expressions.sql.builder;

import java.util.List;

import prerna.sablecc.expressions.IExpressionSelector;

public class SqlPowerSelector implements IExpressionSelector {

	private IExpressionSelector base;
	private double power;
	
	/*
	 * Rounds a specific math output
	 */
	
	public SqlPowerSelector(IExpressionSelector base, double power) {
		this.base = base;
		this.power = power;
	}
	
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("POWER( ").append(base.toString()).append(" , ").append(this.power).append(" )");
		return builder.toString();
	}
	
	@Override
	public List<String> getTableColumns() {
		return this.base.getTableColumns();
	}

	@Override
	public String getName() {
		StringBuilder builder = new StringBuilder();
		builder.append("POWER_").append(base.getName());
		return builder.toString();
	}
	
}