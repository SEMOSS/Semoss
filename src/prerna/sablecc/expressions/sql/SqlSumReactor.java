package prerna.sablecc.expressions.sql;

public class SqlSumReactor extends H2SqlBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	public SqlSumReactor() {
		this.setMathRoutine("SUM");
	}
}
