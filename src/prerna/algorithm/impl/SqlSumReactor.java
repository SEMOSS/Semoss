package prerna.algorithm.impl;

public class SqlSumReactor extends SqlBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	public SqlSumReactor() {
		this.setMathRoutine("SUM");
	}
}
