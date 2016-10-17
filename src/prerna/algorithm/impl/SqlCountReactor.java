package prerna.algorithm.impl;

public class SqlCountReactor extends SqlBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	public SqlCountReactor() {
		this.setMathRoutine("COUNT");
	}
}
