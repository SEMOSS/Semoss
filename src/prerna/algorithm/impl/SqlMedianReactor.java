package prerna.algorithm.impl;

public class SqlMedianReactor extends SqlBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	public SqlMedianReactor() {
		this.setMathRoutine("MEDIAN");
	}
	
}
