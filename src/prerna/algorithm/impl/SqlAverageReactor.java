package prerna.algorithm.impl;

public class SqlAverageReactor extends SqlBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	public SqlAverageReactor() {
		this.setMathRoutine("AVG");
	}
}
