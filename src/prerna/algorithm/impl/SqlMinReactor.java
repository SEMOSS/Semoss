package prerna.algorithm.impl;

public class SqlMinReactor extends SqlBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	public SqlMinReactor() {
		this.setMathRoutine("MIN");
	}
}
