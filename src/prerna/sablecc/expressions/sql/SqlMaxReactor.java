package prerna.sablecc.expressions.sql;

public class SqlMaxReactor extends H2SqlBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	public SqlMaxReactor() {
		this.setMathRoutine("MAX");
	}
}
