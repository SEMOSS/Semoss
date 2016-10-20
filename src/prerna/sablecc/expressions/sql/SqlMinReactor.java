package prerna.sablecc.expressions.sql;

public class SqlMinReactor extends H2SqlBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	public SqlMinReactor() {
		this.setMathRoutine("MIN");
	}
}
