package prerna.sablecc.expressions.sql;

public class SqlCountReactor extends H2SqlBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	public SqlCountReactor() {
		this.setRoutine("COUNT");
		this.setPkqlRoutine("Count");
	}
}
