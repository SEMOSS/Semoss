package prerna.sablecc.expressions.sql;

public class SqlAverageReactor extends H2SqlBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	public SqlAverageReactor() {
		this.setRoutine("AVG");
		this.setPkqlRoutine("Average");
	}
}
