package prerna.sablecc.expressions.sql;

public class SqlStandardDeviationReactor extends SqlBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	public SqlStandardDeviationReactor() {
		this.setMathRoutine("STDEV");
	}
	
}
