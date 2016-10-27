package prerna.sablecc.expressions.sql;

public class H2SqlSampleStandardDeviationReactor extends H2SqlBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	public H2SqlSampleStandardDeviationReactor() {
		this.setMathRoutine("STDDEV_SAMP");
		this.setPkqlMathRoutine("StandardDeviation");
	}
	
}
