package prerna.sablecc.expressions.r;

public class RUniqueCountReactor extends RBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */

	public RUniqueCountReactor() {
		this.setMathRoutine("uniqueN");
		this.setPkqlMathRoutine("UniqueCount");
	}
}
