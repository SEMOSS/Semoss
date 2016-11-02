package prerna.sablecc.expressions.r;

public class RCountReactor extends RBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	
	public RCountReactor() {
		this.setMathRoutine("length");
		this.setPkqlMathRoutine("Count");
	}
}
