package prerna.sablecc.expressions.r;

public class RMedianReactor extends RBasicMathReactor {
	
	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	
	public RMedianReactor() {
		this.setMathRoutine("median");
	}


}
