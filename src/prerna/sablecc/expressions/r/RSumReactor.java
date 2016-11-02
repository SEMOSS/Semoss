package prerna.sablecc.expressions.r;

public class RSumReactor extends RBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	
	public RSumReactor() {
		this.setMathRoutine("sum");
		this.setPkqlMathRoutine("Sum");
	}
	
}
