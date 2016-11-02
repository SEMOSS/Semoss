package prerna.sablecc.expressions.r;

public class RMinReactor extends RBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	
	public RMinReactor() {
		this.setMathRoutine("min");
		this.setPkqlMathRoutine("Min");
	}
	
}
