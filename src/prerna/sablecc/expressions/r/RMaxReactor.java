package prerna.sablecc.expressions.r;

public class RMaxReactor extends RBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	
	public RMaxReactor() {
		this.setMathRoutine("max");
		this.setPkqlMathRoutine("Max");
	}

}
