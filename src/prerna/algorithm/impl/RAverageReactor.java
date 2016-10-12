package prerna.algorithm.impl;

public class RAverageReactor extends RBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	
	public RAverageReactor() {
		this.setMathRoutine("mean");
	}
}
