package prerna.sablecc2.reactor.expression;

import prerna.ds.h2.H2Frame;
import prerna.sablecc.expressions.sql.H2SqlExpressionIterator;
import prerna.sablecc.expressions.sql.builder.SqlColumnSelector;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
import prerna.sablecc.expressions.sql.builder.SqlMathSelector;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public abstract class OpBasicMath extends OpReactor{

	/*
	 * This class is to be extended for basic math operations
	 * To deal with string inputs that need to be evaluated
	 * on the frame
	 * 
	 * TODO: stop casting everything to h2frame
	 * 		make generic expression class that uses
	 * 		existing classes
	 */
	
	/**
	 * Evaluate a string input - assumes the input is a column on the frame
	 * @param operation			The operation to execute - i.e. sum/min/max
	 * @param val				The name of the column to execute on
	 * @return
	 */
	protected double evaluateString(String operation, String frameColName) {
		// to enter here
		// it is assumed that the value is a string within the frame
		IDataMaker frameToEvaluate = this.planner.getFrame();
		
		// TODO: make this generic
		// at the moment, just going to assume this is sql
		if(frameToEvaluate instanceof H2Frame) {
			H2Frame h2Frame = (H2Frame) frameToEvaluate;
			SqlColumnSelector colS = new SqlColumnSelector(h2Frame, frameColName);
			SqlMathSelector sumS = new SqlMathSelector(colS, operation, operation);
			
			SqlExpressionBuilder builder = new SqlExpressionBuilder(h2Frame);
			builder.addSelector(sumS);
			H2SqlExpressionIterator it = new H2SqlExpressionIterator(builder);
			if(it.hasNext()) {
				return ((Number) it.next()[0]).doubleValue();
			} else {
				throw new IllegalArgumentException("Error!!! Failure in execution of " + operation + " " + frameColName);
			}
		} else {
			throw new IllegalArgumentException("Error!!! Cannot execute " + operation + " of " + frameColName + " with the given frame");
		}
	}
}
