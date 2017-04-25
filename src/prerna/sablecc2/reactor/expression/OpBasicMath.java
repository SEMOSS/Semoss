package prerna.sablecc2.reactor.expression;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.sablecc.expressions.sql.H2SqlExpressionIterator;
import prerna.sablecc.expressions.sql.builder.SqlColumnSelector;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
import prerna.sablecc.expressions.sql.builder.SqlMathSelector;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.ArrayUtilityMethods;

public abstract class OpBasicMath extends OpReactor {

	protected String operation;
	protected boolean allIntValue = true;
	
	/*
	 * This class is to be extended for basic math operations
	 * To deal with string inputs that need to be evaluated
	 * on the frame
	 * 
	 * TODO: stop casting everything to h2frame
	 * 		make generic expression class that uses
	 * 		existing classes
	 */
	
	@Override
	public NounMetadata execute() {
		NounMetadata[] nouns = getValues();
		Object[] values = evaluateNouns(nouns);
		NounMetadata result = evaluate(values);
		return result;
	}
	
	protected abstract NounMetadata evaluate(Object[] values);
	
	protected Object[] evaluateNouns(NounMetadata[] nouns) {
		Object[] evaluatedNouns = new Object[nouns.length];
		for(int i = 0; i < nouns.length; i++) {
			NounMetadata val = nouns[i];
			PkslDataTypes valType = val.getNounName();
			if(valType == PkslDataTypes.CONST_DECIMAL) {
				this.allIntValue = false;
				evaluatedNouns[i] = ((Number) val.getValue()).doubleValue();
			} else if(valType == PkslDataTypes.CONST_INT) {
				evaluatedNouns[i] = ((Number) val.getValue()).intValue(); 
			} else if(valType == PkslDataTypes.COLUMN) {
				// at this point, we have already checked if this is a 
				// variable, so it better exist on the frame
				// TODO: expose int vs. double on the frame
				this.allIntValue = false;	
				evaluatedNouns[i] = evaluateString(this.operation, val);
			} else {
				throw new IllegalArgumentException("Invalid input for "+this.operation+". Require all values to be numeric or column names");
			}
		}
		return evaluatedNouns;
	}
	
	protected double evaluateString(String operation, NounMetadata frameColNoun) {
		// to enter here
		// it is assumed that the value is a string within the frame
		IDataMaker dataMaker = this.planner.getFrame();
		String frameColName = frameColNoun.getValue().toString();
		
		if(dataMaker instanceof ITableDataFrame) {
			ITableDataFrame dataframe = (ITableDataFrame) dataMaker;
			String[] headers = dataframe.getColumnHeaders();
			if(headers == null) {
				throw new IllegalArgumentException("Cannot find variable or column with value " + frameColName);
			}
			if(ArrayUtilityMethods.arrayContainsValue(headers, frameColName)) {
				return evaluateString(dataframe, operation,  frameColName);
			} else {
				throw new IllegalArgumentException("Cannot find variable or column with value " + frameColName);
			}
		} else {
			throw new IllegalArgumentException("Cannot execute " + operation + " of " + frameColName + " with the given frame");
		}
	}
	
	/**
	 * Evaluate a string input - assumes the input is a column on the frame
	 * @param operation			The operation to execute - i.e. sum/min/max
	 * @param val				The name of the column to execute on
	 * @return
	 */
	private double evaluateString(ITableDataFrame frame, String operation, String frameColName) {
		// TODO: make this generic
		// at the moment, just going to assume this is sql
		if(frame instanceof H2Frame) {
			H2Frame h2Frame = (H2Frame) frame;
			SqlColumnSelector colS = new SqlColumnSelector(h2Frame, frameColName);
			SqlMathSelector sumS = new SqlMathSelector(colS, operation, operation);
			
			SqlExpressionBuilder builder = new SqlExpressionBuilder(h2Frame);
			builder.addSelector(sumS);
			H2SqlExpressionIterator it = new H2SqlExpressionIterator(builder);
			if(it.hasNext()) {
				return ((Number) it.next()[0]).doubleValue();
			} else {
				throw new IllegalArgumentException("Failure in execution of " + operation + " " + frameColName);
			}
		} else {
			throw new IllegalArgumentException("Cannot execute " + operation + " of " + frameColName + " with the given frame");
		}
	}
}
