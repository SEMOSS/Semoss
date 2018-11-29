package prerna.sablecc2.reactor.expression;

import java.util.Iterator;
import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.JavaExecutable;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.ArrayUtilityMethods;

public abstract class OpBasicMath extends OpReactor {

	protected String operation;
	protected boolean returnInteger = true;

	protected abstract double evaluate(Object[] values);

	/*
	 * This class is to be extended for basic math operations
	 * To deal with string inputs that need to be evaluated
	 * on the frame
	 * 
	 * TODO: stop casting everything to h2frame
	 * 		make generic expression class that uses
	 * 		existing classes
	 */

	public OpBasicMath() {
		this.keysToGet = new String[]{ReactorKeysEnum.NUMERIC_VALUES.getKey()};
	}


	@Override
	public NounMetadata execute() {
		NounMetadata[] nouns = getValues();
		Object[] values = evaluateNouns(nouns);

		NounMetadata retNoun = null;
		double result = evaluate(values);
		if(returnInteger) {
			// even if all the inputs are integers
			// it is possible that the return is a double
			// example is median when you need to compute an 
			// average between entity at index i and i+1
			if(result == Math.rint(result)) {
				retNoun = new NounMetadata((int) result, PixelDataType.CONST_INT);
			} else {
				// not a valid integer
				// return as a double
				retNoun = new NounMetadata(result, PixelDataType.CONST_DECIMAL);
			}
		} else {
			retNoun = new NounMetadata(result, PixelDataType.CONST_DECIMAL);
		}
		return retNoun;
	}

	protected Object[] evaluateNouns(NounMetadata[] nouns) {
		Object[] evaluatedNouns = new Object[nouns.length];
		for(int i = 0; i < nouns.length; i++) {
			NounMetadata val = nouns[i];
			PixelDataType valType = val.getNounType();
			if(valType == PixelDataType.CONST_DECIMAL) {
				this.returnInteger = false;
				evaluatedNouns[i] = ((Number) val.getValue()).doubleValue();
			} else if(valType == PixelDataType.CONST_INT) {
				evaluatedNouns[i] = ((Number) val.getValue()).intValue(); 
			} else if(valType == PixelDataType.COLUMN) {
				// at this point, we have already checked if this is a 
				// variable, so it better exist on the frame
				// TODO: expose int vs. double on the frame
				this.returnInteger = false;	
				evaluatedNouns[i] = evaluateString(this.operation, val);
			} else if(valType == PixelDataType.CONST_STRING) {
				String valueStr = val.getValue().toString(); 
				if(valueStr.toUpperCase().trim().equals("NONE")) {
					evaluatedNouns[i] = 0.0;
				} else {
					throw new IllegalArgumentException("Invalid input for "+this.operation+". Require all values to be numeric or column names");
				}
			} else {
				throw new IllegalArgumentException("Invalid input for "+this.operation+". Require all values to be numeric or column names");
			}
		}
		return evaluatedNouns;
	}

	protected double evaluateString(String operation, NounMetadata frameColNoun) {
		// to enter here
		// it is assumed that the value is a string within the frame
		IDataMaker dataMaker = this.insight.getDataMaker();
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
		QueryColumnSelector columnSelector = new QueryColumnSelector(frameColName);
		QueryFunctionSelector opFunction = new QueryFunctionSelector();
		opFunction.setFunction(operation);
		opFunction.addInnerSelector(columnSelector);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(opFunction);
		Iterator<IHeadersDataRow> it = frame.query(qs);
		if(it.hasNext()) {
			return ((Number) it.next().getValues()[0]).doubleValue();
		} else {
			throw new IllegalArgumentException("Cannot execute " + operation + " of " + frameColName + " with the given frame");
		}
	}

	/**
	 * Convert the object array to a double array
	 * This should only be used when running a required sort
	 * @param values
	 * @return
	 */
	protected static double[] convertToDoubleArray(Object[] values) {
		return convertToDoubleArray(values, 0, values.length);
	}


	/**
	 * Convert the object array to a double array
	 * This is done based on the start/end index passed in
	 * @param values
	 * @param startIndex
	 * @param lastIndex
	 * @return
	 */
	protected static double[] convertToDoubleArray(Object[] values, int startIndex, int lastIndex) {
		double[] dblArray = new double[lastIndex-startIndex];
		for(int i = startIndex; i < lastIndex; i++) {
			dblArray[i] = ((Number)values[i]).doubleValue();
		}

		return dblArray;
	}

	public String getReturnType() {
		return "double";
	}

	public String getJavaSignature() {
		StringBuilder javaSignature = new StringBuilder(this.getClass().getName()+".eval(new double[]{");
		List<NounMetadata> inputs = this.getJavaInputs();
		for(int i = 0; i < inputs.size(); i++) {
			if(i > 0) {
				javaSignature.append(", ");
			}

			String nextArgument;
			NounMetadata nextNoun = inputs.get(i);
			Object nextInput = inputs.get(i).getValue();
			if(nextInput instanceof JavaExecutable) {
				nextArgument = ((JavaExecutable)nextInput).getJavaSignature();
			} else {
				if(nextNoun.getNounType() == PixelDataType.CONST_STRING) {
					nextArgument = "\""+nextInput.toString() +"\"";
				} else {
					nextArgument = nextInput.toString();
				}
			}
			javaSignature.append(nextArgument);
		}
		javaSignature.append("})");

		return javaSignature.toString();
	}
}
