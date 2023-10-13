package prerna.reactor.expression;

import java.math.BigDecimal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpSumIf extends OpBasic {
	
	private static final Logger LOGGER = LogManager.getLogger(OpSumIf.class.getName());
	
	public OpSumIf() {
		this.operation = "SumIf";
		this.keysToGet = new String[]{ReactorKeysEnum.VALUES.getKey(), ReactorKeysEnum.CRITERIA.getKey(), ReactorKeysEnum.SUM_RANGE.getKey()};
	}
	
	@Override
	protected NounMetadata evaluate(Object[] values) {
		/*
		 * The first input is the range of values to be evaluated by the criteria
		 * The second input is the criteria - this is a number, expression, or string,
		 * 		if this is a string, it can also be regex!
		 * The third input is an optional sum range, if this is empty, it will add the first input
		 */
		int numInputs = values.length;
		
		Object[] range = (Object[]) values[0];
		Object criteriaVal = values[1];
		// instead of doing a bunch of null checks later on
		// just determine right now what the sum range will be
		// just set it to range and if optionalSumRange is there
		// change the reference
		Object[] sumRange = range;
		Object[] optionalSumRange = null;
		if(numInputs == 3) {
			optionalSumRange = (Object[]) values[2];
			if(optionalSumRange != null && optionalSumRange.length > 0) {
				sumRange = optionalSumRange;
			}
		}
		
		BigDecimal sum = BigDecimal.valueOf(0);
		
		// first, lets get the criteria type
		PixelDataType criteriaType = this.curRow.getMeta(1);
		if(criteriaType == PixelDataType.CONST_STRING) {
			LOGGER.debug("Sumif evaluating for string input");
			
			// so this is a string input
			// but string can be passed in as ">number"
			// so we need to account for this
			String criteriaExpression = criteriaVal.toString();
			
			boolean isNumFilter = false;
			String filterPrefix = "";
			Number filterNum = null;
			String[] possibleFilters = new String[]{">",">=","<","<=","!=","<>","=", "=="};
			FILTER_TEST : for(int i = 0; i < possibleFilters.length; i++) {
				String filterToTest = possibleFilters[i];
				if(criteriaExpression.startsWith(filterToTest)) {
					// also test that the second portion is a valid number
					try {
						// make sure the rest of the string is a valid number
						filterNum = new BigDecimal(criteriaExpression.substring(filterToTest.length()-1, filterToTest.length()));
						filterPrefix = filterToTest;
						isNumFilter = true;
						break FILTER_TEST ;
					} catch(NumberFormatException e) {
						// do nothing
					}
				}
			}
			
			if(isNumFilter) {
				LOGGER.debug("... but this is actually a number comparison");
				// this is the simple case
				// just do it where the values match
				int arrLength = sumRange.length;
				for(int i = 0; i < arrLength; i++) {
					Number rangeVal = (Number) range[i];
					Number sumVal = (Number) sumRange[i];
					sum = sum.add(evalNumericalExpression(rangeVal, filterPrefix, filterNum, sumVal));
				}
				
			} else {
				// we have an actual string check
				if(criteriaExpression.matches(".*(?<!~)\\*.*") || criteriaExpression.contains(".*(?<!~)\\\\?.*")) {
					LOGGER.debug("... but this is actually a regex match");
					
					// modify the expression so we can just do a 
					// match while we look through
					criteriaExpression = criteriaExpression.replaceAll("(?<!~)\\*", ".*");
					criteriaExpression = criteriaExpression.replaceAll("(?<!~)\\?", ".");

					// need to account for a literal ? or *
					criteriaExpression = criteriaExpression.replace("~*", "\\*");
					criteriaExpression = criteriaExpression.replace("~?", "\\?");

					int arrLength = sumRange.length;
					for(int i = 0; i < arrLength; i++) {
						// just do a numbers equal and call it a day
						String rangeVal = range[i].toString();
						Number sumVal = (Number) sumRange[i];
						if(rangeVal.matches(criteriaExpression)) {
							sum = sum.add(BigDecimal.valueOf(sumVal.doubleValue()));
						}
					}
					
				} else {
					// need to account for a literal ? or *
					criteriaExpression = criteriaExpression.replace("~*", "*");
					criteriaExpression = criteriaExpression.replace("~?", "?");
					
					// easy, just loop through
					int arrLength = sumRange.length;
					for(int i = 0; i < arrLength; i++) {
						// just do a numbers equal and call it a day
						String rangeVal = range[i].toString();
						Number sumVal = (Number) sumRange[i];
						if(rangeVal.equals(criteriaExpression)) {
							sum = sum.add(BigDecimal.valueOf(sumVal.doubleValue()));
						}
					}
				}
			}
		} else if(criteriaType == PixelDataType.CONST_INT || criteriaType == PixelDataType.CONST_DECIMAL) {
			LOGGER.debug("Sumif evaluating for exact number");
			
			double criteriaDouble = ((Number) criteriaVal).doubleValue();
			
			// this is the simple case
			// just do it where the values match
			int arrLength = sumRange.length;
			for(int i = 0; i < arrLength; i++) {
				// just do a numbers equal and call it a day
				Number rangeVal = (Number) range[i];
				Number sumVal = (Number) sumRange[i];
				if(rangeVal.equals(criteriaDouble)) {
					sum = sum.add(BigDecimal.valueOf(sumVal.doubleValue()));
				}
			}
		}
		
		NounMetadata retNoun = null;
		double result = sum.doubleValue();
		if(result == Math.rint(result)) {
			retNoun = new NounMetadata((int) result, PixelDataType.CONST_INT);
		} else {
			// not a valid integer
			// return as a double
			retNoun = new NounMetadata(result, PixelDataType.CONST_DECIMAL);
		}
		
		return retNoun;
	}

	/**
	 * Evaluate the numerical filter on the range value
	 * @param rangeVal
	 * @param filterPrefix
	 * @param filterNum
	 * @param sumVal
	 * @return
	 */
	private static BigDecimal evalNumericalExpression(Number rangeVal, String filterPrefix, Number filterNum, Number sumVal) {
		
		// this is really annoying
		// need to catch which one it is
		// and just do the test
		
		BigDecimal ret = null;
		
		if(filterPrefix.equals(">")) {
			if(rangeVal.doubleValue() > filterNum.doubleValue()) {
				ret = BigDecimal.valueOf(sumVal.doubleValue());
			} else {
				ret = BigDecimal.valueOf(0);
			}
		} else if(filterPrefix.equals(">=")) {
			if(rangeVal.doubleValue() >= filterNum.doubleValue()) {
				ret = BigDecimal.valueOf(sumVal.doubleValue());
			} else {
				ret = BigDecimal.valueOf(0);
			}
		} else if(filterPrefix.equals("<")) {
			if(rangeVal.doubleValue() < filterNum.doubleValue()) {
				ret = BigDecimal.valueOf(sumVal.doubleValue());
			} else {
				ret = BigDecimal.valueOf(0);
			}
		} else if(filterPrefix.equals("<=")) {
			if(rangeVal.doubleValue() <= filterNum.doubleValue()) {
				ret = BigDecimal.valueOf(sumVal.doubleValue());
			} else {
				ret = BigDecimal.valueOf(0);
			}
		} else if(filterPrefix.equals("!=")) {
			if(rangeVal.doubleValue() != filterNum.doubleValue()) {
				ret = BigDecimal.valueOf(sumVal.doubleValue());
			} else {
				ret = BigDecimal.valueOf(0);
			}
		} else if(filterPrefix.equals("<>")) {
			if(rangeVal.doubleValue() != filterNum.doubleValue()) {
				ret = BigDecimal.valueOf(sumVal.doubleValue());
			} else {
				ret = BigDecimal.valueOf(0);
			}
		} else if(filterPrefix.equals("=")) {
			if(rangeVal.doubleValue() == filterNum.doubleValue()) {
				ret = BigDecimal.valueOf(sumVal.doubleValue());
			} else {
				ret = BigDecimal.valueOf(0);
			}
		} else if(filterPrefix.equals("==")) {
			if(rangeVal.doubleValue() == filterNum.doubleValue()) {
				ret = BigDecimal.valueOf(sumVal.doubleValue());
			} else {
				ret = BigDecimal.valueOf(0);
			}
		}
		
		return ret;
	}

	@Override
	public String getReturnType() {
		return "double";
	}
}
