package prerna.sablecc2.om;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

public class QueryFilter {

	/**
	 * Right now, tracking for these types of filters within querying
	 */
	public static enum FILTER_TYPE {COL_TO_COL, COL_TO_VALUES, VALUES_TO_COL, VALUE_TO_VALUE};
		
	private String comparator = null; //'=', '!=', '<', '<=', '>', '>=', '<>', '?like'
	private NounMetadata lComparison = null; //the column we want to filter
	private NounMetadata rComparison = null; //the values to bind the filter on
	
	public QueryFilter(NounMetadata lComparison, String comparator, NounMetadata rComparison)
	{
		this.lComparison = lComparison;
		this.rComparison = rComparison;
		if("<>".equals(comparator)) {
			this.comparator = "!=";
		} else {
			this.comparator = comparator;
		}
	}

	public NounMetadata getLComparison() {
		return lComparison;
	}
	
	public NounMetadata getRComparison() {
		return rComparison;
	}
	
	public String getComparator() {
		return this.comparator;
	}
	
	/**
	 * See if the two query filters are equal in order to be equal, the left
	 * side, right side, and comparator must match
	 * 
	 * @param queryFilter
	 * @return
	 */
	@Override
	public boolean equals(Object obj) {
		QueryFilter queryFilter = (QueryFilter) obj;
		if (queryFilter == null) {
			return false;
		} else {
			int count = 0;
			// compare left side
			if (this.lComparison.getValue().toString().equals(queryFilter.lComparison.getValue().toString())) {
				count++;
			}
			// compare right side
			if (this.rComparison.getValue().toString().equals(queryFilter.rComparison.getValue().toString())) {
				count++;
			}
			// compare comparator
			if (this.comparator.toString().equals(queryFilter.comparator.toString())) {
				count++;
			}
			// if all 3 comparison match then they are equal
			if (count == 3) {
				return true;
			} else {
				return false;
			}
		}
	}

	/**
	 * See if the filter is using a specific column
	 * @param column
	 * @return
	 */
	public boolean containsColumn(String column) {
		// try and see if the left hand side contains the column we want
		if(isCol(lComparison)) {
			if(lComparison.getValue().toString().equals(column)) {
				return true;
			}
		}
		
		// guess the left hand didn't... now try the right hand side
		if(isCol(rComparison)) {
			if(rComparison.getValue().toString().equals(column)) {
				return true;
			}
		}
		
		// guess it also didn't, we are done
		return false;
	}
	
	/**
	 * Get all the used columns for a given filter expression
	 * @return
	 */
	public Set<String> getAllUsedColumns() {
		Set<String> usedCols = new HashSet<String>();
		//is the left hand side a column?
		if(isCol(lComparison)) {
			usedCols.add(lComparison.getValue().toString());
		}

		// guess the left hand didn't... now try the right hand side
		if(isCol(rComparison)) {
			usedCols.add(rComparison.getValue().toString());
		}
		return usedCols;
	}
	
	/**
	 * Check if the noun meta passed 
	 * @param noun
	 * @return
	 */
	private boolean isCol(NounMetadata noun) {
		PixelDataType type = noun.getNounType();
		if(type == PixelDataType.COLUMN) {
			return true;
		}
		return false;
	}
	
	/**
	 * Looks at the type of the query filter
	 * To figure out how to merge values such that the filters are consolidated and 
	 * can be appropriately constructed
	 * @param otherFilter
	 */
	public void merge(QueryFilter otherFilter) {
		FILTER_TYPE thisType = determineFilterType(this);
		FILTER_TYPE otherType = determineFilterType(otherFilter);
		
		if(!this.comparator.equals(otherFilter.comparator)) {
			throw new IllegalArgumentException("Cannot merge these filters. Comparators must match.");
		}
		
		if(thisType == FILTER_TYPE.COL_TO_VALUES && otherType == FILTER_TYPE.COL_TO_VALUES) {
			// both match
			// and are col to values
			// merge the r noun
			
			// take the right hand side for the existing values
			Object myRFilter = this.rComparison.getValue();
			// take the right hand side for the new values
			Object otherRFilter = otherFilter.rComparison.getValue();
			// merge them based on the comparator type
			this.rComparison = determineMerge(this.comparator, myRFilter, otherRFilter, this.rComparison.getNounType());
		
		} else if(thisType == FILTER_TYPE.COL_TO_VALUES && otherType == FILTER_TYPE.VALUES_TO_COL) {
			// merge the rNoun with the lNoun
			
			// take the right hand side for the existing values
			Object myRFilter = this.rComparison.getValue();
			// take the left hand side for the new values
			Object otherLFilter = otherFilter.lComparison.getValue();
			// merge them based on the comparator type
			this.rComparison = determineMerge(this.comparator, myRFilter, otherLFilter, this.rComparison.getNounType());
			
		} else if(thisType == FILTER_TYPE.VALUES_TO_COL && otherType == FILTER_TYPE.COL_TO_VALUES) {
			// merge the lNoun with the rNoun
			
			// take the left hand side for the existing values
			Object myLFilter = this.rComparison.getValue();
			// take the right hand side for the new values
			Object otherRFilter = otherFilter.rComparison.getValue();
			// merge them based on the comparator type
			this.lComparison = determineMerge(this.comparator, myLFilter, otherRFilter, this.lComparison.getNounType());
			
		} else if(thisType == FILTER_TYPE.VALUES_TO_COL && otherType == FILTER_TYPE.VALUES_TO_COL) {
			// both match
			// and are values to col
			// merge the l nouns
			
			// take the left hand side for the existing values
			Object myLFilter = this.lComparison.getValue();
			// take the left hand side for the new values
			Object otherLFilter = otherFilter.lComparison.getValue();
			// merge them based on the comparator type
			this.lComparison = determineMerge(this.comparator, myLFilter, otherLFilter, this.lComparison.getNounType());
		}
	}
	
	private NounMetadata determineMerge(String comparator, Object filter1, Object filter2, PixelDataType type) {
		if(isAdditive(comparator)) {
			List<Object> newFilters = new Vector<Object>();
			mergeValues(newFilters, filter1);
			mergeValues(newFilters, filter2);

			return new NounMetadata(newFilters, type);
		} else {
			Number lVal = null;
			Number rVal = null;
			if(filter1 instanceof List) {
				lVal = (Number) ((List) filter1).get(0);
			} else {
				lVal = (Number) filter1;
			}
			if(filter2 instanceof List) {
				rVal = (Number) ((List) filter2).get(0);
			} else {
				rVal = (Number) filter2;
			}
			Number newFitlerVal = determineNewVal(comparator, lVal, rVal);
			return new NounMetadata(newFitlerVal, type);
		}
	}
	
	/**
	 * Add the merge values into the same list
	 * @param mergeList
	 * @param newValues
	 */
	private void mergeValues(List<Object> mergeList, Object newValues) {
		if(newValues instanceof List) {
			mergeList.addAll( (List) newValues);
		} else {
			mergeList.add(newValues);
		}
	}
	
	/**
	 * Determine if the merge for a query filter is additive
	 * @param comparator
	 * @return
	 */
	private boolean isAdditive(String comparator) {
		if(this.comparator.equals("==") || this.comparator.equals("!=") || this.comparator.equals("<>")) {
			return true;
		}
		return false;
	}
	
	private Number determineNewVal(String comparator, Number lVal, Number rVal) {
		if(comparator.equals(">") || comparator.equals(">=")) {
			// if it is greater than or greater than or equal
			// we want to find the max number
			if(lVal.doubleValue() > rVal.doubleValue()) {
				return lVal;
			} else {
				return rVal;
			}
		} else if(comparator.equals("<") || comparator.equals("<=")) {
			// if it is less than or less than or equal
			// we want to use the min number
			if(lVal.doubleValue() > rVal.doubleValue()) {
				return rVal;
			} else {
				return lVal;
			}
		}
		return null;
	}
	
	/**
	 * Reverse this specific filters comparator
	 */
	public void reverseComparator() {
		this.comparator = getReverseComparator(this.comparator);
	}
	
	public QueryFilter copy() {
		QueryFilter copy = new QueryFilter(lComparison.copy(), comparator, rComparison.copy());
		return copy;
	}
	
	////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////
	/////////////////////// STATIC METHODS /////////////////////////
	////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////

	/**
	 * Determine the filter type
	 * @param filter
	 * @return
	 */
	public static FILTER_TYPE determineFilterType(QueryFilter filter) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();

		// DIFFERENT PROCESSING BASED ON THE TYPE OF VALUE
		PixelDataType lCompType = leftComp.getNounType();
		PixelDataType rCompType = rightComp.getNounType();

		// col to col
		if(lCompType == PixelDataType.COLUMN && rCompType == PixelDataType.COLUMN) 
		{
			return FILTER_TYPE.COL_TO_COL;
		} 
		// col to values
		else if(lCompType == PixelDataType.COLUMN && (rCompType == PixelDataType.CONST_DECIMAL || rCompType == PixelDataType.CONST_INT || rCompType == PixelDataType.CONST_STRING) ) 
		{
			return FILTER_TYPE.COL_TO_VALUES;
		} 
		// values to col
		else if((lCompType == PixelDataType.CONST_DECIMAL || lCompType == PixelDataType.CONST_INT || lCompType == PixelDataType.CONST_STRING) && rCompType == PixelDataType.COLUMN)
		{
			return FILTER_TYPE.VALUES_TO_COL;
		} 
		// values to values
		else if((rCompType == PixelDataType.CONST_DECIMAL || rCompType == PixelDataType.CONST_INT || rCompType == PixelDataType.CONST_STRING) && (lCompType == PixelDataType.CONST_DECIMAL || lCompType == PixelDataType.CONST_INT || lCompType == PixelDataType.CONST_STRING)) 
		{
			return FILTER_TYPE.VALUE_TO_VALUE;
		}

		return null;
	}
	
	
	/**
	 * Method to provide the reverse of a given comparator
	 * @param comparator
	 * @return
	 */
	public static String getReverseComparator(String comparator) {
		if(comparator.equals("==")) {
			return "!=";
		} else if(comparator.equals("!=") || comparator.equals("<>")) {
			return "==";
		} else if(comparator.equals(">")) {
			return "<=";
		} else if(comparator.equals(">=")) {
			return "<";
		} else if(comparator.equals("<")) {
			return ">=";
		} else if(comparator.equals("<=")) {
			return ">";
		} else if(comparator.equals("?like")) {
			// ughhhh... return the same thing
			return "?like";
		}
		return null;
	}
	
	/**
	 * Method to provide the reverse of a given numeric comparator
	 * @param comparator
	 * @return
	 */
	public static String getReverseNumericalComparator(String comparator) {
		if(comparator.equals(">")) {
			return "<=";
		} else if(comparator.equals(">=")) {
			return "<";
		} else if(comparator.equals("<")) {
			return ">=";
		} else if(comparator.equals("<=")) {
			return ">";
		}
		return comparator;
	}
	
	public static boolean comparatorIsNumeric(String comparator) {
		if(comparator.equals(">")) {
			return true;
		} else if(comparator.equals(">=")) {
			return true;
		} else if(comparator.equals("<")) {
			return true;
		} else if(comparator.equals("<=")) {
			return true;
		}
		return false;
	}
	
	public static boolean comparatorIsSameSide(String comparator1, String comparator2) {
		if(comparator1.equals(">")) {
			if(comparator2.equals(">") || comparator2.equals(">=")) {
				return true;
			}
		} else if(comparator1.equals(">=")) {
			if(comparator2.equals(">") || comparator2.equals(">=")) {
				return true;
			}
		} else if(comparator1.equals("<")) {
			if(comparator2.equals("<") || comparator2.equals("<=")) {
				return true;
			}
		} else if(comparator1.equals("<=")) {
			if(comparator2.equals("<") || comparator2.equals("<=")) {
				return true;
			}
		} else if(comparator1.equals("==") && comparator2.equals("==")) {
			return true;
		} else if(comparator1.equals("!=") && comparator2.equals("!=")) {
			return true;
		}
		return false;
	}
	
	/**
	 * Determine if an OR vs AND is required between 2 filter values
	 * @param leftFilterObj
	 * @param rightFilterObj
	 * @return
	 */
	public static boolean requireOrBetweenFilters(QueryFilter leftFilterObj, QueryFilter rightFilterObj) {
		// require both comparators to be numeric
		String lComparator = leftFilterObj.getComparator();
		String rComparator = rightFilterObj.getComparator();
		if(!comparatorIsNumeric(lComparator) || !comparatorIsNumeric(rComparator)) {
			return false;
		}
		// and they cannot be facing the same direction
		if(comparatorIsSameSide(lComparator, rComparator)) {
			return false;
		}
		
		// we only care about filters which are col to values or values to col
		// get the type and see
		FILTER_TYPE lType = determineFilterType(leftFilterObj);
		if( lType != FILTER_TYPE.COL_TO_VALUES && lType != FILTER_TYPE.VALUES_TO_COL) {
			return false;
		}
		FILTER_TYPE rType = determineFilterType(rightFilterObj);
		if( rType != FILTER_TYPE.COL_TO_VALUES && rType != FILTER_TYPE.VALUES_TO_COL) {
			return false;
		}
		
		// we only care if they are about the same column
		String lColumn = null;
		Object lValue = null;
		if(leftFilterObj.getLComparison().getNounType() == PixelDataType.COLUMN) {
			// column is left side
			lColumn = leftFilterObj.getLComparison().getValue().toString();
			lValue = leftFilterObj.getRComparison().getValue();
		} else {
			// column is right side
			lColumn = leftFilterObj.getRComparison().getValue().toString();
			lValue = leftFilterObj.getLComparison().getValue();
		}
		
		String rColumn = null;
		Object rValue = null;
		if(rightFilterObj.getLComparison().getNounType() == PixelDataType.COLUMN) {
			// column is left side
			rColumn = rightFilterObj.getLComparison().getValue().toString();
			rValue = rightFilterObj.getRComparison().getValue();
		} else {
			// column is right side
			rColumn = rightFilterObj.getRComparison().getValue().toString();
			rValue = rightFilterObj.getLComparison().getValue();
		}
		
		if(!rColumn.equals(lColumn)) {
			// not the same column
			return false;
		}
		
		// we need to compare the numeric values
		// if a person passed in more than 1 thing for > or < or >= or <=
		// the query will break anyway, so idc about this right now
		
		double doubleLValue = 0.0;
		double doubleRValue = 0.0;
		if(lValue instanceof List) {
			doubleLValue = ((Number) ((List) lValue).get(0)).doubleValue();
		} else {
			doubleLValue = ((Number) lValue).doubleValue();
		}
		if(rValue instanceof List) {
			doubleRValue = ((Number) ((List) rValue).get(0)).doubleValue();
		} else {
			doubleRValue = ((Number) rValue).doubleValue();
		}

		double inbetweenValue = 0.0;
		if(doubleLValue > doubleRValue) {
			inbetweenValue = (doubleLValue + doubleRValue) / 2;
		} else {
			inbetweenValue = (doubleRValue + doubleLValue) / 2;
		}
		
		// we know the comparators are on opposite sides
		// go through the combinations
		
		// if left comparator is greater than
		if(lComparator.equals(">")) {
			// compare to less than
			if(rComparator.equals("<")) {
				if(inbetweenValue > doubleLValue && inbetweenValue < doubleRValue) {
					return false;
				} else {
					return true;
				} 
			// compare to less than or equal too
			} else {
				if(inbetweenValue > doubleLValue && inbetweenValue <= doubleRValue) {
					return false;
				} else {
					return true;
				} 
			}
		} 
		// if left comparator is greater than or equal to
		else if(lComparator.equals(">=")) {
			// compare to less than
			if(rComparator.equals("<")) {
				if(inbetweenValue >= doubleLValue && inbetweenValue < doubleRValue) {
					return false;
				} else {
					return true;
				}
				// compare to less than or equal to
			} else {
				if(inbetweenValue >= doubleLValue && inbetweenValue <= doubleRValue) {
					return false;
				} else {
					return true;
				} 
			}
		} 
		// if left hand side is less than
		else if(lComparator.equals("<")) {
			// compare to greater than
			if(rComparator.equals(">")) {
				if(inbetweenValue < doubleLValue && inbetweenValue > doubleRValue) {
					return false;
				} else {
					return true;
				}
			// compare to greater than or equal to
			} else {
				if(inbetweenValue < doubleLValue && inbetweenValue >= doubleRValue) {
					return false;
				} else {
					return true;
				}
			}
		// if left hand size is less than or equal to
		} else if(lComparator.equals("<=")) {
			// compare to greater than
			if(rComparator.equals(">")) {
				if(inbetweenValue <= doubleLValue && inbetweenValue > doubleRValue) {
					return false;
				} else {
					return true;
				}
			// compare to greater than or equal to
			} else {
				if(inbetweenValue <= doubleLValue && inbetweenValue >= doubleRValue) {
					return false;
				} else {
					return true;
				}
			}
		}

		return false;
	}
	
}
