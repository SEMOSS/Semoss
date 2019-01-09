package prerna.query.querystruct.filters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SimpleQueryFilter implements IQueryFilter {

	/**
	 * Right now, tracking for these types of filters within querying
	 */
	public static enum FILTER_TYPE {COL_TO_QUERY, QUERY_TO_COL, COL_TO_COL, COL_TO_VALUES, VALUES_TO_COL, VALUE_TO_VALUE};
		
	private String comparator = null; //'=', '!=', '<', '<=', '>', '>=', '<>', '?like'
	private NounMetadata lComparison = null; //the column we want to filter
	private NounMetadata rComparison = null; //the values to bind the filter on
	
	// since we grab this a lot of times
	private transient FILTER_TYPE thisFilterType = null;;
	
	public SimpleQueryFilter(NounMetadata lComparison, String comparator, NounMetadata rComparison)
	{
		this.lComparison = lComparison;
		this.rComparison = rComparison;
		if("<>".equals(comparator)) {
			this.comparator = "!=";
		} else {
			this.comparator = comparator;
		}
		this.thisFilterType = determineFilterType(this);
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
	 * Looks at the type of the query filter
	 * To figure out how to merge values such that the filters are consolidated and 
	 * can be appropriately constructed
	 * @param otherFilter
	 */
	public void merge(SimpleQueryFilter otherFilter) {
		FILTER_TYPE otherType = determineFilterType(otherFilter);
		
		if(!this.comparator.equals(otherFilter.comparator)) {
			throw new IllegalArgumentException("Cannot merge these filters. Comparators must match.");
		}
		
		if(this.thisFilterType == FILTER_TYPE.COL_TO_VALUES && otherType == FILTER_TYPE.COL_TO_VALUES) {
			// both match
			// and are col to values
			// merge the r noun
			
			// take the right hand side for the existing values
			Object myRFilter = this.rComparison.getValue();
			// take the right hand side for the new values
			Object otherRFilter = otherFilter.rComparison.getValue();
			// merge them based on the comparator type
			this.rComparison = determineMerge(this.comparator, myRFilter, otherRFilter, this.rComparison.getNounType());
		
		} else if(this.thisFilterType == FILTER_TYPE.COL_TO_VALUES && otherType == FILTER_TYPE.VALUES_TO_COL) {
			// merge the rNoun with the lNoun
			
			// take the right hand side for the existing values
			Object myRFilter = this.rComparison.getValue();
			// take the left hand side for the new values
			Object otherLFilter = otherFilter.lComparison.getValue();
			// merge them based on the comparator type
			this.rComparison = determineMerge(this.comparator, myRFilter, otherLFilter, this.rComparison.getNounType());
			
		} else if(this.thisFilterType == FILTER_TYPE.VALUES_TO_COL && otherType == FILTER_TYPE.COL_TO_VALUES) {
			// merge the lNoun with the rNoun
			
			// take the left hand side for the existing values
			Object myLFilter = this.rComparison.getValue();
			// take the right hand side for the new values
			Object otherRFilter = otherFilter.rComparison.getValue();
			// merge them based on the comparator type
			this.lComparison = determineMerge(this.comparator, myLFilter, otherRFilter, this.lComparison.getNounType());
			
		} else if(this.thisFilterType == FILTER_TYPE.VALUES_TO_COL && otherType == FILTER_TYPE.VALUES_TO_COL) {
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
			List<Object> newValuesList = (List<Object>) newValues;
			int size = newValuesList.size();
			for(int i = 0; i < size; i++) {
				Object newElement = newValuesList.get(i);
				if(!mergeList.contains(newElement)) {
					mergeList.add(newElement);
				}
			}
		} else {
			if(!mergeList.contains(newValues)) {
				mergeList.add(newValues);
			}
		}
	}
	
	/**
	 * Determine if the merge for a query filter is additive
	 * @param comparator
	 * @return
	 */
	private boolean isAdditive(String comparator) {
		if(this.comparator.equals("==") || this.comparator.equals("!=") || this.comparator.equals("<>") || this.comparator.equals("?like")) {
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
		this.comparator = IQueryFilter.getReverseComparator(this.comparator);
	}
	
	/**
	 * Determine if 2 filters are modifying the same column
	 * @param otherQueryFilter
	 * @return
	 */
	public boolean equivalentColumnModifcation(SimpleQueryFilter otherQueryFilter) {
		// regardless of the order
		// the comparators must match
		if(!this.comparator.toString().equals(otherQueryFilter.comparator.toString())) {
			return false;
		}
		
		FILTER_TYPE thisFilterType = determineFilterType(this);
		FILTER_TYPE otherFilterType = determineFilterType(otherQueryFilter);

		if(thisFilterType == FILTER_TYPE.COL_TO_VALUES && otherFilterType == FILTER_TYPE.COL_TO_VALUES) {
			// compare left hand side
			if(!this.lComparison.getValue().toString().equals(otherQueryFilter.lComparison.getValue().toString())) {
				return false;
			}
		} else if(thisFilterType == FILTER_TYPE.VALUES_TO_COL && otherFilterType == FILTER_TYPE.VALUES_TO_COL) {
			// compare right hand side
			if(!this.rComparison.getValue().toString().equals(otherQueryFilter.rComparison.getValue().toString())) {
				return false;
			}
		} else if(thisFilterType == FILTER_TYPE.COL_TO_VALUES && otherFilterType == FILTER_TYPE.VALUES_TO_COL) {
			// compare left hand side to right hand side
			if(!this.lComparison.getValue().toString().equals(otherQueryFilter.rComparison.getValue().toString())) {
				return false;
			}
		} else if(thisFilterType == FILTER_TYPE.VALUES_TO_COL && otherFilterType == FILTER_TYPE.COL_TO_VALUES) {
			// compare right hand side to left hand side
			if(!this.rComparison.getValue().toString().equals(otherQueryFilter.lComparison.getValue().toString())) {
				return false;
			}
		} else {
			// it is not col to value or value to col
			// just return false
			return false;
		}
		
		// got to this point
		// they match
		return true;
	}
	
	public void subtractInstanceFilters(SimpleQueryFilter otherQueryFilter) {
		FILTER_TYPE otherFilterType = determineFilterType(otherQueryFilter);

		if(this.thisFilterType == FILTER_TYPE.COL_TO_VALUES && otherFilterType == FILTER_TYPE.COL_TO_VALUES) {
			// remove the values of the right hand side from this right hand side

			// lets make sure everything is a list
			// even if it is a single value
			List<Object> thisRHS = new Vector<Object>();
			Object thisRhsObj = this.rComparison.getValue();
			if(thisRhsObj instanceof List) {
				thisRHS = (List<Object>) thisRhsObj;
			} else {
				thisRHS.add(thisRhsObj);
			}
			
			List<Object> otherRHS = new Vector<Object>();
			Object otherRhsObj = otherQueryFilter.rComparison.getValue();
			if(otherRhsObj instanceof List) {
				otherRHS = (List<Object>) otherRhsObj;
			} else {
				otherRHS.add(otherRhsObj);
			}
			
			// remove the values
			thisRHS.removeAll(otherRHS);
			
			// recreate this 
			this.rComparison = new NounMetadata(thisRHS, this.rComparison.getNounType(), this.rComparison.getOpType());
			
		} else if(this.thisFilterType == FILTER_TYPE.VALUES_TO_COL && otherFilterType == FILTER_TYPE.VALUES_TO_COL) {
			// remove the values of the left hand side from this left hand side

			// lets make sure everything is a list
			// even if it is a single value
			List<Object> thisLHS = new Vector<Object>();
			Object thisLhsObj = this.lComparison.getValue();
			if(thisLhsObj instanceof List) {
				thisLHS = (List<Object>) thisLhsObj;
			} else {
				thisLHS.add(thisLhsObj);
			}
			
			List<Object> otherLHS = new Vector<Object>();
			Object otherLhsObj = otherQueryFilter.lComparison.getValue();
			if(otherLhsObj instanceof List) {
				otherLHS = (List<Object>) otherLhsObj;
			} else {
				otherLHS.add(otherLhsObj);
			}
			
			// remove the values
			thisLHS.removeAll(otherLHS);
			
			// recreate this 
			this.lComparison = new NounMetadata(thisLHS, this.lComparison.getNounType(), this.lComparison.getOpType());
			
			
		} else if(this.thisFilterType == FILTER_TYPE.COL_TO_VALUES && otherFilterType == FILTER_TYPE.VALUES_TO_COL) {
			// remove the values of the left hand side from this right hand side

			// lets make sure everything is a list
			// even if it is a single value
			List<Object> thisRHS = new Vector<Object>();
			Object thisRhsObj = this.rComparison.getValue();
			if(thisRhsObj instanceof List) {
				thisRHS = (List<Object>) thisRhsObj;
			} else {
				thisRHS.add(thisRhsObj);
			}
			
			List<Object> otherLHS = new Vector<Object>();
			Object otherLhsObj = otherQueryFilter.lComparison.getValue();
			if(otherLhsObj instanceof List) {
				otherLHS = (List<Object>) otherLhsObj;
			} else {
				otherLHS.add(otherLhsObj);
			}
			
			// remove the values
			thisRHS.removeAll(otherLHS);

			// recreate this 
			this.rComparison = new NounMetadata(thisRHS, this.rComparison.getNounType(), this.rComparison.getOpType());

			
		} else if(this.thisFilterType == FILTER_TYPE.VALUES_TO_COL && otherFilterType == FILTER_TYPE.COL_TO_VALUES) {
			// remove the values of the right hand side from this left hand side

			// lets make sure everything is a list
			// even if it is a single value
			List<Object> thisLHS = new Vector<Object>();
			Object thisLhsObj = this.lComparison.getValue();
			if(thisLhsObj instanceof List) {
				thisLHS = (List<Object>) thisLhsObj;
			} else {
				thisLHS.add(thisLhsObj);
			}
			
			List<Object> otherRHS = new Vector<Object>();
			Object otherRhsObj = otherQueryFilter.rComparison.getValue();
			if(otherRhsObj instanceof List) {
				otherRHS = (List<Object>) otherRhsObj;
			} else {
				otherRHS.add(otherRhsObj);
			}
			
			// remove the values
			thisLHS.removeAll(otherRHS);
			
			// recreate this 
			this.lComparison = new NounMetadata(thisLHS, this.lComparison.getNounType(), this.lComparison.getOpType());
		}
	}
	
	public boolean isEmptyFilterValues() {
		FILTER_TYPE thisFilterType = determineFilterType(this);
		if(thisFilterType == FILTER_TYPE.COL_TO_VALUES) {
			// is right hand side a list
			// and is it empty
			Object rObj = rComparison.getValue();
			if(rObj instanceof List) {
				if(((List) rObj).isEmpty()) {
					return true;
				}
			}
		} else if(thisFilterType == FILTER_TYPE.VALUES_TO_COL) {
			// is left hand side a list
			// and is it empty
			Object lObj = lComparison.getValue();
			if(lObj instanceof List) {
				if(((List) lObj).isEmpty()) {
					return true;
				}
			}
		}
		
		// got to this point
		// it is not empty
		return false;
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
		if(obj instanceof SimpleQueryFilter) {
			SimpleQueryFilter otherQueryFilter = (SimpleQueryFilter) obj;
			// compare comparator
			if(!this.comparator.toString().equals(otherQueryFilter.comparator.toString())) {
				return false;
			}
			// compare left side
			if(!this.lComparison.getValue().toString().equals(otherQueryFilter.lComparison.getValue().toString())) {
				return false;
			}
			// compare right side
			if(!this.rComparison.getValue().toString().equals(otherQueryFilter.rComparison.getValue().toString())) {
				return false;
			}

			// if we get to this point
			// everything was the same
			return true;
		}
		return false;
	}

	////////////////////////////////////////////////////
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// PARENT METHODS
	
	@Override
	public QUERY_FILTER_TYPE getQueryFilterType() {
		return QUERY_FILTER_TYPE.SIMPLE;
	}
	
	@Override
	public IQueryFilter copy() {
		SimpleQueryFilter copy = new SimpleQueryFilter(lComparison.copy(), comparator, rComparison.copy());
		return copy;
	}
	
	@Override
	public boolean containsColumn(String column) {
		// try and see if the left hand side contains the column we want
		if(isCol(lComparison)) {
			if( ((IQuerySelector) lComparison.getValue()).getAlias().equals(column)) {
				return true;
			} else if( ((IQuerySelector) lComparison.getValue()).getQueryStructName().equals(column)) {
				return true;
			} 
		}
		
		// guess the left hand didn't... now try the right hand side
		if(isCol(rComparison)) {
			if( ((IQuerySelector) rComparison.getValue()).getAlias().equals(column)) {
				return true;
			} else if( ((IQuerySelector) rComparison.getValue()).getQueryStructName().equals(column)) {
				return true;
			} 
		}
		
		// guess it also didn't, we are done
		return false;
	}
	
	@Override
	public Set<String> getAllUsedColumns() {
		Set<String> usedCols = new HashSet<String>();
		//is the left hand side a column?
		if(isCol(lComparison)) {
			usedCols.add( ((IQuerySelector) lComparison.getValue()).getAlias());
		}

		// guess the left hand didn't... now try the right hand side
		if(isCol(rComparison)) {
			usedCols.add( ((IQuerySelector) rComparison.getValue()).getAlias());
		}
		return usedCols;
	}
	
	@Override
	public Set<String> getAllQueryStructColumns() {
		Set<String> usedCols = new HashSet<String>();
		//is the left hand side a column?
		if(isCol(lComparison)) {
			usedCols.add( ((IQuerySelector) lComparison.getValue()).getQueryStructName());
		}

		// guess the left hand didn't... now try the right hand side
		if(isCol(rComparison)) {
			usedCols.add( ((IQuerySelector) rComparison.getValue()).getQueryStructName());
		}
		return usedCols;
	}
	
	@Override
	public Set<String> getAllUsedTables() {
		Set<String> usedCols = new HashSet<String>();
		//is the left hand side a column?
		if(isCol(lComparison)) {
			List<QueryColumnSelector> colValues = ((IQuerySelector) lComparison.getValue()).getAllQueryColumns();
			for(QueryColumnSelector c : colValues) {
				usedCols.add(c.getTable());
			}
		}

		// guess the left hand didn't... now try the right hand side
		if(isCol(rComparison)) {
			List<QueryColumnSelector> colValues = ((IQuerySelector) rComparison.getValue()).getAllQueryColumns();
			for(QueryColumnSelector c : colValues) {
				usedCols.add(c.getTable());
			}
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
	
	public FILTER_TYPE getFilterType() {
		return this.thisFilterType;
	}
	
	@Override
	public Object getSimpleFormat() {
		Map<String, Object> lMap = new HashMap<String, Object>();
		PixelDataType lType = lComparison.getNounType();
		lMap.put("type", lType);
		if(lType == PixelDataType.COLUMN) {
			lMap.put("value", ((IQuerySelector) lComparison.getValue()).getQueryStructName());
		} else {
			lMap.put("value", lComparison.getValue());
		}
		
		Map<String, Object> rMap = new HashMap<String, Object>();
		PixelDataType rType = rComparison.getNounType();
		rMap.put("type", rType);
		if(rType == PixelDataType.COLUMN) {
			rMap.put("value", ((IQuerySelector) rComparison.getValue()).getQueryStructName());
		} else {
			rMap.put("value", rComparison.getValue());
		}
		
		Map<String, Object> ret = new HashMap<String, Object>();
		ret.put("filterType", this.getQueryFilterType());
		ret.put("comparator", this.comparator);
		ret.put("left", lMap);
		ret.put("right", rMap);
		return ret;
	}
	
	@Override
	public String getStringRepresentation() {
		if(this.thisFilterType == FILTER_TYPE.COL_TO_VALUES) {
			Object rObj = this.rComparison.getValue();
			if(rObj instanceof List) {
				int size = ((List) rObj).size();
				if(size == 1) {
					return ((IQuerySelector) this.lComparison.getValue()).getQueryStructName() + " " + this.comparator + " " + ((List) rObj).get(0);
				} else {
					StringBuilder builder = new StringBuilder("[");
					boolean first = true;
					for(int i = 0; i < size; i++) {
						if(first) {
							builder.append( ((List) rObj).get(i) );
							first = false;
							continue;
						}
						if(i == 5) {
							builder.append(", ...");
							break;
						}
						builder.append(", ").append( ((List) rObj).get(i) );
					}
					builder.append(" ]");
					return ((IQuerySelector) this.lComparison.getValue()).getQueryStructName() + " " + this.comparator + " " + builder.toString();
				}
			} else {
				return ((IQuerySelector) this.lComparison.getValue()).getQueryStructName() + " " + this.comparator + " " + rObj;
			}
		} else if(this.thisFilterType == FILTER_TYPE.VALUES_TO_COL) {
			Object lObj = this.lComparison.getValue();
			if(lObj instanceof List) {
				int size = ((List) lObj).size();
				if(size == 1) {
					return this.rComparison.getValue() + " " + IQueryFilter.getReverseNumericalComparator(this.comparator) + " " + ((List) lObj).get(0);
				} else {
					StringBuilder builder = new StringBuilder("[");
					for(int i = 0; i < size || i < 5; i++) {
						builder.append( ((List) lObj).get(i) ).append(", ");
					}
					builder.append("... ]");
					return ((IQuerySelector) this.rComparison.getValue()).getQueryStructName() + " " + IQueryFilter.getReverseNumericalComparator(this.comparator) + " " + builder.toString();
				}
			} else {
				return ((IQuerySelector) this.rComparison.getValue()).getQueryStructName() + " " + IQueryFilter.getReverseNumericalComparator(this.comparator) + " " + lObj;
			}
		} else if(this.thisFilterType == FILTER_TYPE.COL_TO_COL) {
			return ((IQuerySelector) this.lComparison.getValue()).getQueryStructName() + " " + this.comparator + " " + ((IQuerySelector) this.rComparison.getValue()).getQueryStructName();
		} else {
			return this.lComparison.getValue() + " " + this.comparator + " " + this.rComparison.getValue();
		}
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
	private static FILTER_TYPE determineFilterType(SimpleQueryFilter filter) {
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
		else if(lCompType == PixelDataType.COLUMN && (rCompType == PixelDataType.CONST_DECIMAL || rCompType == PixelDataType.CONST_INT || rCompType == PixelDataType.CONST_STRING || rCompType == PixelDataType.NULL_VALUE) ) 
		{
			return FILTER_TYPE.COL_TO_VALUES;
		} 
		// values to col
		else if((lCompType == PixelDataType.CONST_DECIMAL || lCompType == PixelDataType.CONST_INT || lCompType == PixelDataType.CONST_STRING || rCompType == PixelDataType.NULL_VALUE) && rCompType == PixelDataType.COLUMN)
		{
			return FILTER_TYPE.VALUES_TO_COL;
		}
		// col to query
		else if(lCompType == PixelDataType.COLUMN && rCompType == PixelDataType.QUERY_STRUCT) 
		{
			return FILTER_TYPE.COL_TO_QUERY;
		} 
		// query to col
		else if(lCompType == PixelDataType.QUERY_STRUCT && rCompType == PixelDataType.COLUMN) 
		{
			return FILTER_TYPE.QUERY_TO_COL;
		}
		
		// values to values
		else if((rCompType == PixelDataType.CONST_DECIMAL || rCompType == PixelDataType.CONST_INT || rCompType == PixelDataType.CONST_STRING || rCompType == PixelDataType.NULL_VALUE) 
				&& (lCompType == PixelDataType.CONST_DECIMAL || lCompType == PixelDataType.CONST_INT || lCompType == PixelDataType.CONST_STRING || rCompType == PixelDataType.NULL_VALUE)) 
		{
			return FILTER_TYPE.VALUE_TO_VALUE;
		}

		return null;
	}
	
	/**
	 * Determine if an OR vs AND is required between 2 filter values
	 * @param leftFilterObj
	 * @param rightFilterObj
	 * @return
	 */
	public static boolean requireOrBetweenFilters(SimpleQueryFilter leftFilterObj, SimpleQueryFilter rightFilterObj) {
		// require both comparators to be numeric
		String lComparator = leftFilterObj.getComparator();
		String rComparator = rightFilterObj.getComparator();
		if(!IQueryFilter.comparatorIsNumeric(lComparator) || !IQueryFilter.comparatorIsNumeric(rComparator)) {
			return false;
		}
		// and they cannot be facing the same direction
		if(IQueryFilter.comparatorIsSameSide(lComparator, rComparator)) {
			return false;
		}
		
		// we only care about filters which are col to values or values to col
		// get the type and see
		FILTER_TYPE lType = leftFilterObj.getFilterType();
		if( lType != FILTER_TYPE.COL_TO_VALUES && lType != FILTER_TYPE.VALUES_TO_COL) {
			return false;
		}
		FILTER_TYPE rType = rightFilterObj.getFilterType();
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

	/**
	 * See if the query is doing a null check
	 * @param filter
	 * @return
	 */
	public static boolean colValuesContainsNull(SimpleQueryFilter filter) {
		if(filter.getFilterType() == SimpleQueryFilter.FILTER_TYPE.COL_TO_VALUES) {
			NounMetadata rComp = filter.getRComparison();
			Object rVal = rComp.getValue();
			if(rVal instanceof List) {
				if(((List) rVal).contains(null)) {
					return true;
				}
			} else if(rVal == null) {
				return true;
			}
		} else if(filter.getFilterType() == SimpleQueryFilter.FILTER_TYPE.COL_TO_VALUES) {
			NounMetadata lComp = filter.getLComparison();
			Object lVal = lComp.getValue();
			if(lVal instanceof List) {
				if(((List) lVal).contains(null)) {
					return true;
				}
			} else if(lVal == null) {
				return true;
			}
		}
		return false;
 	}
	
}
