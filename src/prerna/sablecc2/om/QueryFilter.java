package prerna.sablecc2.om;

import java.util.HashSet;
import java.util.Set;

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
		PkslDataTypes type = noun.getNounType();
		if(type == PkslDataTypes.COLUMN) {
			return true;
		}
		return false;
	}
	
	public static FILTER_TYPE determineFilterType(QueryFilter filter) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();

		// DIFFERENT PROCESSING BASED ON THE TYPE OF VALUE
		PkslDataTypes lCompType = leftComp.getNounType();
		PkslDataTypes rCompType = rightComp.getNounType();

		// col to col
		if(lCompType == PkslDataTypes.COLUMN && rCompType == PkslDataTypes.COLUMN) 
		{
			return FILTER_TYPE.COL_TO_COL;
		} 
		// col to values
		else if(lCompType == PkslDataTypes.COLUMN && (rCompType == PkslDataTypes.CONST_DECIMAL || rCompType == PkslDataTypes.CONST_INT || rCompType == PkslDataTypes.CONST_STRING) ) 
		{
			return FILTER_TYPE.COL_TO_VALUES;
		} 
		// values to col
		else if((lCompType == PkslDataTypes.CONST_DECIMAL || lCompType == PkslDataTypes.CONST_INT || lCompType == PkslDataTypes.CONST_STRING) && rCompType == PkslDataTypes.COLUMN)
		{
			return FILTER_TYPE.VALUES_TO_COL;
		} 
		// values to values
		else if((rCompType == PkslDataTypes.CONST_DECIMAL || rCompType == PkslDataTypes.CONST_INT || rCompType == PkslDataTypes.CONST_STRING) && (lCompType == PkslDataTypes.CONST_DECIMAL || lCompType == PkslDataTypes.CONST_INT || lCompType == PkslDataTypes.CONST_STRING)) 
		{
			return FILTER_TYPE.VALUE_TO_VALUE;
		}

		return null;
	}
}
