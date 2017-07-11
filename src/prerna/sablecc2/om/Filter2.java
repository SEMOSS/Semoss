package prerna.sablecc2.om;

import java.util.HashSet;
import java.util.Set;

public class Filter2 {

	private String comparator = null; //'=', '!=', '<', '<=', '>', '>=', '<>', '?like'
	private NounMetadata lComparison = null; //the column we want to filter
	private NounMetadata rComparison = null; //the values to bind the filter on
	
	public Filter2(NounMetadata lComparison, String comparator, NounMetadata rComparison)
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
		PkslDataTypes type = noun.getNounName();
		if(type == PkslDataTypes.COLUMN) {
			return true;
		}
		return false;
	}
}
