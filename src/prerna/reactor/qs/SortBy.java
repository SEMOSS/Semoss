package prerna.reactor.qs;

import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryCustomOrderBy;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;

public class SortBy extends AbstractQueryStructReactor {

	private static final String COLUMN_KEY = ReactorKeysEnum.COLUMN.getKey();
	private static final String VALUES_KEY = ReactorKeysEnum.VALUES.getKey();
	
	public SortBy() {
		this.keysToGet = new String[]{COLUMN_KEY, VALUES_KEY};
	}
	
	protected AbstractQueryStruct createQueryStruct() {
		/**
		 * Grab the column we are sorting on
		 * Grab the list of values we are using
		 */
		String columnToSort = getColumnToSort();
		if(columnToSort == null || columnToSort.trim().isEmpty()) {
			throw new IllegalArgumentException("Must pass in a column to sort");
		}
		List<Object> valuesForSort = getValuesToOrder();
		if(valuesForSort == null || valuesForSort.isEmpty()) {
			throw new IllegalArgumentException("Must pass in values for the custom sort");
		}
		
		QueryCustomOrderBy customSort = new QueryCustomOrderBy();
		customSort.setColumnToSort(new QueryColumnSelector(columnToSort));
		customSort.setCustomOrder(valuesForSort);
		((SelectQueryStruct) qs).addOrderBy(customSort);
		return qs;
	}

	/**
	 * Get the order by columns
	 * These could be in the store or passed in cur row
	 * @return
	 */
	private String getColumnToSort() {
		// if it was passed based on the key
		GenRowStruct colsGrs = this.store.getNoun(COLUMN_KEY);
		if(colsGrs != null) {
			int size = colsGrs.size();
			if(size > 0) {
				return colsGrs.get(0).toString();
			}
		}
		
		// if it was passed directly in
		if(this.curRow.isEmpty()) {
			throw new IllegalArgumentException("Must pass in a column to sort");
		}
		
		return this.curRow.get(0).toString();
	}
	
	/**
	 * Get the values to use for the ordering
	 * @return
	 */
	private List<Object> getValuesToOrder() {
		// if it was passed based on the key
		List<Object> colInputs = new Vector<>();
		GenRowStruct colsGrs = this.store.getNoun(VALUES_KEY);
		if(colsGrs != null) {
			int size = colsGrs.size();
			if(size > 0) {
				for(int i = 0; i < size; i++) {
					colInputs.add(colsGrs.get(i));
				}
				return colInputs;
			}
		}
		
		// if it was passed directly in
		int size = this.curRow.size();
		// index 0 should be the column
		for(int i = 1; i < size; i++) {
			colInputs.add(this.curRow.get(i));
		}
		return colInputs;
	}
}
