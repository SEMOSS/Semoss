package prerna.reactor.panel.sort;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryCustomOrderBy;
import prerna.reactor.panel.AbstractInsightPanelReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;

public abstract class AbstractPanelSortReactor extends AbstractInsightPanelReactor {

	protected List<String> getColumnsForSort() {
		List<String> colInputs = getOrderByColumns();
		if(colInputs != null && !colInputs.isEmpty()) {
			return colInputs;
		}
		
		List<String> columns = new Vector<>();
		
		String singleSort = getColumnToSort();
		if(singleSort != null && !singleSort.isEmpty()) {
			columns.add(singleSort);
		}
		
		return columns;
	}
	
	/*
	 * THIS IS FOR COLUMN SORTS
	 */
	
	/**
	 * Grab the sort information from the curRow of the reactor
	 * THIS ALWAYS ASSUME A COLUMN SORT IS BEING USED
	 * @return
	 */
	protected List<IQuerySort> getColumnSortBys() {
		List<IQuerySort> sorts = new ArrayList<>();
		List<String> colInputs = getOrderByColumns();
		List<String> sortDirs = getSortDirections();
		int colSize = colInputs.size();
		int sortDirSize = sortDirs.size();
		for(int selectIndex = 0; selectIndex < colSize; selectIndex++) {
			String newSelector = colInputs.get(selectIndex);
			QueryColumnOrderBySelector selector = new QueryColumnOrderBySelector();
			if(newSelector.contains("__")) {
				String[] selectorSplit = newSelector.split("__");
				selector.setTable(selectorSplit[0]);
				selector.setColumn(selectorSplit[1]);
			} else {
				selector.setTable(newSelector);
				selector.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
			}
			if(sortDirSize > selectIndex) {
				selector.setSortDir(sortDirs.get(selectIndex));
			} else {
				selector.setSortDir("ASC");
			}
			sorts.add(selector);
		}
		return sorts;
	}
	
	/**
	 * Get the order by columns
	 * These could be in the store or passed in cur row
	 * @return
	 */
	private List<String> getOrderByColumns() {
		// if it was passed based on the key
		List<String> colInputs = new Vector<>();
		GenRowStruct colsGrs = this.store.getNoun(ReactorKeysEnum.COLUMNS.getKey());
		if(colsGrs != null) {
			int size = colsGrs.size();
			if(size > 0) {
				for(int i = 0; i < size; i++) {
					colInputs.add(colsGrs.get(i).toString());
				}
				return colInputs;
			}
		}
		
		// if it was passed directly in
		int size = this.curRow.size();
		for(int i = 0; i < size; i++) {
			colInputs.add(this.curRow.get(i).toString());
		}
		return colInputs;
	}
	
	/**
	 * Directions will always be put in its key
	 * @return
	 */
	private List<String> getSortDirections() {
		// if it was passed based on the key
		List<String> sortDirections = new Vector<>();
		GenRowStruct colsGrs = this.store.getNoun(ReactorKeysEnum.SORT.getKey());
		if(colsGrs != null) {
			int size = colsGrs.size();
			if(size > 0) {
				for(int i = 0; i < size; i++) {
					sortDirections.add(colsGrs.get(i).toString());
				}
			}
		}
		return sortDirections;
	}
	
	
	///////////////////////////////////////////////////////////////////////////////
	
	/*
	 * THIS IS FOR CUSTOM SORTS
	 */
	
	/**
	 * Grab the custom sort by 
	 * @return
	 */
	protected IQuerySort getCustomSortBy() {
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
		return customSort;
	}
	
	/**
	 * Get the order by columns
	 * These could be in the store or passed in cur row
	 * @return
	 */
	private String getColumnToSort() {
		// if it was passed based on the key
		GenRowStruct colsGrs = this.store.getNoun(ReactorKeysEnum.COLUMN.getKey());
		if(colsGrs != null) {
			int size = colsGrs.size();
			if(size > 0) {
				return colsGrs.get(0).toString();
			}
		}
		
		// if it was passed directly in
		if(this.curRow.isEmpty()) {
			return null;
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
		GenRowStruct colsGrs = this.store.getNoun(ReactorKeysEnum.VALUES.getKey());
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
