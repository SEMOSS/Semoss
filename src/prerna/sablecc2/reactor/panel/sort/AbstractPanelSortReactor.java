package prerna.sablecc2.reactor.panel.sort;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.panel.AbstractInsightPanelReactor;

public abstract class AbstractPanelSortReactor extends AbstractInsightPanelReactor {

	/**
	 * Grab the sort information from the curRow of the reactor
	 * THIS ALWAYS ASSUME A COLUMN SORT IS BEING USED
	 * @return
	 */
	protected List<IQuerySort> getColumnSortBys() {
		List<IQuerySort> sorts = new ArrayList<IQuerySort>();
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
		List<String> colInputs = new Vector<String>();
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
		List<String> sortDirections = new Vector<String>();
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
}
