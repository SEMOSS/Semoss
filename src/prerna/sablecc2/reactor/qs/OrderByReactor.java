package prerna.sablecc2.reactor.qs;

import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;

public class OrderByReactor extends QueryStructReactor {

	private static final String COLUMNS_KEY = "cols";
	private static final String DIRECTION_KEY = "dirs";
	
	QueryStruct2 createQueryStruct() {
		List<String> colInputs = getOrderByColumns();
		List<String> sortDirs = getSortDirections();
		int colSize = colInputs.size();
		int sortDirSize = sortDirs.size();
		for(int selectIndex = 0; selectIndex < colSize; selectIndex++) {
			String newSelector = colInputs.get(selectIndex);
			if(newSelector.contains("__")) {
				String[] selectorSplit = newSelector.split("__");
				if(sortDirSize > selectIndex) {
					qs.addOrderBy(selectorSplit[0], selectorSplit[1], sortDirs.get(selectIndex));
				} else {
					qs.addOrderBy(selectorSplit[0], selectorSplit[1], "ASC");
				}
			} else {
				if(sortDirSize > selectIndex) {
					qs.addOrderBy(newSelector, QueryStruct2.PRIM_KEY_PLACEHOLDER, sortDirs.get(selectIndex));
				} else {
					qs.addOrderBy(newSelector, QueryStruct2.PRIM_KEY_PLACEHOLDER, "ASC");
				}
			}
		}
		return qs;
	}

	/**
	 * Get the order by columns
	 * These could be in the store or passed in cur row
	 * @return
	 */
	private List<String> getOrderByColumns() {
		// if it was passed based on the key
		List<String> colInputs = new Vector<String>();
		GenRowStruct colsGrs = this.store.getNoun(COLUMNS_KEY);
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
		GenRowStruct colsGrs = this.store.getNoun(DIRECTION_KEY);
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
