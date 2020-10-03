package prerna.query.querystruct.selectors;

import com.google.gson.TypeAdapter;

import prerna.util.gson.QueryColumnOrderBySelectorAdapter;
import prerna.util.gson.QueryCustomOrderByAdapter;

public interface IQuerySort {

	public enum QUERY_SORT_TYPE {COLUMN, CUSTOM};

	QUERY_SORT_TYPE getQuerySortType();

	/**
	 * Get the adapter for the sort type
	 * @param type
	 * @return
	 */
	static TypeAdapter getAdapterForSort(QUERY_SORT_TYPE type) {
		if(type == QUERY_SORT_TYPE.COLUMN) {
			return new QueryColumnOrderBySelectorAdapter();
		} else if(type == QUERY_SORT_TYPE.CUSTOM) {
			return new QueryCustomOrderByAdapter();
		}
		
		return null;
	}
	
	/**
	 * Convert string to SELECTOR_TYPE
	 * @param s
	 * @return
	 */
	static QUERY_SORT_TYPE convertStringToSortType(String s) {
		if(s.equals(QUERY_SORT_TYPE.COLUMN.toString())) {
			return QUERY_SORT_TYPE.COLUMN;
		} else if(s.equals(QUERY_SORT_TYPE.CUSTOM.toString())) {
			return QUERY_SORT_TYPE.CUSTOM;
		}
		
		return null;
	}
}
