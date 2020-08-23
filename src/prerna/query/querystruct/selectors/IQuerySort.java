package prerna.query.querystruct.selectors;

public interface IQuerySort {

	// this is a dummy interface
	
	public enum QUERY_SORT_TYPE {COLUMN, CUSTOM};

	QUERY_SORT_TYPE getQuerySortType();
	
}
