package prerna.query.querystruct.selectors;

import java.util.List;
import java.util.Vector;

public class QueryCustomOrderBy implements IQuerySort {

	private List<Object> customOrder = new Vector<Object>();
	private QueryColumnSelector columnToSort = null;
	
	public QueryCustomOrderBy() {
		
	}
	
	public void setCustomOrder(List<Object> customOrder) {
		this.customOrder = customOrder;
	}
	
	public List<Object> getCustomOrder() {
		return this.customOrder;
	}
	
	public QueryColumnSelector getColumnToSort() {
		return columnToSort;
	}

	public void setColumnToSort(QueryColumnSelector columnToSort) {
		this.columnToSort = columnToSort;
	}

	@Override
	public QUERY_SORT_TYPE getQuerySortType() {
		return QUERY_SORT_TYPE.CUSTOM;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof QueryCustomOrderBy) {
			QueryCustomOrderBy orderBy = (QueryCustomOrderBy)obj;
			// see if the 2 lists are the same
			return this.columnToSort.equals(orderBy.columnToSort) && this.customOrder.equals(orderBy.customOrder);
		}
		return false;
	}
	
}
