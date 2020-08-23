package prerna.query.querystruct.selectors;

import java.util.List;
import java.util.Vector;

public class QueryCustomOrderBy implements IQuerySort {

	private List<Object> customOrder = new Vector<Object>();
	
	public QueryCustomOrderBy() {
		
	}
	
	public void setCustomOrder(List<Object> customOrder) {
		this.customOrder = customOrder;
	}
	
	public List<Object> getCustomOrder() {
		return this.customOrder;
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
			return this.customOrder.equals(orderBy.customOrder);
		}
		return false;
	}
	
}
