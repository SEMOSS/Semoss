package prerna.query.querystruct.filters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.query.querystruct.selectors.QueryColumnSelector;

public abstract class AbstractListFilter implements IQueryFilter {

	protected List<IQueryFilter> filterList;
	
	public AbstractListFilter() {
		this.filterList = new Vector<IQueryFilter>();
	}
	
	public AbstractListFilter(List<IQueryFilter> filterList) {
		this.filterList = filterList;
	}
	
	public AbstractListFilter(IQueryFilter... filterList ) {
		this.filterList = new Vector<IQueryFilter>();
		for(IQueryFilter f : filterList) {
			this.filterList.add(f);
		}
	}
	
	@Override
	public Set<String> getAllUsedColumns() {
		Set<String> usedCols = new HashSet<>();
		for(IQueryFilter f : this.filterList) {
			usedCols.addAll(f.getAllUsedColumns());
		}
		return usedCols;
	}
	
	@Override
	public List<QueryColumnSelector> getAllQueryColumns() {
		List<QueryColumnSelector> usedCols = new Vector<>();
		for(IQueryFilter f : this.filterList) {
			usedCols.addAll(f.getAllQueryColumns());
		}
		return usedCols;
	}
	
	@Override
	public Set<String> getAllQueryStructNames() {
		Set<String> usedCols = new HashSet<>();
		for(IQueryFilter f : this.filterList) {
			usedCols.addAll(f.getAllQueryStructNames());
		}
		return usedCols;
	}
	

	@Override
	public Set<String> getAllUsedTables() {
		Set<String> usedCols = new HashSet<>();
		for(IQueryFilter f : this.filterList) {
			usedCols.addAll(f.getAllUsedTables());
		}
		return usedCols;
	}
	
	@Override
	public boolean containsColumn(String column) {
		for(IQueryFilter f : this.filterList) {
			if(f.containsColumn(column)) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public Object getSimpleFormat() {
		Map<String, Object> ret = new HashMap<String, Object>();
		ret.put("filterType", this.getQueryFilterType());
		List<Object> values = new Vector<Object>();
		for(IQueryFilter f : this.filterList) {
			values.add(f.getSimpleFormat());
		}
		ret.put("value", values);
		return ret;
	}
	
	@Override
	public String getStringRepresentation() {
		StringBuilder builder = new StringBuilder();
		String type = this.getQueryFilterType().toString();
		boolean first = true;
		for(IQueryFilter f : this.filterList) {
			QUERY_FILTER_TYPE fType = f.getQueryFilterType();
			if(first) {
				if(fType == QUERY_FILTER_TYPE.SIMPLE) {
					builder.append(f.getStringRepresentation());
				} else {
					builder.append("(").append(f.getStringRepresentation()).append(")");
				}
				first = false;
			} else {
				if(fType == QUERY_FILTER_TYPE.SIMPLE) {
					builder.append(" ").append(type).append(" ").append(f.getStringRepresentation());
				} else {
					builder.append(" ").append(type).append(" (").append(f.getStringRepresentation()).append(")");
				}
			}
		}
		return builder.toString();
	}
	
	/**
	 * Add a filter to the list
	 * @param f
	 */
	public void addFilter(IQueryFilter f) {
		this.filterList.add(f);
	}
	
	/**
	 * Add filters to the list
	 * @param f
	 */
	public void addFilter(List<IQueryFilter> f) {
		this.filterList.addAll(f);
	}
	
	public void setFilterList(List<IQueryFilter> filterList) {
		this.filterList = filterList;
	}
	
	public List<IQueryFilter> getFilterList() {
		return this.filterList;
	}
	
	protected List<IQueryFilter> copy(List<IQueryFilter> filters) {
		List<IQueryFilter> cList = new Vector<IQueryFilter>(filters.size());
		for(IQueryFilter f : filters) {
			cList.add(f.copy());
		}
		return filters;
	}
	
	public int size() {
		return this.filterList.size();
	}
	
	public boolean isEmpty() {
		return this.filterList.isEmpty();
	}
}
