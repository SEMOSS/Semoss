package prerna.query.querystruct.filters;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

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
		Set<String> usedCols = new HashSet<String>();
		for(IQueryFilter f : this.filterList) {
			usedCols.addAll(f.getAllUsedColumns());
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
	
	/**
	 * Add a filter to the list
	 * @param f
	 */
	public void addFilter(IQueryFilter f) {
		this.filterList.add(f);
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
}
