package prerna.query.querystruct.filters;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class GenRowFilters {

	/*
	 * This class is used to store filters within the QueryStruct2
	 * Idea is to allow for more complex filtering scenarios
	 */
	
	// keep the list of filter objects to execute
	private List<IQueryFilter> filterVec = new Vector<IQueryFilter>();
	
	// keep the list of filtered columns instead of iterating through
	private Set<String> filteredColumns = new HashSet<String>();
	private Set<String> qsFilteredColumns = new HashSet<String>();
	
	public GenRowFilters() {
		
	}

	public List<IQueryFilter> getFilters() {
		return this.filterVec;
	}
	
	public void addFilters(IQueryFilter newFilter) {
		this.filterVec.add(newFilter);
		this.filteredColumns.addAll(newFilter.getAllUsedColumns());
		this.qsFilteredColumns.addAll(newFilter.getAllQueryStructColumns());
	}
	
	public void removeFilter(int index) {
		this.filterVec.remove(index);
		redetermineFilteredColumns();
	}

	public boolean hasFilter(String column) {
		return this.filteredColumns.contains(column);
	}

	public boolean isEmpty() {
		return this.filterVec.isEmpty();
	}
	
	public List<Map<String, Object>> getFormatedFilters() {
		List<Map<String, Object>> ret = new Vector<Map<String, Object>>();
		for(IQueryFilter f : filterVec) {
			Map<String, Object> format = new HashMap<String, Object>();
			format.put("filterObj", f.getSimpleFormat());
			format.put("filterStr", f.getStringRepresentation());
			ret.add(format);
		}
		return ret;
	}
	
	/**
	 * Overriding toString for debugging
	 */
	public String toString() {
		StringBuilder toString = new StringBuilder();
		int numFilters = filterVec.size();
		for(int i = 0; i < numFilters; i++) {
			toString.append(filterVec.get(i)).append("\t");
		}
		return toString.toString();
	}

	public void merge(IQueryFilter filter) {
		GenRowFilters grf = new GenRowFilters();
		grf.addFilters(filter);
		merge(grf);
	}
	
	public void merge(GenRowFilters incomingFilters) {
		merge(incomingFilters, false);
	}
	
	public void merge(GenRowFilters incomingFilters, boolean append) {
		if(incomingFilters == null || incomingFilters.size() == 0) {
			return;
		}
		// incoming filters will add to the existing filters that are present
		// all variables pertaining to the main set of filters will start with m_
		// all variables pertaining to the incoming filters will start with i_
		
		List<IQueryFilter> newFiltersToAppend = new Vector<IQueryFilter>();
		Set<String> newColumnsToFilter = new HashSet<String>();
		Set<String> newQsToFilter = new HashSet<String>();
		
		NEW_FILTERS_LOOP : for(IQueryFilter incoming_filter : incomingFilters.filterVec) {
			// if we only want to append
			// we dont need to do through this logic
			// and we just add to the list
			if(!append && incoming_filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
				SimpleQueryFilter i_filter = (SimpleQueryFilter) incoming_filter;
				// get the new filter
				Set<String> i_usedCols = i_filter.getAllUsedColumns();
				String i_comparator = i_filter.getComparator();
				
				// we can only merge if there is only 1 column used
				// if 2 cols are used, it just gets added to the gen row filter
				if(i_usedCols.size() != 1) {
					// add this filter to the existing QueryFilter
					newFiltersToAppend.add(i_filter);
					newColumnsToFilter.addAll(i_filter.getAllUsedColumns());
					newQsToFilter.addAll(i_filter.getAllQueryStructColumns());
					// continue through the loop
					continue;
				}
				
				// compare this new filter will all the existing filters
				// if we find something where we need to merge
				// we will figure out how to merge
				// else, we will just add it
				for(IQueryFilter my_filter : this.filterVec) {
					if(my_filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
						SimpleQueryFilter m_filter = (SimpleQueryFilter) my_filter;
						// get the columns for the existing filter
						Set<String> m_usedCols = m_filter.getAllUsedColumns();
						String m_comparator = m_filter.getComparator();
						
						// remember i_usedCols only contains a single column
						// so m_usedCol must also have that exact same column to merge
						// and, they must have the exact same comparator
						if(i_usedCols.containsAll(m_usedCols) && i_comparator.equals(m_comparator)) {
							// we can merge!
							m_filter.merge(i_filter);
							
							// break out of the existing filters loop
							// since we have already merged it
							continue NEW_FILTERS_LOOP;
						}
					}
				}
				
				// if we came to this point, we were not able to merge
				// so add it to the list to append
				newFiltersToAppend.add(i_filter);
			}
			// i have no idea how i would merge AND/OR filters together
			// so just add it
			else {
				newFiltersToAppend.add(incoming_filter);
			}
			
			// store all the filtered cols
			// regardless of filter type
			newColumnsToFilter.addAll(incoming_filter.getAllUsedColumns());
			newQsToFilter.addAll(incoming_filter.getAllQueryStructColumns());
		}
		
		// now loop through and add all the new filters
		this.filterVec.addAll(newFiltersToAppend);
		this.filteredColumns.addAll(newColumnsToFilter);
		this.qsFilteredColumns.addAll(newQsToFilter);
	}
	
	public int size() {
		return this.filterVec.size();
	}

	/**
	 * Remove any filters that touch a specific column
	 * @param columnName
	 * @return boolean 		did we remove any filters
	 */
	public boolean removeColumnFilter(String column) {
		if(this.filteredColumns.contains(column)) {
			// we have an existing filter that affects this column
			// get an iterator so we can remove while we iterate
			boolean recreateFilterCols = false;
			Iterator<IQueryFilter> filterIt = this.filterVec.iterator();
			while(filterIt.hasNext()) {
				IQueryFilter filter = filterIt.next();
				if(filter.containsColumn(column)) {
					filterIt.remove();
					this.filteredColumns.remove(column);
					if(filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
						// if its simple
						// we will only need to recreate the filtered columns
						// if it is a col-to-col since the other col may no longer be filtered
						if( ((SimpleQueryFilter) filter).getFilterType() == SimpleQueryFilter.FILTER_TYPE.COL_TO_COL) {
							recreateFilterCols = true;
						}
					} else {
						// if we have a complex filter
						// just recreate all the filter cols
						// might come back and try to recalculate this
						recreateFilterCols = true;
					}
				}
			}
			
			if(recreateFilterCols) {
				redetermineFilteredColumns();
			}
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * Recalculate the filtered columns if an alteration has resulted
	 * in us not knowing which columns are still filtered and which are not
	 */
	public void redetermineFilteredColumns() {
		this.filteredColumns.clear();
		this.qsFilteredColumns.clear();
		for (IQueryFilter filter : this.filterVec) {
			this.filteredColumns.addAll(filter.getAllUsedColumns());
			this.qsFilteredColumns.addAll(filter.getAllQueryStructColumns());
		}
	}
	
	/**
	 * Iterate through a list of columns to remove the filters
	 * @param columns
	 */
	public void removeColumnFilters(Collection<String> columns) {
		for(String column : columns) {
			removeColumnFilter(column);
		}
	}
	
	/**
	 * Remove all filters
	 */
	public void removeAllFilters() {
		this.filterVec.clear();
		this.filteredColumns.clear();
	}
	
	/**
	 * Get all filtered columns used in this filter
	 * @return
	 */
	public Set<String> getAllFilteredColumns() {
		return this.filteredColumns;
	}
	
	/**
	 * Get all fitlered qs columns used in this filter
	 * @return
	 */
	public Set<String> getAllQsFilteredColumns() {
		return this.qsFilteredColumns;
	}
	
	/**
	 * Create a copy of the filter
	 * @return
	 */
	public GenRowFilters copy() {
		GenRowFilters copy = new GenRowFilters();
		for(IQueryFilter filter : this.filterVec) {
			IQueryFilter fCopy = filter.copy();
			copy.addFilters(fCopy);
		}
		return copy;
	}
	
	/**
	 * Get all the QueryFilter objects pertaining to a specific column
	 * @param column
	 * @return
	 */
	public List<IQueryFilter> getAllQueryFiltersContainingColumn(String column) {
		List<IQueryFilter> filterList = new Vector<IQueryFilter>();
		// since we already store all the filtered columns
		// if what is passed is not in the list
		// return an empty list
		// else
		// loop through and get all the queryfilter objects that touch this column
		if(!this.filteredColumns.contains(column)) {
			return filterList;
		}
		for(IQueryFilter f : this.filterVec) {
			if(f.containsColumn(column)) {
				filterList.add(f);
			}
		}
		
		return filterList;
	}
	
	/**
	 * Get the list of all simple queries that use a specific column
	 * @param column
	 * @return
	 */
	public List<SimpleQueryFilter> getAllSimpleQueryFiltersContainingColumn(String column) {
		List<SimpleQueryFilter> filterList = new Vector<SimpleQueryFilter>();
		// since we already store all the filtered columns
		// if what is passed is not in the list
		// return an empty list
		// else
		// loop through and get all the queryfilter objects that touch this column
		if(!this.filteredColumns.contains(column)) {
			return filterList;
		}
		for(IQueryFilter f : this.filterVec) {
			if(f.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
				if(f.containsColumn(column)) {
					filterList.add((SimpleQueryFilter)f);
				}
			}
		}
		
		return filterList;
	}
	
	/**
	 * Get the list of all simple queries
	 * @return
	 */
	public List<SimpleQueryFilter> getAllSimpleQueryFilters() {
		List<SimpleQueryFilter> filterList = new Vector<SimpleQueryFilter>();

		for(IQueryFilter f : this.filterVec) {
			if(f.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
				filterList.add((SimpleQueryFilter)f);
			}
		}
		return filterList;
	}
}
