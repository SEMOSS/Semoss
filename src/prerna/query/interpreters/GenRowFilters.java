package prerna.query.interpreters;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.sablecc2.om.QueryFilter;

public class GenRowFilters {

	/*
	 * This class is used to store filters within the QueryStruct2
	 * Idea is to allow for more complex filtering scenarios
	 */
	
	// keep the list of filter objects to execute
	private List<QueryFilter> filterVec = new Vector<QueryFilter>();
	
	// keep the list of filtered columns instead of iterating through
	private Set<String> filteredColumns = new HashSet<String>();
	
	public GenRowFilters() {
		
	}

	public List<QueryFilter> getFilters() {
		return this.filterVec;
	}
	
	public void addFilters(QueryFilter newFilter) {
		this.filterVec.add(newFilter);
		this.filteredColumns.addAll(newFilter.getAllUsedColumns());
	}

	public boolean hasFilter(String column) {
		return this.filteredColumns.contains(column);
	}

	public boolean isEmpty() {
		return this.filterVec.isEmpty();
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

	public void merge(GenRowFilters incomingFilters) {
		//TODO:
		//TODO:
		//TODO:
		//TODO:
		//TODO:
		// actually do a merge... right now just adding all
		this.filterVec.addAll(incomingFilters.filterVec);
		this.filteredColumns.addAll(incomingFilters.filteredColumns);
	}
	
}
