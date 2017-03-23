package prerna.ds.querystruct;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.sablecc2.om.Filter;

public class GenRowFilters {

	// keep the list of filter objects to execute
	private List<Filter> filterVec = new Vector<Filter>();
	
	// keep the list of filtered columns instead of iterating through
	private Set<String> filteredColumns = new HashSet<String>();
	
	public GenRowFilters() {
		
	}

	public List<Filter> getFilters() {
		return this.filterVec;
	}
	
	public void addFilters(Filter newFilter) {
		//TODO : add the validation portion
		//TODO : add the validation portion
		//TODO : add the validation portion
		//TODO : add the validation portion
		//TODO : add the validation portion
		this.filterVec.add(newFilter);
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
