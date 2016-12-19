package prerna.ds;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IteratorOptions {

	private String sortByDirection;
	private boolean distinct;
	private boolean useFilters;
	private int limit;
	private int offset;
	private String sortBy;
	private List<String> selectors;
	private Map<String, List<Object>> temporalBindings;
	
	public IteratorOptions() {
		distinct = false;
		useFilters = true;
		limit = -1;
		offset = -1;
		selectors = new ArrayList<String>(0);
	}
		
	public void setSelectors(List<String> selectors) {this.selectors = selectors;}
	public List<String> getSelectors() {return this.selectors;}
	
	public void setLimit(int limit) {this.limit = limit;}
	public int getLimit() {return this.limit;}
	
	public void setOffset(int offset) {this.offset = offset;}
	public int getOffset() {return this.offset;}
	
	public void withFilters(boolean withFilters) {this.useFilters = withFilters;}
	public boolean useFilters() {return this.useFilters;}
	
	public void setSortColumn(String columnHeader) {this.sortBy = columnHeader;}
	public String getSortColumn() {return this.sortBy;}
	
	public void setSortDirection(String sortDirection) {}
	public String getSortDirection() {return this.sortByDirection;}
	
	public void setSelectDistinct(boolean selectDistinct) {this.distinct = selectDistinct;}
	public boolean selectDistinct() {return this.distinct;}
	
	public void setBindings(Map<String, List<Object>> bindings) {this.temporalBindings = bindings;}
	public Map<String, List<Object>> getBindings() {return this.temporalBindings;}
}
