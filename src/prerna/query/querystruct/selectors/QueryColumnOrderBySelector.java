package prerna.query.querystruct.selectors;

public class QueryColumnOrderBySelector extends QueryColumnSelector {

	public enum ORDER_BY_DIRECTION {ASC, DESC};
	
	private String sortDir = "";
	
	public QueryColumnOrderBySelector() {
		super();
	}
	
	public QueryColumnOrderBySelector(String qsValue) {
		super(qsValue);
	}
	
	public QueryColumnOrderBySelector(String qsValue, String sortDir) {
		super(qsValue);
		setSortDir(sortDir);
	}
	 
	public void setSortDir(String sortDir) {
		this.sortDir = sortDir.toUpperCase();
	}
	
	public String getSortDirString() {
		return this.sortDir;
	}
	
	public ORDER_BY_DIRECTION getSortDir() {
		// if empty, assume ascending
		if(this.sortDir.isEmpty()) {
			return ORDER_BY_DIRECTION.ASC;
		}
		
		/*
		 * Accounting for:
		 * ascending
		 * increasing
		 * up
		 */
		if(this.sortDir.contains("ASC") ||
				this.sortDir.contains("INC") ||
				this.sortDir.contains("UP")) {
			return ORDER_BY_DIRECTION.ASC;
		}
		
		return ORDER_BY_DIRECTION.DESC;
	}
	
}
