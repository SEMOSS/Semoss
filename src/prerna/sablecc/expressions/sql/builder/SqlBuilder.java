package prerna.sablecc.expressions.sql.builder;

import java.util.List;

import prerna.ds.H2.H2Frame;

public class SqlBuilder {

	protected H2Frame frame;
	
	// sql objects
	protected SqlSelectorStatement selectors = new SqlSelectorStatement();
	protected SqlGroupBy groups = new SqlGroupBy();
	
	public SqlBuilder(H2Frame frame) {
		this.frame = frame;
	}
	
	public H2Frame getFrame() {
		return this.frame;
	}
	
	/**
	 * Add a new selector for the sql statement
	 * @param selector
	 */
	public void addSelector(ISqlSelector selector) {
		selectors.addSelector(selector);
	}
	
	/**
	 * Get the list of selector objects
	 * @return
	 */
	public List<ISqlSelector> getSelectors() {
		return this.selectors.getSelectors();
	}
	
	/**
	 * Get the number of selectors
	 * @return
	 */
	public int selectorSize() {
		return this.selectors.size();
	}
	
	/**
	 * Get the selector object at a specific index
	 * @param index
	 * @return
	 */
	public ISqlSelector getSelector(int index) {
		return selectors.get(index);
	}
	
	/**
	 * Get the names of the selectors
	 * @return
	 */
	public List<String> getSelectorNames() {
		return selectors.getSelectorNames();
	}
	
	/**
	 * Get the last selector added
	 * @return
	 */
	public ISqlSelector getLastSelector() {
		return selectors.get(selectors.size()-1);
	}
	
	/**
	 * Replace an existing selector with a new selector
	 * Used when we are building to modify a selector
	 * @param previousSelector
	 * @param newSelector
	 */
	public void replaceSelector(ISqlSelector previousSelector, ISqlSelector newSelector) {
		this.selectors.replaceSelector(previousSelector, newSelector);
	}
	
	/**
	 * Replace an existing selector with a new selector
	 * Used when we are building to modify a selector
	 * @param previousSelector
	 * @param newSelector
	 */
	public void removeSelector(ISqlSelector previousSelector) {
		this.selectors.remove(previousSelector);
	}
	
	/**
	 * Add a new group by for the sql statement
	 * @param groupBy
	 */
	public void addGroupBy(SqlColumnSelector groupBy) {
		groups.addGroupBy(groupBy);
	}
	
	/**
	 * Get the columns used in the group by
	 * @return
	 */
	public List<String> getGroupByColumns() {
		return groups.getGroupByCols();
	}
	
	/**
	 * Get the columns used in the group by
	 * @return
	 */
	public List<SqlColumnSelector> getGroupBySelectors() {
		return groups.getGroupBySelectors();
	}
	
	/**
	 * Get the string for the selectors
	 * @return
	 */
	public String getSelectorString() {
		return selectors.toString();
	}
	
	/**
	 * Get the string for the group by
	 * @return
	 */
	public String getGroupByString() {
		return groups.toString();
	}
	
	public List<String> getTableColumns() {
		return selectors.getTableColumns();
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("SELECT DISTINCT ").append(this.selectors.toString()).append(" FROM ");
		
		// determine if querying view or table
		if(frame.isJoined()) {
			builder.append(frame.getViewTableName());
		} else {
			builder.append(frame.getTableName());
		}
		
		// add filters
		String filters = frame.getSqlFilter();
		if(filters != null && !filters.isEmpty()) {
			builder.append(filters);
		}
		
		builder.append(" ").append(groups.toString());
		
		return builder.toString();
	}
}
