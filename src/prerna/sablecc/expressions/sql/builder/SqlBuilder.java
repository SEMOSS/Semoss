package prerna.sablecc.expressions.sql.builder;

import java.util.List;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.expressions.IExpressionBuilder;
import prerna.sablecc.expressions.IExpressionSelector;

public class SqlBuilder implements IExpressionBuilder {

	// the data frame to execute the expression on
	protected H2Frame frame;
	
	// sql objects
	protected SqlSelectorStatement selectors = new SqlSelectorStatement();
	protected SqlGroupBy groups = new SqlGroupBy();
	
	public SqlBuilder(H2Frame frame) {
		this.frame = frame;
	}
	
	@Override
	public H2Frame getFrame() {
		return this.frame;
	}
	
	@Override
	public void addSelector(IExpressionSelector selector) {
		selectors.addSelector(selector);
	}
	
	@Override
	public void addSelector(int index, IExpressionSelector selector) {
		selectors.addSelector(index, selector);
	}
	
	@Override
	public List<IExpressionSelector> getSelectors() {
		return this.selectors.getSelectors();
	}
	
	
	@Override
	public int numSelectors() {
		return this.selectors.size();
	}
	
	@Override
	public List<String> getGroupByColumns() {
		return groups.getGroupByCols();
	}

	@Override
	public List<IExpressionSelector> getGroupBySelectors() {
		return groups.getGroupBySelectors();
	}
	
	
	@Override
	public IExpressionSelector getSelector(int index) {
		return selectors.get(index);
	}

	@Override
	public List<String> getSelectorNames() {
		return selectors.getSelectorNames();
	}

	@Override
	public IExpressionSelector getLastSelector() {
		return selectors.get(selectors.size()-1);
	}

	@Override
	public void replaceSelector(IExpressionSelector previousSelector, IExpressionSelector newSelector) {
		this.selectors.replaceSelector(previousSelector, newSelector);
	}

	@Override
	public void removeSelector(IExpressionSelector selector) {
		this.selectors.remove(selector);
	}

	@Override
	public void addGroupBy(IExpressionSelector groupBySelector) {
		groups.addGroupBy(groupBySelector);
	}

	@Override
	public String getSelectorString() {
		return selectors.toString();
	}
	
	@Override
	public String getGroupByString() {
		return groups.toString();
	}

	
	@Override
	public List<String> getAllTableColumnsUsed() {
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
