package prerna.sablecc.expressions;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;

public abstract class AbstractExpressionBuilder implements IExpressionBuilder {

	/*
	 * Selector and GroupBy objects need to defined in the constructor of the 
	 * instance
	 */
	protected AbstractExpressionSelectorStatement selectors;
	protected AbstractExpressionGroupBy groups;
	
	protected int offset;
	protected int limit;
	
	protected IExpressionSelector sortBy;
	
	/*
	 * Only two methods that the user needs to define within the expression builder
	 */
	public abstract ITableDataFrame getFrame();
	public abstract String toString();
	
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
		return selectors.getAllTableColumnsUsed();
	}
	
	@Override
	public void addLimit(int limit) {
		this.limit = limit;
	}

	@Override
	public void addOffset(int offset) {
		this.offset = offset;
	}
	
	@Override
	public int getLimit() {
		return this.limit;
	}
	
	@Override
	public int getOffset() {
		return this.offset;
	}

	@Override
	public void addSortSelector(IExpressionSelector sortBy) {
		this.sortBy = sortBy;
	}
	
	@Override
	public IExpressionSelector getSortSelector() {
		return this.sortBy;
	}
}
