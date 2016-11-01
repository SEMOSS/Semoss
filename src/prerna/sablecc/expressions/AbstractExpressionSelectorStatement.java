package prerna.sablecc.expressions;

import java.util.List;
import java.util.Vector;

public abstract class AbstractExpressionSelectorStatement {

	/*
	 * This class will hold the list of selectors for the expression
	 * Note: the only method that needs to be overridden is the toString()
	 * method which actual generates the script in the appropriate format
	 * for proper compilation
	 */
	
	protected List<IExpressionSelector> selectors = new Vector<IExpressionSelector>();

	@Override
	public abstract String toString();
	
	/**
	 * Add a selector to the list
	 * @param selector
	 */
	public void addSelector(IExpressionSelector selector) {
		selectors.add(selector);
	}
	
	/**
	 * Add a selector at a specific index to the list
	 * @param index
	 * @param selector
	 */
	public void addSelector(int index, IExpressionSelector selector) {
		selectors.add(index, selector);
	}
	
	/**
	 * Get the selector objects
	 * @return
	 */
	public List<IExpressionSelector> getSelectors() {
		return this.selectors;
	}
	
	/**
	 * Get the number of selectors
	 * @return
	 */
	public int size() {
		return selectors.size();
	}
	
	/**
	 * Get the selector at a specific index
	 * @param i
	 * @return
	 */
	public IExpressionSelector get(int i) {
		return selectors.get(i);
	}

	/**
	 * Replace an existing selector with a new one
	 * Used when we are building on top of an existing selector
	 * @param previousSelector
	 * @param newSelector
	 */
	public void replaceSelector(IExpressionSelector previousSelector, IExpressionSelector newSelector) {
		int index = selectors.indexOf(previousSelector);
		selectors.remove(index);
		selectors.add(index, newSelector);
	}

	/**
	 * Remove a specific selector
	 * @param previousSelector
	 */
	public void remove(IExpressionSelector previousSelector) {
		selectors.remove(previousSelector);
	}

	/**
	 * Get the list of names for the selectors
	 * @return
	 */
	public List<String> getSelectorNames() {
		List<String> selectorNames = new Vector<String>();
		for(IExpressionSelector selector : selectors) {
			selectorNames.add(selector.toString());
		}
		return selectorNames;
	}
	
	/**
	 * Get all the table columns used throughout all the expressions
	 * @return
	 */
	public List<String> getAllTableColumnsUsed() {
		List<String> tableColumns = new Vector<String>();
		for(IExpressionSelector selector : selectors) {
			tableColumns.addAll(selector.getTableColumns());
		}
		return tableColumns;
	}
	
}
