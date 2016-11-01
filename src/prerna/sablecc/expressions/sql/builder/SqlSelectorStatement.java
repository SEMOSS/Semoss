package prerna.sablecc.expressions.sql.builder;

import java.util.List;
import java.util.Vector;

import prerna.sablecc.expressions.IExpressionSelector;

public class SqlSelectorStatement {

	/*
	 * This class will hold the list of selectors from the sql statement
	 */
	
	protected List<IExpressionSelector> selectors = new Vector<IExpressionSelector>();

	protected void addSelector(IExpressionSelector selector) {
		selectors.add(selector);
	}
	
	protected void addSelector(int index, IExpressionSelector selector) {
		selectors.add(index, selector);
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for(IExpressionSelector selector : selectors) {
			if(builder.length() == 0) {
				builder.append(selector);
			} else {
				builder.append(" , ").append(selector);
			}
		}
		
		return builder.toString();
	}
	
	public List<IExpressionSelector> getSelectors() {
		return this.selectors;
	}
	
	public int size() {
		return selectors.size();
	}
	
	public IExpressionSelector get(int i) {
		return selectors.get(i);
	}

	public void replaceSelector(IExpressionSelector previousSelector, IExpressionSelector newSelector) {
		int index = selectors.indexOf(previousSelector);
		selectors.remove(index);
		selectors.add(index, newSelector);
	}

	public void remove(IExpressionSelector previousSelector) {
		selectors.remove(previousSelector);
	}

	public List<String> getSelectorNames() {
		List<String> selectorNames = new Vector<String>();
		for(IExpressionSelector selector : selectors) {
			selectorNames.add(selector.toString());
		}
		return selectorNames;
	}
	
	public List<String> getTableColumns() {
		List<String> tableColumns = new Vector<String>();
		for(IExpressionSelector selector : selectors) {
			tableColumns.addAll(selector.getTableColumns());
		}
		return tableColumns;
	}

}
