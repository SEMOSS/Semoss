package prerna.sablecc.expressions.sql.builder;

import java.util.List;
import java.util.Vector;

public class SqlSelectorStatement {

	/*
	 * This class will hold the list of selectors from the sql statement
	 */
	
	protected List<ISqlSelector> selectors = new Vector<ISqlSelector>();

	protected void addSelector(ISqlSelector selector) {
		selectors.add(selector);
	}
	
	protected void addSelector(int index, ISqlSelector selector) {
		selectors.add(index, selector);
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for(ISqlSelector selector : selectors) {
			if(builder.length() == 0) {
				builder.append(selector);
			} else {
				builder.append(" , ").append(selector);
			}
		}
		
		return builder.toString();
	}
	
	public List<ISqlSelector> getSelectors() {
		return this.selectors;
	}
	
	public int size() {
		return selectors.size();
	}
	
	public ISqlSelector get(int i) {
		return selectors.get(i);
	}

	public void replaceSelector(ISqlSelector previousSelector, ISqlSelector newSelector) {
		int index = selectors.indexOf(previousSelector);
		selectors.remove(index);
		selectors.add(index, newSelector);
	}

	public void remove(ISqlSelector previousSelector) {
		selectors.remove(previousSelector);
	}

	public List<String> getSelectorNames() {
		List<String> selectorNames = new Vector<String>();
		for(ISqlSelector selector : selectors) {
			selectorNames.add(selector.toString());
		}
		return selectorNames;
	}
	
	public List<String> getTableColumns() {
		List<String> tableColumns = new Vector<String>();
		for(ISqlSelector selector : selectors) {
			tableColumns.addAll(selector.getTableColumns());
		}
		return tableColumns;
	}

}
