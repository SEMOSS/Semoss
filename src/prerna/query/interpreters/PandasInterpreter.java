package prerna.query.interpreters;

import java.util.List;
import java.util.Map;

import prerna.algorithm.api.SemossDataType;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class PandasInterpreter extends AbstractQueryInterpreter {

	private String dataTableName = null;
	private Map<String, SemossDataType> colDataTypes;
	
	private StringBuilder selectorCriteria;
	
	@Override
	public String composeQuery() {
		StringBuilder query = new StringBuilder();
		query.append(this.dataTableName)
			.append("[")
			.append(this.selectorCriteria.toString())
			.append("]");
		
		return query.toString();
	}

	public void addSelectors() {
		this.selectorCriteria = new StringBuilder();
		List<IQuerySelector> selectors = qs.getSelectors();
		int size = selectors.size();
		for(int i = 0; i < size; i++) {
			IQuerySelector selector = selectors.get(i);
			if(i == 0) {
				this.selectorCriteria.append(processSelector(selector));
			} else {
				this.selectorCriteria.append(", ").append(processSelector(selector));
			}
		}
	}
	
	private String processSelector(IQuerySelector selector) {
		if(selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return processColumnSelector( (QueryColumnSelector) selector);
		} else {
			return null;
		}
	}
	
	private String processColumnSelector(QueryColumnSelector selector) {
		String columnName = selector.getColumn();
		String alias = selector.getAlias();
		
		// just return the column name
		return "'" + columnName + "'";
	}
	
	public void setDataTableName(String dataTableName) {
		this.dataTableName = dataTableName;
	}
}
