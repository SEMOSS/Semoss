package prerna.query.querystruct.filters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.google.gson.Gson;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.util.gson.GsonUtility;

public class FunctionQueryFilter implements IQueryFilter {

	private QueryFunctionSelector functionSelector;

	public FunctionQueryFilter() {
		
	}
	
	public void setFunctionSelector(QueryFunctionSelector functionSelector) {
		this.functionSelector = functionSelector;
	}
	
	public QueryFunctionSelector getFunctionSelector() {
		return functionSelector;
	}
	
	@Override
	public QUERY_FILTER_TYPE getQueryFilterType() {
		return QUERY_FILTER_TYPE.FUNCTION;
	}

	@Override
	public Set<String> getAllUsedColumns() {
		Set<String> usedColumns = new HashSet<String>();
		usedColumns.add(functionSelector.getAlias());
		return usedColumns;
	}

	@Override
	public Set<String> getAllQueryStructColumns() {
		Set<String> usedColumns = new HashSet<String>();
		usedColumns.add(functionSelector.getQueryStructName());
		return usedColumns;
	}

	@Override
	public Set<String> getAllUsedTables() {
		Set<String> usedTables = new HashSet<String>();
		List<QueryColumnSelector> colValues = functionSelector.getAllQueryColumns();
		for(QueryColumnSelector c : colValues) {
			usedTables.add(c.getTable());
		}
		return usedTables;
	}

	@Override
	public boolean containsColumn(String column) {
		if( functionSelector.getAlias().equals(column)) {
			return true;
		} else if( functionSelector.getQueryStructName().equals(column)) {
			return true;
		}
		return false;
	}

	@Override
	public IQueryFilter copy() {
		Gson gson = GsonUtility.getDefaultGson();
		String str = gson.toJson(functionSelector);
		QueryFunctionSelector funCopy = gson.fromJson(str, QueryFunctionSelector.class);
		
		FunctionQueryFilter copy = new FunctionQueryFilter();
		copy.setFunctionSelector(funCopy);
		
		return copy;
	}

	@Override
	public String getStringRepresentation() {
		StringBuilder builder = new StringBuilder();
		builder.append(functionSelector.getFunction()).append("(");
		List<IQuerySelector> innerSelectors = functionSelector.getInnerSelector();
		if(!innerSelectors.isEmpty()) {
			builder.append(innerSelectors.get(0).getQueryStructName());
			for(int i = 1; i < innerSelectors.size(); i++) {
				builder.append(",").append(innerSelectors.get(i).getQueryStructName());
			}
		}
		return builder.toString();
	}

	@Override
	public Object getSimpleFormat() {
		Map<String, Object> ret = new HashMap<String, Object>();
		ret.put("filterType", getQueryFilterType());
		List<String> functionInput = new Vector<String>();
		functionInput.add(functionSelector.getQueryStructName());
		ret.put("function", functionInput);
		return ret;
	}

}
