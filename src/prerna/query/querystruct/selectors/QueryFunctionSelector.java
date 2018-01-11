package prerna.query.querystruct.selectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class QueryFunctionSelector extends AbstractQuerySelector {

	private List<IQuerySelector> innerSelectors;
	private String functionName;
	private boolean isDistinct;
	private String colCast;
	
	public QueryFunctionSelector() {
		this.innerSelectors = new ArrayList<IQuerySelector>();
		this.functionName = null;
		this.isDistinct = false;
		this.colCast = "";
	}

	@Override
	public SELECTOR_TYPE getSelectorType() {
		return SELECTOR_TYPE.FUNCTION;
	}

	@Override
	public String getAlias() {
		if(this.alias == null || this.alias.equals("")) {
			StringBuilder qsConcat = new StringBuilder();
			for(int i = 0; i < this.innerSelectors.size(); i++) {
				qsConcat.append(this.innerSelectors.get(i).getAlias());
			}
			return QueryFunctionHelper.getPrettyName(this.functionName) + "_" + qsConcat;
		}
		return this.alias;
	}
	
	@Override
	public boolean isDerived() {
		return true;
	}
	
	@Override
	public String getDataType() {
		return QueryFunctionHelper.determineTypeOfFunction(functionName);
	}

	@Override
	public String getQueryStructName() {
		StringBuilder qsConcat = new StringBuilder();
		for(int i = 0; i < this.innerSelectors.size(); i++) {
			qsConcat.append(this.innerSelectors.get(i).getQueryStructName());
		}
		return this.functionName + "(" + qsConcat + ")";
	}

	public List<IQuerySelector> getInnerSelector() {
		return innerSelectors;
	}

	public void addInnerSelector(IQuerySelector innerSelector) {
		this.innerSelectors.add(innerSelector);
	}
	
	public void setInnerSelector(List<IQuerySelector> innerSelectors) {
		this.innerSelectors = innerSelectors;
	}

	public String getFunction() {
		return functionName;
	}

	public void setFunction(String functionName) {
		this.functionName = functionName;
	}

	public boolean isDistinct() {
		return isDistinct;
	}

	public void setDistinct(boolean isDistinct) {
		this.isDistinct = isDistinct;
	}
	
	public void setColCast(String colCast) {
		this.colCast = colCast;
	}
	
	public String getColCast() {
		return this.colCast;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof QueryFunctionSelector) {
			QueryFunctionSelector selector = (QueryFunctionSelector)obj;
			if(this.innerSelectors.equals(selector.innerSelectors) &&
					this.alias.equals(selector.alias) &&
					this.functionName.equals(selector.functionName) &&
					this.isDistinct == selector.isDistinct) {
					return true;
			}
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		String allString = innerSelectors+":::"+alias+":::"+functionName+":::"+isDistinct;
		return allString.hashCode();
	}

	@Override
	public List<QueryColumnSelector> getAllQueryColumns() {
		// grab all the columns from the inner selector
		List<QueryColumnSelector> usedCols = new Vector<QueryColumnSelector>();
		for(int i = 0; i < this.innerSelectors.size(); i++) {
			usedCols.addAll(this.innerSelectors.get(i).getAllQueryColumns());
		}
		return usedCols;
	}
}