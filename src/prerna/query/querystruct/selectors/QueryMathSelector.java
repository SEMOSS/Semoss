package prerna.query.querystruct.selectors;

import java.util.List;
import java.util.Vector;

public class QueryMathSelector extends AbstractQuerySelector {

	private IQuerySelector innerSelector;
	private QueryAggregationEnum math;
	private boolean isDistinct;
	
	public QueryMathSelector() {
		this.innerSelector = null;
		this.math = null;
		this.isDistinct = false;
	}

	@Override
	public SELECTOR_TYPE getSelectorType() {
		return SELECTOR_TYPE.MATH;
	}

	@Override
	public String getAlias() {
		if(this.alias.equals("")) {
			return this.math.getExpressionName() + "_" + this.innerSelector.getAlias();
		}
		return this.alias;
	}
	
	@Override
	public boolean isDerived() {
		return true;
	}
	
	@Override
	public String getDataType() {
		return math.getDataType();
	}

	@Override
	public String getQueryStructName() {
		return this.math.getExpressionName() + "(" + this.innerSelector.getQueryStructName() + ")";
	}

	public IQuerySelector getInnerSelector() {
		return innerSelector;
	}

	public void setInnerSelector(IQuerySelector innerSelector) {
		this.innerSelector = innerSelector;
	}

	public QueryAggregationEnum getMath() {
		return math;
	}

	public void setMath(QueryAggregationEnum math) {
		this.math = math;
	}

	public boolean isDistinct() {
		return isDistinct;
	}

	public void setDistinct(boolean isDistinct) {
		this.isDistinct = isDistinct;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof QueryMathSelector) {
			QueryMathSelector selector = (QueryMathSelector)obj;
			if(this.innerSelector.equals(selector.innerSelector) &&
					this.alias.equals(selector.alias) &&
					this.math.equals(selector.math) &&
					this.isDistinct == selector.isDistinct) {
					return true;
			}
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		String allString = innerSelector+":::"+alias+":::"+math+":::"+isDistinct;
		return allString.hashCode();
	}

	@Override
	public List<QueryColumnSelector> getAllQueryColumns() {
		// grab all the columns from the inner selector
		List<QueryColumnSelector> usedCols = new Vector<QueryColumnSelector>();
		usedCols.addAll(this.innerSelector.getAllQueryColumns());
		return usedCols;
	}
}
