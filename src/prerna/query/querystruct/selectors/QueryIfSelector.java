package prerna.query.querystruct.selectors;

import java.util.List;

import prerna.query.querystruct.filters.IQueryFilter;

public class QueryIfSelector extends AbstractQuerySelector {
	
	String pixelString = null;
	IQueryFilter condition = null;
	IQuerySelector precedent = null;
	IQuerySelector antecedent = null;

	@Override
	public SELECTOR_TYPE getSelectorType() {
		return SELECTOR_TYPE.IF_ELSE;
	}

	@Override
	public String getAlias() {
		return alias;
	}

	@Override
	public boolean isDerived() {
		return false;
	}

	@Override
	public String getDataType() {
		// need to figure out if this is a string or a number
		// TODO: pushing now so FE doesn't get errors - always string
		return "STRING";
	}

	@Override
	public String getQueryStructName() {
		return pixelString;
	}

	// will come back to this
	@Override
	public List<QueryColumnSelector> getAllQueryColumns() {
		return null;
	}

	public void setCondition(IQueryFilter condition) {
		this.condition = condition;
	}
	
	public void setPrecedent(IQuerySelector precedent) {
		this.precedent = precedent;
	}
	
	public void setAntecedent(IQuerySelector antecedent) {
		this.antecedent = antecedent;
	}
	
	public IQueryFilter getCondition() {
		return this.condition;
	}
	
	public IQuerySelector getPrecedent() {
		return this.precedent;
	}
	
	public IQuerySelector getAntecedent() {
		return this.antecedent;
	}
	
	public void setPixelString(String pixelString) {
		this.pixelString = pixelString;
	}
}
