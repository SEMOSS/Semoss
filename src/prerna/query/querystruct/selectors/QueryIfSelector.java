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
		// TODO Auto-generated method stub
		return SELECTOR_TYPE.IF_ELSE;
	}

	@Override
	public String getAlias() {
		// TODO Auto-generated method stub
		return alias;
	}

	@Override
	public boolean isDerived() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getDataType() {
		// TODO Auto-generated method stub
		// need to figure out if this is a string or a number
		return null;
	}

	@Override
	public String getQueryStructName() {
		// TODO Auto-generated method stub
		return pixelString;
	}

	// will come back to this
	@Override
	public List<QueryColumnSelector> getAllQueryColumns() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void setCondition(IQueryFilter condition)
	{
		this.condition = condition;
	}
	
	public void setPrecedent(IQuerySelector precedent)
	{
		this.precedent = precedent;
	}
	
	public void setAntecedent(IQuerySelector antecedent)
	{
		this.antecedent = antecedent;
	}
	
	public IQueryFilter getCondition()
	{
		return this.condition;
	}
	
	public IQuerySelector getPrecedent()
	{
		return this.precedent;
	}
	
	public IQuerySelector getAntecedent()
	{
		return this.antecedent;
	}
	

}
