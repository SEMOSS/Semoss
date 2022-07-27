package prerna.query.querystruct.selectors;

import java.util.ArrayList;
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
		String dataType1 = precedent.getDataType();
		String dataType2 = antecedent.getDataType();
		if(dataType1 == null && dataType2 == null) {
			return "STRING";
		}
		if(dataType1 != null && dataType2 == null) {
			return dataType1;
		}
		if(dataType1 == null && dataType2 != null) {
			return dataType2;
		}
		if(dataType1.equals(dataType2)) {
			return dataType1;
		}
		return "STRING";
	}

	@Override
	public String getQueryStructName() {
		return pixelString;
	}

	// will come back to this
	@Override
	public List<QueryColumnSelector> getAllQueryColumns() {
		// TODO: when we start allowing parameterization 
		// of the if statement in normal pixel 
		// we can return the actual columns used
		return new ArrayList<>();
//		return this.condition.getAllQueryColumns();
	}

	/**
	 * This is the condition for the if statement
	 * @param condition
	 */
	public void setCondition(IQueryFilter condition) {
		this.condition = condition;
	}
	
	/**
	 * This is the true case for the if statement
	 * @param precedent
	 */
	public void setPrecedent(IQuerySelector precedent) {
		this.precedent = precedent;
	}
	
	/**
	 * This is the false case for the if statement
	 * @param antecedent
	 */
	public void setAntecedent(IQuerySelector antecedent) {
		this.antecedent = antecedent;
	}
	
	/**
	 * Get the condition for the if statement
	 * @return
	 */
	public IQueryFilter getCondition() {
		return this.condition;
	}
	
	/**
	 * Get the true case for the if statement
	 * @return
	 */
	public IQuerySelector getPrecedent() {
		return this.precedent;
	}
	
	/**
	 * Get the false case for the if statement
	 * @return
	 */
	public IQuerySelector getAntecedent() {
		return this.antecedent;
	}
	
	public void setPixelString(String pixelString) {
		this.pixelString = pixelString;
	}
	
	/**
	 * Helper function to create a query if selector
	 * @param condition
	 * @param qsName1
	 * @param qsName2
	 * @param alias
	 * @return
	 */
	public static QueryIfSelector makeQueryIfSelector(IQueryFilter condition, String qsName1, String qsName2, String alias) {
		return makeQueryIfSelector(condition, new QueryColumnSelector(qsName1), new QueryColumnSelector(qsName2), alias);
	}
	
	/**
	 * Helper function to create a query if selector
	 * @param condition
	 * @param precedent
	 * @param antecedent
	 * @param alias
	 * @return
	 */
	public static QueryIfSelector makeQueryIfSelector(IQueryFilter condition, IQuerySelector precedent, IQuerySelector antecedent, String alias) {
		QueryIfSelector ifSelector = new QueryIfSelector();
		ifSelector.setCondition(condition);
		ifSelector.setPrecedent(precedent);
		ifSelector.setAntecedent(antecedent);
		ifSelector.setAlias(alias);
		return ifSelector;
	}
}
