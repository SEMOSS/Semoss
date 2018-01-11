package prerna.query.querystruct.selectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import prerna.poi.main.HeadersException;

public class QueryOpaqueSelector extends AbstractQuerySelector {

	private String querySelectorSyntax;
	
	public QueryOpaqueSelector() {
		
	}
	
	public QueryOpaqueSelector(String querySelectorSyntax) {
		this.querySelectorSyntax = querySelectorSyntax;
	}
	
	@Override
	public SELECTOR_TYPE getSelectorType() {
		return SELECTOR_TYPE.OPAQUE;
	}

	@Override
	public String getAlias() {
		if(this.alias == null || this.alias.equals("")) {
			String cleanSelectorSyntax = HeadersException.getInstance().recursivelyFixHeaders(this.querySelectorSyntax, new ArrayList<String>());
			return cleanSelectorSyntax;
		}
		return this.alias;
	}
	
	public String getQuerySelectorSyntax() {
		return this.querySelectorSyntax;
	}
	
	public void setQuerySelectorSyntax(String querySelectorSyntax) {
		this.querySelectorSyntax = querySelectorSyntax;
	}
	
	@Override
	public boolean isDerived() {
		return true;
	}

	@Override
	public String getQueryStructName() {
		return getAlias();
	}

	@Override
	public String getDataType() {
		return null;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof QueryOpaqueSelector) {
			QueryOpaqueSelector selector = (QueryOpaqueSelector)obj;
			if(this.querySelectorSyntax.equals(selector.querySelectorSyntax) &&
					this.alias.equals(selector.alias)) {
					return true;
			}
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		String allString = querySelectorSyntax+":::"+alias;
		return allString.hashCode();
	}

	@Override
	public List<QueryColumnSelector> getAllQueryColumns() {
		// return empty list
		List<QueryColumnSelector> usedCols = new Vector<QueryColumnSelector>();
		return usedCols;
	}
}
