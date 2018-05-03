package prerna.query.querystruct.update;

import java.util.ArrayList;
import java.util.List;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class UpdateQueryStruct extends AbstractQueryStruct {
	
	private List<QueryColumnSelector> selectors = new ArrayList<>();
	private List<Object> values = new ArrayList<>();
	
	/**
	 * Default constructor
	 */
	public UpdateQueryStruct() {
		
	}
	
	//////////////////////////////////////////// SELECTORS /////////////////////////////////////////////////
	
	public List<QueryColumnSelector> getSelectors() {
		return this.selectors;
	}
	
	public void setSelectors(List<QueryColumnSelector> selectors) {
		this.selectors = selectors;
	}
	
	public void addSelector(String concept, String property) {
		if(property == null) {
			property = AbstractQueryStruct.PRIM_KEY_PLACEHOLDER; 
		}
		QueryColumnSelector selector = new QueryColumnSelector();
		selector.setTable(concept);
		selector.setColumn(property);
		this.selectors.add(selector);
	}
	
	public void addSelector(QueryColumnSelector selector) {
		this.selectors.add(selector);
	}
	//////////////////////////////////////////// end SELECTORS /////////////////////////////////////////////
	
	//////////////////////////////////////////// VALUES ////////////////////////////////////////////////////
	
	public List<Object> getValues() {
		return this.values;
	}
	
	public void setValues(List<Object> values) {
		this.values = values;
	}
	
}
