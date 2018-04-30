package prerna.query.querystruct.update;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class UpdateQueryStruct {
	
	public final static String PRIM_KEY_PLACEHOLDER = "PRIM_KEY_PLACEHOLDER";

	protected transient ITableDataFrame frame;
	protected transient IEngine engine;
	
	private List<QueryColumnSelector> selectors = new ArrayList<>();
	private List<Object> values = new ArrayList<>();
	
	protected Map <String, Map<String, List>> relations = new Hashtable<String, Map<String, List>>();
	
	protected GenRowFilters explicitFilters = new GenRowFilters();
	protected GenRowFilters implicitFilters = new GenRowFilters();
	
	protected boolean overrideImplicit = false;

	
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
			property = PRIM_KEY_PLACEHOLDER; 
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
	
	//////////////////////////////////////////// FILTERING /////////////////////////////////////////////////
	/*
	 * Explicit filters alter entire query
	 * Implicit filters alter visualizations
	 *  
	 */

	
	public void addExplicitFilter(IQueryFilter newFilter) {
		GenRowFilters newGrf = new GenRowFilters();
		newGrf.addFilters(newFilter);
		this.explicitFilters.merge(newGrf);
	}
	
	public GenRowFilters getExplicitFilters() {
		return this.explicitFilters;
	}
	
	public void setExplicitFilters(GenRowFilters filters) {
		this.explicitFilters = filters;
	}
	
	public void addImplicitFilter(IQueryFilter newFilter) {
		GenRowFilters newGrf = new GenRowFilters();
		newGrf.addFilters(newFilter);
		this.implicitFilters.merge(newGrf);
	}
	
	public GenRowFilters getImplicitFilters() {
		return this.implicitFilters;
	}
	
	public void setImplicitFilters(GenRowFilters filters) {
		this.implicitFilters = filters;
	}
	
	public GenRowFilters getCombinedFilters() {
		GenRowFilters combinedFilters = new GenRowFilters();
		combinedFilters.merge(this.explicitFilters.copy());
		if(this.overrideImplicit) {
			// if the user already filtered the column
			// do not try to merge the other filters
			// that are held on the data source
			Set<String> explicitFilteredColumn = combinedFilters.getAllFilteredColumns();
			int numFilters = this.implicitFilters.size();
			List<IQueryFilter> implicitFilterVec = this.implicitFilters.getFilters();
			for(int i = 0; i < numFilters; i++) {
				IQueryFilter f = implicitFilterVec.get(i);
				if(!containsAny(f.getAllUsedColumns(), explicitFilteredColumn)) {
					// user didn't choose a different filter value for the column
					// so we can add it
					combinedFilters.addFilters(f);
				}
			}
		} else {
			combinedFilters.merge(this.implicitFilters.copy());
		}
		return combinedFilters;
	}
	
	private boolean containsAny(Set<String> valuesToFind, Set<String> allValues) {
		for(String valToFind : valuesToFind) {
			if(allValues.contains(valToFind)) {
				return true;
			}
		}
			
		return false;
	}

	//////////////////////////////////////////// JOINING ////////////////////////////////////////////////////
	public void setRelations(Map<String, Map<String, List>> relations) {
		this.relations = relations;
	}
	
	public void addRelation(String fromConcept, String toConcept, String joinType)
	{
		// I need pick the keys from the table based on relationship and then add that to the relation
		// need to figure out type of 
		// find if this property is there
		// ok if the logical name stops being unique this will have some weird results
		
		
		Map <String, List> compHash = new Hashtable<String, List>();
		if(relations.containsKey(fromConcept))
			compHash = relations.get(fromConcept);
		
		List curData = new Vector();
		// next piece is to see if we have the comparator
		if(compHash.containsKey(joinType))
			curData = compHash.get(joinType);
		
		curData.add(toConcept);
		
		// put it back
		compHash.put(joinType, curData);	
		
		// put it back
		relations.put(fromConcept, compHash);
	}
	
	public Map<String, Map<String, List>> getRelations(){
		return this.relations;
	}
	
	//////////////////////////////////////////// OTHERS /////////////////////////////////////////////////////
	public void setOverrideImplicit(boolean overrideImplicit) {
		this.overrideImplicit = overrideImplicit;
	}
	
	public boolean isOverrideImplicit() {
		return this.overrideImplicit;
	}
	//////////////////////////////////////////// SETTERS AND GETTERS ////////////////////////////////////////
	//////////////////////////////////////////// TASK META INFO /////////////////////////////////////////////

	
}
