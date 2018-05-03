package prerna.query.querystruct;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.util.Utility;

public abstract class AbstractQueryStruct {
	
	public final static String PRIM_KEY_PLACEHOLDER = "PRIM_KEY_PLACEHOLDER";
	
	// Creating shared objects between SelectQueryStruct and UpdateQueryStruct
	
	// For filters
	// filters on existing data
	protected GenRowFilters explicitFilters = new GenRowFilters();
	protected GenRowFilters implicitFilters = new GenRowFilters();
	// filters on derived calculations
	protected GenRowFilters havingFilters = new GenRowFilters();
	
	// For joins
	//Hashtable <String, Hashtable<String, Vector>> orfilters = new Hashtable<String, Hashtable<String, Vector>>();
	// relations are of the form
	// item = <relation vector>
	// concept = type of join toCol
	// Movie	 InnerJoin Studio, Genre
	//			 OuterJoin Nominated
	protected Map <String, Map<String, List>> relations = new Hashtable<String, Map<String, List>>();
	
	protected boolean overrideImplicit = false;

	// Datasource
	protected transient ITableDataFrame frame;
	protected transient IEngine engine;
	protected String engineName;
	
	//////////////////////////////////////////// FILTERING /////////////////////////////////////////////////

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
	
	public void addHavingFilter(IQueryFilter newFilter) {
		GenRowFilters newGrf = new GenRowFilters();
		newGrf.addFilters(newFilter);
		this.havingFilters.merge(newGrf);
	}
	
	public GenRowFilters getHavingFilters() {
		return this.havingFilters;
	}
	
	public void setHavingFilters(GenRowFilters filters) {
		this.havingFilters = filters;
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
	
	public void addRelation(String fromConcept, String toConcept, String joinType) {
		// I need pick the keys from the table based on relationship and then add that to the relation
		// need to figure out type of 
		// find if this property is there
		// ok if the logical name stops being unique this will have some weird results
		
		
		Map <String, List> compHash = new Hashtable<String, List>();
		if(relations.containsKey(fromConcept))
			compHash = relations.get(fromConcept);
		
		List curData = new Vector();
		// next piece is to see if we have the comparator
		if(compHash.containsKey(joinType)) {
			curData = compHash.get(joinType);
		}
		
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

	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}
	
	public String getEngineName() {
		if(this.engineName == null && this.engine != null) {
			this.engineName = this.engine.getEngineName();
		}
		return this.engineName;
	}
	
	public void setEngine(IEngine engine) {
		this.engine = engine;
	}
	
	public IEngine getEngine() {
		return this.engine;
	}
	
	public IEngine retrieveQueryStructEngine() {
		if(this.engine == null) {
			this.engine = Utility.getEngine(this.engineName);
		}
		return this.engine;
	}
	
	public ITableDataFrame getFrame() {
		return frame;
	}

	public void setFrame(ITableDataFrame frame) {
		this.frame = frame;
	}
	
	public void setOverrideImplicit(boolean overrideImplicit) {
		this.overrideImplicit = overrideImplicit;
	}
	
	public boolean isOverrideImplicit() {
		return this.overrideImplicit;
	}
}
