package prerna.query.querystruct;

import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.Utility;

public abstract class AbstractQueryStruct {
	
	// Creating shared objects between SelectQueryStruct and UpdateQueryStruct
	
	public final static String PRIM_KEY_PLACEHOLDER = "PRIM_KEY_PLACEHOLDER";
	
	public enum QUERY_STRUCT_TYPE {ENGINE, FRAME, CSV_FILE, EXCEL_FILE, RAW_ENGINE_QUERY, RAW_FRAME_QUERY, LAMBDA};
	
	// qs type
	public QUERY_STRUCT_TYPE qsType = QUERY_STRUCT_TYPE.FRAME;
	
	/*
	 * 3 main parts to a query
	 * 
	 * selectors:
	 * 		table name (equivalent to the name of a vertex for graphs)
	 * 		column name (equivalent to the property name on a vertex for graphs)
	 * 		alias
	 * 		single math operation
	 * 
	 * filters:
	 * 		column to column filtering
	 * 		column to values filtering
	 * 		values to column filtering
	 * 
	 * relationships:
	 * 		start column -> joinType -> end column
	 * 
	 */
	
	// For selectors
	protected List<IQuerySelector> selectors = new Vector<IQuerySelector>();
	
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
	protected Map<String, Map<String, List>> relations = new Hashtable<String, Map<String, List>>();
	protected Set<String[]> relationsSet = new LinkedHashSet<String[]>();
	
	protected boolean overrideImplicit = false;

	// Datasource
	protected transient ITableDataFrame frame;
	protected transient IEngine engine;
	protected String engineId;
	
	
	//////////////////////////////////////////// SELECTORS /////////////////////////////////////////////////
	
	public void setSelectors(List<IQuerySelector> selectors) {
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
	
	public void addSelector(IQuerySelector selector) {
		this.selectors.add(selector);
	}
	
	public List<IQuerySelector> getSelectors(){
		return this.selectors;
	}
	
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
	
	public void addRelationToSet(String fromConcept, String toConcept, String joinType) {
		String[] eachSet = new String[]{fromConcept, joinType, toConcept};
		relationsSet.add(eachSet);
	}
	
	
	public Set<String[]> getRelationsSet(){
		return this.relationsSet;
	}
	
	
	//////////////////////////////////////////// OTHERS /////////////////////////////////////////////////////

	public void setEngineId(String engineId) {
		this.engineId = engineId;
	}
	
	public String getEngineId() {
		if(this.engineId == null && this.engine != null) {
			this.engineId = this.engine.getEngineId();
		}
		return this.engineId;
	}
	
	public void setEngine(IEngine engine) {
		this.engine = engine;
	}
	
	public IEngine getEngine() {
		return this.engine;
	}
	
	public IEngine retrieveQueryStructEngine() {
		if(this.engine == null && this.engineId != null) {
			this.engine = Utility.getEngine(this.engineId);
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
	
	public void setQsType(QUERY_STRUCT_TYPE qsType) {
		this.qsType = qsType;
	}
	
	public QUERY_STRUCT_TYPE getQsType() {
		return this.qsType;
	}
	
	//////////////////////////////////////////// MERGING /////////////////////////////////////////////////////
	
	/**
	 * 
	 * @param incomingQS
	 * 
	 * This method is responsible for merging "incomingQS's" data with THIS querystruct
	 */
	public void merge(AbstractQueryStruct incomingQS) {
		mergeSelectors(incomingQS.selectors);
		mergeExplicitFilters(incomingQS.explicitFilters);
		mergeImplicitFilters(incomingQS.implicitFilters);
		mergeHavingFilters(incomingQS.havingFilters);
		mergeRelations(incomingQS.relations);
		if(incomingQS.getEngineId() != null) {
			setEngineId(incomingQS.getEngineId());
		}
		
		if(incomingQS.getEngine() != null) {
			setEngine(incomingQS.getEngine());
		}
		if(incomingQS.getFrame() != null) {
			setFrame(incomingQS.getFrame());
		}
	}
	
	public void mergeSelectors(List<IQuerySelector> incomingSelectors) {
		//add selectors only if we don't aleady have them as selectors
		for(IQuerySelector incomingSelector : incomingSelectors) {
			if(!this.selectors.contains(incomingSelector)) {
				this.selectors.add(incomingSelector);
			}
		}
	}
	
	/**
	 * This is meant for filters defined by the user on the QS being sent
	 * @param incomingFilters
	 */
	public void mergeExplicitFilters(GenRowFilters incomingFilters) {
		//merge the filters
		this.explicitFilters.merge(incomingFilters);
	}
	
	/**
	 * This is meant for filters that are stored within the frame
	 * @param incomingFilters
	 */
	public void mergeImplicitFilters(GenRowFilters incomingFilters) {
		//merge the filters
		this.implicitFilters.merge(incomingFilters);
	}
	
	/**
	 * 
	 * @param incomingFilters
	 */
	private void mergeHavingFilters(GenRowFilters incomingFilters) {
		//merge the filters
		this.havingFilters.merge(incomingFilters);		
	}
	
	/**
	 * 
	 * @param incomingRelations
	 * merges incomingRelations with this relations
	 */
	public void mergeRelations(Map<String, Map<String, List>> incomingRelations) {
		
		//for each concept in the new relations
		for(String conceptName : incomingRelations.keySet()) {
			
			//Grab the relationships for that concept
			Map<String, List> incomingHash = incomingRelations.get(conceptName);
			
			//if we already have relationships for the same concept we need to merge
			if(this.relations.containsKey(conceptName)) {
				
				//grab this relations for concept
				Map<String, List> thisHash = this.relations.get(conceptName);
				
				//relationKey is inner.join, outer.join, etc.
				//so we want to merge relationships that have the same relationKey
				for(String relationKey : incomingHash.keySet()) {
					List v;
					if(thisHash.containsKey(relationKey)) {
						v = thisHash.get(relationKey);
					} else {
						v = new Vector();
					}
					
					//merge the this vector with new data and add to thisHash
					List mergeList = incomingHash.get(relationKey);
					for(Object newRel : mergeList) {
						if(!v.contains(newRel)) {
							v.add(newRel);
						}
					}
					thisHash.put(relationKey, v);
				}
			} else {
				
				//we don't have the relationship described in the incoming relations so just copy them to this relations
				Map<String, List> newHash = new Hashtable<>();
				for(String relationKey : incomingHash.keySet()) {
					List v = new Vector();
					v.addAll(incomingHash.get(relationKey));
					newHash.put(relationKey, v);
				}
				this.relations.put(conceptName, newHash);
			}
		}
	}
}
