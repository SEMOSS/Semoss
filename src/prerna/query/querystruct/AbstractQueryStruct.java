package prerna.query.querystruct;

import java.util.HashMap;
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
	
	public enum QUERY_STRUCT_TYPE {ENGINE, FRAME, CSV_FILE, EXCEL_FILE, RAW_ENGINE_QUERY, RAW_JDBC_ENGINE_QUERY, RAW_RDF_FILE_ENGINE_QUERY, RAW_FRAME_QUERY, LAMBDA};
	
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
	
	// custom from
	protected String customFrom = null;
	protected String customFromAliasName = null;
	
	// For filters
	// filters on existing data
	protected GenRowFilters explicitFilters = new GenRowFilters();
	protected GenRowFilters implicitFilters = new GenRowFilters();
	// filters on derived calculations
	protected GenRowFilters havingFilters = new GenRowFilters();
	
	// For joins
	// we keep a set of start, comparator, end
	protected Set<String[]> relationsSet = new RelationSet();
	
	protected boolean overrideImplicit = false;

	// Datasource
	protected transient ITableDataFrame frame;
	protected String frameName;
	protected transient IEngine engine;
	protected String engineId;
	protected Boolean bigDataEngine = false;

	// map of pragmas
	protected transient Map pragmap = new HashMap();
	
	
	/////////////////////////////////////// experimental ////////////////////////////////////////
	
	// may be a better idea to keep the hash here
	// and make it look
	
	// do the selectors to be fyu
	// so this is basically the in memory version of the properties table
	public Map <String, SelectQueryStruct> aliasHash = new HashMap<String, SelectQueryStruct>();
	
	public Map randomHash = new HashMap();
	
	// add the joins to this query struct
	public List<GenExpression> joins = new Vector<GenExpression>();
	
	public List<GenExpression> nselectors = new Vector<GenExpression>();
	
	// current table
	public String currentTable = null;
	
	public GenExpression from = null;
	
	
	// sets the body
	public GenExpression body = null;
	
	public GenExpression filter = null;
	
	public String operation = null;
	
	public List<GenExpression> ngroupBy = new Vector<GenExpression>();

	public List<GenExpression> norderBy = new Vector<GenExpression>();
	
	// parent struct
	public GenExpression parentStruct = null;
	
	////////////////////////////////////// experimental /////////////////////////////////////////
	
	// this is the actual query
	//public String aQuery = null;
	
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
	
	//////////////////////////////////////////// FROM  /////////////////////////////////////////////////

	public void setCustomFrom(String customFrom) {
		this.customFrom = customFrom;
	}
	
	public String getCustomFrom() {
		return this.customFrom;
	}
	
	public void setCustomFromAliasName(String customFromAliasName) {
		this.customFromAliasName = customFromAliasName;
	}
	
	public String getCustomFromAliasName() {
		return this.customFromAliasName;
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
			// we want to append these filters
			// we do not want them merged into one
			// so it is possible that the results result in no values
			combinedFilters.merge(this.implicitFilters.copy(), true);
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

	public void setRelations(Set<String[]> relationSet) {
		this.relationsSet = relationSet;
	}
	
	public void addRelation(String fromConcept, String toConcept, String joinType) {
		String[] eachSet = new String[]{fromConcept, joinType, toConcept};
		relationsSet.add(eachSet);
	}
	
	public Set<String[]> getRelations(){
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
		if(frame != null) {
			this.frame = frame;
			this.frameName = frame.getName();
		}
	}
	
	public void setFrameName(String frameName) {
		this.frameName = frameName;
	}
	
	public String getFrameName() {
		return this.frameName;
	}
	
	public void setOverrideImplicit(boolean overrideImplicit) {
		this.overrideImplicit = overrideImplicit;
	}
	
	public boolean isOverrideImplicit() {
		return this.overrideImplicit;
	}
	
	public void setBigDataEngine(boolean bigDataEngine) {
		this.bigDataEngine = bigDataEngine;
	}
	
	public boolean getBigDataEngine() {
		return this.bigDataEngine;
	}
	
	public void setQsType(QUERY_STRUCT_TYPE qsType) {
		this.qsType = qsType;
	}
	
	public QUERY_STRUCT_TYPE getQsType() {
		return this.qsType;
	}
	
	/**
	 * Get a map containing the source information
	 * @return
	 */
	public Map<String, String> getSourceMap() {
		Map<String, String> sourceMap = new HashMap<String, String>();
		sourceMap.put("type", this.qsType.toString());
		if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE ||
				qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY ||
				qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY) {
			
			sourceMap.put("name", getEngineId());
			
		} else if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.FRAME ||
				qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			
			sourceMap.put("name", getFrameName());
		}
		return sourceMap;
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
		mergeRelations(incomingQS.relationsSet);
		
		// setters
		setBigDataEngine(incomingQS.bigDataEngine);
		setCustomFrom(incomingQS.customFrom);
		setCustomFromAliasName(incomingQS.customFromAliasName);
		// setters but null check first
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
	 * @param incomingBigDataEngine
	 */
	private void mergeBigEngine(Boolean bigDataEngine) {
		//merge the filters
		this.bigDataEngine = bigDataEngine;		
	}
	
	
	public void mergeRelations(Set<String[]> relationSet) {
		this.relationsSet.addAll(relationSet);
	}
	
	// sets the pragma map to be used
	public void setPragmap(Map pragmap)
	{
		this.pragmap = pragmap;
	}
	
	// gets the pragma map
	public Map getPragmap()
	{
		return this.pragmap;
	}
	
	public void clearPragmap()
	{
		this.pragmap.clear();
	}
	
	
	/////////////////////////////////////// experimental ////////////////////////////////////////
	
	public void removeSelect(String column)
	{
		this.nselectors = removeExpression(column, nselectors);
		//this.ngroupBy = removeExpression(column, ngroupBy);
	}

	public void removeGroup(String column)
	{
		//this.nselectors = removeExpression(column, nselectors);
		this.ngroupBy = removeExpression(column, ngroupBy);
	}
	
	public void parameterizeColumn(String column, GenExpression userFilter)
	{
		// need to see if the filter is already there
		// if so.. add to that value.. 
		// if not add a new filter
		

		// need a way to find if this column even exists
		
		System.out.println("Filter is set to  " + filter);
		if(filter != null)
		{
			GenExpression thisFilter = new GenExpression();
			thisFilter.setOperation(" AND ");
			((GenExpression)filter).paranthesis = true;
			thisFilter.setLeftExpresion(filter);
			// forcing a random opaque one
			userFilter.paranthesis = true;
			thisFilter.setRightExpresion(userFilter);
			// replace with the new filter
			this.filter = thisFilter;
		}
		// add a new filter otherwise
		else
		{
			this.filter = userFilter;						
		}

	}

	private boolean doesColumnExist(String column)
	{
		boolean retValue = true;
		// this only checks the selectors
		
		return retValue;
	}
	
	private List <GenExpression> removeExpression(String column, List <GenExpression> input)
	{
		// remove first from selectors
		List <GenExpression> output = new Vector<GenExpression>();
		for(int selectIndex = 0;selectIndex < input.size();selectIndex++)
		{
			// compare the left expression
			GenExpression thisSelector = input.get(selectIndex);
			if(!(thisSelector.getLeftExpr() != null && thisSelector.getLeftExpr().equalsIgnoreCase(column)) && !(thisSelector.getLeftAlias() != null && thisSelector.getLeftAlias().equalsIgnoreCase(column)))
				output.add(thisSelector);
		}
		//nselectors = output;
		return output;
		
		// next from parameters i.e. where if we have it
		// this can be recursive
		
		// next from order by if you have it
		
		// next from groupby if we have it		
	}
	
	
	
	
}
