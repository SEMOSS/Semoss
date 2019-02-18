package prerna.engine.impl.rdf;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.google.gson.internal.StringMap;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.engine.api.IEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc.AbstractReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractApiReactor extends AbstractReactor{

	/*
	 * This class is the abstract api reactor used for querying a database or querying a csv file.
	 * Both allow the user to specify selectors, filters, and table joins to merge into existing frames
	 * when used inside a data.import pkql command
	 * 
	 * This class will set the main variables used within the specific reactor in order to 
	 * create the appropriate query.
	 */
	
	// these are the params that are parsed from the abstract that is used by the specific instances
	// of abstract api reactor
	protected String engineName = null;
	protected SelectQueryStruct qs = null;
	protected Map<Object, Object> mapOptions = null;
	protected boolean useCheater = false;
	
	// key to determine if api wants to add filters based on existing values within the frame
	protected static final String ADD_TABLE_FITLERS = "addTableFilters";
	
	private StringMap<StringMap<ArrayList>> rowLevelFilters = new StringMap<StringMap<ArrayList>>();
	
	/**
	 * The abstract reactor reacts to the following:
	 * COL_CSV -> this contains the list of selectors
	 * FILTER -> this contains the filters being applied to limit the data returned from the iterator
	 * JOINS -> this contains the join information to properly create the iterator
	 * KEY_VALUE -> this contains a map for additional information required. this is currently only used
	 * for the CSVApi which will contain the file key pointing to the location and the data types to load each
	 * column of the CSV
	 * TABLE_JOINS -> this contains the information as to how this table will be joined into the existing frame
	 * if present
	 */
	public AbstractApiReactor() {
		String [] thisReacts = {PKQLEnum.COL_CSV, PKQLEnum.FILTER, PKQLEnum.MAP_OBJ, PKQLEnum.JOINS, PKQLEnum.TABLE_JOINS};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.API;
	}

	@Override
	public Iterator process() {
		// grab the engine from the my store
		this.engineName = (String)myStore.get("ENGINE");
		
		// get additional options
		this.mapOptions = (Map<Object, Object>) myStore.get(PKQLEnum.MAP_OBJ);

		// grab the appropriate information to create the proper query struct
		// the query struct is what will determine the query that is used during creation
		Vector <String> selectors = new Vector<String>();
		Vector <Hashtable> filters = new Vector<Hashtable>();
		Vector <Hashtable> joins = new Vector<Hashtable>();
		int limit = -1;
		int offset = -1;
		
		if(myStore.containsKey(PKQLEnum.COL_CSV) && ((Vector)myStore.get(PKQLEnum.COL_CSV)).size() > 0)
			selectors = (Vector<String>) myStore.get(PKQLEnum.COL_CSV);
		if(myStore.containsKey(PKQLEnum.FILTER) && ((Vector)myStore.get(PKQLEnum.FILTER)).size() > 0)
			filters = (Vector<Hashtable>) myStore.get(PKQLEnum.FILTER);
		if(myStore.containsKey(PKQLEnum.JOINS) && ((Vector)myStore.get(PKQLEnum.JOINS)).size() > 0)
			joins = (Vector<Hashtable>) myStore.get(PKQLEnum.JOINS);

		if(this.mapOptions != null) {
			if(this.mapOptions.containsKey("limit")) {
				limit = (Integer)this.mapOptions.get("limit");
			}
			
			if(this.mapOptions.containsKey("offset")) {
				offset = (Integer)this.mapOptions.get("offset");
			}
		}
		//for each inner join and left outer join we want to add filters to the query struct
		//that way only the pieces we need come from the database
		IDataMaker dm = (IDataMaker) myStore.get("G");
		if(dm != null && dm instanceof ITableDataFrame) {
			ITableDataFrame frame = (ITableDataFrame)dm;
			OwlTemporalEngineMeta meta = frame.getMetaData();
			/*
			 * We try to add implicit filters based on the values already existing within the frame
			 * In addition, you can push filtering based on values existing within the frame
			 * through the options map
			 * 
			 */

			// try to add filtering based on options map
			if(mapOptions != null && mapOptions.containsKey(ADD_TABLE_FITLERS)) {
				Map<String, String> filtersMapObj = (Map<String, String>) mapOptions.get(ADD_TABLE_FITLERS);
				for(String fromColumn : filtersMapObj.keySet()) {
					String toColumn = filtersMapObj.get(fromColumn);
					// ugh... since FE is annoying and passes this incorrectly sometimes
					if(frame.isEmpty()) {
						continue;
					}
					String[] headers = frame.getColumnHeaders();
					if(!ArrayUtilityMethods.arrayContainsValue(headers, fromColumn)) {
						continue;
					}
					addTableValuesAsFilter(frame, filters, fromColumn, toColumn);
				}
			}
			
			// try to add implicit filtering based on table joins
			Vector<Hashtable> tableJoins = (Vector<Hashtable>) myStore.get("TABLE_JOINS");
			if(tableJoins != null) {
				for(Hashtable join : tableJoins) {

					String fromColumn = (String)join.get(PKQLEnum.FROM_COL); //what is in my table
					String toColumn = (String)join.get(PKQLEnum.TO_COL); //what is coming to my table
					String joinType = (String)join.get(PKQLEnum.REL_TYPE);
					if(joinType.equalsIgnoreCase("inner.join") || joinType.equalsIgnoreCase("left.outer.join")) {
						//we want to add filters to the column that already exists in the table
						if(fromColumn != null && toColumn != null) {
							//see if this filter already exists
							boolean addFilter = true;
							for(Hashtable filter : filters) {
								if(((String)filter.get("FROM_COL")).equals(toColumn)) {
									Vector values = (Vector) filter.get("TO_DATA");
									if(values != null && values.size() > 0) {
										addFilter = false;
										break;
									}
									break;
								}
							}
							
							//if not contained create a new table and add to filters
							if(addFilter) {
								// need to get the type of the column in the new database
								// if we have different types, we cannot add the implicit filter

//								DATA_TYPES dataType = frame.getDataType(fromColumn);
								
								SemossDataType dataType = SemossDataType.convertStringToDataType(meta.getHeaderTypeAsString(fromColumn, null));
								// TODO: need to expose this for other things aside from engine
								IEngine engine = Utility.getEngine(this.engineName);
								if(engine != null) {
									// we need to make sure we add this correclty
									// is this a concept, or a property
									String eType = null;
									if(selectors.contains(toColumn)) {
										// we have a concept
										String physicalUri = engine.getConceptPhysicalUriFromConceptualUri(toColumn);
										eType = engine.getDataTypes(physicalUri);
									} else {
										// we have a property
										// find the correct conceptual info
										for(String selector : selectors) {
											if(selector.contains("__")) {
												String[] selectorComp = selector.split("__");
												if(selectorComp[1].equals(toColumn)) {
													// add the filter as the parent__child
													// so the interpreter adds it correctly
													toColumn = selector;
													String physicalUri = engine.getConceptPhysicalUriFromConceptualUri("http://semoss.org/ontologies/Relation/Contains/" + selectorComp[1] + "/" + selectorComp[0]);
													eType = engine.getDataTypes(physicalUri);
													break;
												}
											}
										}
									}
									
									
									// for really old db's, they do not have conceptual names
									// so the above logic is not valid....
									// do not want this to break for old dbs though
									if(eType != null) {
										if(eType.contains("TYPE:")) {
											eType = eType.replace("TYPE:", "");
										}
										
										SemossDataType eDataType = SemossDataType.convertStringToDataType(eType);
										if(eDataType != dataType) {
											// this will loop to the next join
											continue;
										}
									}
								}
								
								addTableValuesAsFilter(frame, filters, fromColumn, toColumn);
							}
						}
					}
				}
			} 
		}
		// really bad bifurcation here :(
		// this is entered when the data maker is a legacy GDM
		// this is only used for QueryAPI
		else {
			this.useCheater  = true;
		}

		this.qs = new SelectQueryStruct();
		this.qs.setEngineId(this.engineName);
		this.put(PKQLEnum.QUERY_STRUCT, this.qs);
		processQueryStruct(selectors, filters, joins, limit, offset);
		
		Map<String, Set<String>> edgeHash = this.qs.getReturnConnectionsHash();
		// we store the edge hash in myStore
		this.put("EDGE_HASH", edgeHash);
		
		return null;
	}
	
	public void addTableValuesAsFilter(ITableDataFrame frame, Vector <Hashtable> filters, String fromColumn, String toColumn) {
//		for(Hashtable filter : filters) {
//			if(filter.get(PKQLEnum.FROM_COL).equals(toColumn) && filter.get(PKQLEnum.COMPARATOR).equals("=")) {
//				return; //we don't want to add filters if they already exist in the query struct
//			}
//		}
//
//		//figure out which is the new column and which already exists in the table
//		Iterator<Object> rowIt = frame.uniqueValueIterator(fromColumn, false);
//		List<Object> filterInstances = new Vector<Object>();
//
//		//collect all the filter values
//		while(rowIt.hasNext()){
//			Object val = rowIt.next();
//			if(val != null) {
//				filterInstances.add(val);
//			}
//		}
//
//		if(filterInstances.size() == 0) {
//			// well, you want to join on something that doesn't even exist on the frame
//			// no point in trying to execute this...
//			throw new IllegalArgumentException("Cannot perform this operation since no values exist for column join variable.");
//		}
//
//		Hashtable joinfilter = new Hashtable();
//		joinfilter.put(PKQLEnum.FROM_COL, toColumn);
//		joinfilter.put("TO_DATA", filterInstances);
//		joinfilter.put(PKQLEnum.COMPARATOR, "=");
//		filters.add(joinfilter);
	}

	public void processQueryStruct(Vector <String> selectors, Vector <Hashtable> filters, Vector <Hashtable> joins, int limit, int offset)
	{
		Map<String, Map<String, Object>> varMap = (Map<String, Map<String, Object>>) myStore.get("VARMAP");
		for(int selectIndex = 0;selectIndex < selectors.size();selectIndex++)
		{
			String thisSelector = selectors.get(selectIndex);
			if(thisSelector.contains("__")){
				String concept = thisSelector.substring(0, thisSelector.indexOf("__"));
				String property = thisSelector.substring(thisSelector.indexOf("__")+2);
				this.qs.addSelector(concept, property);
			}
			else
			{
				this.qs.addSelector(thisSelector, null);
			}
			
			// For this column, see if there is a param set that references it
			// If so, grab it's value and add as a filter to apply that param
			for(String var : varMap.keySet()) {
				Map<String, Object> paramValues = varMap.get(var);
				if(paramValues != null && paramValues.get(Constants.TYPE).equals(thisSelector)) {
					Vector<String> filterValues = new Vector<String>();
					filterValues.add(paramValues.get(Constants.VALUE).toString());
					this.qs.addImplicitFilter(new SimpleQueryFilter(new NounMetadata(thisSelector, PixelDataType.COLUMN),
							"==", new NounMetadata(filterValues, PixelDataType.CONST_STRING)));
				}
			}
		}
		
		if(Boolean.parseBoolean(DIHelper.getInstance().getLocalProp(Constants.SECURITY_ENABLED).toString())) {
			// Look at the selectors to figure out which tables and columns we are grabbing
			// Get the row-level permissions filters, match them up, and insert/remove as necessary
			IDataMaker datamaker = (IDataMaker) myStore.get("G");
			String userId = datamaker.getUserId();
//			UserPermissionsMasterDB permissions = new UserPermissionsMasterDB();
//			rowLevelFilters = permissions.getRowLevelSeedsForUserAndEngine(userId, this.engineName);
			Set<String> addedConcepts = new HashSet<String>();
			
			for(String s : selectors) {
				String table = "";
				String col = "";
				if(s.contains("__")) {
					table = s.split("__")[0];
					col = s.split("__")[1];
				} else {
					table = col = s;
				}
				addedConcepts.add(table);
				
				//For each selector, see if it exists in the RLS and if it's not in the filter list, add the RLS values as a filter
				if(rowLevelFilters.containsKey(table)) {
					StringMap<ArrayList> tableFilters = (StringMap<ArrayList>)rowLevelFilters.get(table);
					if(tableFilters.containsKey(col)) {
						boolean filterExists = false;
						for(int filterIndex = 0;filterIndex < filters.size();filterIndex++)
						{
							Hashtable thisFilter = (Hashtable)filters.get(filterIndex);
							if(thisFilter.get("FROM_COL").toString().equals(s)) {
								filterExists = true;
								break;
							}
						}
						if(!filterExists) {
							ArrayList rlsValues = ((StringMap<ArrayList>)rowLevelFilters.get(table)).get(col);
//							this.qs.addFilter(s, "=", rlsValues);
							this.qs.addImplicitFilter(new SimpleQueryFilter(new NounMetadata(s, PixelDataType.COLUMN),
									"==", new NounMetadata(rlsValues, PixelDataType.CONST_STRING)));
						}
					}
				}
			}
			
			//For each column in the RLS filters, check to see if the concept/table is being selected
			//If so, add any property filters to trim down the concept list as a result
			for(String table : rowLevelFilters.keySet()) {
				for(String col : rowLevelFilters.get(table).keySet()) {
					if(!selectors.contains(table + "__" + col)) {
						ArrayList rlsValues = ((StringMap<ArrayList>)rowLevelFilters.get(table)).get(col);
//						this.qs.addFilter(table + "__" + col, "=", rlsValues);
						this.qs.addImplicitFilter(new SimpleQueryFilter(new NounMetadata(table + "__" + col, PixelDataType.COLUMN),
								"==", new NounMetadata(rlsValues, PixelDataType.CONST_STRING)));
					}
				}
			}
		}
		
		for(int filterIndex = 0;filterIndex < filters.size();filterIndex++)
		{
			Hashtable thisFilter = (Hashtable)filters.get(filterIndex);
			String fromCol = (String)thisFilter.get("FROM_COL");
			String toCol = null;
			Vector filterData = new Vector();
			if(thisFilter.containsKey("TO_COL"))
			{
				toCol = (String)thisFilter.get("TO_COL");
				//filtersToBeElaborated.add(thisFilter);
				//tinkerSelectors.add(toCol);
				// need to pull this from tinker frame and do the due
				// interestingly this could be join
			}
			else
			{
				// this is a vector do some processing here					
				filterData = (Vector)thisFilter.get("TO_DATA");
				String comparator = (String)thisFilter.get("COMPARATOR");
				//				String concept = fromCol.substring(0, fromCol.indexOf("__"));
				//				String property = fromCol.substring(fromCol.indexOf("__")+2);
				
				if(Boolean.parseBoolean(DIHelper.getInstance().getLocalProp(Constants.SECURITY_ENABLED).toString())) {
					// Check filter values against row level security values, if any
					String table = "";
					String col = "";
					if(fromCol.contains("__")) {
						table = fromCol.split("__")[0];
						col = fromCol.split("__")[1];
					} else {
						table = col = fromCol;
					}
					
					if(rowLevelFilters.containsKey(table) && ((StringMap<ArrayList>)rowLevelFilters.get(table)).containsKey(col)) {
						ArrayList rlsValues = ((StringMap<ArrayList>)rowLevelFilters.get(table)).get(col);
						
						if(comparator.equals("=")) {
							for(Iterator it = filterData.iterator(); it.hasNext(); ) {
								if(!rlsValues.contains(it.next())) {
									it.remove();
								}
							}
						} else {
							//TODO: Build out applying RLS for other comparators
						}
					}
				}
				
				// For this column filter, see if there is a param set that references it
				// If so, grab it's value and add as a filter to apply that param
				for(String var : varMap.keySet()) {
					Map<String, Object> paramValues = varMap.get(var);
					if(paramValues != null && paramValues.get(Constants.TYPE).equals(fromCol)) {
						filterData.clear();
						filterData.add(paramValues.get(Constants.VALUE).toString());
					}
				}
				
//				this.qs.addFilter(fromCol, comparator, filterData);
				this.qs.addImplicitFilter(new SimpleQueryFilter(new NounMetadata(fromCol, PixelDataType.COLUMN),
						comparator, new NounMetadata(filterData, PixelDataType.CONST_STRING)));
			}
		}
		for(int joinIndex = 0;joinIndex < joins.size();joinIndex++)
		{
			Hashtable thisJoin = (Hashtable)joins.get(joinIndex);

			String fromCol = (String)thisJoin.get("FROM_COL");
			String toCol = (String)thisJoin.get("TO_COL");

			String relation = (String)thisJoin.get("REL_TYPE");	
			this.qs.addRelation(fromCol, toCol, relation);
		}
		
		this.qs.setLimit(limit);
		this.qs.setOffSet(offset);
	}
	
	
//	/**
//	 * Add a components and its value into the current reactor
//	 * @param componenet			The type of component -> COL_DEF, COL_CSV, etc.
//	 * @param value					The value of the component -> Title, [Title, Studio, Genre]
//	 */
//	public void addComponentValue(String componenet, Object value) {
//		int counter = 1;
//		String valueToStore = componenet;
//		while(componentToValue.containsKey(valueToStore)) {
//			valueToStore = componenet + "_" + counter;
//			counter++;
//		}
//		componentToValue.put(valueToStore, value);
//	}
//	
//	/**
//	 * This reactor does not need to perform any expression replacements as its inputs are well defined
//	 */
//	public void addExpressionToValue(String componenet, Object value) {
////		expressionStrToValue.put(componenet, value);
//	}
	
	
}
