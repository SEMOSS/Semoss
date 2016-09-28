package prerna.engine.impl.rdf;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.sablecc.AbstractReactor;
import prerna.sablecc.PKQLEnum;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Constants;

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
	protected String engine = null;
	protected QueryStruct qs = null;
	protected Map<Object, Object> mapOptions = null;
	protected boolean useCheater = false;
	
	
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
		String [] thisReacts = {PKQLEnum.COL_CSV, PKQLEnum.FILTER, PKQLEnum.MAP_OBJ, PKQLEnum.JOINS, "KEY_VALUE", PKQLEnum.TABLE_JOINS};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.API;
	}

	@Override
	public Iterator process() {
		// grab the engine from the my store
		this.engine = (String)myStore.get("ENGINE");
		
		// we have an engine name of "csvFile" when loading a csv file
//		if(engine.equals("csvFile")) {
			// we are loading a csv file
			// grab the input maps from the user
			this.mapOptions = (Map<Object, Object>) myStore.get(PKQLEnum.MAP_OBJ);
//		}

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
		if(dm instanceof ITableDataFrame) {
			ITableDataFrame frame = (ITableDataFrame)dm;
			Vector<Hashtable> tableJoins = (Vector<Hashtable>) myStore.get("TABLE_JOINS");
			if(tableJoins != null) {
				for(Hashtable join : tableJoins) {

					String fromColumn = (String)join.get(PKQLEnum.FROM_COL); //what is in my table
					String toColumn = (String)join.get(PKQLEnum.TO_COL); //what is coming to my table
					String joinType = (String)join.get(PKQLEnum.REL_TYPE);
					if(joinType.equalsIgnoreCase("inner.join") || joinType.equalsIgnoreCase("left.outer.join")) {
						//figure out which is the new column and which already exists in the table
						Iterator<Object> rowIt = null;

						//we want to add filters to the column that already exists in the table
						if(fromColumn != null && toColumn != null) {
							rowIt = frame.uniqueValueIterator(fromColumn, false);
							List<Object> uris = new Vector<Object>();

							//collect all the filter values
							while(rowIt.hasNext()){
								uris.add(rowIt.next());
							}

							//see if this filter already exists
							boolean addFilter = true;
							for(Hashtable filter : filters) {
								if(((String)filter.get("FROM_COL")).equals(toColumn)) {
									Vector values = (Vector) filter.get("TO_DATA");
									if(values != null && values.size() > 0) {
										//									values.addAll(uris);
										addFilter = false;
										break;
									}
									break;
								}
							}

							//if not contained create a new table and add to filters
							if(addFilter) {
								Hashtable joinfilter = new Hashtable();
								joinfilter.put(PKQLEnum.FROM_COL, toColumn);
								joinfilter.put("TO_DATA", uris);
								joinfilter.put(PKQLEnum.COMPARATOR, "=");
								filters.add(joinfilter);
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

		this.qs = new QueryStruct();
		processQueryStruct(selectors, filters, joins, limit, offset);
		
		Map<String, Set<String>> edgeHash = this.qs.getReturnConnectionsHash();
		// we store the edge hash in myStore
		this.put("EDGE_HASH", edgeHash);
		
		return null;
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
					filterValues.add(paramValues.get(Constants.NAME).toString());
					this.qs.addFilter(thisSelector, "=", filterValues);
				}
			}
		}
		for(int filterIndex = 0;filterIndex < filters.size();filterIndex++)
		{
			Object thisObject = filters.get(filterIndex);
			System.out.println(thisObject.getClass());
			if(thisObject instanceof Hashtable)
				System.out.println("I just dont what is wrong.. ");

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
				
				// For this column filter, see if there is a param set that references it
				// If so, grab it's value and add as a filter to apply that param
				for(String var : varMap.keySet()) {
					Map<String, Object> paramValues = varMap.get(var);
					if(paramValues != null && paramValues.get(Constants.TYPE).equals(fromCol)) {
						filterData.clear();
						filterData.add(paramValues.get(Constants.NAME).toString());
					}
				}
				
				this.qs.addFilter(fromCol, comparator, filterData);
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
