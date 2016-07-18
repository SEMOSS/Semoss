package prerna.sablecc;

import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.rosuda.REngine.Rserve.RserveException;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.ds.H2.H2Frame;
import prerna.engine.api.IApi;
import prerna.engine.api.WebApi;
import prerna.engine.api.RApi;
import prerna.engine.impl.rdf.CSVApi;
import prerna.engine.impl.rdf.QueryAPI;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Constants;

public class ApiReactor extends AbstractReactor {

	private IApi api = null;

	public ApiReactor()
	{
		String [] thisReacts = {PKQLEnum.COL_CSV, PKQLEnum.FILTER, PKQLEnum.JOINS, "KEY_VALUE", "TABLE_JOINS"};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.API;
	}


	@Override
	public Iterator process() {

		System.out.println("Processed.. " + myStore);
		// TODO Auto-generated method stub
		/*
		 * Come to you in a min
		 * 
		 */
		//System.out.println("Will pull the data on this one and then make the calls to add to other things");
		String nodeStr = (String)myStore.get(whoAmI);
		Vector <Hashtable> filtersToBeElaborated = new Vector<Hashtable>();
		Vector <String> tinkerSelectors = new Vector<String>();

		String engine = (String)myStore.get("ENGINE");
		api = new QueryAPI();

		// I need to instantiate the engine here
		// for now hard coding it

			//processing for import.io web data extract feature
			if(engine.equalsIgnoreCase("ImportIO")){
				if (myStore.get(PKQLEnum.G) instanceof H2Frame) {
					api = new WebApi();					
					String url;
					if(myStore.containsKey("KEY_VALUE") && ((Map) ((Vector) myStore.get("KEY_VALUE")).get(0)).containsKey("url")){
						url = (String) ((Map) ((Vector) myStore.get("KEY_VALUE")).get(0)).get("url");
						api.set("URL", url);
					}else{
						System.out.println("Invalid PKQL: Missing URL. Required Syntax: data.import(api:ImportIO.Query({'url':'enter_your_url_here'})); ");
						myStore.put("RESPONSE", "Error: Invalid PKQL: Missing URL. Required Syntax: data.import(api:ImportIO.Query({'url':'enter_your_url_here'}));");
						myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
						return null;
					}
					
					Iterator thisIterator = api.process();
					
					myStore.put(nodeStr, thisIterator);
					myStore.put("RESPONSE", "success");
					myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);
				}

				return null;

			}else if(engine.equalsIgnoreCase("AmazonProduct")){
				if (myStore.get(PKQLEnum.G) instanceof H2Frame) {
					api = new WebApi();	//create WebApi interface				
					String itemSearch, itemLookup;
					if(myStore.containsKey("KEY_VALUE") && ((Map) ((Vector) myStore.get("KEY_VALUE")).get(0)).containsKey("itemSearch")){
						itemSearch = (String) ((Map) ((Vector) myStore.get("KEY_VALUE")).get(0)).get("itemSearch");
						api.set("ITEM_SEARCH", itemSearch);
					}else if(myStore.containsKey("KEY_VALUE") && ((Map) ((Vector) myStore.get("KEY_VALUE")).get(0)).containsKey("itemLookup")){
						itemLookup = (String) ((Map) ((Vector) myStore.get("KEY_VALUE")).get(0)).get("itemLookup");
						api.set("ITEM_LOOKUP", itemLookup);
					}else{
						System.out.println("Invalid PKQL: Required Syntax: data.import(api:AmazonProduct.Query({'itemSearch':'enter_search_keywords_here'})); OR data.import(api:AmazonProduct.Query({'itemLookup':'enter_item_ASIN_here'}));");
						myStore.put("RESPONSE", "Error: Invalid PKQL: Required Syntax: data.import(api:AmazonProduct.Query({'itemSearch':'enter_search_keywords_here'})); OR data.import(api:AmazonProduct.Query({'itemLookup':'enter_item_ASIN_here'}));");
						myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
						return null;
					}
					
					Iterator thisIterator = api.process();
					
					myStore.put(nodeStr, thisIterator);
					myStore.put("RESPONSE", "success");
					myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);
				}

				return null;

			}else if(engine.equalsIgnoreCase("r")) {
				if (myStore.get(PKQLEnum.G) instanceof H2Frame) {
					api = new RApi();
					H2Frame frame = (H2Frame) myStore.get(PKQLEnum.G);
					try {
						api.set("TABLE_NAME", frame.getDatabaseMetaData().get("tableName"));
						api.set("R_RUNNER", frame.getRRunner());
					} catch (RserveException e) {
						myStore.put("RESPONSE", "Error: R server is down.");
						myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
						e.printStackTrace();
					} catch (SQLException e) {
						myStore.put("RESPONSE", "Error: Invalid database connection.");
						myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
						e.printStackTrace();
					}
				} else {
					myStore.put("RESPONSE", "Error: Dataframe must be in Grid format.");
					myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
					return null;
				} 
		} else if(engine.equals("csvFile")) {
			api = new CSVApi();
			if(myStore.containsKey("KEY_VALUE") && ((Map) ((Vector) myStore.get("KEY_VALUE")).get(0)).containsKey("file")){
				String fileLocation = (String) ((Map) ((Vector) myStore.get("KEY_VALUE")).get(0)).get("file");
				api.set("FILE", fileLocation);
			}
		} else {
			api.set("ENGINE", engine);
		}

		Vector <String> selectors = new Vector<String>();
		Vector <Hashtable> filters = new Vector<Hashtable>();
		Vector <Hashtable> joins = new Vector<Hashtable>();

		if(myStore.containsKey(PKQLEnum.COL_CSV) && ((Vector)myStore.get(PKQLEnum.COL_CSV)).size() > 0)
			selectors = (Vector<String>) myStore.get(PKQLEnum.COL_CSV);
		if(myStore.containsKey(PKQLEnum.FILTER) && ((Vector)myStore.get(PKQLEnum.FILTER)).size() > 0)
			filters = (Vector<Hashtable>) myStore.get(PKQLEnum.FILTER);
		if(myStore.containsKey(PKQLEnum.JOINS) && ((Vector)myStore.get(PKQLEnum.JOINS)).size() > 0)
			joins = (Vector<Hashtable>) myStore.get(PKQLEnum.JOINS);

		//for each inner join we want to add filters to the query struct
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
						//				if(joinType.equalsIgnoreCase("inner.join")) {
						String[] columnHeaders = frame.getColumnHeaders();

						//figure out which is the new column and which already exists in the table
						Iterator<Object> rowIt = null;
						//				String filterColumn = null;
						//				if(ArrayUtilityMethods.arrayContainsValue(columnHeaders, fromColumn)) {
						//					filterColumn = fromColumn;
						//				} else if(ArrayUtilityMethods.arrayContainsValue(columnHeaders, toColumn)) {
						//					filterColumn = toColumn;
						//				}

						//we want to add filters to the column that already exists in the table
						if(fromColumn != null && toColumn != null) {
							rowIt = frame.uniqueValueIterator(fromColumn, false, false);
							List<Object> uris = new Vector<Object>();

							//collect all the filter values
							while(rowIt.hasNext()){
								uris.add(rowIt.next() + "");
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
		// really crappy bifurcation here
		// this is entered when the data maker is a legacy GDM
		else {
			api.set(QueryAPI.USE_CHEATER, true);
		}

		QueryStruct qs = new QueryStruct();
		processQueryStruct(qs, selectors, filters, joins);
		api.set("QUERY_STRUCT", qs);
		Iterator thisIterator = api.process();

		Map<String, Set<String>> edgeHash = qs.getReturnConnectionsHash();
		this.put("EDGE_HASH", edgeHash);

		myStore.put(nodeStr, thisIterator);
		myStore.put("RESPONSE", "success");
		myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);

		// eventually I need this iterator to set this back for this particular node
		return null;
	}

	public void processQueryStruct(QueryStruct qs, Vector <String> selectors, Vector <Hashtable> filters, Vector <Hashtable> joins)
	{
		Map<String, Map<String, Object>> varMap = (Map<String, Map<String, Object>>) myStore.get("VARMAP");
		for(int selectIndex = 0;selectIndex < selectors.size();selectIndex++)
		{
			String thisSelector = selectors.get(selectIndex);
			if(thisSelector.contains("__")){
				String concept = thisSelector.substring(0, thisSelector.indexOf("__"));
				String property = thisSelector.substring(thisSelector.indexOf("__")+2);
				qs.addSelector(concept, property);
			}
			else
			{
				qs.addSelector(thisSelector, null);
			}
			
			// For this column, see if there is a param set that references it
			// If so, grab it's value and add as a filter to apply that param
			for(String var : varMap.keySet()) {
				Map<String, Object> paramValues = varMap.get(var);
				if(paramValues != null && paramValues.get(Constants.TYPE).equals(thisSelector)) {
					Vector<String> filterValues = new Vector<String>();
					filterValues.add(paramValues.get(Constants.VALUE).toString());
					qs.addFilter(thisSelector, "=", filterValues);
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
						filterData.add(paramValues.get(Constants.VALUE).toString());
					}
				}
				
				qs.addFilter(fromCol, comparator, filterData);
			}
		}
		for(int joinIndex = 0;joinIndex < joins.size();joinIndex++)
		{
			Hashtable thisJoin = (Hashtable)joins.get(joinIndex);

			String fromCol = (String)thisJoin.get("FROM_COL");
			String toCol = (String)thisJoin.get("TO_COL");

			String relation = (String)thisJoin.get("REL_TYPE");	
			qs.addRelation(fromCol, toCol, relation);
		}	
	}
}
