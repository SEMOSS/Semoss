package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.engine.api.IApi;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.QueryAPI;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.DIHelper;

public class NativeApiReactor extends ApiReactor {

	private IApi api = null;

	public NativeApiReactor() {
		String [] thisReacts = {PKQLEnum.COL_CSV, PKQLEnum.FILTER, PKQLEnum.JOINS, "KEY_VALUE", "TABLE_JOINS"};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.API;
	}

	@Override
	public Iterator process() {

		System.out.println("Processed.. " + myStore);

		String nodeStr = (String)myStore.get(whoAmI);
		Vector <Hashtable> filtersToBeElaborated = new Vector<Hashtable>();
		Vector <String> tinkerSelectors = new Vector<String>();

		String engine = (String)myStore.get("ENGINE");

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
						
						String[] columnHeaders = frame.getColumnHeaders();

						//figure out which is the new column and which already exists in the table
						Iterator<Object> rowIt = null;

						//we want to add filters to the column that already exists in the table
						if(fromColumn != null && toColumn != null) {
							rowIt = frame.uniqueValueIterator(fromColumn, false, false);
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
		
		// get the engine
		IEngine eng = (IEngine) DIHelper.getInstance().getLocalProp(engine.trim()); 

		// logic that if a person is trying to query an engine
		// and if the query struct is empty
		// just pull all the information from the engine
		if(qs.isEmpty()) {
			// we cannot override the existing query struct since
			// it is used in APIReactor to get the edgeHash
			// so we need to update the existing one to get the new values
			// TODO should expose this on the QueryStruct object itself
			QueryStruct newQs = eng.getDatabaseQueryStruct();
			qs.selectors = newQs.selectors;
			qs.relations = newQs.relations;
			qs.andfilters = newQs.andfilters;
		}
		qs.print();
		IQueryInterpreter interp = eng.getQueryInterpreter();
		interp.setQueryStruct(qs);
		String query = interp.composeQuery();
//		query = "("+query+")";
//		query = "CREATE OR REPLACE VIEW AS VIEWTABLE "+query;

		//run this query on the native frame
		
		Map<String, Set<String>> edgeHash = qs.getReturnConnectionsHash();
		this.put("EDGE_HASH", edgeHash);

		myStore.put(nodeStr, query);
		myStore.put("RESPONSE", "success");
		myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);

		return null;
	}
}
