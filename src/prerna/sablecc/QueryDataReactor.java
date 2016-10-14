package prerna.sablecc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.util.Utility;

public class QueryDataReactor extends AbstractReactor {

	// this stores the specific values that need to be aggregated from the child reactors
	// based on the child, different information is needed in order to properly add the 
	// data into the frame
	Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();
	
	public QueryDataReactor() {
		String [] thisReacts = {PKQLEnum.API, PKQLEnum.JOINS};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.QUERY_DATA;

		// when the data is coming from an API (i.e. an engine or a file)
		String [] dataFromApi = {PKQLEnum.COL_CSV, "ENGINE", "EDGE_HASH"};
		values2SyncHash.put(PKQLEnum.API, dataFromApi);

	}
	
	@Override
	public Iterator process() {
		
		modExpression();
		System.out.println("My Store on IMPORT DATA REACTOR: " + myStore);
		
		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
		
		// 1) get the starting headers
		// the starting headers is important to keep for the frame specific import data reactors 
		// They are responsible for knowing when to perform an addRow vs. an addRelationship 
		// (i.e. insert vs. update for H2ImportDataReactor)
		String[] startingHeaders = frame.getColumnHeaders();
		// store in mystore
		myStore.put("startingHeaders", startingHeaders);

		QueryStruct qs = (QueryStruct)myStore.get(PKQLEnum.API);
		
		// 2) format and process the join information
		Vector<Map<String, String>> joins = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		if(joins!=null){
			//do the logic here of getting the filters from the frame
			for(Map<String,String> join : joins){
				String joinType = join.get(PKQLEnum.REL_TYPE);
				if(joinType.contains("inner") || joinType.contains("left")) {
					String toCol = join.get(PKQLEnum.TO_COL);
					String fromCol = join.get(PKQLEnum.FROM_COL);
				
					Object[] bindings = frame.getColumn(fromCol);
					qs.addFilter(toCol, "=", Arrays.asList(bindings));
				}
			}
		}
		
		IEngine engine = Utility.getEngine((this.getValue(PKQLEnum.API + "_ENGINE")+"").trim());		
		IQueryInterpreter interp = engine.getQueryInterpreter();
		interp.setQueryStruct(qs);
		String query = interp.composeQuery();
		
		IRawSelectWrapper thisIterator = WrapperManager.getInstance().getRawWrapper(engine, query);
		
		List searchData = new ArrayList();
		while(thisIterator.hasNext()) {
			searchData.add(thisIterator.next().getValues()[0]);
		}
		// store the search data
		myStore.put("searchData", searchData);
		myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);
		return null;
	}
	
	/**
	 * Gets the values to load into the reactor
	 * This is used to synchronize between the various reactors that can feed into this reactor
	 * @param input			The type of child reactor
	 */
	public String[] getValues2Sync(String input)
	{
		return values2SyncHash.get(input);
	}
}
