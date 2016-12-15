package prerna.sablecc;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngineWrapper;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.sablecc.meta.IPkqlMetadata;
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
		
		QueryStruct qs = (QueryStruct)myStore.get(PKQLEnum.API);
		
		// 2) format and process the join information
		Vector<Map<String, String>> joins = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		if(joins != null && !joins.isEmpty()){
			ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
			if(frame == null) {
				throw new IllegalArgumentException("Cannot have a table join in a state less PKQL call");
			}
//			//do the logic here of getting the filters from the frame
//			for(Map<String,String> join : joins){
//				String joinType = join.get(PKQLEnum.REL_TYPE);
//				if(joinType.contains("inner") || joinType.contains("left")) {
//					
//					String toCol = join.get(PKQLEnum.TO_COL);
//					String fromCol = join.get(PKQLEnum.FROM_COL);
//				
//					Object[] bindings = frame.getColumn(fromCol);
//					qs.addFilter(toCol, "=", Arrays.asList(bindings));
//				}
//			}
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
		
		createResponseString(thisIterator);
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

	/**
	 * Create the return response from a engine wrapper
	 * @param it				The iterator used to insert data
	 * @return					String returning the response
	 */
	protected void createResponseString(IEngineWrapper it){
		String nodeStr = (String)myStore.get(PKQLEnum.EXPR_TERM);

		// get map containing the response metadata from the iterator
		Map<String, Object> map = it.getResponseMeta();
		// format fields from meta data map into a string
		String mssg = "";
		for(String key : map.keySet()){
			if(!mssg.isEmpty()){
				mssg = mssg + " \n";
			}
			mssg = mssg + key + ": " + map.get(key).toString();
		}
		String retStr = "Sucessfully added data using : \n" + mssg;
		myStore.put(nodeStr, retStr);
	}
	
	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}
