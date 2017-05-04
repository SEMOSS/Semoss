package prerna.sablecc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngineWrapper;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.tinker.TinkerEngine;
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
		String [] thisReacts = {PKQLEnum.API, PKQLEnum.JOINS, PKQLEnum.MAP_OBJ};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.QUERY_DATA;

		// when the data is coming from an API (i.e. an engine or a file)
		String [] dataFromApi = {PKQLEnum.COL_CSV, "ENGINE", "EDGE_HASH", PKQLEnum.MAP_OBJ};
		values2SyncHash.put(PKQLEnum.API, dataFromApi);
		
		String [] dataFromRawApi = {PKQLEnum.RAW_API, "ENGINE", PKQLEnum.MAP_OBJ};
		values2SyncHash.put(PKQLEnum.RAW_API, dataFromRawApi);
	}
	
	@Override
	public Iterator process() {
		modExpression();
		System.out.println("My Store on IMPORT DATA REACTOR: " + myStore);
		
		Map<Object, Object> optionsMap = (Map<Object, Object>) myStore.get(PKQLEnum.API + "_" + PKQLEnum.MAP_OBJ);
		if(optionsMap == null) {
			optionsMap = (Map<Object, Object>) myStore.get(PKQLEnum.RAW_API + "_" + PKQLEnum.MAP_OBJ);
		}
		boolean grabAll = false;
		if(optionsMap != null) {
			if(optionsMap.containsKey("grabAll")) {
				grabAll = Boolean.parseBoolean(optionsMap.get("grabAll").toString());
			}
		}
		
		QueryStruct qs = (QueryStruct) this.getValue(PKQLEnum.API);
		if(qs != null) {
			return processApi(qs, grabAll);
		} else {
			return processRawApi();
		}
	}
	
	public Iterator processApi(QueryStruct qs, boolean grabAll) {
		// 2) format and process the join information
		Vector<Map<String, String>> joins = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		if(joins != null && !joins.isEmpty()){
			ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
			if(frame == null) {
				throw new IllegalArgumentException("Cannot have a table join in a state less PKQL call");
			}
		}
		
		String engineName = this.getValue(PKQLEnum.API + "_ENGINE").toString().trim();
		Iterator<IHeadersDataRow> thisIterator;
		if(engineName.equals("frame")) {
			ITableDataFrame frame = (ITableDataFrame)myStore.get("G");
			thisIterator = frame.query(qs);
			
			List searchData = new ArrayList();
			while(thisIterator.hasNext()) {
				IHeadersDataRow nextRow = thisIterator.next();
				String[] headers = nextRow.getHeaders();
				Object[] values = nextRow.getValues();
				Map<String, Object> rowMap = new HashMap<>(headers.length);
				for(int i = 0; i < headers.length; i++) {
					rowMap.put(headers[i], values[i]);
				}
				searchData.add(rowMap);
			}
			myStore.put("searchData", searchData);
			myStore.put("source", "frame");
		} else {
			IEngine engine = Utility.getEngine(engineName);	
			IQueryInterpreter interp = engine.getQueryInterpreter();
			interp.setQueryStruct(qs);
			String query = interp.composeQuery();
			
			if(engine instanceof TinkerEngine) {
				((TinkerEngine) engine).setQueryStruct(qs);
			}
			thisIterator = WrapperManager.getInstance().getRawWrapper(engine, query);
			List searchData = new ArrayList();
			if(!grabAll) {
				while(thisIterator.hasNext()) {
					searchData.add(thisIterator.next().getValues()[0]);
				}
			} else {
				while(thisIterator.hasNext()) {
					IHeadersDataRow nextRow = thisIterator.next();
					String[] headers = nextRow.getHeaders();
					Object[] values = nextRow.getValues();
					Map<String, Object> rowMap = new HashMap<>(headers.length);
					for(int i = 0; i < headers.length; i++) {
						rowMap.put(headers[i], values[i]);
					}
					searchData.add(rowMap);
				}
			}
			myStore.put("searchData", searchData);
			myStore.put("source", "engine");
		}
		
		createResponseString(thisIterator);
		myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);
		return null;
	}
	
	public Iterator processRawApi() {
		Iterator<IHeadersDataRow> thisIterator = (Iterator<IHeadersDataRow>) this.getValue(PKQLEnum.RAW_API);
		List searchData = new ArrayList();
		while(thisIterator.hasNext()) {
			IHeadersDataRow nextRow = thisIterator.next();
			String[] headers = nextRow.getHeaders();
			Object[] values = nextRow.getValues();
			Map<String, Object> rowMap = new HashMap<>(headers.length);
			for(int i = 0; i < headers.length; i++) {
				rowMap.put(headers[i], values[i]);
			}
			searchData.add(rowMap);
		}
		
		myStore.put("searchData", searchData);
		String engineName = this.getValue(PKQLEnum.RAW_API + "_ENGINE").toString().trim();
		if(engineName.equals("frame")) {
			myStore.put("source", "frame");
		} else {
			myStore.put("source", "engine");
		}
		
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
	protected void createResponseString(Iterator it){
		String nodeStr = (String)myStore.get(PKQLEnum.EXPR_TERM);

		if(it instanceof IEngineWrapper) {
			// get map containing the response metadata from the iterator
			Map<String, Object> map = ((IEngineWrapper)it).getResponseMeta();
			// format fields from meta data map into a string
			String mssg = "";
			for(String key : map.keySet()){
				if(!mssg.isEmpty()){
					mssg = mssg + " \n";
				}
				mssg = mssg + key + ": " + map.get(key).toString();
			}
			String retStr = "Sucessfully retrieved data using : \n" + mssg;
			myStore.put(nodeStr, retStr);
		} else {
			String retStr = "Successfully retrieved data from the frame";
			myStore.put(nodeStr, retStr);
		}
	}
	
	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}
