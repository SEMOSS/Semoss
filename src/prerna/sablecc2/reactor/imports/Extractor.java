package prerna.sablecc2.reactor.imports;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import prerna.ds.TinkerMetaHelper;
import prerna.ds.querystruct.HardQueryStruct;
import prerna.ds.querystruct.QueryStruct2;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.SQLInterpreter2;
import prerna.sablecc.PKQLEnum;
import prerna.util.Utility;

public class Extractor {

	IEngine engine; //there should be an interface that covers everything that i can query, something on top of frame and engine
	String importQuery;
	Iterator iterator;
	QueryStruct2 queryStruct;
	String keyBase;
	
	Map<String, Object> returnData;
	
	public Extractor(QueryStruct2 queryStruct) {
		returnData = new HashMap<>();
		this.queryStruct = queryStruct;
		String engineName = queryStruct.getEngineName();
		engine = Utility.getEngine(engineName);
	}
		
	public Map<String, Object> extractData() {		
		createIterator();
		setReturnData();
		return returnData;
	}
	
	//TODO: the iterator should be able to come from an engine or a frame
	private void createIterator() {
		SQLInterpreter2 interp = getInterpreter();
		interp.setQueryStruct(queryStruct);
		importQuery = interp.composeQuery();
		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, importQuery); //we can only import from a db...or can we import from a frame?
		this.iterator = iterator;
	}
	
	//TODO: the interpreter should be any interpreter
	//Need to overload to get an intpreter from a frame as well
	private SQLInterpreter2 getInterpreter() {
		return new SQLInterpreter2(engine);
	}
	
	private void setEdgeHash() {
		if(this.queryStruct instanceof HardQueryStruct) {
			
			// determine the edge hash
			// testing -> create a prim key
			String[] headers = ((IRawSelectWrapper)iterator).getDisplayVariables();
			Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(headers);
			returnData.put("EDGE_HASH", edgeHash);

			if(iterator instanceof RawRDBMSSelectWrapper) {
				RawRDBMSSelectWrapper rdbmsIt = (RawRDBMSSelectWrapper) iterator;
				try {
					ResultSetMetaData meta = rdbmsIt.getMetaData();
					int numCols = meta.getColumnCount();
					
					Map<String, String> dataTypeMap = new HashMap<String, String>();
					Map<String, String> logicalToValueMap = new HashMap<String, String>();
					for(int index = 0; index < numCols; index++) {
						dataTypeMap.put(meta.getColumnLabel(index+1), meta.getColumnTypeName(index+1));
						logicalToValueMap.put(meta.getColumnLabel(index+1), meta.getColumnLabel(index+1));
					}
					
					returnData.put(PKQLEnum.RAW_API + "_DATA_TYPE_MAP", dataTypeMap);
					returnData.put(PKQLEnum.RAW_API + "_LOGICAL_TO_VALUE", logicalToValueMap);
				} catch (SQLException e) {
					e.printStackTrace();
					throw new IllegalArgumentException("ERROR WITH EXECUTION OF SQL QUERY");
				}
				
			} else {
				// HOW DO I DETERMINE THE DATA TYPES PROPERLY??? :(
				int numCols = headers.length;
				Map<String, String> dataTypeMap = new HashMap<String, String>();
				Map<String, String> logicalToValueMap = new HashMap<String, String>();
				
				for(int index = 0; index < numCols; index++) {
					dataTypeMap.put(headers[index], "STRING");
					logicalToValueMap.put(headers[index], headers[index]);
				}
				returnData.put(PKQLEnum.RAW_API + "_DATA_TYPE_MAP", dataTypeMap);
				returnData.put(PKQLEnum.RAW_API + "_LOGICAL_TO_VALUE", logicalToValueMap);
			}
			
			returnData.put(PKQLEnum.RAW_API, iterator);
			
		} else {
			returnData.put(PKQLEnum.API, iterator);
			returnData.put(PKQLEnum.API + "_EDGE_HASH", queryStruct.getReturnConnectionsHash());
		}
	}
	
	private void setReturnData() {
		setEdgeHash();
		returnData.put(PKQLEnum.API + "_QUERY_NUM_CELLS", 1.0);
		returnData.put(PKQLEnum.API + "_ENGINE", engine.getEngineName());
		
	}
}
