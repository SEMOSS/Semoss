package prerna.sablecc2.reactor;


import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.IScriptReactor;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.SQLInterpreter2;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.util.Utility;

import java.util.Iterator;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.querystruct.QueryStruct2;

public class ImportDataReactor extends AbstractReactor {


	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	//greedy execution
	public Object Out() {
		importToFrame();
		return parentReactor;
	}
	
	public void updatePlan() {
		//there is no
	}

	
	public Object reduce(Iterator it) {
		return Out();
	}

	@Override
	protected void mergeUp() {
		//this reactor should not need to merge up
		if(parentReactor != null) {
			
		}
	}
	
	private void importToFrame()  {
		
//		GenRowStruct object = store.getNoun("qs");
//		String aliasName = (String)object.get(0);
//
//		Map<String, Object> map = (Map<String, Object>)this.planner.getProperty(aliasName, "STORE");
//		QueryStruct queryStruct = (QueryStruct)map.get("qs");
//		String engineName = (String)map.get("db");
		
		
		QueryStruct2 queryStruct = getQueryStruct();
		String engineName = queryStruct.getEngineName();
		ITableDataFrame frame = (ITableDataFrame)this.planner.getProperty("FRAME", "FRAME");
		String className = frame.getScriptReactors().get(PKQLEnum.IMPORT_DATA);
		
		try {
			IScriptReactor curReactor = (IScriptReactor) Class.forName(className).newInstance();
			
			IEngine engine = Utility.getEngine(removeQuotes(engineName.trim()));
			SQLInterpreter2 interp = new SQLInterpreter2(engine);
//			IQueryInterpreter interp = engine.getQueryInterpreter();
			interp.setQueryStruct(queryStruct);
			String importQuery = interp.composeQuery();
			IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, importQuery);
			
			//set values into the curReactor
			curReactor.put("G", frame);
			curReactor.put(PKQLEnum.API + "_EDGE_HASH", queryStruct.getReturnConnectionsHash());
			curReactor.put(PKQLEnum.API + "_QUERY_NUM_CELLS", 1.0);
			curReactor.put(PKQLEnum.API + "_ENGINE", removeQuotes(engineName.trim()));
			curReactor.put(PKQLEnum.API, iterator);
			curReactor.process();
			ITableDataFrame importedFrame = (ITableDataFrame)curReactor.getValue("G");
			System.out.println("IMPORTED FRAME CREATED WITH ROW COUNT: "+importedFrame.getNumRows());
			this.planner.addProperty("FRAME", "FRAME", importedFrame);
//			this.planner.addProperty("QUERYSTRUCT", "QUERYSTRUCT", new QueryStruct2());
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Vector<NounMetadata> getInputs() {
		return null;
	}
	
	private QueryStruct2 getQueryStruct() {
		GenRowStruct allNouns = getNounStore().getNoun("QUERYSTRUCT");
		QueryStruct2 queryStruct = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.get(0);
			return (QueryStruct2)object.getValue();
		} else {
			NounMetadata result = this.planner.getVariable("$RESULT");
			if(result.getNounName().equals("QUERYSTRUCT")) {
				queryStruct = (QueryStruct2)result.getValue();
			}
		}
		return queryStruct;
	}
	
	private String removeQuotes(String value) {
		if(value.startsWith("'") || value.startsWith("\"")) {
			value = value.trim().substring(1, value.length() - 1);
		}
		return value;
	}
}


