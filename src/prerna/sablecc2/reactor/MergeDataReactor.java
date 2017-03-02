package prerna.sablecc2.reactor;


import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.IScriptReactor;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.rdf.query.builder.SQLInterpreter2;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;
import prerna.util.Utility;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.ds.QueryStruct2;
import prerna.ds.h2.H2Frame;

public class MergeDataReactor extends AbstractReactor {


	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		importToFrame();
		if(parentReactor != null) {
			return this.parentReactor;
		}
		return null;
	}
	
	public void updatePlan() {
		//just need to set the frame to the planner here...that's it
		getType();
		Enumeration <String> keys = store.nounRow.keys();
		
		String reactorOutput = reactorName;
		
		while(keys.hasMoreElements())
		{
			String singleKey = keys.nextElement();
			GenRowStruct struct = store.nounRow.get(singleKey);
			Vector <String> inputs = struct.getAllColumns();
			
			// need a better way to do it
			if(asName == null)
				reactorOutput = reactorOutput + "_" + struct.getColumns();
	
			// find if code exists
			if(!propStore.containsKey("CODE"))
			{
				if(inputs.size() > 0)
					planner.addInputs(signature, inputs, type);
			}
		}
	}


	@Override
	protected void mergeUp() {
		//this reactor should not need to merge up
	}
	
	private void importToFrame()  {
		//get the inputs
		
		QueryStruct2 queryStruct = (QueryStruct2)this.planner.getProperty("QUERYSTRUCT", "QUERYSTRUCT");
		String engineName = queryStruct.getEngineName();
		
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all); //should be only joins
		ITableDataFrame frame = (ITableDataFrame)this.planner.getProperty("FRAME", "FRAME");
		String className = frame.getScriptReactors().get(PKQLEnum.IMPORT_DATA);
		
		try {
			IScriptReactor curReactor = (IScriptReactor) Class.forName(className).newInstance();
			SQLInterpreter2 interp;
			Iterator<IHeadersDataRow> iterator;
			if(engineName != null) {
				IEngine engine = Utility.getEngine(removeQuotes(engineName.trim()));
				interp = new SQLInterpreter2(engine);
				interp.setQueryStruct(queryStruct);
				String importQuery = interp.composeQuery();
				iterator = WrapperManager.getInstance().getRawWrapper(engine, importQuery);
			} else {
				interp = new SQLInterpreter2();
				interp.setQueryStruct(queryStruct);
				String query = interp.composeQuery();
				iterator = frame.query(query);
			}
			
			//set values into the curReactor
			curReactor.put("G", frame);
			curReactor.put(PKQLEnum.API + "_EDGE_HASH", queryStruct.getReturnConnectionsHash());
			curReactor.put(PKQLEnum.API + "_QUERY_NUM_CELLS", 1.0);
			curReactor.put(PKQLEnum.API + "_ENGINE", removeQuotes(engineName.trim()));
			curReactor.put(PKQLEnum.API, iterator);
			if(allNouns != null) {
				Vector<Map<String, String>> joinCols = getJoinCols(allNouns);
				curReactor.put(PKQLEnum.JOINS, joinCols);
				
			}
			curReactor.process();
			ITableDataFrame importedFrame = (ITableDataFrame)curReactor.getValue("G");
			System.out.println("IMPORTED FRAME CREATED WITH ROW COUNT: "+importedFrame.getNumRows());
			this.planner.addProperty("FRAME", "FRAME", importedFrame);
			
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Vector<NounMetadata> getInputs() {
		return null;
	}
	
	private String removeQuotes(String value) {
		if(value.startsWith("'") || value.startsWith("\"")) {
			value = value.trim().substring(1, value.length() - 1);
		}
		return value;
	}
	
	private Vector<Map<String,String>> getJoinCols(GenRowStruct joins) {
		
		Vector<Map<String, String>> joinCols = new Vector<>();
		for(int i = 0; i < joins.size(); i++) {
			if(joins.get(i) instanceof Join) {
				Join join = (Join)joins.get(i);
				String toCol = join.getQualifier();
				String fromCol = join.getSelector();
				String joinType = join.getJoinType();
				
				Map<String, String> joinMap = new HashMap<>(1);
				joinMap.put(PKQLEnum.TO_COL, toCol);
				joinMap.put(PKQLEnum.FROM_COL, fromCol);
				joinMap.put(PKQLEnum.REL_TYPE, joinType);
				
				joinCols.add(joinMap);
			}
		}
		return joinCols;
	}
}


