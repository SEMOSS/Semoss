package prerna.sablecc2.reactor;


import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.IScriptReactor;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc2.JobStore;
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

public class IterateReactor extends AbstractReactor {


	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		createJob();
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
	
	private void createJob()  {
		//get the inputs
		
		//every job should be created via a query struct run either on a database or a frame
		//or a code block run on a frame --this is done via runMyPlan
		//we will temporarily introduce handling math operations until those math operations are absorbed by the query struct and interpreted by the query interpreters
		
		
		QueryStruct queryStruct = (QueryStruct)this.planner.getProperty("QUERYSTRUCT", "QUERYSTRUCT"); //this should not be a single query...need to somehow store this with a signature
		String engineName = queryStruct.getEngineName();
		//TODO: add tableJoins to query
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all); //should be only joins
		if(engineName != null) {
			IEngine engine = Utility.getEngine(removeQuotes(engineName.trim()));
			IQueryInterpreter interp = engine.getQueryInterpreter();
			interp.setQueryStruct(queryStruct);
			String importQuery = interp.composeQuery();
			IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, importQuery);
			JobStore.INSTANCE.addJob("job", iterator);
		} else {
			ITableDataFrame frame = (ITableDataFrame)this.planner.getProperty("FRAME", "FRAME");
			Iterator iterator = frame.query(queryStruct);
			JobStore.INSTANCE.addJob("job", iterator);
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


