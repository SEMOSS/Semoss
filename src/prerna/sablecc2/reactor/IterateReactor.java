package prerna.sablecc2.reactor;


import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.IScriptReactor;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.rdf.query.builder.SQLInterpreter2;
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
import prerna.ds.QueryStruct2;

//TODO : Hardcoding this to use QueryStruct2 and SQLInterpreter2 to test changes
public class IterateReactor extends AbstractReactor {


	private Iterator output;
	
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
		
		
		QueryStruct2 queryStruct = getQueryStruct(); //this should not be a single query...need to somehow store this with a signature
		String engineName = queryStruct.getEngineName();
		//TODO: add tableJoins to query
		//TODO: remove hard coded classes when we establish querystruct2 and sqlinterpreter2 function properly 
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all); //should be only joins
		String id;
		if(engineName != null) {
			IEngine engine = Utility.getEngine(removeQuotes(engineName.trim()));
//			IQueryInterpreter interp = engine.getQueryInterpreter();
			SQLInterpreter2 interp = new SQLInterpreter2(engine);
			interp.setQueryStruct(queryStruct);
			String importQuery = interp.composeQuery();
			IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, importQuery);
			this.output = iterator;
			id = JobStore.INSTANCE.addJob(iterator);
		} else {
			ITableDataFrame frame = (ITableDataFrame)this.planner.getProperty("FRAME", "FRAME");
			SQLInterpreter2 interp = new SQLInterpreter2();
			interp.setQueryStruct(queryStruct);
			String importQuery = interp.composeQuery();
			Iterator iterator = frame.query(importQuery);
			this.output = iterator;
			id = JobStore.INSTANCE.addJob(iterator);
		}	
		
		Map<String, Object> returnData = new HashMap<>();
		returnData.put("jobId", id);
		this.planner.addProperty("DATA", "DATA", returnData);
		this.planner.addProperty("RESULT", "RESULT", getOutput());
	}
	
	@Override
	public NounMetadata getOutput() {
		NounMetadata output = new NounMetadata(this.output, "ITERATOR");
		output.setExplanation("Iterator created from iterate reactor");
		return output;
	}

	@Override
	public Vector<NounMetadata> getInputs() {
		return null;
	}
	
	private QueryStruct2 getQueryStruct() {
		GenRowStruct allNouns = getNounStore().getNoun("QUERY");
		QueryStruct2 queryStruct = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.get(0);
			return (QueryStruct2)object.getValue();
		} else {
			NounMetadata result = (NounMetadata)this.planner.getProperty("RESULT", "RESULT");
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


