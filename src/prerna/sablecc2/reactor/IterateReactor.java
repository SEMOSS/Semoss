package prerna.sablecc2.reactor;


import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.querystruct.QueryStruct2;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.SQLInterpreter2;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc2.JobStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.util.Utility;

//TODO : Hardcoding this to use QueryStruct2 and SQLInterpreter2 to test changes
public class IterateReactor extends AbstractReactor {

	private Iterator output;
	
	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}
	
	public NounMetadata execute() {
		return createJob();
	}
	
	private NounMetadata createJob()  {
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
			IEngine engine = Utility.getEngine(engineName);
//			IQueryInterpreter interp = engine.getQueryInterpreter();
			SQLInterpreter2 interp = new SQLInterpreter2(engine);
			interp.setQueryStruct(queryStruct);
			String importQuery = interp.composeQuery();
			IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, importQuery);
			this.output = iterator;
			Job job = new Job(iterator, queryStruct);
			id = JobStore.INSTANCE.addJob(job);
		} else {
			ITableDataFrame frame = (ITableDataFrame)this.planner.getProperty("FRAME", "FRAME");
			SQLInterpreter2 interp = new SQLInterpreter2();
			interp.setQueryStruct(queryStruct);
			String importQuery = interp.composeQuery();
			Iterator iterator = frame.query(queryStruct);
			this.output = iterator;
			Job job = new Job(iterator, queryStruct);
			id = JobStore.INSTANCE.addJob(job);
		}	
		
		Map<String, Object> returnData = new HashMap<>();
		returnData.put("jobId", id);
		this.planner.addProperty("DATA", "DATA", returnData);
		
		// create the return
		NounMetadata output = new NounMetadata(this.output, PkslDataTypes.JOB);
		output.setExplanation("Iterator created from iterate reactor");
		return output;
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) return outputs;
		
		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.output, PkslDataTypes.JOB);
		output.setExplanation("Iterator created from iterate reactor");
		outputs.add(output);
		return outputs;
	}

	private QueryStruct2 getQueryStruct() {
		GenRowStruct allNouns = getNounStore().getNoun("QUERYSTRUCT");
		QueryStruct2 queryStruct = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.getNoun(0);
			return (QueryStruct2)object.getValue();
		} else {
			NounMetadata result = (NounMetadata)this.planner.getProperty("RESULT", "RESULT");
			if(result.getNounName().equals("QUERYSTRUCT")) {
				queryStruct = (QueryStruct2)result.getValue();
			}
		}
		return queryStruct;
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


