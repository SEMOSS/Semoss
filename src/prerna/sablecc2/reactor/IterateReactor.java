package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.querystruct.QueryStruct2;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.SQLInterpreter2;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc2.JobStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.storage.InMemStore;
import prerna.util.Utility;

public class IterateReactor extends AbstractReactor {

	private Job output;
	private String IN_MEM_STORE = "store";
	
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
		// the iterator is what creates a job
		// we need to take into consideration when we want to output
		// the following data sources
		
		// 1) data from an engine
		// 2) data from a frame
		// 3) data from a in-memory source
		
		String jobId;
		
		// try to get a QS
		// ... not everything has a qs
		// ... primarily a key-value pair
		// ... TODO: should figure out a better way to bifurcate
		QueryStruct2 queryStruct = getQueryStruct();
		
		// try to get an in memory store being used
		InMemStore inMemStore = getInMemoryStore();
		
		if(inMemStore != null) 
		{
			// TODO: figure out how to use a QS if present with this query
			Iterator<IHeadersDataRow> iterator = inMemStore.getIterator();
			Job job = new Job(iterator, queryStruct);
			this.output = job;
			jobId = JobStore.INSTANCE.addJob(job);
			
		} 
		else 
		{
			//TODO: add tableJoins to query
			//TODO: remove hard coded classes when we establish querystruct2 and sqlinterpreter2 function properly 
			//TODO : Hard coding this to use QueryStruct2 and SQLInterpreter2 to test changes

			// okay, we want to query an engine or a frame
			// do this based on if the key is defined in the QS
			String engineName = queryStruct.getEngineName();
			if(engineName != null) {
				IEngine engine = Utility.getEngine(engineName);
				SQLInterpreter2 interp = new SQLInterpreter2(engine);
				interp.setQueryStruct(queryStruct);
				String importQuery = interp.composeQuery();
				IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, importQuery);
				Job job = new Job(iterator, queryStruct);
				this.output = job;
				jobId = JobStore.INSTANCE.addJob(job);
			} else {
				ITableDataFrame frame = (ITableDataFrame)this.planner.getProperty("FRAME", "FRAME");
				SQLInterpreter2 interp = new SQLInterpreter2();
				interp.setQueryStruct(queryStruct);
				Iterator iterator = frame.query(queryStruct);
				Job job = new Job(iterator, queryStruct);
				this.output = job;
				jobId = JobStore.INSTANCE.addJob(job);
			}	
		}
		
		Map<String, Object> returnData = new HashMap<>();
		returnData.put("jobId", jobId);
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

	/**
	 * Get the query struct that is defined 
	 * @return
	 */
	private QueryStruct2 getQueryStruct() {
		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.QUERY_STRUCT.toString());
		QueryStruct2 queryStruct = null;
		if(allNouns != null) {
			queryStruct = (QueryStruct2) allNouns.get(0);
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
	

	private InMemStore getInMemoryStore() {
		InMemStore inMemStore = null;
		GenRowStruct grs = getNounStore().getNoun(this.IN_MEM_STORE);
		if(grs != null) {
			inMemStore = (InMemStore) grs.get(0);
		} else {
			grs = getNounStore().getNoun(PkslDataTypes.IN_MEM_STORE.toString());
			if(grs != null) {
				inMemStore = (InMemStore) grs.get(0);
			}
		}
		
		return inMemStore;
	}
}


