package prerna.sablecc2.reactor.export;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.JobStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public class CollectReactor extends AbstractReactor{

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}

	public NounMetadata execute() {
		Job job = getJob();
		int collectThisMany = getTotalToCollect();
		
		Object data = job.collect(collectThisMany);
		this.planner.addProperty("DATA", "DATA", data);
		NounMetadata result = new NounMetadata(data, PkslDataTypes.FORMATTED_DATA_SET); //change this to job
		return result;
	}
	
	private Job getJob() {
		Job job;
		
		List<Object> jobs = curRow.getColumnsOfType(PkslDataTypes.JOB);
		if(jobs == null || jobs.size() == 0) {
			job = (Job) getNounStore().getNoun(PkslDataTypes.JOB.toString()).get(0);
		} else {
			job = (Job) curRow.getColumnsOfType(PkslDataTypes.JOB).get(0);
		}
		return job;
	}
	
	private int getTotalToCollect() {
		Number collectThisMany = (Number) curRow.getColumnsOfType(PkslDataTypes.CONST_DECIMAL).get(0);
		return collectThisMany.intValue();
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) return outputs;
		
		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PkslDataTypes.FORMATTED_DATA_SET);
		outputs.add(output);
		return outputs;
	}
}
