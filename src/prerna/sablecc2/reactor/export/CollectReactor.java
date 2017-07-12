package prerna.sablecc2.reactor.export;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

/**
 * 
 * This class is responsible for collecting data from a job and returning it
 *
 */
public class CollectReactor extends AbstractReactor{

	public NounMetadata execute() {
		Job job = getJob();
		int collectThisMany = getTotalToCollect();
		
		Object data = job.collect(collectThisMany);
		NounMetadata result = new NounMetadata(data, PkslDataTypes.FORMATTED_DATA_SET);
		return result;
	}
	
	//This gets the Job collect reactor needs to collect from
	private Job getJob() {
		Job job;
		
		List<Object> jobs = curRow.getColumnsOfType(PkslDataTypes.JOB);
		//if we don't have jobs in the curRow, check if it exists in genrow under the key job
		if(jobs == null || jobs.size() == 0) {
			job = (Job) getNounStore().getNoun(PkslDataTypes.JOB.toString()).get(0);
		} else {
			job = (Job) curRow.getColumnsOfType(PkslDataTypes.JOB).get(0);
		}
		return job;
	}
	
	//returns how much do we need to collect
	private int getTotalToCollect() {
		Number collectThisMany = (Number) curRow.getAllNumericColumns().get(0);
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
