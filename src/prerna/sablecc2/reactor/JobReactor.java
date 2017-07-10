package prerna.sablecc2.reactor;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.JobStore;
import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class JobReactor extends AbstractReactor {
	
	@Override
	public NounMetadata execute() {
		// this just returns the job id
		String jobId = (String)curRow.get(0);
		Job job = JobStore.getInstance().getJob(jobId);
		return new NounMetadata(job, PkslDataTypes.JOB);
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) return outputs;
		
		outputs = new Vector<NounMetadata>();
		// since output is lazy
		// just return the execute
		outputs.add( (NounMetadata) execute());
		return outputs;
	}
}
