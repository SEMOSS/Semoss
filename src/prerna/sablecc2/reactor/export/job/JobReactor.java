package prerna.sablecc2.reactor.export.job;

import prerna.sablecc2.JobStore;

public class JobReactor extends JobBuilderReactor {
	
	@Override
	protected void buildJob() {
		// this just returns the job id
		String jobId = (String)curRow.get(0);
		job = JobStore.INSTANCE.getJob(jobId);
	}
}
