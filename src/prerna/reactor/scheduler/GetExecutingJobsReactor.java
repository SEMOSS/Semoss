package prerna.reactor.scheduler;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.JobExecutionContext;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.rpa.config.JobConfigKeys;

public class GetExecutingJobsReactor extends AbstractReactor {
	private static final Logger classLogger = LogManager.getLogger(GetExecutingJobsReactor.class);
	
	@Override
    public NounMetadata execute() {
		if(Utility.schedulerForceDisable()) {
			throw new IllegalArgumentException("Scheduler is not enabled");
		}
		
		Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
		// start up scheduler if it isn't on
		SchedulerDatabaseUtility.startScheduler(scheduler);
		
		// Retrieve the list of currently executing jobs
        try {
			List<JobExecutionContext> executingJobs = scheduler.getCurrentlyExecutingJobs();
			
	        return new NounMetadata(getExecutingJobsAsMap(executingJobs), PixelDataType.FORMATTED_DATA_SET);
		} catch (SchedulerException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occured while trying to retrieve executing jobs");
		}
	}
	
	  private List<Map<String, Object>> getExecutingJobsAsMap(List<JobExecutionContext> executingJobs) {		  
		  return IntStream.range(0, executingJobs.size())
			        .mapToObj(i -> {
			            JobExecutionContext ctx = executingJobs.get(i);
			            Map<String, Object> jobMap = new HashMap<>();
			            jobMap.put("id", i); // index as id
			            jobMap.put("jobName",  ctx.getMergedJobDataMap().get(JobConfigKeys.JOB_NAME));
			            jobMap.put("jobGroup", ctx.getJobDetail().getKey().getGroup());
			            jobMap.put("execStart", ctx.getFireTime() != null ? ctx.getFireTime().getTime() : 0L);
			            return jobMap;
			        })
			        .collect(Collectors.toList());
	    }
	
}
