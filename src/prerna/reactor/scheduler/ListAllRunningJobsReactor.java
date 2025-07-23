package prerna.reactor.scheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ListAllRunningJobsReactor extends AbstractReactor {

    public ListAllRunningJobsReactor() {

    }

    @Override
    public NounMetadata execute() {
        Map<String, Map<String, String>> runningJobsMap = getRunningJobsMap();
        return new NounMetadata(runningJobsMap, PixelDataType.MAP, PixelOperationType.LIST_JOB);
    }

    private Map<String, Map<String, String>> getRunningJobsMap() {
        Map<String, Map<String, String>> jobMap = new HashMap<>();
        try {
            Scheduler quartzScheduler = SchedulerFactorySingleton.getInstance().getScheduler();
            List<JobExecutionContext> executingJobs = quartzScheduler.getCurrentlyExecutingJobs();
            for (JobExecutionContext context : executingJobs) {
                String jobKey = context.getJobDetail().getKey().toString();
                Map<String, String> jobDetails = new HashMap<>();
                jobDetails.put("jobName", context.getJobDetail().getKey().getName());
                jobDetails.put("jobGroup", context.getJobDetail().getKey().getGroup());
                jobDetails.put("trigger", context.getTrigger().getKey().toString());
                jobDetails.put("fireTime", String.valueOf(context.getFireTime()));
                jobDetails.put("scheduledFireTime", String.valueOf(context.getScheduledFireTime()));
                jobDetails.put("jobClass", context.getJobDetail().getJobClass().getName());

                jobMap.put(jobKey, jobDetails);
            }
        } catch (SchedulerException e) {
            Map<String, String> errorDetails = new HashMap<>();
            errorDetails.put("error", e.getMessage());
            jobMap.put("SchedulerException", errorDetails);
        }
        return jobMap;
    }
}
