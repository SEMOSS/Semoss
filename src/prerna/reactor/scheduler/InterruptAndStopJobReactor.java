package prerna.reactor.scheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.JobExecutionContext;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class InterruptAndStopJobReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(InterruptAndStopJobReactor.class);

    public static final String JOB_ID = "jobId";
    public static final String JOB_GROUP = "jobGroup";

    protected Scheduler scheduler = null;

    public InterruptAndStopJobReactor() {
        this.keysToGet = new String[] { JOB_ID, JOB_GROUP };
    }

    @Override
    public NounMetadata execute() {
        if (Utility.schedulerForceDisable()) {
            throw new IllegalArgumentException("Scheduler is not enabled");
        }
        organizeKeys();

        String jobId = this.keyValue.get(JOB_ID);
        String jobGroup = this.keyValue.get(JOB_GROUP);

        if (jobId == null) {
            throw new IllegalArgumentException("jobId is required");
        }

        if (jobGroup == null || jobGroup.trim().isEmpty()) {
            jobGroup = "defaultGroup";
        }

        User user = this.insight.getUser();
        if (!SecurityAdminUtils.userIsAdmin(user) && !SecurityProjectUtils.userCanEditProject(user, jobGroup)) {
            throw new IllegalArgumentException("User does not have proper permissions to cancel jobs");
        }

        Map<String, String> result = new HashMap<>();

        try {
            scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
            JobKey jobKey = JobKey.jobKey(jobId, jobGroup);

            if (scheduler.checkExists(jobKey)) {
                try {
                    boolean interrupted = scheduler.interrupt(jobKey);
                    classLogger.info("Interrupt requested for job " + jobKey + ", success=" + interrupted);
                    result.put("status", "stopped");
                    result.put("message", "Job " + jobId + " in group " + jobGroup + " was stopped.");
                } catch (SchedulerException se) {
                    classLogger.warn("Unable to interrupt job " + jobKey + ": " + se.getMessage());
                    result.put("status", "error");
                    result.put("message", "Unable to interrupt job: " + se.getMessage());
                }
            } else {
                classLogger.info("Job " + jobKey + " does not exist in scheduler.");
                result.put("status", "not_found");
                result.put("message", "Job " + jobId + " in group " + jobGroup + " does not exist.");
            }

            return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.LIST_JOB);

        } catch (SchedulerException se) {
            classLogger.error(Constants.STACKTRACE, se);
            throw new IllegalArgumentException("Unable to cancel/remove the job. Error message = " + se.getMessage());
        }
    }
}
