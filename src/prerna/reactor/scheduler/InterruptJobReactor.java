package prerna.reactor.scheduler;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.UnableToInterruptJobException;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class InterruptJobReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(InterruptJobReactor.class);

    public InterruptJobReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.JOB_ID.getKey(), ReactorKeysEnum.JOB_GROUP.getKey() };
    }

    @Override
    public NounMetadata execute() {
        if (Utility.schedulerForceDisable()) {
            throw new IllegalArgumentException("Scheduler is not enabled");
        }

        /**
         * InterruptJob(jobName = ["sample_job_name"], jobGroup=["sample_job_group"]);
         *
         * This reactor will send an interrupt signal to a currently running job.
         */

        organizeKeys();
        String jobId = this.keyValue.get(ReactorKeysEnum.JOB_ID.getKey());
        String jobGroup = this.keyValue.get(ReactorKeysEnum.JOB_GROUP.getKey());
        
        if (jobId == null) {
            throw new IllegalArgumentException("jobId is required");
        }

        if (jobGroup == null || jobGroup.trim().isEmpty()) {
            jobGroup = "defaultGroup";
        }

        User user = this.insight.getUser();
        if (!SecurityAdminUtils.userIsAdmin(user) &&
            !SecurityProjectUtils.userCanEditProject(user, jobGroup)) {
            throw new IllegalArgumentException("User does not have proper permissions to interrupt jobs");
        }
        
        Map<String, String> result = new HashMap<>();

        try {
            Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
            SchedulerDatabaseUtility.startScheduler(scheduler);

            JobKey jobKey = JobKey.jobKey(jobId, jobGroup);

            if (scheduler.checkExists(jobKey)) {
            	 try {
                scheduler.interrupt(jobKey);
                logger.info("Interrupt signal sent for job: {}", jobKey);
                result.put("status", "stopped");
                result.put("message", "Job " + jobId + " in group " + jobGroup + " has stopped.");
            } catch (SchedulerException se) {
            	logger.warn("Unable to interrupt job " + jobKey + ": " + se.getMessage());
                result.put("status", "error");
                result.put("message", "Unable to interrupt job: " + se.getMessage());
            }
            } else {
            	logger.info("Job " + jobKey + " not exist.");
                result.put("status", "not-found");
                result.put("message", "Job " + jobId + " in group " + jobGroup + " does not exist.");
            }

            return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.LIST_JOB);
        } catch (UnableToInterruptJobException e) {
            logger.error("Job does not support interruption: " + jobGroup + "/" + jobId, e);
        } catch (SchedulerException se) {
            logger.error(Constants.STACKTRACE, se);
        }

        return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.UNSCHEDULE_JOB);
    }
}

