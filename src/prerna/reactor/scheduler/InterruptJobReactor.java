package prerna.reactor.scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
import prerna.reactor.ReactorExecutionTracker;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class InterruptJobReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(InterruptJobReactor.class);

	private static final ReactorExecutionTracker executionTracker = ReactorExecutionTracker.getInstance();

	public InterruptJobReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_ID.getKey(), ReactorKeysEnum.JOB_GROUP.getKey(),
				"reactorName", "threadId" };
	}

	@Override
	public NounMetadata execute() {
		if (Utility.schedulerForceDisable()) {
			throw new IllegalArgumentException("Scheduler is not enabled");
		}

		/**
		 * InterruptJob(jobName = ["sample_job_name"], jobGroup=["sample_job_group"]);
		 * InterruptJob(reactorName = ["LongRunningReactor"], threadId=[12345]);
		 *
		 */

		organizeKeys();
		String jobId = this.keyValue.get(ReactorKeysEnum.JOB_ID.getKey());
		String jobGroup = this.keyValue.get(ReactorKeysEnum.JOB_GROUP.getKey());
		String reactorName = this.keyValue.get("reactorName");
		String threadIdStr = this.keyValue.get("threadId");

		// Check if we're interrupting a specific reactor
		if (reactorName != null || threadIdStr != null) {
			return interruptSpecificReactor(reactorName, threadIdStr);
		}

		if (jobId == null) {
			throw new IllegalArgumentException("Either jobId or reactorName must be specified");
		}

		if (jobGroup == null || jobGroup.trim().isEmpty()) {
			jobGroup = "defaultGroup";
		}

		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user) && !SecurityProjectUtils.userCanEditProject(user, jobGroup)) {
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

		} catch (UnableToInterruptJobException e) {
			logger.error("Job does not support interruption: " + jobGroup + "/" + jobId, e);
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.LIST_JOB);
	}

	/**
	 * Interrupt a specific reactor by name or thread ID
	 */
	private NounMetadata interruptSpecificReactor(String reactorName, String threadIdStr) {
		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user) && !SecurityProjectUtils.userCanEditProject(user, "system")) {
			throw new IllegalArgumentException("User does not have proper permissions to interrupt reactors");
		}

		Map<String, String> result = new HashMap<>();

		try {
			Set<ReactorExecutionTracker.RunningReactorInfo> runningReactors = executionTracker.getAllRunningReactors();
			boolean foundAndInterrupted = false;

			for (ReactorExecutionTracker.RunningReactorInfo info : runningReactors) {
				boolean shouldInterrupt = false;

				// Match by reactor name if specified
				if (reactorName != null && info.getReactorName().equals(reactorName)) {
					shouldInterrupt = true;
				}

				// Match by thread ID if specified
				if (threadIdStr != null) {
					try {
						long targetThreadId = Long.parseLong(threadIdStr);
						if (info.getThreadId() == targetThreadId) {
							shouldInterrupt = true;
						}
					} catch (NumberFormatException e) {
						logger.warn("Invalid threadId format: " + threadIdStr);
					}
				}

				if (shouldInterrupt) {
					boolean interrupted = executionTracker.interruptReactor(info.getReactor());
					if (interrupted) {
						logger.info("Interrupted reactor: {} on thread {}", info.getReactorName(), info.getThreadId());
						result.put("status", "interrupted");
						result.put("message",
								"Interrupted reactor " + info.getReactorName() + " on thread " + info.getThreadId());
						result.put("reactorName", info.getReactorName());
						result.put("threadId", String.valueOf(info.getThreadId()));
						foundAndInterrupted = true;
						break;
					}
				}
			}

			if (!foundAndInterrupted) {
				String errorMsg = "No matching reactor found";
				if (reactorName != null) {
					errorMsg += " with name: " + reactorName;
				}
				if (threadIdStr != null) {
					errorMsg += " on thread: " + threadIdStr;
				}

				logger.info(errorMsg);
				result.put("status", "not-found");
				result.put("message", errorMsg);
			}

			return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.INTERRUPT_REACTOR);

		} catch (Exception e) {
			logger.error("Error interrupting reactor: " + e.getMessage(), e);
			result.put("status", "error");
			result.put("message", "Failed to interrupt reactor: " + e.getMessage());
			return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.ERROR);
		}
	}

}