package prerna.rpa.quartz;

import static org.quartz.impl.matchers.GroupMatcher.jobGroupEquals;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.UnableToInterruptJobException;

public class QuartzUtility {

	private static final Logger LOGGER = LogManager.getLogger(QuartzUtility.class.getName());
	
	public static final void removeAllJobsInGroup(Scheduler scheduler, String jobGroup) {
		LOGGER.info("Removing jobs in the " + jobGroup + " group.");
		for (JobKey jobKey : getJobKeysInGroup(scheduler, jobGroup)) {
			removeJob(scheduler, jobKey);
		}
	}
	
	public static final void removeJob(Scheduler scheduler, JobKey jobKey) {
		try {
			scheduler.deleteJob(jobKey);
			LOGGER.info("Removing " + jobKey.getName() + " job from the scheduler.");
		} catch (SchedulerException e) {
			LOGGER.error("Failed to remove " + jobKey.getName() + " job from the scheduler.", e);
		}
	}
	
	public static final void terminateAllJobsInGroup(Scheduler scheduler, String jobGroup) {
		LOGGER.info("Terminating jobs in the " + jobGroup + " group.");
		for (JobKey jobKey : getJobKeysInGroup(scheduler, jobGroup)) {
			terminateJob(scheduler, jobKey);
		}
	}
	
	public static final void terminateJob(Scheduler scheduler, JobKey jobKey) {
		try {
			scheduler.interrupt(jobKey);
			LOGGER.info("Terminating the " + jobKey.getName() + " job.");
		} catch (UnableToInterruptJobException e) {
			LOGGER.error("Unable to terminate the " + jobKey.getName() + " job.", e);
		}
	}
	
	public static final Set<JobKey> getJobKeysInGroup(Scheduler scheduler, String jobGroup) {
		Set<JobKey> jobKeysInGroup = new HashSet<JobKey>();
		try {
			jobKeysInGroup = scheduler.getJobKeys(jobGroupEquals(jobGroup));
		} catch (SchedulerException e) {
			LOGGER.error("Failed to retrieve jobs in the " + jobGroup + " group.", e);
		}
		return jobKeysInGroup;
	}
	
	// For testing
	public static final String composeCronForNowPlus(int seconds) {
		LocalDateTime now = LocalDateTime.now();
		now.plusSeconds(seconds);
		int hour = now.getHour();
		int minute = now.getMinute();
		int second = now.getSecond();
		StringBuilder cronStringBuilder = new StringBuilder();
		cronStringBuilder.append(String.format("%02d", second));
		cronStringBuilder.append(" ");
		cronStringBuilder.append(String.format("%02d", minute));
		cronStringBuilder.append(" ");
		cronStringBuilder.append(String.format("%02d", hour));
		cronStringBuilder.append("  1/1 * ? *");
		return cronStringBuilder.toString();
	}
	
	public static long minutesSinceStartTime(long startTimeMillis) {
		return (System.currentTimeMillis() - startTimeMillis)/60000;
	}
	
	public static long secondsSinceStartTime(long startTimeMillis) {
		return (System.currentTimeMillis() - startTimeMillis)/1000;
	}
	
}
