package prerna.rpa.quartz;

import static org.quartz.impl.StdSchedulerFactory.getDefaultScheduler;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;

public class SchedulerUtil {

	private SchedulerUtil() {
		throw new IllegalStateException("Utility class");
	}
	
	public static Scheduler getScheduler() throws SchedulerException {
		Scheduler scheduler = getDefaultScheduler();
		if (!scheduler.isStarted())	scheduler.start();
		return scheduler;
	}
	
	public static void shutdownScheduler(boolean waitForJobsToComplete) throws SchedulerException {
		Scheduler scheduler = getDefaultScheduler();
		if (scheduler.isStarted()) scheduler.shutdown(waitForJobsToComplete);
	}
		
}
