package prerna.rpa.quartz;

import static org.quartz.impl.StdSchedulerFactory.getDefaultScheduler;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;

public class SchedulerUtil {

	// TODO may want to have our own (not default) scheduler - maybe a singleton
	// But this gets the job done for now
	public static Scheduler getScheduler() throws SchedulerException {
		Scheduler scheduler = getDefaultScheduler();
		if (!scheduler.isStarted())
			scheduler.start();
		return scheduler;
	}

}
