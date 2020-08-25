package prerna.sablecc2.reactor.scheduler;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;

public class ExecuteScheduledJobReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(ExecuteScheduledJobReactor.class);

	public ExecuteScheduledJobReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.JOB_NAME.getKey(), ReactorKeysEnum.JOB_GROUP.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();

		// Get inputs
		String jobName = this.keyValue.get(this.keysToGet[0]);
		String jobGroup = this.keyValue.get(this.keysToGet[1]);
		
		JobKey jobKey = JobKey.jobKey(jobName, jobGroup);
		Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
		try {
			if (scheduler.checkExists(jobKey)) {
				scheduler.triggerJob(jobKey);
			} else {
				throw new IllegalArgumentException("Could not find job with name = " + jobName + " and group = " + jobGroup);
			}
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
