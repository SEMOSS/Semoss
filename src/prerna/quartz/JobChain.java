package prerna.quartz;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.impl.matchers.GroupMatcher.jobGroupEquals;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import prerna.util.Constants;

public class JobChain implements org.quartz.Job {

	private static final Logger classLogger = LogManager.getLogger(JobChain.class);

	private String jobGroup;
	private JobDataMap dataMap;
	private Scheduler scheduler;
	private List<Class<? extends Job>> sequence;

	private int terminal;
	private int offset = 0;

	public static final String IN_SEQUENCE = "jobChainSequence";

	@SuppressWarnings("unchecked")
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		String name = context.getJobDetail().getKey().getName();
		jobGroup = context.getJobDetail().getKey().getGroup() + "JobChainGroup";
		dataMap = context.getMergedJobDataMap();
		scheduler = context.getScheduler();
		sequence = (List<Class<? extends Job>>) dataMap.get(IN_SEQUENCE);
		terminal = sequence.size();
		JobListener chainedJobListener = new ChainedJobListener(name + "JobListener", this);
		try {
			scheduler.getListenerManager().addJobListener(chainedJobListener, jobGroupEquals(jobGroup));
			executeElement();
		} catch (SchedulerException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	protected void executeElement() throws SchedulerException {
		if (offset < terminal) {
			try {
				Class<? extends Job> jobClass = sequence.get(offset);
				String jobName = jobClass.getName() + "At" + offset;
				JobDetail job = newJob(jobClass).withIdentity(jobName, jobGroup).usingJobData(dataMap).build();
				scheduler.addJob(job, true, true);
				System.out.println();
				System.out.println("Added the job " + jobName + " to " + jobGroup);
				System.out.println();
				JobKey jobKey = job.getKey();
				scheduler.triggerJob(jobKey);
				dataMap = scheduler.getJobDetail(jobKey).getJobDataMap();
				offset++;
			} catch (SchedulerException e) {
				// TODO what do I want to do with the error here?
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
	}

	public void setDataMap(JobDataMap dataMap) {
		this.dataMap = dataMap;
	}

}
