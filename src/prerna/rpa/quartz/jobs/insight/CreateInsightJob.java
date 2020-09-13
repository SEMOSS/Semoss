package prerna.rpa.quartz.jobs.insight;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.rpa.quartz.CommonDataKeys;

public class CreateInsightJob implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(CreateInsightJob.class.getName());
	
	/** {@code String} */
	public static final String IN_PIXEL_KEY = CreateInsightJob.class + ".pixel";
	
	/** {@code String} */
	public static final String OUT_INSIGHT_ID_KEY = CommonDataKeys.INSIGHT_ID;
	
	private String jobName;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		////////////////////
		// Get inputs
		////////////////////
		JobDataMap dataMap = context.getMergedJobDataMap();
		jobName = context.getJobDetail().getKey().getName();
		String pixel = dataMap.getString(IN_PIXEL_KEY);
		
		////////////////////
		// Do work
		////////////////////
		Insight insight = new Insight();
		String insightId = InsightStore.getInstance().put(insight);
		if(!pixel.endsWith(";")) {
			pixel = pixel + ";";
		}
		LOGGER.info("Running pixel: " + pixel);
		long start = System.currentTimeMillis();
		insight.runPixel(pixel);
		long end = System.currentTimeMillis();
		LOGGER.info("Execution time: " + (end - start)/1000 + " seconds.");
		
		////////////////////
		// Store outputs
		////////////////////
		dataMap.put(OUT_INSIGHT_ID_KEY, insightId);
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOGGER.warn("Received request to interrupt the " + jobName + " job. However, there is nothing to interrupt for this job.");		
	}

}
