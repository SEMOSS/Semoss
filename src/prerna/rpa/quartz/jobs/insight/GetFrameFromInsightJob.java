package prerna.rpa.quartz.jobs.insight;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.rpa.quartz.CommonDataKeys;

public class GetFrameFromInsightJob implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(GetFrameFromInsightJob.class.getName());
	
	/** {@code String} */
	public static final String IN_INSIGHT_ID_KEY = CommonDataKeys.INSIGHT_ID;
	
	/** {@code ITableDataFrame} */
	public static final String OUT_FRAME_KEY = CommonDataKeys.FRAME;
		
	private String jobName;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		////////////////////
		// Get inputs
		////////////////////
		JobDataMap dataMap = context.getMergedJobDataMap();
		jobName = context.getJobDetail().getKey().getName();
		String insightId = dataMap.getString(IN_INSIGHT_ID_KEY);
		
		////////////////////
		// Do work
		////////////////////
		Insight insight = InsightStore.getInstance().get(insightId);
		ITableDataFrame frame = (ITableDataFrame) insight.getDataMaker();
		
		////////////////////
		// Store outputs
		////////////////////
		dataMap.put(OUT_FRAME_KEY, frame);
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOGGER.warn("Received request to interrupt the " + jobName + " job. However, there is nothing to interrupt for this job.");				
	}

}
