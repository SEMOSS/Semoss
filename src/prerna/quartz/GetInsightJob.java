package prerna.quartz;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import prerna.om.Insight;
import prerna.util.insight.InsightUtility;

public class GetInsightJob implements org.quartz.Job {

	public static final String IN_ENGINE_NAME = CommonDataKeys.ENGINE_NAME;
	public static final String IN_RDBMS_ID = "getInsightRdbmsId";
	
	public static final String OUT_INSIGHT_ID_KEY = CommonDataKeys.INSIGHT_ID;
		
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		// Get inputs
		JobDataMap dataMap = context.getMergedJobDataMap();
		String engineName = dataMap.getString(IN_ENGINE_NAME);
		String rdbmsId = dataMap.getString(IN_RDBMS_ID);
		
		// Do work
		Insight insight = InsightUtility.getSavedInsight(engineName, rdbmsId);
		
		// Store outputs
		dataMap.put(OUT_INSIGHT_ID_KEY, insight.getInsightId());
		
		System.out.println();
		System.out.println("Retrieved saved insight from the engine " + engineName + " with the RDBMS ID " + rdbmsId);
		System.out.println("Its insight ID is: " + insight.getInsightId());
		System.out.println();
	}
	
}
