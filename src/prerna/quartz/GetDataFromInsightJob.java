package prerna.quartz;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
import prerna.om.InsightStore;

public class GetDataFromInsightJob implements org.quartz.Job {

	public static final String IN_INSIGHT_ID_KEY = CommonDataKeys.INSIGHT_ID;

	public static final String OUT_DATA_FRAME_KEY = CommonDataKeys.DATA_FRAME;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		// Get inputs
		JobDataMap dataMap = context.getMergedJobDataMap();
		String insightID = dataMap.getString(IN_INSIGHT_ID_KEY);

		// Do work
		Insight insight = InsightStore.getInstance().get(insightID);
		ITableDataFrame data = (ITableDataFrame) insight.getDataMaker();

		// Store outputs
		dataMap.put(OUT_DATA_FRAME_KEY, data);

		System.out.println();
		System.out.println("Retrieved data from the insight with id " + insightID);
		System.out.println();
	}

}
