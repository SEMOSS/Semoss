package prerna.quartz;

import java.util.Vector;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.util.Utility;

public class GetInsightJob implements org.quartz.Job {

	public static final String IN_ENGINE_NAME_KEY = "createInsightEngineName";

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		// Get inputs
		JobDataMap dataMap = context.getMergedJobDataMap();
		String rdbmsId = dataMap.getString(LinkedDataKeys.RDBMS_ID);
		String engineName = dataMap.getString(IN_ENGINE_NAME_KEY);

		// Do work
		IEngine engine = Utility.getEngine(engineName);
		Vector<Insight> ins = engine.getInsight(rdbmsId);
		if(ins == null || ins.isEmpty()) {
			// throw an error
		}
		
		// when we grab from the engine, the insight is loaded
		// with the recipe, but it is not executed yet!!!
		Insight in = ins.get(0);
		// we need to store this insight
		// so that we can use it externally from other jobs
		InsightStore.getInstance().put(in);
		// we want to execute the recipe so the data is all created
		in.reRunPixelInsight(false);
		// Store outputs
		dataMap.put(LinkedDataKeys.INSIGHT_ID, in.getInsightId());

		System.out.println("Created insight with the id " + in.getInsightId() + " using the following recipe: ");
	}

}
