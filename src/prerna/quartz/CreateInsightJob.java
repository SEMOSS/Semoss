package prerna.quartz;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import prerna.om.Insight;
import prerna.util.insight.InsightUtility;

public class CreateInsightJob implements org.quartz.Job {

	public static final String IN_RECIPE_KEY = "createInsightRecipe";
	public static final String IN_ENGINE_NAME_KEY = "createInsightEngineName";

	public static final String OUT_INSIGHT_ID_KEY = LinkedDataKeys.INSIGHT_ID;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		// Get inputs
		JobDataMap dataMap = context.getMergedJobDataMap();
		String recipe = dataMap.getString(IN_RECIPE_KEY);
		String engineName = dataMap.getString(IN_ENGINE_NAME_KEY);

		// Do work
		Insight insight = InsightUtility.createInsight(engineName);
		InsightUtility.runPkql(insight, recipe);

		// Store outputs
		dataMap.put(OUT_INSIGHT_ID_KEY, insight.getInsightId());

		System.out.println();
		System.out.println("Created insight with the id " + insight.getInsightId() + " using the following recipe: ");
		System.out.println(recipe);
		System.out.println();
	}

}
