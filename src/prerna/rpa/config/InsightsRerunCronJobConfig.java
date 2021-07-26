//package prerna.rpa.config;
//
//import com.google.gson.JsonObject;
//
//import prerna.rpa.quartz.jobs.insight.InsightsRerunCronJob;
//
//public class InsightsRerunCronJobConfig extends JobConfig {
//		
//	public InsightsRerunCronJobConfig(JsonObject jobDefinition) {
//		super(jobDefinition);
//	}
//
//	@Override
//	public void populateJobDataMap() throws ParseConfigException {
//		putString(InsightsRerunCronJob.ENGINES_KEY);
//	}
//
//}
