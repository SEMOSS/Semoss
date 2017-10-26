package prerna.rpa.config;

import org.quartz.JobDataMap;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.insight.Comparator;
import prerna.rpa.quartz.jobs.insight.OneColConditionJob;

public class OneColConditionJobConfig extends JobConfig {

	private JsonObject jobDefinition;
	
	public OneColConditionJobConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}
	
	@Override
	public JobDataMap getJobDataMap() throws Exception {
		JobDataMap jobDataMap = new JobDataMap();
		
		// The input for the frame will be added to the context by another job

		String columnHeader = getString(jobDefinition, OneColConditionJob.IN_COLUMN_HEADER_KEY);
		jobDataMap.put(OneColConditionJob.IN_COLUMN_HEADER_KEY, columnHeader);
		
		Comparator comparator = Comparator.getComparatorFromSymbol(getString(jobDefinition, OneColConditionJob.IN_COMPARATOR_KEY));
		jobDataMap.put(OneColConditionJob.IN_COMPARATOR_KEY, comparator);
	
		// Yes, technically the value can be any object,
		// but b/c there is no such thing as a java Object in json I am just doing string
		Object value = getString(jobDefinition, OneColConditionJob.IN_VALUE_KEY);
		jobDataMap.put(OneColConditionJob.IN_VALUE_KEY, value);
		
		return jobDataMap;
	}

}
