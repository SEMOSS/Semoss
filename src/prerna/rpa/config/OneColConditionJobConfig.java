package prerna.rpa.config;

import com.google.gson.JsonObject;

import prerna.rpa.quartz.jobs.insight.Comparator;
import prerna.rpa.quartz.jobs.insight.OneColConditionJob;

public class OneColConditionJobConfig extends JobConfig {
	
	public OneColConditionJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}
	
	@Override
	public void populateJobDataMap() throws ParseConfigException {		
		// The input for the frame will be added to the context by another job

		putString(OneColConditionJob.IN_COLUMN_HEADER_KEY);
		
		Comparator comparator = Comparator.getComparatorFromSymbol(getString(OneColConditionJob.IN_COMPARATOR_KEY));
		jobDataMap.put(OneColConditionJob.IN_COMPARATOR_KEY, comparator);
	
		// Yes, technically the value can be any object,
		// but b/c there is no such thing as a java Object in json I am just doing string
		Object value = getString(OneColConditionJob.IN_VALUE_KEY);
		jobDataMap.put(OneColConditionJob.IN_VALUE_KEY, value);
	}

}
