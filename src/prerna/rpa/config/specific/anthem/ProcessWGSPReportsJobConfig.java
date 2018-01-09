package prerna.rpa.config.specific.anthem;

import com.google.gson.JsonObject;

import prerna.rpa.config.JobConfig;
import prerna.rpa.config.ParseConfigException;
import prerna.rpa.quartz.jobs.reporting.kickout.specific.anthem.ProcessWGSPReportsJob;

public class ProcessWGSPReportsJobConfig extends JobConfig {
		
	public ProcessWGSPReportsJobConfig(JsonObject jobDefinition) {
		super(jobDefinition);
	}
	
	@Override
	protected void populateJobDataMap() throws ParseConfigException {
		
		// Jedis prefix
		putString(ProcessWGSPReportsJob.IN_PREFIX_KEY);
		
		// Report directory or shared drive
		putString(ProcessWGSPReportsJob.IN_REPORT_DIRECTORY_KEY);
		
		// Parse semicolon-delimited list of systems to ignore
		putStringSet(ProcessWGSPReportsJob.IN_IGNORE_SYSTEMS_KEY);
		
		// Parse ignore-before date
		putDate(ProcessWGSPReportsJob.IN_IGNORE_BEFORE_DATE_KEY);
	}
	
}
