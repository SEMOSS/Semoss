package prerna.rpa.quartz.jobs.insight;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import com.google.gson.JsonObject;

import prerna.engine.api.IHeadersDataRow;
import prerna.rpa.config.ContextualJobConfig;
import prerna.rpa.quartz.BatchedJobInput;
import prerna.rpa.quartz.CommonDataKeys;

public class ConditionalJob implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(ConditionalJob.class.getName());
	
	/** {@code com.google.gson.JsonObject} */
	public static final String IN_JOB_DEFINITION_KEY = ConditionalJob.class + ".conditionalJob";
	
	/** {@code Set<IHeadersDataRow>} - the rows satisfying the condition */
	public static final String IN_ROWS_SATISFYING_CONDITION_KEY = CommonDataKeys.ROWS;
	
	/** {@code Map<String, BatchedJobInput>} - (identifier, (JobDataMap, Class<? extends InterruptableJob>)) */
	public static final String OUT_BATCH_INPUT_MAP_KEY = CommonDataKeys.BATCH_INPUT_MAP;
	
	private String jobName;
	
	@SuppressWarnings("unchecked")
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		////////////////////
		// Get inputs
		////////////////////
		JobDataMap jobDataMap = context.getMergedJobDataMap();
		jobName = context.getJobDetail().getKey().getName();
		JsonObject jobDefinition = (JsonObject) jobDataMap.get(IN_JOB_DEFINITION_KEY);
		Set<IHeadersDataRow> rowsSatisfyingCondition = (Set<IHeadersDataRow>) jobDataMap.get(IN_ROWS_SATISFYING_CONDITION_KEY);
		
		////////////////////
		// Do work
		////////////////////
		
		// Create a batch of sub jobs for each row satisfying the condition
		Map<String, BatchedJobInput> batchInputMap = new HashMap<String, BatchedJobInput>();
		int counter = 0;
		for (IHeadersDataRow row : rowsSatisfyingCondition) {
			
			// For naming the jobs
			counter++;
			
			// Construct the contextualData based on the row (map format header -> value)
			// This is a little easier to parse
			Map<String, Object> contextualData = new HashMap<String, Object>();
			String[] headers = row.getHeaders();
			Object[] values = row.getValues();
			for (int i = 0; i < row.getRecordLength(); i++) {
				contextualData.put(headers[i], values[i]);
			}
			ContextualJobConfig contextualJobConfig = ContextualJobConfig.initialize(jobDefinition);
			contextualJobConfig.accept(contextualData);
			try {
				BatchedJobInput batchJobInput = new BatchedJobInput(contextualJobConfig.getJobDataMap(), contextualJobConfig.getJobClass());
				batchInputMap.put(contextualJobConfig.getJobName() + counter, batchJobInput);
			} catch (Exception e) {
				String getJobDataMapExceptionMessage = "An exception occurred parsing a job data map in the " + jobName + " conditional job.";
				LOGGER.error(getJobDataMapExceptionMessage);
				throw new JobExecutionException(getJobDataMapExceptionMessage, e);
			}
			
		}
		
		////////////////////
		// Store outputs
		////////////////////
		jobDataMap.put(OUT_BATCH_INPUT_MAP_KEY, batchInputMap);
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOGGER.warn("Received request to interrupt the " + jobName + " job. However, there is nothing to interrupt for this job.");
	}

}
