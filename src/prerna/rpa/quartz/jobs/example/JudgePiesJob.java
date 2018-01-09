package prerna.rpa.quartz.jobs.example;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import prerna.rpa.quartz.BatchedJobOutput;
import prerna.rpa.quartz.CommonDataKeys;

public class JudgePiesJob implements org.quartz.InterruptableJob {
	
	private static final Logger LOGGER = LogManager.getLogger(JudgePiesJob.class.getName());
	
	/** {@code Map<String, BatchedJobOutput>} - (pie identifier, (JobDataMap with pie's condition, whether the job completed)) */
	public static final String IN_PIES_DATA = CommonDataKeys.BATCH_OUTPUT_MAP;
		
	/** {@code String} - the winner of the baking contest */
	public static final String OUT_PIE_WINNER = JudgePiesJob.class + ".pieWinner";
	
	private String jobName;
	
	@SuppressWarnings("unchecked")
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		////////////////////
		// Get inputs
		////////////////////
		jobName = context.getJobDetail().getKey().getName();
		JobDataMap dataMap = context.getMergedJobDataMap();
		Map<String, BatchedJobOutput> batchData = (Map<String, BatchedJobOutput>) dataMap.get(IN_PIES_DATA);

		////////////////////
		// Do work
		////////////////////
		// Judge the pies
		String pieWinner = "tie";
		for (Entry<String, BatchedJobOutput> entry : batchData.entrySet()) {
			String pie = entry.getKey();
			BatchedJobOutput output = entry.getValue();
			JobDataMap pieData = output.getJobDataMap();
			boolean qualified = output.wasSuccessful();
			if (!qualified) {
				LOGGER.info("The baker of pie " + pie + " was disqualified");
			} else {
				String pieCondition = pieData.getString(BakePieJob.OUT_PIE_CONDITION_KEY);
				LOGGER.info("The pie " + pie + " was judged to be " + pieCondition);
				
				// Winner happens to favor the last one in the list
				// whatever
				if (pieCondition.equals("just right")) {
					pieWinner = pie;
				}
			}
		}
		
		////////////////////
		// Store outputs
		////////////////////
		dataMap.put(OUT_PIE_WINNER, pieWinner);
		
		LOGGER.info("The baker of pie " + pieWinner + " is the winner!");
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOGGER.warn("Received request to interrupt the " + jobName + " job. However, there is nothing to interrupt for this job.");
	}
	
}
