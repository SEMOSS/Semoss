package prerna.rpa.quartz.jobs.insight;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.rpa.RPAProps;
import prerna.rpa.config.JobConfigKeys;
import prerna.rpa.quartz.CommonDataKeys;

public class RunPixelJob implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(RunPixelJob.class.getName());
	
	/** {@code String} */
	public static final String IN_PIXEL_KEY = RunPixelJob.class + ".pixel";
	
	/** {@code String} */
	public static final String OUT_INSIGHT_ID_KEY = CommonDataKeys.INSIGHT_ID;
	
	private String jobName;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		////////////////////
		// Get inputs
		////////////////////
		JobDataMap dataMap = context.getMergedJobDataMap();
		jobName = context.getJobDetail().getKey().getName();
		String pixel = dataMap.getString(IN_PIXEL_KEY);
		String userAccess = RPAProps.getInstance().decrypt(dataMap.getString(JobConfigKeys.USER_ACCESS));
		
		////////////////////
		// Do work
		////////////////////
		Insight insight = new Insight();
		
		// Add user info to the insight
		User user = new User();
		String[] accessPairs = userAccess.split(",");
		for (String accessPair : accessPairs) {
			String[] providerAndId = accessPair.split(":");
			
			// Get the auth provider
			AuthProvider provider = AuthProvider.valueOf(providerAndId[0]);
			
			// Get the id
			String id = providerAndId[1];
			
			// Create the access token
			AccessToken token = new AccessToken();
			token.setProvider(provider);
			token.setId(id);
			
			user.setAccessToken(token);
		}
		insight.setUser(user);
		
		String insightId = InsightStore.getInstance().put(insight);
		if(!pixel.endsWith(";")) {
			pixel = pixel + ";";
		}
		LOGGER.info("Running pixel: " + pixel);
		long start = System.currentTimeMillis();
		insight.runPixel(pixel);
		long end = System.currentTimeMillis();
		LOGGER.info("Execution time: " + (end - start)/1000 + " seconds.");
		
		////////////////////
		// Store outputs
		////////////////////
		dataMap.put(OUT_INSIGHT_ID_KEY, insightId);
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOGGER.warn("Received request to interrupt the " + jobName + " job. However, there is nothing to interrupt for this job.");		
	}

}
