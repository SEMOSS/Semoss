package prerna.rpa.quartz.jobs.insight;

import java.sql.Connection;
import java.util.UUID;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.rpa.RPAProps;
import prerna.rpa.config.JobConfigKeys;
import prerna.rpa.quartz.CommonDataKeys;
import prerna.sablecc2.reactor.scheduler.SchedulerH2DatabaseUtility;

public class RunPixelJobFromDB implements InterruptableJob {

	private static final Logger logger = LogManager.getLogger(RunPixelJobFromDB.class);

	public static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	public static final String OUT_INSIGHT_ID_KEY = CommonDataKeys.INSIGHT_ID;

	private String jobName;
	private String jobGroup;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		//if(!ClusterUtil.isSchedulerExecutor()) {
		//	return;
		//}
		// Get inputs
		JobDataMap dataMap = context.getMergedJobDataMap();
		jobName = context.getJobDetail().getKey().getName();
		jobGroup = context.getJobDetail().getKey().getGroup();

		String pixel = dataMap.getString(JobConfigKeys.PIXEL);
		String userAccess = RPAProps.getInstance().decrypt(dataMap.getString(JobConfigKeys.USER_ACCESS));

		// Execute job
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
		if (!pixel.endsWith(";")) {
			pixel = pixel + ";";
		}

		// make a random session id
		ThreadStore.setInsightId(insightId);
		ThreadStore.setSessionId("scheduledJob_" + UUID.randomUUID().toString());
		ThreadStore.setJobId(insightId);
		ThreadStore.setUser(user);
		
		logger.info("Running pixel: " + pixel);
		long start = System.currentTimeMillis();
		insight.runPixel(pixel);
		long end = System.currentTimeMillis();
		logger.info("Execution time: " + (end - start) / 1000 + " seconds.");

		// store execution time and date in SMSS_AUDIT_TRAIL table
		Connection connection = SchedulerH2DatabaseUtility.connectToSchedulerH2();
		SchedulerH2DatabaseUtility.insertIntoAuditTrailTable(connection, jobName, jobGroup, start, end, true);

		// Store outputs
		dataMap.put(OUT_INSIGHT_ID_KEY, insightId);
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		logger.warn("Received request to interrupt the " + jobName
				+ " job. However, there is nothing to interrupt for this job.");
	}
}
