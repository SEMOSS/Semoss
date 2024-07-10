//package prerna.quartz;
//
//import static org.quartz.impl.StdSchedulerFactory.getDefaultScheduler;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Properties;
//
//import javax.mail.Session;
//
//import static org.quartz.JobBuilder.newJob;
//
//import org.quartz.Job;
//import org.quartz.JobDataMap;
//import org.quartz.JobDetail;
//import org.quartz.Scheduler;
//import org.quartz.SchedulerException;
//
//import prerna.algorithm.learning.unsupervised.anomaly.AnomalyDetector.AnomDirection;
//import prerna.engine.api.IEngine;
//import prerna.quartz.specific.anthem.DetermineIfAnomalyJob;
//import prerna.util.AbstractFileWatcher;
//import prerna.util.Utility;
//
//public class WebWatcherTest extends AbstractFileWatcher {
//
//	private static final String ENGINE_NAME = "TS_Anthem_KO";
//	private static final String TIMESERIES_ID_KEY = "TIMESERIES_ID";
//	private static final String TIME_COLUMN = "KO_DATE";
//	private static final String SERIES_COLUMN = "NUM_ERRORS";
//
//	private static final String SMTP_SERVER = "127.0.0.1";
//	private static final int SMTP_PORT = 25;
//
//	@Override
//	public void loadFirst() {
//		// Not really a web watcher, just want to run the test while the server
//		// is running
//	}
//
//	@Override
//	public void process(String fileName) {
//		// Not really a web watcher, just want to run the test while the server
//		// is running
//	}
//
//	@Override
//	public void run() {
//		// Wait until the engine is loaded to try and run a job on it
//		boolean loadingEngine = true;
//		while (loadingEngine) {
//			IEngine engine = Utility.getEngine(ENGINE_NAME);
//			if (engine == null) {
//				try {
//					Thread.sleep(1000);
//				} catch (InterruptedException e) {
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//			} else {
//				loadingEngine = false;
//			}
//		}
//		System.out.println("Testing out the job sequence");
//
//		// Logic here:
//		// Chain 1) Import TS data, get data from insight, detect anomalies,
//		// determine if anomaly, if anomaly then chain 2
//		// Chain 2) Compose the email, send the email
//		// Need to create 2) first because it is passed into chain 1
//
//		//////////////////////////////////////////////////
//		// Chain 2
//		//////////////////////////////////////////////////
//
//		List<Class<? extends Job>> chain2Sequence = new ArrayList<Class<? extends Job>>();
//		// chain2Sequence.add(ComposeTSAnomalyEmailJob.class);
//		chain2Sequence.add(SendEmailJob.class);
//
//		//////////////////////////////
//		// Define the initial data map
//		//////////////////////////////
//		JobDataMap chain2DataMap = new JobDataMap();
//
//		// The job sequence
//		chain2DataMap.put(JobChain.IN_SEQUENCE, chain2Sequence);
//
//		// Email params
//		Properties sessionProps = new Properties();
//		sessionProps.put("mail.smtp.host", SMTP_SERVER);
//		sessionProps.put("mail.smtp.port", Integer.toString(SMTP_PORT));
//		Session session = Session.getInstance(sessionProps);
//		chain2DataMap.put(SendEmailJob.IN_FROM_KEY, "adaclarke@deloitte.com");
//		chain2DataMap.put(SendEmailJob.IN_TO_KEY, new String[] { "adaclarke@deloitte.com" });
//		chain2DataMap.put(SendEmailJob.IN_SUBJECT_KEY, "hello world");
//		chain2DataMap.put(SendEmailJob.IN_BODY_KEY, "this is an automated message from SEMOSS");
//		chain2DataMap.put(SendEmailJob.IN_BODY_IS_HTML_KEY, false);
//		chain2DataMap.put(SendEmailJob.IN_SESSION_KEY, session);
//
//		JobDetail jobChain2 = newJob(JobChain.class).withIdentity("jobChain2", "tsAnomaly").usingJobData(chain2DataMap)
//				.build();
//
//		//////////////////////////////////////////////////
//		// Chain 1
//		//////////////////////////////////////////////////
//
//		// Specify the chain of jobs
//		List<Class<? extends Job>> chain1Sequence = new ArrayList<Class<? extends Job>>();
//		chain1Sequence.add(CreateInsightJob.class);
//		chain1Sequence.add(GetDataFromInsightJob.class);
//		chain1Sequence.add(DetectAnomaliesJob.class);
//		chain1Sequence.add(DetermineIfAnomalyJob.class);
//		chain1Sequence.add(IfJob.class);
//
//		//////////////////////////////
//		// Define the initial data map
//		//////////////////////////////
//		JobDataMap chain1DataMap = new JobDataMap();
//
//		// The job sequence
//		chain1DataMap.put(JobChain.IN_SEQUENCE, chain1Sequence);
//
//		// Import params
//		String importRecipe = "data.import ( api: " + ENGINE_NAME + " . query ( [ c: " + TIMESERIES_ID_KEY + " , c: "
//				+ TIMESERIES_ID_KEY + "__" + TIME_COLUMN + " , c: " + TIMESERIES_ID_KEY + "__" + SERIES_COLUMN
//				+ " ] ) ) ;";
//
//		// For testing add another pkql
//		importRecipe += "panel[0].viz ( Line , [ label= c: " + TIME_COLUMN + " , value= m: Average ( [ c: "
//				+ SERIES_COLUMN + " ] [ c: " + TIME_COLUMN + " ] ) ] , { \"offset\" : 0 , \"limit\" : 1000 } ) ; ";
//		chain1DataMap.put(CreateInsightJob.IN_RECIPE_KEY, importRecipe);
//
//		// Anomaly params
//		chain1DataMap.put(DetectAnomaliesJob.IN_TIME_COLUMN_KEY, TIME_COLUMN);
//		chain1DataMap.put(DetectAnomaliesJob.IN_SERIES_COLUMN_KEY, SERIES_COLUMN);
//		chain1DataMap.put(DetectAnomaliesJob.IN_AGGREGATE_FUNCTION_KEY, "sum");
//		chain1DataMap.put(DetectAnomaliesJob.IN_MAX_ANOMS_KEY, 0.1);
//		chain1DataMap.put(DetectAnomaliesJob.IN_DIRECTION_KEY, AnomDirection.POSITIVE);
//		chain1DataMap.put(DetectAnomaliesJob.IN_ALPHA_KEY, 0.05);
//		chain1DataMap.put(DetectAnomaliesJob.IN_PERIOD_KEY, 7);
//		chain1DataMap.put(DetectAnomaliesJob.IN_KEEP_EXISTING_COLUMNS_KEY, false);
//
//		// The second chain for notification if anomaly
//		chain1DataMap.put(IfJob.IN_IF_TRUE_JOB, jobChain2);
//
//		//////////////////////////////
//		// Create the job chain
//		//////////////////////////////
//		JobDetail jobChain1 = newJob(JobChain.class).withIdentity("jobChain1", "tsAnomaly").usingJobData(chain1DataMap)
//				.build();
//
//		//////////////////////////////////////////////////
//		// Execute the work
//		//////////////////////////////////////////////////
//		// Only need to kick-off the first job chain
//		try {
//			Scheduler scheduler = getDefaultScheduler();
//			scheduler.start();
//			scheduler.addJob(jobChain1, true, true);
//			scheduler.triggerJob(jobChain1.getKey());
//		} catch (SchedulerException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//	}
//
//}
