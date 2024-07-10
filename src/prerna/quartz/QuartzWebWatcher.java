package prerna.quartz;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.AbstractFileWatcher;

@Deprecated
public class QuartzWebWatcher extends AbstractFileWatcher {

	private static final String ACTION_TYPE_EMAIL = "email";
	private static final String SEMOSS_JOB_GROUP = "semossJobs";
	
	protected static final Logger LOGGER = LogManager.getLogger(QuartzWebWatcher.class.getName());

	@Override
	public void loadFirst() {
		String[] fileNames = new File(folderToWatch).list();
		for (String fileName : fileNames) {
			if (needToProcess(fileName)) {
				process(fileName);
			}
		}		
	}

	@Override
	public void process(String fileName) {
		/*
		 * COMMENTING OUT THE LOGIC SINCE WE ARE SHIFTING TO RDBMS
		 */
//		// Get the json string from a file
//		String jsonString;
//		try {
//			jsonString = new String(Files.readAllBytes(Paths.get(folderToWatch + "/" + fileName)), "UTF-8");
//		} catch (IOException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//			LOGGER.error("Unable to read the file " + fileName);
//			return;
//		}
//		
//		// Parse the json string into an object
//		JSONObject json;
//		try {
//			json = new JSONObject(jsonString);
//		} catch (JSONException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//			LOGGER.error("Unable to parse json object from " + fileName);
//			return;
//		}
//		
//		// Add data from the json object into the job data map
//		try {
//			JSONObject job = json.getJSONObject("schedule").getJSONObject("job");
//			JSONObject values = job.getJSONObject("values");
//			JSONObject action = job.getJSONObject("action");
//			JSONObject trigger = json.getJSONObject("schedule").getJSONObject("trigger");
//
//			String rdbmsId = values.getString("rdbms-id");
//			String engine = values.getString("engine");
//			String returnColumn = values.getString("return-column");
//			String compareColumn = values.getString("compare-column");
//			String comparator = values.getString("comparator");
//			String valueType = values.getString("value-type");
//			String value = values.getString("value");
////			String valuePkql = values.getString("value-pkql");
//			
//			// Setup the job chain
//			JobDataMap jobChainMap = new JobDataMap();
//			List<Class<? extends Job>> jobSequence = new ArrayList<Class<? extends Job>>();
//			
//			// WaitForEngineToLoadJob
//			jobSequence.add(WaitForEngineToLoadJob.class);
//			jobChainMap.put(WaitForEngineToLoadJob.IN_ENGINE_NAME_KEY, engine);
//			
//			// GetInsightJob
//			jobSequence.add(GetInsightJob.class);
//			jobChainMap.put(LinkedDataKeys.RDBMS_ID, rdbmsId);
//			jobChainMap.put(GetInsightJob.IN_ENGINE_NAME_KEY, engine);
//			
//			// GetDataFromInsightJob
//			jobSequence.add(GetDataFromInsightJob.class);
//		
//			// PrintDataToConsoleJob
////			jobSequence.add(PrintDataToConsoleJob.class);
//			
//			//CheckCriteriaJob
//			jobSequence.add(CheckCriteriaJob.class);
//			jobChainMap.put(CheckCriteriaJob.IN_RETURN_COLUMN, returnColumn);
//			jobChainMap.put(CheckCriteriaJob.IN_COMPARE_COLUMN, compareColumn);
//			jobChainMap.put(CheckCriteriaJob.IN_COMPARATOR, comparator);
//			jobChainMap.put(CheckCriteriaJob.IN_VALUE_TYPE, valueType);
//			jobChainMap.put(CheckCriteriaJob.IN_VALUE, value);
//			
//			// TODO make the comparison
//			
//			// TODO IfJob <- action job needs to be passed in as well
//					
//			// Add the sequence
//			jobChainMap.put(JobChain.IN_SEQUENCE, jobSequence);
//			
//			// The job chain itself
//			JobDetail jobChain = newJob(JobChain.class).withIdentity("jobChain", SEMOSS_JOB_GROUP).usingJobData(jobChainMap).build();
//	
//			// Trigger the job chain
//			try {
//				String dateValue = trigger.getString("start-date");
//				Date date = new Date();
//				SimpleDateFormat format = new SimpleDateFormat(
//			            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
//				format.setTimeZone(TimeZone.getTimeZone("UTC"));
//				if (dateValue != null) {
//					date = format.parse(dateValue);
//				}
//				Integer frequency = Integer.parseInt(trigger.getString("frequency"));
//				
//				Trigger jobTrigger = newTrigger()
//						.withIdentity("defaultTrigger")
//						.startAt(date)
//						.withSchedule(simpleSchedule()
//								.withIntervalInHours(frequency)
//								.repeatForever())
//						.build();
//				
//				Scheduler scheduler = getDefaultScheduler();
//				scheduler.start();
//				scheduler.scheduleJob(jobChain, jobTrigger);
////				scheduler.triggerJob(jobChain.getKey());
//			} catch (SchedulerException e) {
//				classLogger.error(Constants.STACKTRACE, e);
//			} catch (ParseException e) {
//				// TODO Auto-generated catch block
//				classLogger.error(Constants.STACKTRACE, e);
//			}
//			
//			String actionType = action.getString("actiontype");
//			System.out.println(actionType);
//									
//			if (actionType.equals(ACTION_TYPE_EMAIL)) {
//				JSONObject email = action.getJSONObject(ACTION_TYPE_EMAIL);
//				String emailAddress = email.getString("email-address");
//				System.out.println(emailAddress);
//			}
//			
//		} catch (JSONException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//			LOGGER.error("Unable to find necessary key in " + fileName);
//			return;
//		}
				

		// Construct the job chain
		
		// Create a trigger using json data
		
		// Schedule the job chain using the trigger
	}
	
	private boolean needToProcess(String fileName) {
		return fileName.endsWith(extension);
	}

	@Override
	public void run() {
		LOGGER.info("Starting SMSSWebWatcher thread");
		loadFirst();
		super.run();
	}

}
