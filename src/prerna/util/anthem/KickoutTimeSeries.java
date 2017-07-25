package prerna.util.anthem;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.HashMap;
import java.util.Map;

import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.Trigger;

import prerna.algorithm.learning.unsupervised.anomaly.AnomalyDetector;
import prerna.algorithm.learning.unsupervised.anomaly.AnomalyDetector.AnomDirection;
import prerna.util.DIHelper;

public class KickoutTimeSeries extends AbstractKickoutWebWatcher {
	
	private JobDetail anomalyJob;
	
	private static final String TIME_COLUMN = DATE_KEY;
	private static final String SERIES_COLUMN = "NUM_ERRORS";
	private static final String AGGREGATE_FUNCTION = "sum";
	private static final double MAX_ANOMS = 0.1;
	private static final AnomDirection DIRECTION = AnomDirection.POSITIVE;
	private static final double ALPHA = 0.05;
	private static final int PERIOD = 7;
	private static final boolean KEEP_EXISTING_COLUMNS = false;
	
	private static final String TIMESERIES_ID_KEY = "TIMESERIES_ID";
	
	public KickoutTimeSeries() {
		
		// Derive the parameters passed into the job
		// First get the proper direction
		String stringDirection = AnomalyDetector.determineStringDirectionFromAnomDirection(DIRECTION);
		
		// Define the import pkql
		String importPkql = "data.import ( api: " + dbName + " . query ( [ c: " + TIMESERIES_ID_KEY + " , c: " + TIMESERIES_ID_KEY
				+ "__" + TIME_COLUMN + " , c: " + TIMESERIES_ID_KEY + "__" + SERIES_COLUMN + " ] ) ) ;";
		
		// Pass in data to the job
		// TODO 
//		anomalyJob = newJob(DetectAnomaliesJob.class)
//				.withIdentity("anomaly_detection", "kickout_timeseries")
//				.usingJobData("dbName", dbName)
//				.usingJobData("importPkql", importPkql)
//			    .usingJobData("timeColumn", TIME_COLUMN)
//			    .usingJobData("seriesColumn", SERIES_COLUMN)
//			    .usingJobData("aggregateFunction", AGGREGATE_FUNCTION)
//			    .usingJobData("maxAnoms", MAX_ANOMS)
//			    .usingJobData("direction", stringDirection)
//			    .usingJobData("alpha", ALPHA)
//			    .usingJobData("period", PERIOD)
//			    .usingJobData("keepExistingColumns", KEEP_EXISTING_COLUMNS)
//				.build();
	}
	
	@Override
	protected String giveDbName() {
		return DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_TS_DB_NAME");
	}
	
	@Override
	protected String givePropFile() {
		return DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_TS_PROP_FILE");
	}

	@Override
	protected String giveProcessedDbName() {
		return DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_TS_PROCESSED_DB_NAME");
	}

	@Override
	protected String giveXlsToCsvScript() {
		return DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_TS_XLS_CSV_SCRIPT");
	}

	@Override
	protected String giveXlsTempDir() {
		return DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_TS_XLS_TEMP_DIR");
	}

	@Override
	protected String giveCsvTempDir() {
		return DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_TS_CSV_TEMP_DIR");
	}

	@Override
	protected void initialize() {
		// Nothing else to initialize here that's not already done in AbstractKickoutWebWatcher
	}
	
	@Override
	protected Map<String, String> populateValueMap(String zipFileName, String excelFileName) {
		
		// Grab the system name from the excel file
		String systemName = excelFileName.substring(11, 13);
		
		// Grab the date and time from the zip file
		String date = zipFileName.substring(12, 22); // yyyy-MM-dd
		String time = zipFileName.substring(23, 31).replace('.', ':'); // hh:mm:ss
		String timestamp = date + " " + time; // yyyy-MM-dd hh:mm:ss
		String[] parsedDate = date.split("-"); // Y, M, D
		String[] parsedTime = time.split(":"); // h, m, s
		
		// Map to store data not found in the csv but specified in the prop file
		Map<String, String> valueMap = new HashMap<String, String>();
		
		// Needed for concatenated unique id
		// TODO map system to match names in other KO db, for now leave it as is
		valueMap.put(XLS_SYS_KEY, systemName);		
		valueMap.put(TIMESTAMP_KEY, timestamp);
		valueMap.put(TIMESERIES_ID_KEY, XLS_SYS_KEY + "+" + TIMESTAMP_KEY);
		
		// Needed for sql date and time objects
		valueMap.put(DATE_KEY, date);
		valueMap.put(TIME_KEY, time);
		
		// Needed for parsed date and time
		valueMap.put(YEAR_KEY, parsedDate[0]);
		valueMap.put(MONTH_KEY, parsedDate[1]);
		valueMap.put(DAY_KEY, parsedDate[2]);
		valueMap.put(HOUR_KEY, parsedTime[0]);
		valueMap.put(MINUTE_KEY, parsedTime[1]);
		valueMap.put(SECOND_KEY, parsedTime[2]);
		return valueMap;
	}

	@Override
	protected Map<String, String> populateTypeMap(String zipFileName, String excelFileName) {
		
		// Map to store the data types (defaults to string otherwise)
		// Uses SEMOSS UI types (see AbstractEngineCreator createSqlTypes)
		Map<String, String> typeMap = new HashMap<String, String>();
		
		// Needed for concatenated unique id
		typeMap.put(TIMESTAMP_KEY, TIMESTAMP_TYPE);
				
		// Needed for sql date and time objects
		typeMap.put(DATE_KEY, DATE_TYPE);
		typeMap.put(TIME_KEY, TIME_TYPE);
		
		// Needed for parsed date and time
		typeMap.put(YEAR_KEY, YEAR_TYPE);
		typeMap.put(MONTH_KEY, MONTH_TYPE);
		typeMap.put(DAY_KEY, DAY_TYPE);
		typeMap.put(HOUR_KEY, HOUR_TYPE);
		typeMap.put(MINUTE_KEY, MINUTE_TYPE);
		typeMap.put(SECOND_KEY, SECOND_TYPE);
		return typeMap;
	}
	
	@Override
	protected void ingestMetadataIntoSeparateDB(Map<String, String> valueMap, Map<String, String> typeMap, boolean createIndexes) {
		// Nothing to do here, don't want to ingest metadata into a separate db		
	}

	@Override
	protected void executeJobs() {
		try {
			scheduler.triggerJob(anomalyJob.getKey());
		} catch (SchedulerException e) {
			LOGGER.error("Failed to trigger anomaly detection");
			e.printStackTrace();
		}
	}
	
	@Override
	protected void scheduleJobs() {
		try {
			
			// First boolean is for replace
			// Second is storeNonDurableWhileAwaitingScheduling, from 
			// http://www.quartz-scheduler.org/api/2.2.1/org/quartz/Scheduler.html#addJob(org.quartz.JobDetail, boolean):
			// "With the storeNonDurableWhileAwaitingScheduling parameter set to
			// true, a non-durable job can be stored. Once it is scheduled, it
			// will resume normal non-durable behavior (i.e. be deleted once
			// there are no remaining associated triggers)."
//			scheduler.addJob(anomalyJob, true, true);
			
			// Create a new trigger that fires every 1 second forever
			// TODO
			Trigger trigger = newTrigger()
					.withIdentity("anomaly_detection_trigger", "kickout_timeseries")
					.startNow()
					.withSchedule(simpleSchedule()
							.withIntervalInSeconds(60 * 60 * 24)
							.repeatForever())
					.build();
			
			// Schedule our instance of HelloJob to fire using our trigger
			scheduler.scheduleJob(anomalyJob, trigger);
		} catch (SchedulerException e) {
			LOGGER.error("Failed to schedule anomaly detection");
			e.printStackTrace();
		}
	}
	
	// Test case
	public static void main(String[] args) {
		DIHelper.getInstance().loadCoreProp(System.getProperty("user.dir") + "/RDF_Map.prop");		
		KickoutTimeSeries kts = new KickoutTimeSeries();
		kts.setFolderToWatch(DIHelper.getInstance().getProperty("AnthemWebWatcher_DIR"));
		kts.setExtension(DIHelper.getInstance().getProperty("AnthemWebWatcher_EXT"));
		kts.setMonitor(new Object());
		new Thread(kts).start();
	}
	
}
