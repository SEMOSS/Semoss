//package prerna.quartz;
//
//import org.quartz.JobDataMap;
//import org.quartz.JobExecutionContext;
//import org.quartz.JobExecutionException;
//
//import prerna.algorithm.api.ITableDataFrame;
//import prerna.algorithm.learning.r.RRoutineException;
//import prerna.algorithm.learning.unsupervised.anomaly.AnomalyDetector;
//import prerna.algorithm.learning.unsupervised.anomaly.AnomalyDetector.AnomDirection;
//import prerna.sablecc.PKQLRunner;
//
//public class DetectAnomaliesJob implements org.quartz.Job {
//
//	public static final String IN_DATA_FRAME_KEY = CommonDataKeys.DATA_FRAME;
//	public static final String IN_TIME_COLUMN_KEY = "anomalyTimeColumn";
//	public static final String IN_SERIES_COLUMN_KEY = "anomalySeriesColumn";
//	public static final String IN_AGGREGATE_FUNCTION_KEY = "anomalyAggregateFunction";
//	public static final String IN_MAX_ANOMS_KEY = "anomalyMaxAnoms";
//	public static final String IN_DIRECTION_KEY = "anomalyDirection";
//	public static final String IN_ALPHA_KEY = "anomalyAlpha";
//	public static final String IN_PERIOD_KEY = "anomalyPeriod";
//	public static final String IN_KEEP_EXISTING_COLUMNS_KEY = "anomalyKeepExistingColumns";
//
//	public static final String OUT_DATA_FRAME_KEY = CommonDataKeys.DATA_FRAME;
//
//	@Override
//	public void execute(JobExecutionContext context) throws JobExecutionException {
//
//		// Get inputs
//		JobDataMap dataMap = context.getMergedJobDataMap();
//		ITableDataFrame data = (ITableDataFrame) dataMap.get(IN_DATA_FRAME_KEY);
//		String timeColumn = dataMap.getString(IN_TIME_COLUMN_KEY);
//		String seriesColumn = dataMap.getString(IN_SERIES_COLUMN_KEY);
//		String aggregateFunction = dataMap.getString(IN_AGGREGATE_FUNCTION_KEY);
//		double maxAnoms = dataMap.getDouble(IN_MAX_ANOMS_KEY);
//		AnomDirection direction = (AnomDirection) dataMap.get(IN_DIRECTION_KEY);
//		double alpha = dataMap.getDouble(IN_ALPHA_KEY);
//		int period = dataMap.getInt(IN_PERIOD_KEY);
//		boolean keepExistingColumns = dataMap.getBoolean(IN_KEEP_EXISTING_COLUMNS_KEY);
//
//		// Do work
//		PKQLRunner pkql = new PKQLRunner();
//
//		// Detect anomalies
//		AnomalyDetector detector = new AnomalyDetector(data, pkql, timeColumn, seriesColumn, aggregateFunction,
//				maxAnoms, direction, alpha, period, keepExistingColumns);
//		try {
//			ITableDataFrame results = detector.detectAnomalies();
//
//			// Store outputs
//			dataMap.put(OUT_DATA_FRAME_KEY, results);
//		} catch (RRoutineException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//	}
//
//}
