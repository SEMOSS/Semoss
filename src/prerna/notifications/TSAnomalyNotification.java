//package prerna.notifications;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Properties;
//
//import javax.mail.Session;
//
//import org.quartz.Job;
//import org.quartz.JobDataMap;
//import org.quartz.JobDetail;
//
//import prerna.algorithm.learning.unsupervised.anomaly.AnomalyDetector.AnomDirection;
//import prerna.quartz.DetectAnomaliesJob;
//import prerna.quartz.GetDataFromInsightJob;
//import prerna.quartz.IfJob;
//import prerna.quartz.JobChain;
//import prerna.quartz.SendEmailJob;
//import prerna.quartz.specific.anthem.DetermineIfAnomalyJob;
//
//public class TSAnomalyNotification {
//
//	public static class Builder {
//
//		// Required
//		private String engineName;
//		private String importRecipe;
//		private String timeColumn;
//		private String seriesColumn;
//
//		private JobDetail notificationJob;
//
//		// Optional
//		private String aggregateFunction;
//		private Double maxAnoms;
//		private AnomDirection direction;
//		private Double alpha;
//		private Integer period;
//		private Boolean keepExistingColumns;
//
//		public Builder(String engineName, String importRecipe, String timeColumn, String seriesColumn,
//				JobDetail notificationJob) {
//			this.engineName = engineName;
//			this.importRecipe = importRecipe;
//			this.timeColumn = timeColumn;
//			this.seriesColumn = seriesColumn;
//
//			this.notificationJob = notificationJob;
//		}
//
//		public Builder aggregateFunction(String aggregateFunction) {
//			this.aggregateFunction = aggregateFunction;
//			return this;
//		}
//
//		public Builder maxAnoms(double maxAnoms) {
//			this.maxAnoms = maxAnoms;
//			return this;
//		}
//
//		public Builder direction(AnomDirection direction) {
//			this.direction = direction;
//			return this;
//		}
//
//		public Builder alpha(double alpha) {
//			this.alpha = alpha;
//			return this;
//		}
//
//		public Builder period(int period) {
//			this.period = period;
//			return this;
//		}
//
//		public Builder keepExistingColumns(boolean keepExistingColumns) {
//			this.keepExistingColumns = keepExistingColumns;
//			return this;
//		}
//
//		public TSAnomalyNotification build() {
//			TSAnomalyNotification generator = new TSAnomalyNotification();
//			generator.engineName = engineName;
//			generator.importRecipe = importRecipe;
//			generator.timeColumn = timeColumn;
//			generator.seriesColumn = seriesColumn;
//
//			generator.notificationJob = notificationJob;
//
//			// Set defaults if not set
//			generator.aggregateFunction = (aggregateFunction == null) ? "sum" : aggregateFunction;
//			generator.maxAnoms = (maxAnoms == null) ? 0.1 : maxAnoms;
//			generator.direction = (direction == null) ? AnomDirection.POSITIVE : direction;
//			generator.alpha = (alpha == null) ? 0.05 : alpha;
//			generator.period = (period == null) ? 7 : period;
//			generator.keepExistingColumns = (keepExistingColumns == null) ? false : keepExistingColumns;
//
//			return generator;
//		}
//	}
//
//	// Required
//	private String engineName;
//	private String importRecipe;
//	private String timeColumn;
//	private String seriesColumn;
//
//	private JobDetail notificationJob;
//
//	// Optional
//	private String aggregateFunction;
//	private double maxAnoms;
//	private AnomDirection direction;
//	private double alpha;
//	private int period;
//	private boolean keepExistingColumns;
//
//	public JobDataMap generateJobDataMap() {
//
//		// Specify the chain of jobs
//		List<Class<? extends Job>> anomalySequence = new ArrayList<Class<? extends Job>>();
////		anomalySequence.add(CreateInsightJob.class);
//		anomalySequence.add(GetDataFromInsightJob.class);
//		anomalySequence.add(DetectAnomaliesJob.class);
//		anomalySequence.add(DetermineIfAnomalyJob.class);
//		anomalySequence.add(IfJob.class);
//
//		// Initialize the map
//		JobDataMap anomalyEmailDataMap = new JobDataMap();
//
//		// The job sequence
//		anomalyEmailDataMap.put(JobChain.IN_SEQUENCE, anomalySequence);
//
//		// For testing add another pkql
////		anomalyEmailDataMap.put(CreateInsightJob.IN_RECIPE_KEY, importRecipe);
////		anomalyEmailDataMap.put(CreateInsightJob.IN_ENGINE_NAME_KEY, engineName);
//
//		// Anomaly params
//		anomalyEmailDataMap.put(DetectAnomaliesJob.IN_TIME_COLUMN_KEY, timeColumn);
//		anomalyEmailDataMap.put(DetectAnomaliesJob.IN_SERIES_COLUMN_KEY, seriesColumn);
//		anomalyEmailDataMap.put(DetectAnomaliesJob.IN_AGGREGATE_FUNCTION_KEY, aggregateFunction);
//		anomalyEmailDataMap.put(DetectAnomaliesJob.IN_MAX_ANOMS_KEY, maxAnoms);
//		anomalyEmailDataMap.put(DetectAnomaliesJob.IN_DIRECTION_KEY, direction);
//		anomalyEmailDataMap.put(DetectAnomaliesJob.IN_ALPHA_KEY, alpha);
//		anomalyEmailDataMap.put(DetectAnomaliesJob.IN_PERIOD_KEY, period);
//		anomalyEmailDataMap.put(DetectAnomaliesJob.IN_KEEP_EXISTING_COLUMNS_KEY, keepExistingColumns);
//
//		// The second chain for notification if anomaly
//		anomalyEmailDataMap.put(IfJob.IN_IF_TRUE_JOB, notificationJob);
//
//		return anomalyEmailDataMap;
//	}
//
//	public static JobDataMap generateEmailJobDataMap(String smtpServer, int smtpPort, String from, String[] to,
//			String subject, String body, boolean bodyIsHTML) {
//
//		// Initialize the map
//		JobDataMap emailDataMap = new JobDataMap();
//
//		// Email params
//		Properties sessionProps = new Properties();
//		sessionProps.put("mail.smtp.host", smtpServer);
//		sessionProps.put("mail.smtp.port", Integer.toString(smtpPort));
//		Session session = Session.getInstance(sessionProps);
//		emailDataMap.put(SendEmailJob.IN_FROM_KEY, from);
//		emailDataMap.put(SendEmailJob.IN_TO_KEY, to);
//		emailDataMap.put(SendEmailJob.IN_SUBJECT_KEY, subject);
//		emailDataMap.put(SendEmailJob.IN_BODY_KEY, body);
//		emailDataMap.put(SendEmailJob.IN_BODY_IS_HTML_KEY, bodyIsHTML);
//		emailDataMap.put(SendEmailJob.IN_SESSION_KEY, session);
//
//		return emailDataMap;
//	}
//
//}
