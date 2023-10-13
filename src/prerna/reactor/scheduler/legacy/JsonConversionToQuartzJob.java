package prerna.reactor.scheduler.legacy;
//package prerna.sablecc2.reactor.scheduler.legacy;
//
//import static org.quartz.JobBuilder.newJob;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.io.UnsupportedEncodingException;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.Properties;
//import java.util.UUID;
//
//import org.apache.commons.io.FilenameUtils;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.h2.tools.Server;
//import org.quartz.CronScheduleBuilder;
//import org.quartz.Job;
//import org.quartz.JobDetail;
//import org.quartz.JobKey;
//import org.quartz.Scheduler;
//import org.quartz.SchedulerException;
//import org.quartz.Trigger;
//import org.quartz.TriggerBuilder;
//import org.quartz.impl.StdSchedulerFactory;
//
//import com.google.gson.Gson;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonIOException;
//import com.google.gson.JsonParser;
//import com.google.gson.JsonSyntaxException;
//
//import prerna.sablecc2.reactor.scheduler.SchedulerDatabaseUtility;
//import prerna.sablecc2.reactor.scheduler.SchedulerFactorySingleton;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//import prerna.util.Utility;
//
//@Deprecated
//public class JsonConversionToQuartzJob {
//
//	private static final Logger logger = LogManager.getLogger(JsonConversionToQuartzJob.class);
//
//	private static final String MY_DIRECTORY_PATH = "C:\\workspace\\Semoss_Dev\\rpa\\json";
//	private static final String QUARTZ_CONFIGURATION_FILE = "quartz.properties";
//	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
//	private static final String JDBC_DRIVER = "org.h2.Driver";
//	private static final String CONNECTION_URL = "jdbc:h2:nio:C:/workspace/Semoss_Dev/db/scheduler/db";
//	private static final String JOB_QUALIFIED_NAME = "prerna.rpa.quartz.jobs.insight.";
//
//	private static String serverUrl = null;
//
//	private static StdSchedulerFactory factory;
//	public static Scheduler scheduler = null;
//
////	public static void main(String[] args) throws NotDirectoryException {
////		serverUrl = startServer(CONNECTION_URL);
////
////		setUpPropertiesFile();
////		startScheduler();
////
////		File directory = new File(MY_DIRECTORY_PATH);
////		File[] directoryFileList = directory.listFiles();
////
////		if (directoryFileList == null) {
////			throw new NotDirectoryException("The directory does not exist");
////		} else {
////			for (File child : directoryFileList) {
////				Gson gson = new Gson();
////				JsonParser jsonParser = new JsonParser();
////
////				JsonElement jsonObject = null;
////				try {
////					jsonObject = jsonParser.parse(new InputStreamReader(new FileInputStream(child), "UTF-8"));
////				} catch (JsonIOException | JsonSyntaxException | UnsupportedEncodingException
////						| FileNotFoundException se) {
////					logger.error(Constants.STACKTRACE, se);
////				}
////				OldJobs oldJobs = gson.fromJson(jsonObject, OldJobs.class);
////
////				convertJob(oldJobs);
////			}
////		}
////	}
//
//	public static String startServer(String connectionUrl) {
//		Server server = null;
//		String serverUrl = null;
//
//		if (connectionUrl.startsWith("jdbc:h2:nio:")) {
//			connectionUrl = connectionUrl.substring("jdbc:h2:nio:".length());
//		}
//
//		try {
//			String port = "5358";
//			server = Server.createTcpServer("-tcpPort", port, "-tcpAllowOthers");
//			serverUrl = "jdbc:h2:" + server.getURL() + "/nio:" + connectionUrl;
//			server.start();
//
//		} catch (SQLException e) {
//			logger.error(Constants.STACKTRACE, e);
//		}
//
//		logger.info("DATABASE RUNNING ON " + Utility.cleanLogString(serverUrl));
//
//		return serverUrl;
//	}
//	
//	public static void runUpdateFromLegacyFormat() {
//		scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
//
//		File directory = new File(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/rpa/json");
//		if(directory.exists()) {
//			File[] directoryFileList = directory.listFiles();
//			if (directoryFileList != null && directoryFileList.length > 0) {
//				for (File child : directoryFileList) {
//					if(FilenameUtils.getExtension(child.getAbsolutePath()).equals("json")) {
//						Gson gson = new Gson();
//						JsonParser jsonParser = new JsonParser();
//		
//						JsonElement jsonObject = null;
//						InputStreamReader is = null;
//						FileInputStream fs = null;
//						try {
//							fs = new FileInputStream(child);
//							is = new InputStreamReader(fs, "UTF-8");
//							jsonObject = jsonParser.parse(is);
//						} catch (JsonIOException | JsonSyntaxException | UnsupportedEncodingException | FileNotFoundException se) {
//							logger.error(Constants.STACKTRACE, se);
//						} finally {
//							if(is != null) {
//								try {
//									is.close();
//								} catch (IOException e) {
//									e.printStackTrace();
//								}
//							}
//							if(fs != null) {
//								try {
//									fs.close();
//								} catch (IOException e) {
//									e.printStackTrace();
//								}
//							}
//						}
//						
//						OldJobs oldJobs = gson.fromJson(jsonObject, OldJobs.class);
//						try {
//							convertJob(oldJobs);
//							// renmae the file to loaded
//							File loadedF = new File(directory.getAbsolutePath() + "/" + FilenameUtils.getBaseName(child.getAbsolutePath()) + ".json_loaded");
//							boolean worked = child.renameTo(loadedF);
//						} catch(Exception e) {
//							logger.error(Constants.STACKTRACE, e);
//						}
//					}
//				}
//			}
//		}
//	}
//
//	@SuppressWarnings("unchecked")
//	private static void convertJob(OldJobs oldJobs) {
//		// ignore hidden jobs
//		if(Boolean.parseBoolean(oldJobs.getHidden())) {
//			return;
//		}
//		String jobId = UUID.randomUUID().toString();
// 		String jobName = oldJobs.getJobName();
//		String jobGroup = oldJobs.getJobGroup();
//		String cronExpression = oldJobs.getJobCronExpression();
//		String jobClassName = oldJobs.getJobClass();
//		Boolean active = Boolean.parseBoolean(oldJobs.getActive());
//		String userAccess = oldJobs.getUserAccess();
//		Boolean triggerOnLoad = Boolean.parseBoolean(oldJobs.getJobTriggerOnLoad());
//		String parameters = oldJobs.getParameters();
//		if(parameters == null) {
//			parameters = "";
//		}
//		String pixel = oldJobs.getPixel();
//
//		JobKey jobKey = JobKey.jobKey(jobId, jobGroup);
//		Class<? extends Job> jobClass = null;
//
//		try {
//			jobClass = (Class<? extends Job>) Class.forName(JOB_QUALIFIED_NAME + jobClassName);
//		} catch (ClassNotFoundException cne) {
//			logger.error(Constants.STACKTRACE, cne);
//		}
//
//		// if job exists throw error, job already exists
//		try {
//			if (scheduler.checkExists(jobKey)) {
//				logger.error("job" + Utility.cleanLogString(jobKey.toString()) + "already exists");
//				throw new IllegalArgumentException("job " + Utility.cleanLogString(jobKey.toString()) + " already exists");
//			}
//
//			// Schedule the job
//			JobDetail job = newJob(jobClass).withIdentity(jobId, jobGroup).storeDurably().build();
//			Trigger trigger = TriggerBuilder.newTrigger().withIdentity(jobId+ "Trigger", jobGroup + "TriggerGroup")
//					.withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)).build();
//
//			// insert into SMSS_JOB_RECIPES table
//			// THERE WAS NO POSSIBILITY OF RUNNING OLD JOBS WITH RECIPE PARAMETERS
//			SchedulerDatabaseUtility.insertIntoJobRecipesTable(userAccess, jobId, jobName, jobGroup, cronExpression, pixel, null, "Default", triggerOnLoad, parameters, new ArrayList<String>());
//
//			if (active) {
//				scheduler.scheduleJob(job, trigger);
//			} else {
//				// if inactive, add job as dormant with no trigger
//				scheduler.addJob(job, false);
//			}
//		} catch (SchedulerException se) {
//			logger.error(Constants.STACKTRACE, se);
//		}
//	}
//
//	private static void setUpPropertiesFile() {
//		Properties quartzProperties = new Properties();
//
//		try (InputStream input = new FileInputStream(
//				"C:\\workspace\\Semoss_Dev" + DIR_SEPARATOR + QUARTZ_CONFIGURATION_FILE)) {
//			quartzProperties.load(input);
//			quartzProperties.setProperty("org.quartz.dataSource.myDS.URL", serverUrl);
//
//			factory = new StdSchedulerFactory();
//			factory.initialize(quartzProperties);
//		} catch (IOException ex) {
//			logger.error("Error with loading properties in config file" + ex.getMessage());
//		} catch (SchedulerException se) {
//			logger.error(Constants.STACKTRACE, se);
//		}
//	}
//
//	private static void startScheduler() {
//		try {
//			scheduler = factory.getScheduler();
//			scheduler.start();
//		} catch (SchedulerException se) {
//			logger.error(Constants.STACKTRACE, se);
//		}
//	}
//
//	public static Connection connectToSchedulerH2(String serverUrl) {
//		Connection connection = null;
//
//		try {
//			// Register JDBC Driver
//			Class.forName(JDBC_DRIVER);
//			connection = DriverManager.getConnection(serverUrl, "admin", "admin");
//		} catch (ClassNotFoundException e) {
//			logger.error(Constants.STACKTRACE, e);
//		} catch (SQLException se) {
//			logger.error(Constants.STACKTRACE, se);
//		} catch (Exception ex) {
//			logger.error(Constants.STACKTRACE, ex);
//		}
//
//		if (connection == null) {
//			throw new NullPointerException("Connection wasn't able to be created.");
//		}
//
//		return connection;
//	}
//
//}
