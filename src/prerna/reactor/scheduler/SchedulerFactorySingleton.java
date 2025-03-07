package prerna.reactor.scheduler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.rdbms.H2EmbeddedServerEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class SchedulerFactorySingleton {

	private static final Logger logger = LogManager.getLogger(SchedulerFactorySingleton.class);
	private static final String QUARTZ_CONFIGURATION_FILE = "quartz.properties";

	public static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private static StdSchedulerFactory factory;
	private static SchedulerFactorySingleton singleton = null;
	private static Scheduler scheduler = null;

	/**
	 * 
	 */
	private SchedulerFactorySingleton() {
	
	}

	/**
	 * 
	 * @return
	 */
	public static SchedulerFactorySingleton getInstance() {
		if (singleton == null) {
			singleton = new SchedulerFactorySingleton();
			singleton.init();
		}
		return singleton;
	}
	
	/**
	 * 
	 * @return
	 */
	public static boolean isInit() {
		return singleton != null;
	}

	/**
	 * 
	 */
	private void init() {
		Properties quartzProperties = null;
		RDBMSNativeEngine schedulerDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SCHEDULER_DB);
		AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
		String username = queryUtil.getUsername();
		String password = queryUtil.getPassword();
		RdbmsTypeEnum rdbmsType = queryUtil.getDbType();
		factory = new StdSchedulerFactory();

		if (schedulerDb instanceof H2EmbeddedServerEngine) {
			quartzProperties = setUpQuartzProperties(((H2EmbeddedServerEngine) schedulerDb).getServerUrl(), 
					username, password, rdbmsType);
		} else { // instanceof RDBMSNativeEngine
			quartzProperties = setUpQuartzProperties(schedulerDb.getConnectionUrl(),
					username, password, rdbmsType);
		}

		try {
			factory.initialize(quartzProperties);
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}
	}

	/**
	 * 
	 * @param serverUrl
	 * @param username
	 * @param password
	 * @param rdbmsType
	 * @return
	 */
	public static Properties setUpQuartzProperties(String serverUrl, String username, String password, RdbmsTypeEnum rdbmsType) {
		Properties quartzProperties = new Properties();
		try (InputStream input = new FileInputStream(Utility.getBaseFolder() + DIR_SEPARATOR + QUARTZ_CONFIGURATION_FILE)) {
			quartzProperties.load(input);
			quartzProperties.setProperty("org.quartz.dataSource.myDS.URL", serverUrl);
			quartzProperties.setProperty("org.quartz.dataSource.myDS.driver", rdbmsType.getDriver());
			quartzProperties.setProperty("org.quartz.dataSource.myDS.user", username);
			quartzProperties.setProperty("org.quartz.dataSource.myDS.password", password);
			quartzProperties.setProperty("org.quartz.jobStore.driverDelegateClass", SchedulerDatabaseUtility.getQuartzDelegateForRdbms(rdbmsType));
			if(ClusterUtil.IS_CLUSTER) {
				quartzProperties.setProperty("org.quartz.scheduler.instanceId", "AUTO");
				quartzProperties.setProperty("org.quartz.jobStore.isClustered", "true");
			}
		} catch (IOException ex) {
			logger.error("Error with loading properties in config file " + ex.getMessage());
		}

		return quartzProperties;
	}

	/**
	 * 
	 * @return
	 */
	public Scheduler getScheduler() {
		if(scheduler != null) {
			return scheduler;
		}
		
		try {
			scheduler = factory.getScheduler();
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}

		if (scheduler == null) {
			throw new NullPointerException("Scheduler is null as factory failed to retrieve scheduler");
		}

		return scheduler;
	}

	/**
	 * 
	 * @param waitForJobsToComplete
	 */
	public void shutdownScheduler(boolean waitForJobsToComplete) {
		if(!ClusterUtil.IS_CLUSTER) {
			try {
				if (scheduler != null && scheduler.isStarted()) {
					scheduler.shutdown(waitForJobsToComplete); 
				}
			} catch (SchedulerException se) {
				logger.error(Constants.STACKTRACE, se);
			}
		}
	}
}
