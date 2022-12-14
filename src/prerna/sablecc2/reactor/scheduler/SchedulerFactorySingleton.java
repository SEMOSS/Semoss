package prerna.sablecc2.reactor.scheduler;

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
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class SchedulerFactorySingleton {

	private static final Logger logger = LogManager.getLogger(SchedulerFactorySingleton.class);

	private static final String QUARTZ_CONFIGURATION_FILE = "quartz.properties";

	private static StdSchedulerFactory factory;

	public static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	static SchedulerFactorySingleton singleton = null;
	static Scheduler scheduler = null;
	int count = 0;

	private SchedulerFactorySingleton() {
	}

	public static SchedulerFactorySingleton getInstance() {
		if (singleton == null) {
			singleton = new SchedulerFactorySingleton();
			singleton.init();
		}
		return singleton;
	}

	private void init() {
		Properties quartzProperties = null;
		RDBMSNativeEngine schedulerDb = (RDBMSNativeEngine) Utility.getEngine(Constants.SCHEDULER_DB);
		AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
		String username = queryUtil.getUsername();
		String password = queryUtil.getPassword();
		String driver = queryUtil.getDriver();
		factory = new StdSchedulerFactory();

		if (schedulerDb instanceof H2EmbeddedServerEngine) {
			quartzProperties = setUpQuartzProperties(((H2EmbeddedServerEngine) schedulerDb).getServerUrl(), 
					username, password, driver);
		} else { // instanceof RDBMSNativeEngine
			quartzProperties = setUpQuartzProperties(schedulerDb.getConnectionUrl(),
					username, password, driver);
		}

		try {
			factory.initialize(quartzProperties);
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		}
	}

	public static Properties setUpQuartzProperties(String serverUrl, String username, String password, String driver) {
		Properties quartzProperties = new Properties();
		try (InputStream input = new FileInputStream(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
				+ DIR_SEPARATOR + QUARTZ_CONFIGURATION_FILE)) {
			quartzProperties.load(input);
			quartzProperties.setProperty("org.quartz.dataSource.myDS.URL", serverUrl);
			quartzProperties.setProperty("org.quartz.dataSource.myDS.driver", driver);
			quartzProperties.setProperty("org.quartz.dataSource.myDS.user", username);
			quartzProperties.setProperty("org.quartz.dataSource.myDS.password", password);
			if(ClusterUtil.IS_CLUSTERED_SCHEDULER) {
				quartzProperties.setProperty("org.quartz.scheduler.instanceId", "AUTO");
				quartzProperties.setProperty("org.quartz.jobStore.isClustered", "true");
			}
		} catch (IOException ex) {
			logger.error("Error with loading properties in config file " + ex.getMessage());
		}

		return quartzProperties;
	}

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

	public void shutdownScheduler(boolean waitForJobsToComplete) {
		if(!ClusterUtil.IS_CLUSTERED_SCHEDULER) {
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
