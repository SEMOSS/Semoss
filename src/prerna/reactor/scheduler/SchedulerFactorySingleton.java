/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.scheduler;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEmbeddedRDBMSServerEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class SchedulerFactorySingleton {

	private static final Logger classLogger = LogManager.getLogger(SchedulerFactorySingleton.class);

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

	public static boolean isInit() {
		return singleton != null;
	}

	private void init() {
		Properties quartzProperties = null;
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
		String username = queryUtil.getUsername();
		String password = queryUtil.getPassword();
		RdbmsTypeEnum rdbmsType = queryUtil.getDbType();
		factory = new StdSchedulerFactory();

		if (schedulerDb instanceof IEmbeddedRDBMSServerEngine) {
			quartzProperties = setUpQuartzProperties(schedulerDb,
					((IEmbeddedRDBMSServerEngine) schedulerDb).getServerUrl(), username, password, rdbmsType);
		} else { // instanceof RDBMSNativeEngine
			quartzProperties = setUpQuartzProperties(schedulerDb, schedulerDb.getConnectionUrl(), username, password,
					rdbmsType);
		}

		factory.initialize(quartzProperties);
	}

	/**
	 * 
	 * @param schedulerDb
	 * @param serverUrl
	 * @param username
	 * @param password
	 * @param rdbmsType
	 * @return
	 */
	public static Properties setUpQuartzProperties(IRDBMSEngine schedulerDb, String serverUrl, String username,
			String password, RdbmsTypeEnum rdbmsType) {
		Properties quartzProperties = Utility
				.loadProperties(Utility.getBaseFolder() + DIR_SEPARATOR + QUARTZ_CONFIGURATION_FILE);
		quartzProperties.setProperty("org.quartz.jobStore.driverDelegateClass",
				SchedulerDatabaseUtility.getQuartzDelegateForRdbms(rdbmsType));
		quartzProperties.setProperty("org.quartz.dataSource.myDS.URL", serverUrl);
		quartzProperties.setProperty("org.quartz.dataSource.myDS.driver", rdbmsType.getDriver());
		quartzProperties.setProperty("org.quartz.dataSource.myDS.user", username);
		quartzProperties.setProperty("org.quartz.dataSource.myDS.password", password);
		if (ClusterUtil.IS_CLUSTERED_SCHEDULER) {
			quartzProperties.setProperty("org.quartz.scheduler.instanceId", "AUTO");
			quartzProperties.setProperty("org.quartz.jobStore.isClustered", "true");
		}

		return quartzProperties;
	}

	public Scheduler getScheduler() {
		if (scheduler != null) {
			return scheduler;
		}

		try {
			scheduler = factory.getScheduler();
		} catch (SchedulerException se) {
			classLogger.error("Failed to retrieve Quartz scheduler from factory: {}", se.getMessage(), se);
		}

		if (scheduler == null) {
			throw new NullPointerException("Scheduler is null as factory failed to retrieve scheduler");
		}

		return scheduler;
	}

	public void shutdownScheduler(boolean waitForJobsToComplete) {
		if (!ClusterUtil.IS_CLUSTERED_SCHEDULER) {
			try {
				if (scheduler != null && scheduler.isStarted()) {
					scheduler.shutdown(waitForJobsToComplete);
				}
			} catch (SchedulerException se) {
				classLogger.error("Failed to shut down Quartz scheduler: {}", se.getMessage(), se);
			}
		}
	}
}
