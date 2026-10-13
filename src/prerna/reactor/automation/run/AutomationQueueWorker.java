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
package prerna.reactor.automation.run;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.scheduler.SchedulerUserAccess;

/**
 * Bounded per-host executor for scheduler-database Automation admissions.
 * Every host atomically claims from the same oldest-first queue before handing
 * work to its executor; the conditional run transition and lease fence
 * duplicate execution across hosts.
 */
final class AutomationQueueWorker {

	private static final Logger classLogger = LogManager.getLogger(AutomationQueueWorker.class);
	private static final int CONCURRENCY = positiveProperty("semoss.automation.worker.concurrency",
			AutomationConstants.DEFAULT_WORKER_CONCURRENCY);
	private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(CONCURRENCY, runnable -> {
		Thread thread = new Thread(runnable, "automation-worker");
		thread.setDaemon(true);
		return thread;
	});
	private static final Semaphore AVAILABLE = new Semaphore(CONCURRENCY);
	private static final AtomicBoolean STOPPED = new AtomicBoolean();

	private AutomationQueueWorker() {
	}

	static void poll() {
		if (STOPPED.get()) {
			return;
		}
		while (AVAILABLE.tryAcquire()) {
			AutomationRunStore.ClaimedRun claimedRun = AutomationRunStore.claimOldestSubmittedRun();
			if (claimedRun == null) {
				AVAILABLE.release();
				return;
			}
			try {
				EXECUTOR.execute(() -> execute(claimedRun));
			} catch (RejectedExecutionException e) {
				AutomationRunStore.reconcileRejectedClaim(claimedRun);
				AVAILABLE.release();
				classLogger.warn("Automation executor rejected claimed run '{}'", claimedRun.runId(), e);
				return;
			}
		}
	}

	private static void execute(AutomationRunStore.ClaimedRun claimedRun) {
		String runId = claimedRun.runId();
		try {
			User user = SchedulerUserAccess.reconstruct(
					(String) claimedRun.admission().get(AutomationConstants.PRINCIPAL_ACCESS));
			Insight requestInsight = new Insight();
			requestInsight.setUser(user);
			requestInsight.setSchedulerMode(true);
			ThreadStore.setUser(user);
			ThreadStore.setSchedulerMode(true);
			new AutomationRunExecutionService(requestInsight, null).executeClaimedPersistedRun(claimedRun);
		} catch (Exception e) {
			classLogger.error("Queued Automation run '{}' could not be executed", runId, e);
			AutomationRunStore.failClaimedRun(claimedRun,
					"Queued execution admission failed; review server logs");
		} finally {
			ThreadStore.remove();
			AVAILABLE.release();
		}
	}

	static void shutdown() {
		STOPPED.set(true);
		EXECUTOR.shutdownNow();
	}

	private static int positiveProperty(String name, int defaultValue) {
		try {
			int value = Integer.parseInt(System.getProperty(name, String.valueOf(defaultValue)));
			return value > 0 ? value : defaultValue;
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}
}
