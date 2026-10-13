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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.scheduler.SchedulerUserAccess;

/**
 * Bounded per-host worker for durable Automation business items.
 */
final class AutomationItemWorker {

	private static final Logger classLogger = LogManager.getLogger(AutomationItemWorker.class);
	private static final int CONCURRENCY = positiveProperty("semoss.automation.itemWorker.concurrency",
			AutomationConstants.DEFAULT_ITEM_WORKER_CONCURRENCY);
	private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(CONCURRENCY, runnable -> {
		Thread thread = new Thread(runnable, "automation-item-worker");
		thread.setDaemon(true);
		return thread;
	});
	private static final ScheduledExecutorService HEARTBEATS = Executors.newSingleThreadScheduledExecutor(runnable -> {
		Thread thread = new Thread(runnable, "automation-item-heartbeat");
		thread.setDaemon(true);
		return thread;
	});
	private static final Semaphore AVAILABLE = new Semaphore(CONCURRENCY);
	private static final AtomicBoolean STOPPED = new AtomicBoolean();

	private AutomationItemWorker() {
	}

	static void poll() {
		if (STOPPED.get()) {
			return;
		}
		while (AVAILABLE.tryAcquire()) {
			AutomationRunStore.ItemClaim claim = AutomationRunStore.claimOldestDurableItem();
			if (claim == null) {
				AVAILABLE.release();
				return;
			}
			try {
				EXECUTOR.execute(() -> execute(claim));
			} catch (RejectedExecutionException e) {
				AVAILABLE.release();
				classLogger.warn("Automation item executor rejected claim '{}'; its lease will be recovered",
						claim.itemId(), e);
				return;
			}
		}
	}

	private static void execute(AutomationRunStore.ItemClaim claim) {
		ScheduledFuture<?> heartbeat = null;
		String loopOutputVar = null;
		try {
			AutomationRunStore.activateItemClaim(claim);
			AutomationRunStore.DurableItemAdmission admission =
					AutomationRunStore.getDurableItemAdmission(claim);
			User user = SchedulerUserAccess.reconstruct(admission.principalAccess());
			Insight requestInsight = new Insight();
			requestInsight.setUser(user);
			requestInsight.setSchedulerMode(true);
			ThreadStore.setUser(user);
			ThreadStore.setSchedulerMode(true);
			heartbeat = HEARTBEATS.scheduleAtFixedRate(() -> {
				if (!AutomationRunStore.heartbeatDurableItem(claim)) {
					classLogger.warn("Durable Automation item lease was lost for attempt '{}'", claim.attemptId());
				}
			}, AutomationConstants.HEARTBEAT_INTERVAL_SECONDS, AutomationConstants.HEARTBEAT_INTERVAL_SECONDS,
					TimeUnit.SECONDS);
			loopOutputVar = new AutomationRunExecutionService(requestInsight, null)
					.executeClaimedDurableItem(claim, admission);
			AutomationRunStore.completeDurableItem(claim);
		} catch (Exception e) {
			classLogger.error("Durable Automation item attempt '{}' failed", claim.attemptId(), e);
			try {
				AutomationRunStore.failDurableItem(claim, safeMessage(e));
			} catch (Exception publicationFailure) {
				classLogger.error("Unable to publish durable Automation item attempt '{}'", claim.attemptId(),
						publicationFailure);
			}
		} finally {
			if (heartbeat != null) {
				heartbeat.cancel(false);
			}
			AutomationRunStore.clearItemClaim(claim);
			ThreadStore.remove();
			try {
				AutomationRunStore.coordinateDurableLoop(claim.runId(), claim.nodeId(), loopOutputVar);
			} catch (Exception e) {
				classLogger.warn("Durable Automation loop coordinator deferred for run '{}'", claim.runId(), e);
			}
			AVAILABLE.release();
			poll();
		}
	}

	static void shutdown() {
		STOPPED.set(true);
		EXECUTOR.shutdownNow();
		HEARTBEATS.shutdownNow();
	}

	private static int positiveProperty(String name, int defaultValue) {
		try {
			int value = Integer.parseInt(System.getProperty(name, String.valueOf(defaultValue)));
			return value > 0 ? value : defaultValue;
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	private static String safeMessage(Exception error) {
		String message = error.getMessage();
		return message == null || message.isBlank() ? error.getClass().getSimpleName() : message;
	}
}
