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
package prerna.reactor.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobKey;

import prerna.engine.impl.model.ModelRouterEngine;
import prerna.reactor.scheduler.SchedulerFactorySingleton;

/**
 * Starts router learning when the volume is reached instead of on a clock. A router in subtask mode with learning on calls
 * {@link #taskStarted} when a new task starts; at most once per {@value #CHECK_EVERY_MS} ms per router that hands a check to one
 * daemon thread, which counts the new tasks since the last attempt (all users') and, when {@code min_new_tasks} is reached, triggers the
 * standing learning job through Quartz, so it runs as the user who created it. The job checks again and refuses to start while an
 * attempt is running, so a spurious trigger starts nothing. Failures are logged, never raised to the routed call.
 */
public final class RouterLearningTrigger {

	private static final Logger classLogger = LogManager.getLogger(RouterLearningTrigger.class);

	/** One check per router per ten minutes: a check reads the run history and may cost a judge pass, and a run it started is long recorded by the next. */
	static final long CHECK_EVERY_MS = 600_000;
	private static ExecutorService worker;
	private static final RouterLearningTrigger INSTANCE = new RouterLearningTrigger(System::currentTimeMillis, command -> {
		synchronized (RouterLearningTrigger.class) {
			if (worker == null) {
				worker = Executors.newSingleThreadExecutor(r -> {
					Thread t = new Thread(r, "router-learning-trigger");
					t.setDaemon(true);
					return t;
				});
			}
		}
		worker.execute(command);
	}, RunRouterLearningIfDueReactor::pooledNewTasks, RouterLearningTrigger::fire);

	private final Map<String, Long> lastCheck = new ConcurrentHashMap<>();
	private final LongSupplier clock;
	private final Executor executor;
	private final ToLongFunction<String> newTasks;
	private final BiConsumer<String, String> trigger;

	RouterLearningTrigger(LongSupplier clock, Executor executor, ToLongFunction<String> newTasks, BiConsumer<String, String> trigger) {
		this.clock = clock;
		this.executor = executor;
		this.newTasks = newTasks;
		this.trigger = trigger;
	}

	/** Called by a subtask router when a task starts; never blocks and never throws. */
	public static void taskStarted(String engineId, ModelRouterEngine.Improvement improvement) {
		INSTANCE.onTaskStarted(engineId, improvement);
	}

	void onTaskStarted(String engineId, ModelRouterEngine.Improvement improvement) {
		try {
			if (improvement == null || !improvement.active() || improvement.scheduler_job_id == null) {
				return;
			}
			long now = clock.getAsLong();
			boolean[] due = new boolean[1];
			lastCheck.compute(engineId, (id, last) -> {
				due[0] = last == null || now - last >= CHECK_EVERY_MS;
				return due[0] ? now : last;
			});
			if (!due[0]) {
				return;
			}
			int needed = improvement.min_new_tasks;
			String jobId = improvement.scheduler_job_id;
			executor.execute(() -> check(engineId, jobId, needed));
		} catch (Throwable t) {
			classLogger.warn("Router learning trigger for '{}' failed: {}", engineId, t.getMessage());
		}
	}

	private void check(String engineId, String jobId, int needed) {
		try {
			if (newTasks.applyAsLong(engineId) >= needed) {
				trigger.accept(engineId, jobId);
			}
		} catch (Throwable t) {
			classLogger.warn("Router learning check for '{}' failed: {}", engineId, t.getMessage());
		}
	}

	/** Runs the standing learning job of the router now, under the identity it was scheduled with. */
	private static void fire(String engineId, String jobId) {
		try {
			var scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
			var bound = SetRouterLearningReactor.lookupJob(scheduler, engineId, jobId);
			if (bound != null && bound.verified()) {
				scheduler.triggerJob(JobKey.jobKey(jobId, SetRouterLearningReactor.JOB_GROUP));
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
}
