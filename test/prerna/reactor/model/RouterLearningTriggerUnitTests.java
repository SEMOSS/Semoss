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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.ModelRouterEngine;

/** The volume trigger: checks at most once per interval per router, triggers at the threshold, never throws. */
class RouterLearningTriggerUnitTests {

	private final AtomicLong now = new AtomicLong(1_000_000);
	private final AtomicLong tasks = new AtomicLong();
	private final List<String> triggered = new ArrayList<>();
	private final RouterLearningTrigger trigger = new RouterLearningTrigger(now::get, Runnable::run, id -> tasks.get(), (router, job) -> triggered.add(router + ":" + job));

	private static ModelRouterEngine.Improvement learning(boolean on) {
		ModelRouterEngine.Improvement improvement = new ModelRouterEngine.Improvement();
		improvement.enabled = on;
		improvement.scheduler_job_id = "saved-id";
		improvement.min_new_tasks = 100;
		return improvement;
	}

	@Test
	void belowTheThresholdNothingIsTriggeredAtItOnceIsAndTheCheckIsLimitedToOncePerInterval() {
		tasks.set(99);
		trigger.onTaskStarted("rt", learning(true));
		assertEquals(List.of(), triggered);

		tasks.set(100);
		now.addAndGet(RouterLearningTrigger.CHECK_EVERY_MS - 1);
		trigger.onTaskStarted("rt", learning(true));
		assertEquals(List.of(), triggered, "the threshold was reached but the router was checked a moment ago");

		now.addAndGet(1);
		trigger.onTaskStarted("rt", learning(true));
		trigger.onTaskStarted("rt", learning(true));
		assertEquals(List.of("rt:saved-id"), triggered, "one check per interval");
	}

	@Test
	void routersAreCountedSeparatelyAndLearningOffDoesNothing() {
		tasks.set(500);
		trigger.onTaskStarted("rt", learning(false));
		trigger.onTaskStarted("rt", null);
		assertEquals(List.of(), triggered);
		trigger.onTaskStarted("a", learning(true));
		trigger.onTaskStarted("b", learning(true));
		assertEquals(List.of("a:saved-id", "b:saved-id"), triggered);
	}

	@Test
	void failuresAreSwallowed() {
		RouterLearningTrigger failing = new RouterLearningTrigger(now::get, Runnable::run, id -> {
			throw new IllegalStateException("db down");
		}, (router, job) -> triggered.add(router + ":" + job));
		failing.onTaskStarted("rt", learning(true));
		RouterLearningTrigger noExecutor = new RouterLearningTrigger(now::get, r -> {
			throw new IllegalStateException("full");
		}, id -> 1000, (router, job) -> triggered.add(router + ":" + job));
		noExecutor.onTaskStarted("rt", learning(true));
		assertEquals(List.of(), triggered);
	}
}
