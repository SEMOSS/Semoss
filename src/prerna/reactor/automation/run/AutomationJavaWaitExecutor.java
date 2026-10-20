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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import prerna.reactor.automation.AutomationConstants;

/**
 * Executes a cancellable Java-owned Automation wait.
 */
final class AutomationJavaWaitExecutor {

	private AutomationJavaWaitExecutor() {
	}

	static Map<String, Object> execute(String runId, double seconds) {
		if (!Double.isFinite(seconds) || seconds < AutomationConstants.WAIT_MIN_SECONDS
				|| seconds > AutomationConstants.WAIT_MAX_SECONDS) {
			throw new IllegalArgumentException("Automation wait duration must be between "
					+ AutomationConstants.WAIT_MIN_SECONDS + " and " + AutomationConstants.WAIT_MAX_SECONDS
					+ " seconds.");
		}
		long durationNanos = Math.round(seconds * TimeUnit.SECONDS.toNanos(1));
		CountDownLatch cancellation = new CountDownLatch(1);
		Runnable cancellationSignal = cancellation::countDown;
		AutomationRunRegistry.attachJavaCancellation(runId, cancellationSignal);
		try {
			if (cancellation.await(durationNanos, TimeUnit.NANOSECONDS)) {
				throw new IllegalStateException("Run cancelled by user");
			}
			return Map.of("waitedSeconds", seconds);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Automation Java wait was interrupted.", e);
		} finally {
			AutomationRunRegistry.detachJavaCancellation(runId, cancellationSignal);
		}
	}
}
