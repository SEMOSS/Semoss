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
package prerna.reactor.automation.nodes;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.automation.AutomationCancelledException;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.AutomationDatabaseUtility;
import prerna.reactor.automation.utils.AutomationExecutionUtils;

/**
 * Executes a "wait" node: sleeps for the configured number of seconds.
 * The {@code seconds} value supports {@code ${var}} template substitution.
 * Maximum {@value AutomationConstants#WAIT_MAX_SECONDS} seconds (1 hour) per invocation.
 *
 * <p>Sleeps in {@value AutomationConstants#WAIT_CANCEL_CHECK_INTERVAL_SECONDS}-second chunks,
 * checking the cancellation flag between each chunk. This fixes the bug where a single
 * {@code Thread.sleep(N)} call would block for the full duration even after a cancel request
 * was received - the cancel check was only between nodes, so a long wait could not be
 * interrupted until it completed naturally.
 */
public final class WaitNodeExecutor implements IAutomationNodeExecutor {

	private static final Logger classLogger = LogManager.getLogger(WaitNodeExecutor.class);

	@Override
	public Object execute(AutomationNodeContext ctx) throws Exception {
		Map<String, Object> config = ctx.config();
		String nodeLabel = ctx.nodeLabel();

		String secondsTemplate = NodeConfigHelper.optional(config, AutomationConstants.CONFIG_SECONDS,
				String.valueOf(AutomationConstants.WAIT_DEFAULT_SECONDS));
		String resolved = AutomationExecutionUtils.resolve(secondsTemplate, ctx.scope(), ctx.configMap());

		int seconds;
		try {
			seconds = Integer.parseInt(resolved.trim());
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Wait node \"" + nodeLabel +
					"\" - seconds value is not a valid integer after resolution: \"" + resolved + "\"");
		}
		seconds = Math.min(Math.max(seconds, AutomationConstants.WAIT_MIN_SECONDS), AutomationConstants.WAIT_MAX_SECONDS);
		classLogger.debug("Wait node \"{}\" sleeping {} seconds", nodeLabel, seconds);

		// Sleep in chunks so cancel requests are honored mid-wait rather than waiting
		// for the full duration to complete.
		int remaining = seconds;
		while (remaining > 0) {
			// isCancelRequested checks the DB; this is intentionally bounded by
			// WAIT_CANCEL_CHECK_INTERVAL_SECONDS to avoid thrashing the JDBC pool.
			if (ctx.cancelFlag().get() || AutomationDatabaseUtility.isCancelRequested(ctx.runId())) {
				throw new AutomationCancelledException("Wait node \"" + nodeLabel + "\" cancelled");
			}
			int chunk = Math.min(remaining, AutomationConstants.WAIT_CANCEL_CHECK_INTERVAL_SECONDS);
			try {
				TimeUnit.SECONDS.sleep(chunk);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("Wait node \"" + nodeLabel + "\" was interrupted");
			}
			remaining -= chunk;
		}

		return seconds + " seconds";
	}
}
